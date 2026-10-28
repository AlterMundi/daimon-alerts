"""Critical-service truth, durable transitions and exact uncertain replay."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
sys.path.insert(0,str(Path(__file__).parents[1]/'scripts'))
from client import Unavailable
from service_monitor import Monitor, probe, relocate_monitor, binding_digest


def configuration(path):
    return dict(producer_id='fictional-services',observer_id='fictional-observer',
                receiving_scope='fictional-human',base_url='http://127.0.0.1:9',
                token_file=str(path/'token'),state_dir=str(path/'state'),
                targets=[dict(id='browser',resource_ref='fictional-browser',kind='webbridge',base_url='http://127.0.0.1:10086')])


class Probes(unittest.TestCase):
    def test_external_health_requires_actual_bounded_remote_response(self):
        target = dict(kind='http_health', base_url='http://127.0.0.1:12345')
        for value, expected in [({'ok': True}, 'healthy'), ({'ok': False}, 'health_unavailable'),
                                ({'ok': 'true'}, 'health_unavailable'), ({}, 'health_unavailable')]:
            with patch('service_monitor.read_json', return_value=value) as read:
                self.assertEqual(probe(target), expected)
                read.assert_called_once_with('http://127.0.0.1:12345/health')
        for error in (OSError(), ValueError()):
            with patch('service_monitor.read_json', side_effect=error):
                self.assertEqual(probe(target), 'health_unavailable')

    def test_process_up_is_not_connected_or_operational(self):
        for status,expected in [({'running':False,'extension_connected':False},'daemon_unavailable'),
                                ({'running':True,'extension_connected':False},'extension_disconnected'),
                                ({'running':True},'observation_unreadable')]:
            with patch('service_monitor.read_json',return_value=status) as read:
                self.assertEqual(probe(dict(kind='webbridge',base_url='http://127.0.0.1:10086')),expected)
                self.assertEqual(read.call_count,1)
        with patch('service_monitor.read_json',side_effect=[{'running':True,'extension_connected':True},{'ok':False,'error':'private server text'}]):
            self.assertEqual(probe(dict(kind='webbridge',base_url='http://127.0.0.1:10086')),'extension_unresponsive')

    def test_system_and_user_unit_observation_use_the_selected_scope(self):
        result=Mock(returncode=0,stdout='LoadState=loaded\nActiveState=active\n')
        for scope in ['user','system']:
            with patch('service_monitor.subprocess.run',return_value=result) as run:
                self.assertEqual(probe(dict(kind='unit',unit='fictional.service',scope=scope)),'healthy')
                self.assertEqual('--user' in run.call_args.args[0],scope=='user')

    def test_on_demand_stop_is_standby_but_failure_is_not(self):
        target = dict(kind='webbridge', base_url='http://127.0.0.1:10086',
                      demand_unit='fictional-browser.service')
        for state, sub, expected in [('inactive', 'dead', 'standby'),
                                     ('failed', 'failed', 'unit_inactive'),
                                     ('activating', 'start', 'observation_unreadable')]:
            result = Mock(returncode=0, stdout=f'LoadState=loaded\nActiveState={state}\nSubState={sub}\n')
            with patch('service_monitor.subprocess.run', return_value=result), patch('service_monitor.read_json') as read:
                self.assertEqual(probe(target), expected)
                read.assert_not_called()
        result = Mock(returncode=0, stdout='LoadState=loaded\nActiveState=active\nSubState=running\n')
        with patch('service_monitor.subprocess.run', return_value=result), patch('service_monitor.read_json', side_effect=OSError()):
            self.assertEqual(probe(target), 'daemon_unavailable')

    def test_roundtrip_discards_page_urls(self):
        private='https://example.org/?private-token=fictional'
        with patch('service_monitor.read_json',side_effect=[{'running':True,'extension_connected':True},{'ok':True,'data':{'success':True,'tabs':[{'url':private}]}}]) as read:
            self.assertEqual(probe(dict(kind='webbridge',base_url='http://127.0.0.1:10086')),'healthy')
            self.assertEqual(read.call_args.args[1]['action'],'list_tabs')


class Durable(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.directory=Path(self.tmp.name);self.sent=[];self.client=Mock()
        def call(method,path,value):
            if '/incidents/' in path:
                self.sent.append(dict(value))
                return {k:value[k] for k in ('producer_id','incident_id','revision')}
            return {}
        self.client.call.side_effect=call
        self.config=configuration(self.directory)

    def test_failure_resolution_recurrence_and_unknown_are_distinct(self):
        monitor=Monitor(self.config,self.client)
        try:
            monitor.tick(lambda _:'extension_disconnected',now=10)
            monitor.tick(lambda _:'extension_disconnected',now=11)
            self.assertEqual(len(self.sent),1)
            first=self.sent[0]['incident_id']
            monitor.tick(lambda _:'observation_unreadable',now=12)
            self.assertEqual(self.sent[-1]['processing_state'],'open')
            monitor.tick(lambda _:'healthy',now=13)
            self.assertEqual(self.sent[-1]['incident_id'],first)
            self.assertEqual(self.sent[-1]['revision'],3)
            self.assertEqual(self.sent[-1]['processing_state'],'resolved')
            monitor.tick(lambda _:'extension_disconnected',now=14)
            self.assertNotEqual(self.sent[-1]['incident_id'],first)
        finally:monitor.close()

    def test_lost_acceptance_freezes_exact_revision_across_restart(self):
        self.client.call.side_effect=Unavailable()
        monitor=Monitor(self.config,self.client)
        monitor.tick(lambda _:'extension_disconnected',now=10)
        pending=monitor.db.execute('select content from outbox').fetchone()[0];monitor.close()
        monitor=Monitor(self.config,self.client)
        try:
            monitor.tick(lambda _:'healthy',now=11)
            self.assertEqual(monitor.db.execute('select content from outbox').fetchone()[0],pending)
            self.assertEqual(monitor.db.execute('select count(*) from outbox').fetchone()[0],1)
            sent=[]
            def accept(method,path,value):
                if '/incidents/' in path:
                    sent.append(dict(value));return {k:value[k] for k in ('producer_id','incident_id','revision')}
                return {}
            self.client.call.side_effect=accept
            monitor.tick(lambda _:'healthy',now=12)
            self.assertEqual(sent[0],json.loads(pending))
            self.assertEqual(sent[-1]['processing_state'],'resolved')
        finally:monitor.close()

    def test_demand_policy_preserves_journal_and_resolves_as_standby(self):
        monitor = Monitor(self.config, self.client)
        monitor.tick(lambda _: 'extension_disconnected', now=10)
        first = self.sent[-1]['incident_id']
        monitor.close()
        changed = dict(self.config, demand_units={'browser': 'fictional-browser.service'})
        monitor = Monitor(changed, self.client)
        try:
            seen = []
            monitor.tick(lambda target: seen.append(target) or 'standby', now=11)
            self.assertEqual(seen[0]['demand_unit'], 'fictional-browser.service')
            self.assertEqual(self.sent[-1]['incident_id'], first)
            self.assertEqual(self.sent[-1]['processing_state'], 'resolved')
            self.assertIn('standby', self.sent[-1]['summary'])
            monitor.tick(lambda _: 'extension_disconnected', now=12)
            self.assertEqual(self.sent[-1]['processing_state'], 'open')
        finally:
            monitor.close()
        with self.assertRaises(ValueError):
            Monitor(dict(self.config, demand_units={'unknown': 'wrong.service'}), self.client)

    def test_binding_changes_and_nonlocal_targets_refused(self):
        monitor=Monitor(self.config,self.client);monitor.close()
        changed={**self.config,'targets':[dict(self.config['targets'][0],base_url='https://example.com')]}
        with self.assertRaises(ValueError):Monitor(changed,self.client)
        changed={**self.config,'producer_id':'another-producer'}
        with self.assertRaises(ValueError):Monitor(changed,self.client)

    def test_relocation_preserves_incident_history_and_restarts_only_changed_baselines(self):
        memory = dict(id='memory', resource_ref='old-body', kind='cgroup_memory',
                      cgroup_root='/old-cgroup', full_avg60_percent=5)
        cfg = dict(self.config, targets=[memory, *self.config['targets']])
        monitor = Monitor(cfg,self.client)
        monitor.tick(lambda _: 'observation_unreadable',now=10)
        first = {v['resource_ref']:v['incident_id'] for v in self.sent}
        with monitor.db:
            for key in ('memory','browser'):
                monitor.db.execute('INSERT INTO runtime_counters VALUES(?,?)',(key,'{"oom_kill":7}'))
        monitor.close()
        new = dict(cfg, targets=[dict(memory,resource_ref='receiving-vm',cgroup_root='/new-cgroup'),cfg['targets'][1]])
        result = relocate_monitor(cfg,new,self.client,now=11)
        self.assertEqual(result['relocated_targets'],['memory'])
        self.assertFalse(result['receiving_health_verified'])
        retired = self.sent[-1]
        self.assertEqual((retired['incident_id'],retired['resource_ref'],retired['processing_state']),
                         (first['old-body'],'old-body','resolved'))
        monitor = Monitor(new,self.client)
        try:
            self.assertEqual(monitor.db.execute('SELECT id FROM runtime_counters').fetchall(),[('browser',)])
            retained=json.loads(monitor.db.execute('SELECT content FROM relocations').fetchone()[0])
            self.assertEqual(retained['previous_counters']['memory']['oom_kill'],7)
            monitor.tick(lambda _: 'observation_unreadable',now=12)
            fresh = next(v for v in reversed(self.sent) if v['resource_ref']=='receiving-vm')
            self.assertNotEqual(fresh['incident_id'],first['old-body'])
        finally:monitor.close()
        count=len(self.sent)
        self.assertTrue(relocate_monitor(cfg,new,self.client)['already_relocated'])
        self.assertEqual(len(self.sent),count)

    def test_relocation_replays_exact_uncertain_resolution_before_binding_switch(self):
        monitor=Monitor(self.config,self.client)
        monitor.tick(lambda _: 'extension_disconnected',now=10);monitor.close()
        new=dict(self.config,targets=[dict(self.config['targets'][0],resource_ref='receiving-browser')])
        self.client.call.side_effect=Unavailable()
        with self.assertRaises(Unavailable):relocate_monitor(self.config,new,self.client,now=11)
        monitor=Monitor(self.config,self.client)
        queued=monitor.db.execute('SELECT content FROM outbox').fetchone()[0];monitor.close()
        self.assertEqual(json.loads(queued)['processing_state'],'resolved')
        calls=[]
        def accept(method,path,value):
            calls.append(value);return {k:value[k] for k in ('producer_id','incident_id','revision')}
        self.client.call.side_effect=accept
        relocate_monitor(self.config,new,self.client,now=12)
        self.assertEqual(calls,[json.loads(queued)])

    def test_relocation_rejects_authority_journal_or_probe_identity_changes(self):
        monitor=Monitor(self.config,self.client);monitor.close()
        for patch_value in (dict(producer_id='other'),dict(token_file='/other'),
                            dict(state_dir=str(self.directory/'other-state')),
                            dict(targets=[dict(self.config['targets'][0],id='new-id')]),
                            dict(targets=[dict(id='browser',resource_ref='x',kind='unit',unit='x.service')])):
            with self.assertRaises(ValueError):relocate_monitor(self.config,dict(self.config,**patch_value),self.client)
        monitor=Monitor(self.config,self.client)
        self.assertEqual(monitor.db.execute('SELECT value FROM binding').fetchone()[0],binding_digest(self.config))
        monitor.close()

    def test_relocation_pending_original_blocks_retirement_and_local_history_is_bounded(self):
        self.client.call.side_effect=Unavailable()
        monitor=Monitor(self.config,self.client)
        monitor.tick(lambda _: 'extension_disconnected',now=10)
        original=monitor.db.execute('SELECT content FROM outbox').fetchone()[0]
        new=dict(self.config,targets=[dict(self.config['targets'][0],resource_ref='receiving-browser')])
        with self.assertRaises(Unavailable):monitor.relocate(new,now=11)
        self.assertEqual(monitor.db.execute('SELECT content FROM outbox').fetchall(),[(original,)])
        self.assertEqual(monitor.db.execute('SELECT value FROM binding').fetchone()[0],binding_digest(self.config))
        def accept(method,path,value):
            return {k:value[k] for k in ('producer_id','incident_id','revision')}
        self.client.call.side_effect=accept
        try:
            for index in range(34):
                new=dict(self.config,targets=[dict(self.config['targets'][0],resource_ref='receiver-'+str(index))])
                monitor.relocate(new,now=12+index)
            self.assertEqual(monitor.db.execute('SELECT count(*) FROM relocations').fetchone()[0],32)
            self.assertEqual(monitor.db.execute('SELECT count(*) FROM incidents').fetchone()[0],1)
        finally:monitor.close()


import http_contract


class HTTP(unittest.TestCase):
    setUp=http_contract.Contract.setUp
    start=http_contract.Contract.start
    tearDown=http_contract.Contract.tearDown
    call=http_contract.Contract.call

    def test_actual_core_accepts_scoped_failure_and_verified_recovery(self):
        self.process.terminate();self.process.communicate()
        self.config['actors'][0]['grants'].append('observe')
        self.config['actors'][0]['producers'] += ['fictional-services','fictional-observer']
        self.path.write_text(json.dumps(self.config));self.start()
        token=self.directory/'token';token.write_text(self.tokens['producer']);token.chmod(0o600)
        cfg=configuration(self.directory);cfg['base_url']=self.base
        monitor=Monitor(cfg)
        try:
            self.assertEqual(monitor.tick(lambda _:'extension_disconnected')['service_state'],'available')
            value=json.loads(monitor.db.execute('select content from incidents').fetchone()[0])
            route='/v1/incidents/fictional-services/'+value['incident_id']
            self.assertEqual(self.call('GET',route)[1]['projection']['processing_state'],'open')
            monitor.tick(lambda _:'healthy')
            result=self.call('GET',route)[1]['projection']
            self.assertEqual(result['processing_state'],'resolved')
            self.assertEqual(result['revision'],2)
            relocated=dict(cfg,targets=[dict(cfg['targets'][0],resource_ref='receiving-browser')])
            monitor.close()
            monitor=None
            relocate_monitor(cfg,relocated)
            monitor=Monitor(relocated)
            monitor.tick(lambda _:'extension_disconnected')
            value=json.loads(monitor.db.execute('select content from incidents').fetchone()[0])
            actual=self.call('GET','/v1/incidents/fictional-services/'+value['incident_id'])[1]['projection']
            self.assertEqual(actual['resource_ref'],'receiving-browser')
            self.assertNotEqual(actual['incident_id'],result['incident_id'])
        finally:
            if monitor:monitor.close()


if __name__=='__main__':unittest.main()
