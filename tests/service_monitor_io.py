"""Critical-service truth, durable transitions and exact uncertain replay."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
sys.path.insert(0,str(Path(__file__).parents[1]/'scripts'))
from client import Unavailable
from service_monitor import Monitor, probe, relocate_monitor, binding_digest, matrix_receiving, extend_monitor


def configuration(path):
    return dict(producer_id='fictional-services',observer_id='fictional-observer',
                receiving_scope='fictional-human',base_url='http://127.0.0.1:9',
                token_file=str(path/'token'),state_dir=str(path/'state'),
                targets=[dict(id='browser',resource_ref='fictional-browser',kind='webbridge',base_url='http://127.0.0.1:10086')])


class Probes(unittest.TestCase):
    def test_explicit_https_group_is_bounded_and_credential_free(self):
        target=dict(id='deployment',resource_ref='fictional-host',kind='https_health_group',
                    endpoints=[dict(id='one',url='https://one.example/interaction/health'),
                               dict(id='two',url='https://two.example/interaction/health')])
        with tempfile.TemporaryDirectory() as directory:
            cfg=configuration(Path(directory));Monitor.validate(dict(cfg,targets=[target]))
            invalid=[None,[],target['endpoints']*3,[dict(id='one',url='http://one.example/health')],
                     [dict(id='one',url='https://user:secret@one.example/health')],
                     [dict(id='one',url='https://one.example/health?token=secret')],
                     [dict(id='one',url='https://one.example/health#fragment')],
                     [dict(id='one',url='https://one.example:invalid/health')],
                     [dict(id='one',url='https://one.example/health',extra=True)],
                     [target['endpoints'][0],target['endpoints'][0]]]
            for entries in invalid:
                with self.subTest(entries=entries),self.assertRaises(ValueError):
                    Monitor.validate(dict(cfg,targets=[dict(target,endpoints=entries)]))
            groups=[dict(target,id=str(i)) for i in range(3)]
            with self.assertRaises(ValueError):Monitor.validate(dict(cfg,targets=groups))

    def test_group_needs_every_endpoint_and_retains_no_payload(self):
        target=dict(kind='https_health_group',endpoints=[dict(id=str(i),url=str(i)) for i in range(4)])
        for fault in (None,False,'unavailable','bad-json'):
            def read(url):
                if url=='2' and fault=='unavailable':raise OSError('private network detail')
                if url=='2' and fault=='bad-json':raise ValueError('private response')
                return {'ok':False if url=='2' and fault is False else True,'private':'not retained'}
            with self.subTest(fault=fault),patch('service_monitor.read_json',side_effect=read) as calls:
                self.assertEqual(probe(target),'healthy' if fault is None else 'health_unavailable')
                self.assertEqual(calls.call_count,4)

    def test_scheduled_dependency_needs_actual_timer_and_ordered_pull_in(self):
        target=dict(id='packages',resource_ref='host',kind='scheduled_job',scope='system',
                    timer='daily.timer',trigger_unit='daily.service',unit='packages.service',
                    max_age_seconds=93600,max_run_seconds=900)
        with tempfile.TemporaryDirectory() as directory:
            cfg=configuration(Path(directory));Monitor.validate(dict(cfg,targets=[target]))
            for bad in [dict(target,trigger_unit=None),dict(target,trigger_unit='other.service'),
                        dict(target,trigger_unit='daily.timer'),dict(target,timer='unrelated.timer')]:
                with self.subTest(target=bad),self.assertRaises(ValueError):
                    Monitor.validate(dict(cfg,targets=[bad]))
        timer='LoadState=loaded\nActiveState=active\nUnitFileState=enabled\nTriggers=daily.service\n'
        job='LoadState=loaded\nActiveState=inactive\nResult=success\nExecMainStatus=0\nExecMainExitTimestampMonotonic=9000000000\n'
        trigger='LoadState=loaded\nWants=network.service packages.service\nAfter=packages.service network.service\n'
        cases=[(timer,job,trigger,'healthy'),
               (timer,job,trigger.replace('Wants=network.service packages.service','Wants=network.service'),'unit_inactive'),
               (timer,job,trigger.replace('After=packages.service network.service','After=network.service'),'unit_inactive'),
               (timer.replace('Triggers=daily.service','Triggers=other.service'),job,trigger,'unit_inactive'),
               (timer,job,trigger.replace('LoadState=loaded','LoadState=not-found'),'unit_inactive'),
               (timer,job.replace('Result=success','Result=exit-code').replace('Status=0','Status=1'),trigger,'health_unavailable'),
               (timer,job.replace('9000000000','1000000000'),trigger,'health_unavailable'),
               (timer,job,trigger.replace('Wants=','Requires='),'healthy')]
        for t,j,s,expected in cases:
            with self.subTest(expected=expected,t=t,j=j,s=s),patch('service_monitor.subprocess.run',
                    side_effect=[Mock(returncode=0,stdout=t),Mock(returncode=0,stdout=j),Mock(returncode=0,stdout=s)]),\
                 patch('service_monitor.Path.read_text',return_value='100000 0'):
                self.assertEqual(probe(target),expected)

    def test_scheduled_job_distinguishes_disabled_failed_stale_and_running(self):
        target = dict(kind='scheduled_job', scope='user', timer='cleanup.timer',
                      unit='cleanup.service', max_age_seconds=7200, max_run_seconds=300)
        timer = 'LoadState=loaded\nActiveState=active\nUnitFileState=enabled\n'
        job = 'LoadState=loaded\nActiveState=inactive\nResult=success\nExecMainStatus=0\nExecMainExitTimestampMonotonic=9000000000\n'
        cases = [(timer, job, 'healthy'),
                 (timer.replace('enabled','disabled'), job, 'unit_inactive'),
                 (timer.replace('active','inactive'), job, 'unit_inactive'),
                 (timer, job.replace('success','exit-code').replace('Status=0','Status=1'), 'health_unavailable'),
                 (timer, job.replace('9000000000','1000000000'), 'health_unavailable'),
                 (timer, job.replace('9000000000','0'), 'health_unavailable'),
                 (timer, job.replace('9000000000','11000000000'), 'health_unavailable'),
                 (timer, 'LoadState=loaded\nActiveState=activating\nExecMainStartTimestampMonotonic=9900000000\n', 'healthy'),
                 (timer, 'LoadState=loaded\nActiveState=activating\nExecMainStartTimestampMonotonic=9000000000\n', 'health_unavailable')]
        for t,j,expected in cases:
            with self.subTest(expected=expected,t=t,j=j), patch('service_monitor.subprocess.run',
                    side_effect=[Mock(returncode=0,stdout=t),Mock(returncode=0,stdout=j)]), patch(
                    'service_monitor.Path.read_text', return_value='10000 0'):
                self.assertEqual(probe(target), expected)

    def test_daily_cadence_grace_never_hides_failed_or_missed_runs(self):
        target = dict(kind='scheduled_job', scope='system', timer='daily.timer',
                      unit='daily.service', max_age_seconds=93600, max_run_seconds=900)
        timer = 'LoadState=loaded\nActiveState=active\nUnitFileState=enabled\n'
        job = ('LoadState=loaded\nActiveState=inactive\nResult=success\n'
               'ExecMainStatus=0\nExecMainExitTimestampMonotonic=5000000000\n')
        cases = [(job, 'healthy'),
                 (job.replace('5000000000', '1000000'), 'health_unavailable'),
                 (job.replace('success', 'exit-code').replace('Status=0', 'Status=2'),
                  'health_unavailable'),
                 ('LoadState=loaded\nActiveState=activating\n'
                  'ExecMainStartTimestampMonotonic=94000000000\n', 'health_unavailable')]
        for actual, expected in cases:
            with self.subTest(expected=expected), patch('service_monitor.subprocess.run',
                    side_effect=[Mock(returncode=0,stdout=timer),Mock(returncode=0,stdout=actual)]), patch(
                    'service_monitor.Path.read_text', return_value='95000 0'):
                self.assertEqual(probe(target), expected)

    def test_scheduled_job_age_is_bounded_with_daily_grace(self):
        with tempfile.TemporaryDirectory() as directory:
            cfg = configuration(Path(directory))
            target = dict(id='daily',resource_ref='host',kind='scheduled_job',scope='system',
                          timer='daily.timer',unit='daily.service',max_run_seconds=900)
            for age in (60, 86400, 93600, 172800):
                Monitor.validate(dict(cfg,targets=[dict(target,max_age_seconds=age)]))
            for age in (59, 172801, True, '93600'):
                with self.subTest(age=age), self.assertRaises(ValueError):
                    Monitor.validate(dict(cfg,targets=[dict(target,max_age_seconds=age)]))

    def test_matrix_process_is_not_receiving_readiness(self):
        target = dict(kind='matrix_receiving', command=['/fictional/adapter'])
        for ready, expected in [(True, 'healthy'), (False, 'matrix_reader_unavailable')]:
            value = dict(schema='dm.monitor.matrix-receiving/v1', ready=ready)
            result = Mock(returncode=0, stdout=json.dumps(value))
            with patch('service_monitor.subprocess.run', return_value=result) as run:
                self.assertEqual(matrix_receiving(target['command']), expected)
                self.assertEqual(run.call_args.kwargs['timeout'], 12)
        for state in ('matrix_local_capability_expiring', 'matrix_listener_unavailable'):
            result = Mock(returncode=0, stdout=json.dumps(dict(schema='dm.monitor.matrix-receiving/v2', state=state)))
            with patch('service_monitor.subprocess.run', return_value=result):
                self.assertEqual(matrix_receiving(target['command']), state)
        for result in [Mock(returncode=1, stdout='private failure'),
                       Mock(returncode=0, stdout='{}'),
                       Mock(returncode=0, stdout='x'*4097)]:
            with patch('service_monitor.subprocess.run', return_value=result):
                self.assertEqual(matrix_receiving(target['command']), 'observation_unreadable')
        with patch('service_monitor.subprocess.run', side_effect=OSError()):
            self.assertEqual(matrix_receiving(target['command']), 'observation_unreadable')

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

    def test_selected_matrix_unit_also_requires_its_reader_gate(self):
        target = dict(kind='unit', unit='fictional.service', scope='system',
                      receiving_command=['/fictional/health'])
        for state, calls in [('active', 1), ('inactive', 0)]:
            result = Mock(returncode=0, stdout=f'LoadState=loaded\nActiveState={state}\n')
            with patch('service_monitor.subprocess.run', return_value=result), patch(
                    'service_monitor.matrix_receiving', return_value='matrix_reader_unavailable') as reader:
                self.assertEqual(probe(target), 'matrix_reader_unavailable' if calls else 'unit_inactive')
                self.assertEqual(reader.call_count, calls)

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


class Extension(unittest.TestCase):
    def test_addition_preserves_history_and_lost_reply_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory); cfg = configuration(p)
            client = Mock()
            client.call.side_effect = lambda method,path,value: {k:value[k] for k in ('producer_id','incident_id','revision') if k in value}
            m = Monitor(cfg, client)
            m.tick(lambda _: 'extension_disconnected', now=100)
            history = m.db.execute('SELECT content FROM incidents').fetchall()
            m.close()
            extra = dict(id='cleanup',resource_ref='host',kind='scheduled_job',scope='user',
                         timer='cleanup.timer',unit='cleanup.service',max_age_seconds=7200,max_run_seconds=300)
            new = dict(cfg,targets=cfg['targets']+[extra])
            self.assertEqual(extend_monitor(cfg,new,client)['added_targets'], ['cleanup'])
            self.assertTrue(extend_monitor(cfg,new,client)['already_extended'])
            m = Monitor(new,client)
            self.assertEqual(m.db.execute('SELECT content FROM incidents').fetchall(), history)
            with self.assertRaises(ValueError): m.extend(dict(new,targets=[extra]))
            m.close()
            for change in [dict(extra,max_age_seconds=0),dict(extra,timer='other.timer'),dict(extra,scope='unknown')]:
                with self.assertRaises(ValueError): Monitor.validate(dict(cfg,targets=[change]))


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
