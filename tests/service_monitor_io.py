"""Critical-service truth, durable transitions and exact uncertain replay."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
sys.path.insert(0,str(Path(__file__).parents[1]/'scripts'))
from client import Unavailable
from service_monitor import Monitor, probe


def configuration(path):
    return dict(producer_id='fictional-services',observer_id='fictional-observer',
                receiving_scope='fictional-human',base_url='http://127.0.0.1:9',
                token_file=str(path/'token'),state_dir=str(path/'state'),
                targets=[dict(id='browser',resource_ref='fictional-browser',kind='webbridge',base_url='http://127.0.0.1:10086')])


class Probes(unittest.TestCase):
    def test_process_up_is_not_connected_or_operational(self):
        for status,expected in [({'running':False,'extension_connected':False},'daemon_unavailable'),
                                ({'running':True,'extension_connected':False},'extension_disconnected'),
                                ({'running':True},'observation_unreadable')]:
            with patch('service_monitor.read_json',return_value=status) as read:
                self.assertEqual(probe(dict(kind='webbridge',base_url='http://127.0.0.1:10086')),expected)
                self.assertEqual(read.call_count,1)
        with patch('service_monitor.read_json',side_effect=[{'running':True,'extension_connected':True},{'ok':False,'error':'private server text'}]):
            self.assertEqual(probe(dict(kind='webbridge',base_url='http://127.0.0.1:10086')),'extension_unresponsive')

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

    def test_binding_changes_and_nonlocal_targets_refused(self):
        monitor=Monitor(self.config,self.client);monitor.close()
        changed={**self.config,'targets':[dict(self.config['targets'][0],base_url='https://example.com')]}
        with self.assertRaises(ValueError):Monitor(changed,self.client)
        changed={**self.config,'producer_id':'another-producer'}
        with self.assertRaises(ValueError):Monitor(changed,self.client)


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
        finally:monitor.close()


if __name__=='__main__':unittest.main()
