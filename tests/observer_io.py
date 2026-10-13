"""Safe HMK journal projection, explicit resolution and independent observer faults."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts')); sys.path.insert(0,str(ROOT/'tests'))
from http_contract import Contract
from observer import Observer

HMK=Path(os.environ.get('ALERTS_TEST_HMK_SOURCE','/home/debian/Projects/hermes-memory-kit'))

class ObserverHTTP(unittest.TestCase):
    setUp=Contract.setUp
    start=Contract.start
    tearDown=Contract.tearDown
    call=Contract.call
    pending=Contract.pending
    def test_safe_source_and_observer_faults(self):
        self.process.terminate(); self.process.communicate()
        producer=self.config['actors'][0]; producer['grants'].append('observe'); producer['producers'].append('fictional-observer')
        self.path.write_text(json.dumps(self.config)); self.start()
        token=self.directory/'producer-token'; token.write_text(self.tokens['producer']); token.chmod(0o600)
        state=self.directory/'worker'; state.mkdir()
        worker=state/'worker-status.json'; worker.write_text(json.dumps(dict(schema='hmk.worker-status/v1',updated_ns=time.time_ns(),state='pending')))
        sys.path.insert(0,str(HMK/'scripts'))
        from capture_inbox import Inbox
        from provider_status import Alerts,ProviderProblem
        inbox=Inbox(self.directory/'capture.sqlite',{'subject':'fictional'})
        inbox.observe('fictional-source',{},b'PRIVATE source must stay intact')
        alerts=Alerts(inbox); alerts.report('fictional-operation','index',ProviderProblem('fixture-provider','dispatch-1',{'code':'payment_required'}))
        config=dict(base_url=self.base,token_file=str(token),state_dir=str(self.directory/'observer'),hmk_source=str(HMK),producer_id='fictional-hmk',observer_id='fictional-observer',receiving_scope='fictional-human',resource_ref='fictional-worker',inbox=str(inbox.path),inbox_binding={'subject':'fictional'},worker_state_dir=str(state),source_freshness_seconds=10,stalled_seconds=1)
        observer=Observer(config)
        first=observer.tick(); self.assertEqual(first['service_state'],'available')
        projection=self.call('GET','/v1/incidents')[1]['items'][0]['projection']
        self.assertEqual(projection['problem_code'],'payment_required'); self.assertEqual(len(self.pending()),1)
        self.assertNotIn('PRIVATE',json.dumps(projection)); self.assertEqual(alerts.status()['alerts'][0]['delivery'],'pending')
        with inbox.connect() as con: self.assertEqual(con.execute('SELECT delta FROM revisions').fetchone()[0],b'PRIVATE source must stay intact')
        observer.tick(); self.assertEqual(len(self.pending()),1)
        # Explicit producer recovery; the observer itself never invokes it.
        alerts.resolve('fictional-operation'); observer.tick()
        self.assertEqual(self.call('GET','/v1/incidents')[1]['items'][0]['projection']['processing_state'],'resolved')
        self.assertEqual(len(self.pending()),2)
        worker.write_text(json.dumps(dict(schema='hmk.worker-status/v1',updated_ns=time.time_ns()-3_000_000_000,state='running')))
        self.assertEqual(observer.tick()['source_state'],'worker_stalled')
        self.assertEqual(len(self.pending()),3)
        observer.tick(); self.assertEqual(len(self.pending()),3)
        worker.write_text('unreadable private error'); self.assertEqual(observer.tick()['source_state'],'observation_unreadable')
        self.process.terminate(); self.process.communicate()
        self.assertEqual(observer.tick()['service_state'],'unavailable')
        observed=json.loads((Path(config['state_dir'])/'status.json').read_text()); self.assertEqual(observed['service_state'],'unavailable')
        observer.db.close(); observer.lock.close()
        self.start(); restored=Observer(config); self.assertEqual(restored.tick()['service_state'],'available')
        restored.db.close(); restored.lock.close()

if __name__=='__main__': unittest.main(defaultTest='ObserverHTTP.test_safe_source_and_observer_faults',verbosity=2)
