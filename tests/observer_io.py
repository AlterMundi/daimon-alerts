"""Safe HMK journal projection, explicit resolution and independent observer faults."""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import unittest
import tempfile
from unittest.mock import patch

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
    def test_first_receipt_and_outbox_are_not_fabricated(self):
        self.process.terminate(); self.process.communicate()
        self.config['actors'][0]['grants'].append('observe')
        self.config['actors'][0]['producers'].append('fictional-observer')
        self.path.write_text(json.dumps(self.config)); self.start()
        token=self.directory/'producer-token'; token.write_text(self.tokens['producer']); token.chmod(0o600)
        sys.path.insert(0,str(HMK/'scripts'))
        from capture_inbox import Inbox
        inbox=Inbox(self.directory/'capture.sqlite',{'subject':'fictional'})
        before=inbox.path.read_bytes()
        config=dict(base_url=self.base,token_file=str(token),state_dir=str(self.directory/'observer'),hmk_source=str(HMK),producer_id='fictional-hmk',observer_id='fictional-observer',receiving_scope='fictional-human',resource_ref='fictional-worker',inbox=str(inbox.path),inbox_binding={'subject':'fictional'},worker_state_dir=str(self.directory/'not-started'),source_freshness_seconds=10,stalled_seconds=1)
        observer=Observer(config)
        try:
            result=observer.tick()
            self.assertEqual(result['observation_kind'],'unobserved')
            self.assertIsNone(result['source_at'])
            self.assertEqual(result['service_state'],'available')
            incident=self.call('GET','/v1/incidents')[1]['items'][0]['projection']
            self.assertIn('not started or unobserved',incident['summary'])
            observer.tick(); self.assertEqual(len(self.pending()),1)
            self.assertEqual(inbox.path.read_bytes(),before)
            self.assertFalse(Path(config['worker_state_dir']).exists())
        finally:
            observer.db.close(); observer.lock.close()
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

class FiniteSupervision(unittest.TestCase):
    def test_missing_receipt_launch_failure_and_daily_cadence(self):
        with tempfile.TemporaryDirectory() as directory:
            observer=Observer.__new__(Observer)
            observer.config=dict(worker_state_dir=directory,worker_unit='hmk-memory-worker.service',
                worker_mode='oneshot',source_freshness_seconds=90000,stalled_seconds=1800,
                worker_daily_utc='00:00',worker_cadence_grace_seconds=2400)
            def result(**changes):
                values=dict(LoadState='loaded',Type='oneshot',ActiveState='inactive',Result='success',ExecMainCode='0',ExecMainStatus='0')
                values.update(changes)
                return subprocess.CompletedProcess([],0,'\n'.join(f'{k}={v}' for k,v in values.items()),'')
            with patch('observer.subprocess.run',return_value=result()):
                self.assertEqual(observer.source_state(),('observation_unreadable',None))
                self.assertEqual(observer.observation_kind,'unobserved')
            with patch('observer.subprocess.run',return_value=result(ActiveState='failed',Result='exit-code',ExecMainStatus='1')):
                self.assertEqual(observer.source_state(),('worker_stopped',None))
                self.assertEqual(observer.observation_kind,'launch_failure')
            # An updated receipt cannot hide a missing invocation for today's cadence.
            midnight=int(time.time())//86400*86400; now=midnight+3600
            path=Path(directory)/'worker-status.json'
            def write(start):
                path.write_text(json.dumps(dict(schema='hmk.worker-status/v1',state='idle',
                    updated_ns=now*1_000_000_000,started_ns=start*1_000_000_000)))
            with patch('observer.subprocess.run',return_value=result()), patch('observer.time.time',return_value=now), patch('observer.time.time_ns',return_value=now*1_000_000_000):
                write(midnight-3600)
                self.assertEqual(observer.source_state()[0],'observation_stale')
                self.assertEqual(observer.observation_kind,'missed_invocation')
                write(midnight+10)
                self.assertEqual(observer.source_state()[0],'healthy')
            # During the grace window, yesterday's selected invocation suffices.
            write(midnight-3600)
            with patch('observer.subprocess.run',return_value=result()), patch('observer.time.time',return_value=midnight+1200), patch('observer.time.time_ns',return_value=(midnight+1200)*1_000_000_000):
                path.write_text(json.dumps(dict(schema='hmk.worker-status/v1',state='idle',updated_ns=(midnight+1200)*1_000_000_000,started_ns=(midnight-3600)*1_000_000_000)))
                self.assertEqual(observer.source_state()[0],'healthy')

    def test_completed_pending_failed_and_overdue(self):
        with tempfile.TemporaryDirectory() as directory:
            worker=Path(directory)/'worker-status.json'
            observer=Observer.__new__(Observer)
            observer.config=dict(worker_state_dir=directory,worker_unit='hmk-memory-worker.service',
                                 worker_mode='oneshot',worker_systemd_scope='system',
                                 source_freshness_seconds=60,stalled_seconds=10)
            def write(state='idle',age=0):
                worker.write_text(json.dumps(dict(schema='hmk.worker-status/v1',
                    updated_ns=time.time_ns()-age*1_000_000_000,state=state)))
            def observed(**changes):
                values=dict(LoadState='loaded',Type='oneshot',ActiveState='inactive',
                            Result='success',ExecMainCode='1',ExecMainStatus='0')
                values.update(changes)
                return subprocess.CompletedProcess([],0,'\n'.join(f'{k}={v}' for k,v in values.items()),'')
            write()
            with patch('observer.subprocess.run',return_value=observed()) as run:
                self.assertEqual(observer.source_state()[0],'healthy')
                self.assertNotIn('--user',run.call_args.args[0])
                write(age=90)
                self.assertEqual(observer.source_state()[0],'observation_stale')
            write('pending')
            with patch('observer.subprocess.run',return_value=observed(ActiveState='failed',Result='exit-code',ExecMainStatus='2')):
                self.assertEqual(observer.source_state()[0],'healthy')
            for result in (observed(ActiveState='failed',Result='exit-code',ExecMainStatus='1'),
                           observed(ActiveState='failed',Result='signal',ExecMainCode='2',ExecMainStatus='9')):
                with patch('observer.subprocess.run',return_value=result):
                    self.assertEqual(observer.source_state()[0],'worker_stopped')
            with patch('observer.subprocess.run',return_value=observed(ExecMainCode='0')):
                self.assertEqual(observer.source_state()[0],'healthy')
            write('idle')
            with patch('observer.subprocess.run',return_value=observed(ActiveState='failed',Result='exit-code',ExecMainStatus='2')):
                self.assertEqual(observer.source_state()[0],'worker_stopped')
            write('running')
            with patch('observer.subprocess.run',return_value=observed()):
                self.assertEqual(observer.source_state()[0],'worker_stopped')
            write('running',age=20)
            with patch('observer.subprocess.run',return_value=observed(ActiveState='activating')):
                self.assertEqual(observer.source_state()[0],'worker_stalled')
            for result in (observed(Type='simple'),observed(LoadState='not-found'),
                           subprocess.CompletedProcess([],1,'','')):
                with patch('observer.subprocess.run',return_value=result):
                    self.assertEqual(observer.source_state()[0],'observation_unreadable')
            observer.config.update(worker_mode='continuous',worker_systemd_scope='user')
            write()
            with patch('observer.subprocess.run',return_value=observed()) as run:
                self.assertEqual(observer.source_state()[0],'worker_stopped')
                self.assertIn('--user',run.call_args.args[0])

if __name__=='__main__': unittest.main(defaultTest=['FiniteSupervision','ObserverHTTP'],verbosity=2)
