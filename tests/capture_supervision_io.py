"""Real capture subprocesses: pre-inbox gaps, timeout, killed parent and recovery."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from capture_supervisor import safe_projection,resolve
HMK=Path(os.environ.get('ALERTS_TEST_HMK_SOURCE',ROOT.parent/'hermes-memory-kit'))

class CaptureSupervision(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='alerts-capture-');self.root=Path(self.temp.name)
        self.source=self.root/'source';self.source.mkdir();self.transcript=self.source/'fixture.jsonl'
        self.inbox=self.root/'inbox.sqlite';self.state=self.root/'supervision'
        self.binding=dict(schema='hmk.codex-hook-binding/v2',subject='fictional',source_instance='fictional',
            source_root=str(self.source),allowed_cwds=[str(self.root)],inbox=str(self.inbox),max_source_bytes=65536,
            sessions_started_at_or_after='2020-01-01T00:00:00+00:00')
        self.path=self.root/'binding.json';self.path.write_text(json.dumps(self.binding));self.digest=hashlib.sha256(self.path.read_bytes()).hexdigest()
        self.payload=dict(cwd=str(self.root),hook_event_name='Stop',session_id='fictional-session',transcript_path=str(self.transcript),prompt='PRIVATE_PAYLOAD_MARKER')
    def tearDown(self):self.temp.cleanup()
    def command(self,source=None,timeout=1.5):
        return [sys.executable,'-B',str(ROOT/'scripts/capture_supervisor.py'),'--binding',str(self.path),'--binding-sha256',self.digest,
                '--handler-source',str(source or HMK),'--state-dir',str(self.state),'--timeout-seconds',str(timeout)]
    def invoke(self,source=None,timeout=1.5):
        result=subprocess.run(self.command(source,timeout),input=json.dumps(self.payload),capture_output=True,text=True,check=True)
        return json.loads(result.stdout)
    def attempts(self):return [json.loads(p.read_text()) for p in self.state.glob('attempt-*.json')]
    def transcript_present(self):
        data=json.dumps(dict(type='session_meta',payload=dict(id='fictional-session',cwd=str(self.root),timestamp='2026-10-14T00:00:00+00:00')))+'\nPRIVATE_SOURCE_MARKER\n'
        self.transcript.write_text(data)
    def sleeping_handler(self):
        source=self.root/'sleeping';(source/'scripts').mkdir(parents=True)
        (source/'scripts/codex_hook_capture.py').write_text('import time\ndef capture(binding,payload):\n time.sleep(30)\n')
        return source
    def test_initialization_is_not_capture_and_rejects_binding_change(self):
        from capture_supervisor import initialize
        initialize(self.state,self.digest);initialize(self.state,self.digest)
        self.assertEqual(safe_projection(self.state,self.digest),{'items':[]})
        self.assertFalse(self.inbox.exists());self.assertEqual(self.attempts(),[])
        with self.assertRaises(ValueError):initialize(self.state,'0'*64)
    def test_gap_before_journal_then_explicit_matching_recovery(self):
        self.assertIn('systemMessage',self.invoke());self.assertFalse(self.inbox.exists())
        failure=self.attempts()[0];projection=safe_projection(self.state,self.digest)['items'][0]
        self.assertFalse(projection['work_preserved']);self.assertEqual(projection['phase'],'capture')
        self.transcript_present();self.assertEqual(self.invoke(),{})
        recovery=next(a for a in self.attempts() if a['state']=='complete')
        self.assertTrue(self.inbox.exists())
        # Successful capture alone cannot silently clear a previous failed event.
        self.assertEqual(safe_projection(self.state,self.digest)['items'][0]['processing_state'],'open')
        resolve(self.state,failure['attempt_id'],recovery['attempt_id'])
        resolved=safe_projection(self.state,self.digest)['items'][0]
        self.assertEqual(resolved['incident_id'],projection['incident_id']);self.assertEqual(resolved['processing_state'],'resolved')
        self.assertTrue(resolved['work_preserved'])
        telemetry=''.join(p.read_text() for p in self.state.glob('*.json'))
        self.assertNotIn('PRIVATE_',telemetry);self.assertNotIn(str(self.transcript),telemetry)
        self.assertNotIn('PRIVATE_',json.dumps(resolved))
    def test_timeout_before_any_inbox(self):
        result=self.invoke(self.sleeping_handler(),.1);self.assertIn('hook_timeout',result['systemMessage'])
        self.assertFalse(self.inbox.exists());self.assertEqual(safe_projection(self.state,self.digest)['items'][0]['problem_code'],'timeout')
    def excluded_recovery(self):
        self.invoke();failure=self.attempts()[0]
        meta=dict(type='session_meta',payload=dict(id='fictional-session',cwd=str(self.root),timestamp='2019-12-31T23:59:59+00:00'))
        self.transcript.write_text(json.dumps(meta)+'\nPRIVATE_EXCLUDED_CONVERSATION\n')
        self.assertEqual(self.invoke(),{})
        recovery=next(a for a in self.attempts() if a['state']=='skipped')
        return failure,recovery
    def test_explicit_exclusion_preserves_failure_without_import_or_capture_claim(self):
        failure,recovery=self.excluded_recovery()
        self.assertFalse(self.inbox.exists())
        with self.assertRaises(ValueError):resolve(self.state,failure['attempt_id'],recovery['attempt_id'])
        command=[sys.executable,str(ROOT/'scripts/capture_supervisor.py'),'--state-dir',str(self.state),
                 '--exclude-attempt',failure['attempt_id'],'--recovery-attempt',recovery['attempt_id']]
        for _ in range(2):
            result=subprocess.run(command,check=True,capture_output=True,text=True)
            self.assertTrue(json.loads(result.stdout)['source_excluded'])
        resolved=next(a for a in self.attempts() if a['state']=='resolved')
        self.assertEqual(resolved['reason'],failure['reason'])
        self.assertEqual(resolved['exclusion_reason'],'session_precedes_selected_boundary')
        projection=safe_projection(self.state,self.digest)['items'][0]
        self.assertEqual(projection['processing_state'],'resolved');self.assertFalse(projection['work_preserved'])
        self.assertIn('excluded',projection['summary']);self.assertNotIn('restored',projection['summary'])
        self.assertFalse(self.inbox.exists());self.assertNotIn('PRIVATE_',json.dumps(self.attempts()))
        self.assertNotIn(str(self.transcript),json.dumps(self.attempts()))
    def test_exclusion_requires_same_source_event_binding_and_verified_reason(self):
        failure,recovery=self.excluded_recovery();path=self.state/('attempt-'+recovery['attempt_id']+'.json')
        for field,value in [('source_ref','0'*64),('event','SessionEnd'),('binding_sha256','0'*64),
                            ('exclusion_reason',None),('exclusion_reason','operator_acknowledged'),('state','complete')]:
            with self.subTest(field=field,value=value):
                altered={**recovery,field:value};path.write_text(json.dumps(altered))
                with self.assertRaises(ValueError):resolve(self.state,failure['attempt_id'],recovery['attempt_id'],excluded=True)
                retained=json.loads((self.state/('attempt-'+failure['attempt_id']+'.json')).read_text())
                self.assertEqual(retained,failure)
        path.write_text(json.dumps(recovery))
        original=self.state/('attempt-'+failure['attempt_id']+'.json')
        original.write_text(json.dumps({**failure,'source_ref':None,'event':None}))
        with self.assertRaises(ValueError):resolve(self.state,failure['attempt_id'],recovery['attempt_id'],excluded=True)
        self.assertFalse(self.inbox.exists())
    def test_native_kill_preserves_uncertain_attempt(self):
        process=subprocess.Popen(self.command(self.sleeping_handler(),.1),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,start_new_session=True)
        process.stdin.write(json.dumps(self.payload));process.stdin.close();process.stdin=None
        try:
            for _ in range(100):
                paths=list(self.state.glob('attempt-*.json'))
                if paths and json.loads(paths[0].read_text()).get('source_ref'):break
                time.sleep(.01)
            else:self.fail('capture intent not persisted')
            os.killpg(process.pid,signal.SIGKILL);process.communicate(timeout=5)
            self.assertEqual(self.attempts()[0]['state'],'started')
            time.sleep(2.2)
            first=safe_projection(self.state,self.digest)['items'][0];second=safe_projection(self.state,self.digest)['items'][0]
            self.assertEqual(first,second);self.assertEqual(first['problem_code'],'dispatch_unresolved')
            self.assertFalse(self.inbox.exists());self.assertFalse(first['work_preserved'])
            failure=self.attempts()[0];self.transcript_present();self.assertEqual(self.invoke(),{})
            recovery=next(a for a in self.attempts() if a['state']=='complete')
            resolve(self.state,failure['attempt_id'],recovery['attempt_id'])
            resolve(self.state,failure['attempt_id'],recovery['attempt_id'])
            self.assertEqual(safe_projection(self.state,self.digest)['items'][0]['processing_state'],'resolved')
        finally:
            if process.poll() is None:os.killpg(process.pid,signal.SIGKILL);process.communicate(timeout=5)

if __name__=='__main__':unittest.main(verbosity=2)
