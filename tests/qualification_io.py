"""The fictional recovery driver must use a source-valid selection and finish once."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
HMK=Path(os.environ.get('ALERTS_TEST_HMK_SOURCE',ROOT.parent/'hermes-memory-kit'))
class FictionalRecovery(unittest.TestCase):
    def test_failure_explicit_recovery_and_replay(self):
        with tempfile.TemporaryDirectory(prefix='alerts-fictional-recovery-') as state:
            def invoke(operation):
                result=subprocess.run(['python3',str(ROOT/'scripts/qualify_hmk.py'),operation,'--hmk-source',str(HMK),'--state-dir',state],capture_output=True,text=True,check=True)
                return json.loads(result.stdout)
            failed=invoke('failure'); self.assertEqual(failed['processing_result'],'pending');self.assertEqual(failed['chapters'],0);self.assertTrue(failed['source_preserved'])
            complete=invoke('recover');self.assertEqual(complete['processing_result'],'complete');self.assertEqual(complete['chapters'],1);self.assertEqual(complete['open_incidents'],0)
            replay=invoke('recover');self.assertEqual(replay['processing_result'],'idle');self.assertEqual(replay['chapters'],1)
            self.assertEqual(failed['source_sha256'],replay['source_sha256']);self.assertEqual(replay['producer_notice_states'],['pending']);self.assertFalse(replay['real_provider_used'])
if __name__=='__main__': unittest.main(verbosity=2)
