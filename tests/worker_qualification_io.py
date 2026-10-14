"""Finite worker receipt-bound recovery, with isolated non-network transports."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
HMK=Path(os.environ.get('ALERTS_TEST_HMK_SOURCE',ROOT.parent/'hermes-memory-kit'))

class FiniteWorkerRecovery(unittest.TestCase):
    def test_failed_attempt_explicit_recovery_and_replay(self):
        with tempfile.TemporaryDirectory(prefix='alerts-finite-worker-') as state:
            def run(operation):
                p=subprocess.run(['python3','-B',str(ROOT/'scripts/qualify_worker.py'),operation,'--hmk-source',str(HMK),'--state-dir',state],capture_output=True,text=True)
                self.assertEqual(p.returncode,2 if operation=='failure' else 0,p.stderr)
                return json.loads(p.stdout)
            failed=run('failure');self.assertEqual(failed['chapters'],0);self.assertEqual(failed['open_incidents'],1)
            repeat=run('failure');self.assertEqual(repeat['counts'],failed['counts'])
            completed=run('recover');self.assertEqual(completed['worker_state'],'idle');self.assertEqual(completed['chapters'],1);self.assertEqual(completed['open_incidents'],0)
            self.assertEqual(completed['counts']['successful_dispatches'],1)
            replay=run('replay');self.assertEqual(replay['counts'],completed['counts']);self.assertEqual(replay['chapters'],1)
            self.assertTrue(replay['source_preserved']);self.assertFalse(replay['real_provider_used'])
            processor=Path(state)/'processor'
            self.assertEqual(len(list(processor.rglob('retries/*.json'))),1)
            states=[json.loads(p.read_text())['state'] for p in processor.rglob('dispatches/*.json')]
            self.assertCountEqual(states,['failed','completed'])

if __name__=='__main__':unittest.main(verbosity=2)
