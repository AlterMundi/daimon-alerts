"""Once-only owner action, real producer journal and exact acceptance recovery."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
from client import Unavailable
from owner_repair import Refused, recover
from service_monitor import Monitor


class Recovery(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)
        self.target = dict(id='matrix', resource_ref='matrix-body', kind='unit',
                           unit='fixture-matrix.service', scope='system', receiving_command=['/fixture/reader'])
        config = dict(producer_id='fixture', observer_id='fixture', receiving_scope='fixture',
                      targets=[self.target], state_dir=str(self.path / 'state'),
                      base_url='http://127.0.0.1:9', token_file=str(self.path / 'token'))
        self.client = Mock()
        self.client.call.side_effect = lambda method, path, value: value
        self.monitor = Monitor(config, self.client)
        self.monitor.transition(self.target, 'unit_inactive', 100)
        self.monitor.flush()
        self.original = json.loads(self.monitor.db.execute('SELECT content FROM incidents').fetchone()[0])
        self.raw = json.dumps(dict(schema='daimon-alerts-repair-input/v1', incident=dict(
            id=self.original['incident_id'], revision=1, resource_id='matrix-body'))).encode()
        self.selection = dict(enabled=True, target_id='matrix', unit='fixture-matrix.service',
                              scope='system', hold_file=str(self.path / 'hold'))
        self.run = Mock(side_effect=[subprocess.CompletedProcess([], 0,
            'LoadState=loaded\nActiveState=failed\nResult=exit-code\n'), subprocess.CompletedProcess([], 0, '')])
        self.check = Mock(return_value='healthy')

    def tearDown(self):
        self.monitor.close()
        self.temp.cleanup()

    def act(self, **kwargs):
        return recover(self.monitor, self.selection, self.raw, run=self.run, check=self.check, **kwargs)

    def test_same_incident_resolution_and_duplicate_refusal(self):
        receipt = self.act()
        self.assertEqual(receipt['source_sha256'], hashlib.sha256(self.raw).hexdigest())
        self.assertEqual(receipt['resolution_revision'], 2)
        self.assertEqual(receipt['incident_id'], self.original['incident_id'])
        self.assertEqual(self.run.call_count, 2)
        self.assertEqual(self.run.call_args.args[0], ['systemctl', 'start', 'fixture-matrix.service'])
        with self.assertRaises(Refused):
            self.act()
        self.assertEqual(self.run.call_count, 2)
        inspected = self.act(inspect=True)
        self.assertEqual(inspected['operation_id'], receipt['operation_id'])
        self.assertEqual(inspected['state'], 'existing_resolution_verified')
        self.assertEqual(self.run.call_count, 2)

    def test_intentional_stop_or_active_service_not_started(self):
        for state in ('inactive', 'active', 'activating'):
            self.run.reset_mock(side_effect=True)
            self.run.return_value = subprocess.CompletedProcess([], 0,
                'LoadState=loaded\nActiveState=' + state + '\nResult=success\n')
            with self.assertRaises(Refused):
                self.act()
            self.assertEqual(self.run.call_count, 1)
        self.check.assert_not_called()

    def test_pending_acceptance_recovers_by_inspection_without_start(self):
        self.client.call.side_effect = Unavailable()
        with self.assertRaises(Unavailable):
            self.act()
        self.client.call.side_effect = lambda method, path, value: value
        result = self.act(inspect=True)
        self.assertEqual(result['resolution_revision'], 2)
        self.assertEqual(self.run.call_count, 2)
        self.assertEqual(self.monitor.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 0)

    def test_start_timeout_retains_claim_no_replay(self):
        self.run.side_effect = [subprocess.CompletedProcess([], 0,
            'LoadState=loaded\nActiveState=failed\nResult=signal\n'), subprocess.TimeoutExpired('start', 3)]
        with self.assertRaises(subprocess.TimeoutExpired):
            self.act()
        with self.assertRaises(Refused):
            self.act()
        self.assertEqual(self.run.call_count, 2)
        self.check.return_value = 'matrix_reader_unavailable'
        with self.assertRaises(Refused):
            self.act(inspect=True)
        self.assertEqual(self.run.call_count, 2)
        current = json.loads(self.monitor.db.execute('SELECT content FROM incidents').fetchone()[0])
        self.assertEqual(current['processing_state'], 'open')

    def test_reader_failure_cannot_resolve_active_process(self):
        self.check.return_value = 'matrix_reader_unavailable'
        with self.assertRaises(Refused):
            self.act()
        self.assertEqual(json.loads(self.monitor.db.execute('SELECT content FROM incidents').fetchone()[0])['revision'], 1)

    def test_hold_binding_and_stale_input_refuse_before_systemd(self):
        for field, value in [('unit', 'other.service'), ('enabled', False)]:
            saved = self.selection[field]
            self.selection[field] = value
            with self.assertRaises(Refused):
                self.act()
            self.selection[field] = saved
        Path(self.selection['hold_file']).touch()
        with self.assertRaises(Refused):
            self.act()
        Path(self.selection['hold_file']).unlink()
        request = json.loads(self.raw)
        request['incident']['revision'] = 7
        self.raw = json.dumps(request).encode()
        with self.assertRaises(Refused):
            self.act()
        self.run.assert_not_called()

    def test_repair_does_not_flush_unrelated_pending_incident(self):
        other = dict(self.target, id='other', resource_ref='other-resource')
        self.monitor.transition(other, 'unit_inactive', 100)
        self.client.call.reset_mock()
        self.act()
        rows = self.monitor.db.execute('SELECT content FROM outbox').fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(json.loads(rows[0][0])['resource_ref'], 'other-resource')
        self.assertEqual(self.client.call.call_count, 1)

    def test_monitor_lock_prevents_concurrent_operator(self):
        with self.assertRaises(BlockingIOError):
            Monitor(self.monitor.config, self.client)


if __name__ == '__main__':
    unittest.main()
