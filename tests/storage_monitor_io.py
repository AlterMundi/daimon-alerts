"""Filesystem thresholds, retained uncertainty and real HTTP incident lifecycle."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'scripts'))
sys.path.insert(0, str(ROOT/'tests'))
from client import Unavailable
from storage_monitor import Monitor, pressure, sample
from http_contract import Contract

POLICY = dict(warning_percent=82, critical_percent=90, warning_free_gib=15,
              critical_free_gib=7, warning_inode_percent=82, critical_inode_percent=90,
              recovery_percent=3, recovery_gib=2)
HEALTHY = dict(used_percent=60, available_gib=40, inode_percent=20)
CRITICAL = dict(used_percent=95, available_gib=4, inode_percent=20)


def config(directory, base='http://127.0.0.1:1'):
    return dict(base_url=base, token_file=str(directory/'token'), state_dir=str(directory/'monitor'),
                producer_id='fictional-hmk', observer_id='fictional-storage-observer',
                receiving_scope='fictional-human', thresholds=POLICY,
                targets=[dict(id='root', path='/', resource_ref='fictional-host')])


class Thresholds(unittest.TestCase):
    def test_bytes_inodes_and_recovery_hysteresis(self):
        self.assertEqual(pressure(HEALTHY, POLICY), 'healthy')
        self.assertEqual(pressure(dict(HEALTHY, available_gib=6), POLICY), 'critical')
        self.assertEqual(pressure(dict(HEALTHY, inode_percent=92), POLICY), 'critical')
        self.assertEqual(pressure(dict(HEALTHY, available_gib=8), POLICY, 'critical'), 'critical')
        self.assertEqual(pressure(dict(HEALTHY, available_gib=10), POLICY, 'critical'), 'warning')
        self.assertEqual(pressure(dict(HEALTHY, available_gib=16), POLICY, 'warning'), 'warning')
        self.assertEqual(pressure(dict(HEALTHY, available_gib=18), POLICY, 'warning'), 'healthy')

    def test_user_available_space_and_missing_mount(self):
        # Reserved blocks are not available to active unprivileged sessions.
        v = Mock(f_blocks=100, f_bfree=10, f_bavail=5, f_frsize=1024**3,
                 f_files=100, f_ffree=50, f_favail=40)
        with patch('storage_monitor.os.statvfs', return_value=v):
            result = sample({'path':'/'})
        self.assertAlmostEqual(result['used_percent'], 100*90/95)
        self.assertEqual(result['available_gib'], 5)
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(OSError): sample({'path':tmp})

    def test_lost_acceptance_exact_replay_restart_and_no_blind_resolution(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            client = Mock()
            sent = []
            fail = True
            def call(method, route, value):
                nonlocal fail
                if '/incidents/' in route:
                    sent.append(json.loads(json.dumps(value)))
                    if fail: raise Unavailable()
                    return {k:value[k] for k in ('producer_id','incident_id','revision')}
                return {}
            client.call.side_effect = call
            m = Monitor(config(directory), client)
            self.assertEqual(m.tick(lambda _:CRITICAL)['service_state'], 'unavailable')
            m.close()
            m = Monitor(config(directory), client)
            self.assertEqual(m.tick(lambda _:HEALTHY)['service_state'], 'unavailable')
            self.assertEqual(sent[0], sent[1])
            self.assertEqual(m.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0], 1)
            fail = False
            m.tick(lambda _:HEALTHY)
            self.assertEqual(sent[0], sent[2])
            self.assertEqual(sent[-1]['processing_state'], 'resolved')
            self.assertEqual(sent[-1]['incident_id'], sent[0]['incident_id'])
            m.close()


class StorageHTTP(unittest.TestCase):
    setUp = Contract.setUp
    start = Contract.start
    tearDown = Contract.tearDown
    call = Contract.call
    pending = Contract.pending

    def test_pressure_escalation_resolution_recurrence_and_probe_failure(self):
        self.process.terminate(); self.process.communicate()
        self.config['actors'][0]['grants'].append('observe')
        self.config['actors'][0]['producers'].append('fictional-storage-observer')
        self.path.write_text(json.dumps(self.config)); self.start()
        token = self.directory/'token'; token.write_text(self.tokens['producer']); token.chmod(0o600)
        m = Monitor(config(self.directory, self.base))
        try:
            m.tick(lambda _:dict(HEALTHY, used_percent=85))
            first = self.call('GET','/v1/incidents')[1]['items'][0]['projection']
            self.assertEqual(first['severity'], 'warning')
            m.tick(lambda _:CRITICAL)
            current = self.call('GET','/v1/incidents')[1]['items'][0]['projection']
            self.assertEqual(current['severity'], 'critical')
            self.assertEqual(current['incident_id'], first['incident_id'])
            self.assertEqual(current['revision'], 2)
            m.tick(lambda _:CRITICAL)
            self.assertEqual(len(self.pending()), 1)
            def missing(_): raise OSError('private path must never leak')
            result = m.tick(missing)
            self.assertIsNone(result['targets'][0]['metrics'])
            records = self.call('GET','/v1/incidents')[1]['items']
            self.assertEqual(len(records), 2)
            self.assertTrue(all(r['projection']['processing_state']=='open' for r in records))
            self.assertNotIn('private path', json.dumps(records))
            m.tick(lambda _:HEALTHY)
            records = self.call('GET','/v1/incidents')[1]['items']
            self.assertTrue(all(r['projection']['processing_state']=='resolved' for r in records))
            m.tick(lambda _:CRITICAL)
            records = self.call('GET','/v1/incidents')[1]['items']
            self.assertEqual(len(records), 3)
            self.assertNotEqual(records[-1]['projection']['incident_id'], first['incident_id'])
        finally:
            m.close()


if __name__ == '__main__': unittest.main()
