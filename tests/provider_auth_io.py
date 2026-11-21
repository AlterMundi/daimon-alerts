"""Provider availability must not be inferred from process or public health."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
import children_provider_health as children
from service_monitor import Monitor, probe


class ProviderHealth(unittest.TestCase):
    def test_all_four_required_and_failure_is_closed(self):
        self.assertEqual(children.aggregate(['healthy'] * 4), 'healthy')
        self.assertEqual(children.aggregate(['healthy'] * 3), 'provider_unavailable')
        self.assertEqual(children.aggregate(['healthy'] * 3 + ['provider_login_required']),
                         'provider_login_required')
        self.assertEqual(children.aggregate(['healthy'] * 3 + ['provider_unavailable']),
                         'provider_unavailable')

    def test_cache_bounds_provider_reads_and_fresh_recovery_rechecks(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / 'state'
            with patch.object(children, 'check', return_value='provider_login_required') as calls:
                self.assertEqual(children.observe(directory), 'provider_login_required')
                self.assertEqual(children.observe(directory), 'provider_login_required')
                self.assertEqual(calls.call_count, 4)
            with patch.object(children, 'check', return_value='healthy') as calls:
                self.assertEqual(children.observe(directory, fresh=True), 'healthy')
                self.assertEqual(calls.call_count, 4)
            value = json.loads((directory / 'current.json').read_text())
            self.assertEqual(set(value), {'checked_at', 'states', 'model_inputs'})
            self.assertEqual(value['model_inputs'], 0)

    def test_probe_discards_private_or_unexpected_payload(self):
        target = dict(kind='provider_auth', command=['/operator/adapter'])
        for value, expected in [
            ({'schema':'dm.monitor.provider-auth/v1','state':'healthy'}, 'healthy'),
            ({'schema':'dm.monitor.provider-auth/v1','state':'provider_login_required'}, 'provider_login_required'),
            ({'schema':'dm.monitor.provider-auth/v1','state':'healthy','private':'discard'}, 'observation_unreadable'),
            ({'schema':'wrong','state':'healthy'}, 'observation_unreadable')]:
            with patch('service_monitor.subprocess.run', return_value=Mock(returncode=0, stdout=json.dumps(value))):
                self.assertEqual(probe(target), expected)

    def test_same_incident_requires_authenticated_recovery(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)
            target = dict(id='children-provider-auth', resource_ref='children-host',
                          kind='provider_auth', command=['/operator/adapter'])
            config = dict(producer_id='fictional', observer_id='fictional-observer',
                          receiving_scope='fictional-human', base_url='http://127.0.0.1:9',
                          token_file=str(path/'token'), state_dir=str(path/'state'), targets=[target])
            monitor = Monitor(config, Mock())
            try:
                monitor.transition(target, 'provider_login_required', 100)
                before = json.loads(monitor.db.execute('SELECT content FROM incidents').fetchone()[0])
                monitor.transition(target, 'provider_unavailable', 101)
                unavailable = json.loads(monitor.db.execute('SELECT content FROM incidents').fetchone()[0])
                self.assertEqual(unavailable['processing_state'], 'open')
                monitor.transition(target, 'healthy', 102)
                after = json.loads(monitor.db.execute('SELECT content FROM incidents').fetchone()[0])
                self.assertEqual(after['incident_id'], before['incident_id'])
                self.assertGreater(after['revision'], before['revision'])
                self.assertEqual(after['processing_state'], 'resolved')
            finally:
                monitor.close()


if __name__ == '__main__':
    unittest.main()
