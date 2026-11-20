"""Opt-in controlled systemd + actual Alerts HTTP journey; no production outage."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from client import Client
from fixture_server import configuration
from service_monitor import Monitor


@unittest.skipUnless(os.environ.get('ALERTS_SYSTEMD_FIXTURE') == '1', 'explicit disposable user-service fixture only')
class SystemdRecovery(unittest.TestCase):
    def test_actual_start_acceptance_and_inspection(self):
        unit = 'daimon-alerts-owner-fixture-' + str(os.getpid()) + '.service'
        with tempfile.TemporaryDirectory(prefix='alerts-owner-repair-') as tmp:
            directory = Path(tmp)
            config, tokens = configuration(directory)
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                config['listen'] = '127.0.0.1:' + str(sock.getsockname()[1])
            config_path = directory / 'config.json'
            config_path.write_text(json.dumps(config))
            token_path = directory / 'producer.token'
            token_path.write_text(tokens['producer'])
            token_path.chmod(0o600)
            server = subprocess.Popen([str(ROOT / 'target/debug/daimon-alerts'), 'serve', str(config_path)],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            monitor = None
            try:
                client = Client('http://' + config['listen'], str(token_path))
                deadline = time.monotonic() + 10
                while True:
                    try:
                        client.call('GET', '/v1/status')
                        break
                    except Exception:
                        if time.monotonic() >= deadline or server.poll() is not None:
                            raise
                        time.sleep(.05)
                marker, ready = directory / 'permit', directory / 'ready'
                program = ('from pathlib import Path; import sys; '
                           'sys.exit(1) if not Path(' + repr(str(marker)) + ').exists() else None; '
                           'Path(' + repr(str(ready)) + ').write_text("ready")')
                failed = subprocess.run(['systemd-run', '--user', '--wait', '--unit=' + unit,
                    '--property=RemainAfterExit=yes', '/usr/bin/python3', '-c', program],
                    capture_output=True, timeout=10)
                self.assertNotEqual(failed.returncode, 0)
                reader = ('import json; from pathlib import Path; '
                          'print(json.dumps(dict(schema="dm.monitor.matrix-receiving/v1", '
                          'ready=Path(' + repr(str(ready)) + ').exists())))')
                target = dict(id='matrix-fixture', resource_ref='fixture-resource', kind='unit',
                              scope='user', unit=unit, receiving_command=['/usr/bin/python3', '-c', reader])
                cfg = dict(producer_id='fictional-hmk', observer_id='fixture-observer',
                           receiving_scope='fictional-human', base_url='http://' + config['listen'],
                           token_file=str(token_path), state_dir=str(directory / 'producer'), targets=[target])
                monitor = Monitor(cfg)
                monitor.transition(target, 'unit_inactive', int(time.time()))
                monitor.flush()
                original = json.loads(monitor.db.execute('SELECT content FROM incidents').fetchone()[0])
                monitor.close()
                monitor = None
                raw = json.dumps(dict(schema='daimon-alerts-repair-input/v1', incident=dict(
                    id=original['incident_id'], revision=1, resource_id=target['resource_ref']))).encode()
                selected = dict(enabled=True, target_id=target['id'], unit=unit, scope='user',
                                hold_file=str(directory / 'maintenance-hold'))
                producer_path, selected_path = directory / 'producer.json', directory / 'selection.json'
                for path, value in ((producer_path, cfg), (selected_path, selected)):
                    path.write_text(json.dumps(value))
                    path.chmod(0o600)
                marker.touch()
                command = [sys.executable, str(ROOT / 'scripts/owner_repair.py'),
                           '--config', str(producer_path), '--selection', str(selected_path)]
                result = subprocess.run(command, input=raw, capture_output=True, timeout=20)
                # Type=simple may acknowledge start before the reader is ready.
                # Reconcile the original claim; never invoke its start twice.
                if result.returncode == 2:
                    self.assertEqual(json.loads(result.stdout)['reason'], 'authenticated receiving unavailable')
                    deadline = time.monotonic() + 3
                    while not ready.exists() and time.monotonic() < deadline:
                        time.sleep(.02)
                    result = subprocess.run(command + ['--inspect-stdin'], input=raw,
                                            capture_output=True, timeout=20)
                self.assertEqual(result.returncode, 0, result.stdout.decode())
                receipt = json.loads(result.stdout)
                self.assertEqual(receipt['resolution_revision'], 2)
                live = client.call('GET', '/v1/incidents/fictional-hmk/' + original['incident_id'])
                self.assertEqual(live['projection']['processing_state'], 'resolved')
                self.assertEqual(live['projection']['revision'], 2)
                self.assertTrue(ready.is_file())
                inspected = subprocess.run(command + ['--inspect-stdin'], input=raw,
                                           capture_output=True, timeout=20)
                self.assertEqual(inspected.returncode, 0, inspected.stdout.decode())
                self.assertEqual(json.loads(inspected.stdout)['operation_id'], receipt['operation_id'])
                print(json.dumps(dict(fixture='actual-systemd-and-alerts-http', receipt=receipt,
                                      production_service_changes=0, native_models=0), sort_keys=True))
            finally:
                if monitor is not None:
                    monitor.close()
                subprocess.run(['systemctl', '--user', 'stop', unit], capture_output=True, timeout=5)
                subprocess.run(['systemctl', '--user', 'reset-failed', unit], capture_output=True, timeout=5)
                server.terminate()
                server.wait(timeout=5)


if __name__ == '__main__':
    unittest.main()
