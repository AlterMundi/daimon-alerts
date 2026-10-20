"""Functional recovery is distinct from process liveness and bounded by state."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock
sys.path.insert(0, str(Path(__file__).parents[1] / 'scripts'))
from browser_watchdog import tick


class Watchdog(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = dict(base_url='http://127.0.0.1:9', state_dir=self.tmp.name,
                           browser_unit='fixture-browser.service', daemon_unit='fixture-daemon.service',
                           display_unit='fixture-display.service')
        self.control = Mock(return_value=Mock(returncode=0, stdout='active\n'))

    def run_tick(self, reason, now):
        return tick(self.config, now=now, check=lambda target: reason, control=self.control)

    def test_confirmed_failure_restarts_exact_browser_then_health_resets(self):
        self.assertEqual(self.run_tick('extension_disconnected', 100)['recovery'], 'await_confirmation')
        self.assertEqual(self.run_tick('extension_disconnected', 105)['recovery'], 'await_next_sample')
        self.assertEqual(self.run_tick('extension_unresponsive', 130)['recovery'], 'accepted')
        self.control.assert_any_call('restart', 'fixture-browser.service')
        self.assertEqual(self.run_tick('healthy', 160)['recovery'], 'none')
        state = json.loads((Path(self.tmp.name) / 'recovery.json').read_text())
        self.assertEqual(state['failures'], 0)
        self.assertEqual(len(state['attempts']), 1)

    def test_manual_stop_failed_units_and_unknown_observations_do_not_restart(self):
        self.control.return_value.stdout = 'inactive\n'
        self.assertEqual(self.run_tick('extension_disconnected', 100)['state'], 'units_not_active')
        self.control.return_value.stdout = 'active\n'
        self.run_tick('observation_unreadable', 130)
        self.assertFalse(any(c.args[0] == 'restart' for c in self.control.call_args_list))

    def test_restart_intent_survives_timeout_and_limits_recovery_budget(self):
        def control(verb, unit):
            if verb == 'restart':
                state = json.loads((Path(self.tmp.name) / 'recovery.json').read_text())
                self.assertEqual(state['attempts'][-1]['outcome'], 'unknown')
                raise subprocess.TimeoutExpired('fictional', 35)
            return Mock(returncode=0, stdout='active\n')
        self.control.side_effect = control
        self.run_tick('daemon_unavailable', 100)
        self.assertEqual(self.run_tick('daemon_unavailable', 130)['recovery'], 'unknown')
        self.assertEqual(self.run_tick('daemon_unavailable', 160)['recovery'], 'rate_limited')
        self.run_tick('daemon_unavailable', 430)
        self.run_tick('daemon_unavailable', 730)
        self.assertEqual(self.run_tick('daemon_unavailable', 1030)['recovery'], 'rate_limited')
        self.control.assert_any_call('restart', 'fixture-daemon.service')

    @unittest.skipUnless(os.environ.get('ALERTS_TEST_USER_SYSTEMD') == '1', 'explicit receiving systemd qualification')
    def test_real_systemd_restarts_only_the_selected_fixture(self):
        unit = 'alerts-watchdog-fixture-' + str(os.getpid()) + '.service'
        self.config.update(browser_unit=unit, daemon_unit=unit, display_unit=unit)
        subprocess.run(['systemd-run', '--user', '--unit=' + unit, '/usr/bin/sleep', '120'], check=True, capture_output=True)
        try:
            def pid():
                return subprocess.check_output(['systemctl', '--user', 'show', unit, '--value', '-p', 'MainPID'], text=True).strip()
            before = pid()
            self.assertNotEqual(before, '0')
            from browser_watchdog import systemctl
            tick(self.config, now=100, check=lambda t: 'extension_disconnected', control=systemctl)
            result = tick(self.config, now=130, check=lambda t: 'extension_disconnected', control=systemctl)
            self.assertEqual(result['recovery'], 'accepted')
            self.assertNotEqual(before, pid())
            def stop_during_probe(target):
                subprocess.run(['systemctl', '--user', 'stop', unit], check=True, capture_output=True)
                return 'extension_disconnected'
            # The operator stops after active-state inspection but before repair.
            tick(self.config, now=500, check=stop_during_probe, control=systemctl)
            self.assertEqual(pid(), '0')
            self.assertEqual(tick(self.config, now=530, check=lambda t: 'extension_disconnected', control=systemctl)['recovery'], 'none')
        finally:
            subprocess.run(['systemctl', '--user', 'stop', unit], capture_output=True)


if __name__ == '__main__':
    unittest.main()
