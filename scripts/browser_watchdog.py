#!/usr/bin/env python3
"""Explicit, bounded systemd recovery for an operator-selected managed browser."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import time
from service_monitor import local_url, probe


def systemctl(verb, unit):
    # Atomically preserve a manual stop occurring after the health sample.
    action = 'try-restart' if verb == 'restart' else verb
    return subprocess.run(['systemctl', '--user', action, unit], capture_output=True,
                          text=True, timeout=35 if verb == 'restart' else 3)


def save(path, value):
    temporary = path.with_suffix('.new')
    with temporary.open('w') as stream:
        json.dump(value, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def tick(config, *, now=None, check=probe, control=systemctl):
    now = int(time.time()) if now is None else now
    local_url(config['base_url'])
    units = {key: config[key] for key in ('browser_unit', 'daemon_unit', 'display_unit')}
    if any(not re.fullmatch(r'[A-Za-z0-9_.@:-]+\.service', value) for value in units.values()):
        raise ValueError('explicit service units required')
    directory = Path(config['state_dir'])
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = directory.stat()
    if directory.is_symlink() or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('owner-private state required')
    binding = {**units, 'base_url': config['base_url']}
    with (directory / 'watchdog.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        path = directory / 'recovery.json'
        state = json.loads(path.read_text()) if path.exists() else {'binding': binding, 'failures': 0, 'attempts': []}
        if state['binding'] != binding:
            raise ValueError('recovery binding changed; reconcile retained attempts')
        # Do not start manually stopped units or bypass a failed restart limit.
        try:
            active = all(control('is-active', unit).stdout.strip() == 'active' for unit in units.values())
        except (OSError, subprocess.TimeoutExpired):
            active = False
        if not active:
            state['failures'] = 0
            save(path, state)
            return {'state': 'units_not_active', 'recovery': 'none'}
        reason = check({'kind': 'webbridge', 'base_url': config['base_url']})
        eligible = reason in ('daemon_unavailable', 'extension_disconnected', 'extension_unresponsive')
        if not eligible:
            state['failures'] = 0
            save(path, state)
            return {'state': reason, 'recovery': 'none'}
        if now - state.get('last_sample', -100000) < 10:
            return {'state': reason, 'recovery': 'await_next_sample'}
        state['last_sample'] = now
        state['failures'] += 1
        attempts = state['attempts'] = [a for a in state['attempts'] if now - a['at'] < 3600]
        if state['failures'] < 2:
            save(path, state)
            return {'state': reason, 'recovery': 'await_confirmation'}
        if len(attempts) >= 3 or (attempts and now - attempts[-1]['at'] < 300):
            save(path, state)
            return {'state': reason, 'recovery': 'rate_limited'}
        unit = units['daemon_unit' if reason == 'daemon_unavailable' else 'browser_unit']
        attempt = {'at': now, 'unit': unit, 'outcome': 'unknown'}
        attempts.append(attempt)
        # Retain the attempt before the effect. A lost acknowledgment consumes budget.
        save(path, state)
        try:
            result = control('restart', unit)
            attempt['outcome'] = 'accepted' if result.returncode == 0 else 'failed'
        except (OSError, subprocess.TimeoutExpired):
            pass
        save(path, state)
        return {'state': reason, 'recovery': attempt['outcome'], 'unit': unit,
                'note': 'restart acceptance is not functional recovery'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, type=Path)
    args = parser.parse_args()
    info = args.config.stat()
    if args.config.is_symlink() or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('owner-private configuration required')
    os.umask(0o077)
    print(json.dumps(tick(json.loads(args.config.read_text()))))


if __name__ == '__main__':
    main()
