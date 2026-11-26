#!/usr/bin/env python3
"""Read owner-local DMK lifecycle status through the existing fleet transport."""
import argparse
import json
from pathlib import Path
import subprocess
import sys


CHECK = r"""
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path

def properties(unit, *keys):
    result = subprocess.run(
        ['systemctl', *(['--user'] if sys.argv[1] == 'user' else []), 'show',
         unit, '-p', ','.join(keys)], capture_output=True, text=True,
        timeout=2, check=True)
    if len(result.stdout) > 16384:
        raise ValueError('unit properties too large')
    return dict(row.split('=', 1) for row in result.stdout.splitlines() if '=' in row)

def read(path):
    with Path(path).open('rb') as stream:
        raw = stream.read(65537)
    if len(raw) > 65536:
        raise ValueError('receipt too large')
    return json.loads(raw)

def classify():
    timer = properties('hmk-memory-worker.timer', 'LoadState', 'ActiveState', 'Triggers')
    if timer.get('LoadState') == 'not-found' or timer.get('ActiveState') != 'active':
        return 'dmk_review_not_scheduled'
    if 'hmk-memory-worker.service' not in timer.get('Triggers', '').split():
        return 'dmk_review_not_scheduled'
    job = properties('hmk-memory-worker.service', 'LoadState', 'ExecStart')
    if job.get('LoadState') != 'loaded':
        return 'dmk_review_not_scheduled'
    argv = shlex.split(job['ExecStart'].split('argv[]=', 1)[1].split(' ;', 1)[0])
    profile = read(argv[argv.index('--profile') + 1])
    try:
        status = read(Path(profile['state_dir']) / 'worker-status.json')
    except FileNotFoundError:
        return 'dmk_review_unconfirmed'
    stamp = status['updated_ns']
    if status['schema'] != 'hmk.worker-status/v1' or type(stamp) is not int:
        raise ValueError('unexpected worker receipt')
    if not -60 <= time.time() - stamp / 1e9 <= 90000:
        return 'dmk_review_unconfirmed'
    phases = [status['phases'].get(name, {}) for name in ('capture', 'organization', 'index')]
    backup = status.get('backup', {}).get('state')
    sleep_selected = 'sleep_triage' in profile
    sleep = status.get('sleep_triage', {}) if sleep_selected else {}
    if any(
        phase.get('state') in ('pending', 'deferred', 'blocked')
        and phase.get('reason_code') == 'dispatch_unresolved'
        for phase in [*phases, sleep]
    ):
        return 'dmk_review_dispatch_unresolved'
    if any(
        phase.get('state') in ('pending', 'deferred')
        and phase.get('reason_code') == 'source_context_budget_exceeded'
        for phase in phases
    ):
        return 'dmk_review_source_context_limited'
    stalled = any(
        phase.get('state') in ('pending', 'deferred')
        and phase.get('reason_code') not in ('navigation_budget_exhausted', 'index_budget_exhausted')
        for phase in phases
    )
    if stalled or backup == 'failed':
        return 'dmk_review_incomplete'
    if sleep_selected:
        if sleep.get('state') == 'blocked':
            return 'dmk_review_sleep_blocked'
        if sleep.get('state') != 'caught_up':
            return 'dmk_review_unconfirmed'
    if status.get('state') == 'idle' and all(
            phase.get('state') in ('idle', 'complete') for phase in phases
    ) and backup == 'current':
        return 'healthy'
    return 'dmk_review_unconfirmed'

try:
    state = classify()
except (KeyError, IndexError, TypeError, ValueError, OSError, subprocess.SubprocessError):
    state = 'observation_unreadable'
print(json.dumps({'schema': 'dm.monitor.dmk-lifecycle/v1', 'state': state}))
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--transport-module', type=Path, required=True)
    parser.add_argument('--inventory', type=Path, required=True)
    parser.add_argument('--target', required=True)
    parser.add_argument('--scope', choices=('system', 'user'), required=True)
    args = parser.parse_args()
    # The observer chooses the same installed transport and inventory as SysOps.
    sys.path.insert(0, str(args.transport_module))
    from preventive_sysops import target_command, validate

    config = json.loads(args.inventory.read_text())
    validate(config)
    target, = [t for t in config['targets'] if t['id'] == args.target]
    process = subprocess.run(
        target_command(target, ['/usr/bin/python3', '-I', '-B', '-', args.scope]),
        input=CHECK, capture_output=True, text=True, timeout=8, check=True,
    )
    if len(process.stdout) > 4096:
        raise ValueError('unexpected receipt size')
    print(process.stdout.strip())


if __name__ == '__main__':
    main()
