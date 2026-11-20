#!/usr/bin/env python3
"""Finite owner-selected Matrix recovery; no model, inbox, queue or scheduler."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sqlite3
import sys
import time

from client import Unavailable, private_text
from service_monitor import Monitor, probe


class Refused(ValueError):
    pass


def recover(monitor, selection, raw, inspect=False, run=subprocess.run, check=probe):
    if len(raw) > 2 * 1024 * 1024:
        raise Refused('input bound exceeded')
    request = json.loads(raw)
    incident = request['incident']
    if (request.get('schema') != 'daimon-alerts-repair-input/v1'
            or type(incident.get('revision')) is not int or incident['revision'] < 1):
        raise Refused('invalid incident selection')
    targets = [t for t in monitor.config['targets'] if t['id'] == selection['target_id']]
    if len(targets) != 1:
        raise Refused('target unavailable')
    target = targets[0]
    # Only the explicitly configured model-free Matrix service is eligible.
    # A native harness target or an arbitrary requested command cannot enter here.
    if (target['kind'] != 'unit' or 'receiving_command' not in target
            or target['unit'] != selection['unit']
            or target.get('scope', 'user') != selection['scope']
            or selection.get('enabled') is not True):
        raise Refused('repair selection mismatch')
    hold = Path(selection['hold_file'])
    if not hold.is_absolute() or hold.exists() or hold.is_symlink():
        raise Refused('maintenance hold or invalid hold path')
    row = monitor.db.execute('SELECT content FROM incidents WHERE id=?', (target['id'],)).fetchone()
    if not row:
        raise Refused('incident unavailable')
    current = json.loads(row[0])
    if (current['incident_id'] != incident['id']
            or current['resource_ref'] != incident['resource_id']
            or current['resource_ref'] != target['resource_ref']):
        raise Refused('incident binding mismatch')
    digest = hashlib.sha256(raw).hexdigest()
    operation = 'repair-' + digest
    monitor.db.execute('CREATE TABLE IF NOT EXISTS owner_repairs(target TEXT PRIMARY KEY, content TEXT)')
    row = monitor.db.execute('SELECT content FROM owner_repairs WHERE target=?', (target['id'],)).fetchone()
    prior = json.loads(row[0]) if row else None
    if inspect:
        if not prior or prior['source_sha256'] != digest:
            raise Refused('original invocation unavailable')
    else:
        if prior and prior['incident_id'] == incident['id']:
            raise Refused('already attempted; inspect original input')
        if current['revision'] != incident['revision'] or current['processing_state'] != 'open':
            raise Refused('incident changed')
        if current['summary'] != target['id'] + ': unit inactive.':
            raise Refused('fault requires a different owner repair')
        command = ['systemctl'] + (['--user'] if selection['scope'] == 'user' else [])
        observed = run(command + ['show', target['unit'],
            '--property=LoadState,ActiveState,Result'], capture_output=True, text=True, timeout=3)
        if observed.returncode or len(observed.stdout) > 4096:
            raise Refused('unit observation unavailable')
        state = dict(line.split('=', 1) for line in observed.stdout.splitlines() if '=' in line)
        if (state.get('LoadState') != 'loaded' or state.get('ActiveState') != 'failed'
                or state.get('Result') not in {'exit-code', 'signal', 'core-dump', 'timeout', 'watchdog', 'oom-kill'}):
            raise Refused('not an eligible failed service')
        # Commit before invoking systemd. A crash/lost reply never permits replay.
        # One current receipt per configured target, not an accumulating queue.
        prior = dict(incident_id=incident['id'], source_sha256=digest,
                     operation_id=operation, state='claimed', claimed_at=int(time.time()))
        with monitor.db:
            monitor.db.execute('INSERT OR REPLACE INTO owner_repairs VALUES(?,?)',
                               (target['id'], json.dumps(prior, sort_keys=True)))
        if hold.exists() or hold.is_symlink():
            raise Refused('maintenance hold')
        started = run(command + ['start', target['unit']], capture_output=True, text=True, timeout=3)
        if started.returncode:
            raise Refused('service recovery unconfirmed; inspect original input')
    # The real authenticated reader probe decides recovery, not process startup.
    if check(target) != 'healthy':
        raise Refused('authenticated receiving unavailable')
    if current['processing_state'] == 'open':
        monitor.transition(target, 'healthy', int(time.time()))
    monitor.flush(incident_id=incident['id'])
    report = monitor.heartbeat_report()
    resolved = next((v for v in report['incidents'] if v['incident_id'] == incident['id']), None)
    if (not resolved or resolved['processing_state'] != 'resolved'
            or resolved['revision'] <= incident['revision'] or resolved['acceptance'] != 'accepted'):
        raise Refused('resolution acceptance unconfirmed')
    return dict(schema='daimon-alerts-owner-repair/v1', source_sha256=digest,
                state='existing_resolution_verified' if inspect else 'resolved_verified',
                incident_id=incident['id'], incident_revision=incident['revision'],
                resource_id=incident['resource_id'], resolution_state='resolved',
                resolution_revision=resolved['revision'], operation_id=prior['operation_id'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--selection', required=True)
    parser.add_argument('--inspect-stdin', action='store_true')
    args = parser.parse_args()
    monitor = None
    try:
        config = json.loads(private_text(args.config))
        selection = json.loads(private_text(args.selection))
        # Never initialize a replacement incident store when recovery data is absent.
        database = Path(config['state_dir']) / 'projection.sqlite'
        if not database.is_file() or database.is_symlink():
            raise Refused('existing incident journal required')
        monitor = Monitor(config)
        result = recover(monitor, selection, sys.stdin.buffer.read(2 * 1024 * 1024 + 1),
                         inspect=args.inspect_stdin)
        print(json.dumps(result, sort_keys=True))
        return 0
    except Refused as error:
        print(json.dumps(dict(state="unconfirmed", reason=str(error))))
        return 2
    except (OSError, ValueError, KeyError, TypeError, AttributeError, StopIteration,
            sqlite3.Error, Unavailable, subprocess.TimeoutExpired):
        # No command output, credentials or private configuration in the receipt.
        print('{"state":"unconfirmed","reason":"inspect_original_owner_operation"}')
        return 2
    finally:
        if monitor is not None:
            monitor.close()


if __name__ == '__main__':
    sys.exit(main())
