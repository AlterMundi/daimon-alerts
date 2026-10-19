#!/usr/bin/env python3
"""Finite critical-service observer. No models, page navigation or automatic repair."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import subprocess
import time
import urllib.parse
import urllib.request

from client import Client, NoRedirect, Unavailable, private_text


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch('[A-Za-z0-9_-]{1,70}', value):
        raise ValueError('bounded opaque identifier required')


def local_url(value):
    p = urllib.parse.urlsplit(value)
    if (p.scheme != 'http' or p.hostname not in ('localhost', '127.0.0.1', '::1')
            or p.username or p.password or p.query or p.fragment or p.path not in ('', '/')):
        raise ValueError('explicit loopback base required')


def read_json(url, payload=None):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    request = urllib.request.Request(url, data=json.dumps(payload).encode() if payload is not None else None,
                                     headers={'Content-Type': 'application/json'})
    with opener.open(request, timeout=3) as response:
        raw = response.read(65537)
    if len(raw) > 65536:
        raise ValueError('bounded health response required')
    return json.loads(raw)


def probe(target):
    """Return only a closed health code; never expose response/tab/process data."""
    if target['kind'] == 'unit':
        command = ['systemctl', '--user', 'show', target['unit'],
                   '--property=LoadState,ActiveState']
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=3)
            props = dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
            return 'healthy' if (result.returncode == 0 and props.get('LoadState') == 'loaded'
                                 and props.get('ActiveState') == 'active') else 'unit_inactive'
        except (OSError, subprocess.TimeoutExpired):
            return 'observation_unreadable'
    base = target['base_url'].rstrip('/')
    try:
        value = read_json(base + '/status')
        if not isinstance(value, dict) or type(value.get('running')) is not bool:
            return 'observation_unreadable'
        if not value['running']:
            return 'daemon_unavailable'
        if type(value.get('extension_connected')) is not bool:
            return 'observation_unreadable'
        if not value['extension_connected']:
            return 'extension_disconnected'
    except OSError:
        return 'daemon_unavailable'
    except (ValueError, TypeError):
        return 'observation_unreadable'
    try:
        value = read_json(base + '/command', {'action': 'list_tabs', 'args': {},
                                            'session': 'critical-service-health'})
        # Discard the entire payload after checking success; no URLs are retained.
        if not isinstance(value, dict) or value.get('ok') is not True:
            return 'extension_unresponsive'
        data = value.get('data')
        return 'healthy' if isinstance(data, dict) and data.get('success') is True else 'extension_unresponsive'
    except (OSError, ValueError, TypeError):
        return 'extension_unresponsive'


REASONS = {'healthy', 'unit_inactive', 'observation_unreadable', 'daemon_unavailable',
           'extension_disconnected', 'extension_unresponsive'}


class Monitor:
    def __init__(self, config, client=None):
        for k in ('producer_id', 'observer_id', 'receiving_scope'):
            identifier(config[k])
        targets = config['targets']
        if not 1 <= len(targets) <= 16 or len({t['id'] for t in targets}) != len(targets):
            raise ValueError('bounded unique target selection required')
        for target in targets:
            identifier(target['id']); identifier(target['resource_ref'])
            if target['kind'] == 'webbridge':
                local_url(target['base_url'])
            elif target['kind'] == 'unit':
                if not re.fullmatch('[A-Za-z0-9_.@:-]+\\.service', target['unit']):
                    raise ValueError('explicit service unit required')
            else:
                raise ValueError('unsupported health probe')
        self.config = config
        directory = Path(config['state_dir'])
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        info = directory.stat()
        if directory.is_symlink() or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError('owner-private state required')
        self.lock = (directory / 'monitor.lock').open('a+b')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            self.lock.close()
            raise
        self.db = sqlite3.connect(directory / 'projection.sqlite')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('CREATE TABLE IF NOT EXISTS binding(value TEXT);'
                             'CREATE TABLE IF NOT EXISTS incidents(id TEXT PRIMARY KEY, content TEXT);'
                             'CREATE TABLE IF NOT EXISTS outbox(seq INTEGER PRIMARY KEY, content TEXT);'
                             'CREATE TABLE IF NOT EXISTS status(value TEXT);')
        selection = {k: config[k] for k in ('producer_id', 'observer_id', 'receiving_scope', 'targets', 'base_url', 'token_file')}
        binding = hashlib.sha256(json.dumps(selection, sort_keys=True).encode()).hexdigest()
        prior = self.db.execute('SELECT value FROM binding').fetchone()
        if prior and prior[0] != binding:
            self.db.close(); self.lock.close()
            raise ValueError('changed observer binding requires explicit reconciliation')
        with self.db:
            if not prior:
                self.db.execute('INSERT INTO binding VALUES(?)', (binding,))
        self.client = client or Client(config['base_url'], config['token_file'])

    def close(self):
        self.db.close(); self.lock.close()

    def flush(self):
        for seq, raw in self.db.execute('SELECT seq,content FROM outbox ORDER BY seq').fetchall():
            value = json.loads(raw)
            result = self.client.call('PUT', '/v1/incidents/' + value['producer_id'] + '/' + value['incident_id'], value)
            if any(result.get(k) != value[k] for k in ('producer_id', 'incident_id', 'revision')):
                raise Unavailable('acceptance mismatch')
            with self.db:
                self.db.execute('DELETE FROM outbox WHERE seq=?', (seq,))

    def transition(self, target, reason, now):
        identifier(target['id'])
        if reason not in REASONS:
            raise ValueError('unsafe probe result')
        prior = self.db.execute('SELECT content FROM incidents WHERE id=?', (target['id'],)).fetchone()
        prior = json.loads(prior[0]) if prior else None
        active = reason != 'healthy'
        if not active and (not prior or prior['processing_state'] == 'resolved'):
            return
        summary = target['id'] + ': ' + reason.replace('_', ' ') + '.'
        if prior and prior['processing_state'] == 'open':
            if active and prior['summary'] == summary:
                return
            value = dict(prior, revision=prior['revision'] + 1, updated_at=max(now, prior['updated_at']),
                         summary=summary, processing_state='open' if active else 'resolved',
                         severity='error' if active else 'info')
        else:
            value = dict(schema='daimon-alerts/v1', producer_id=self.config['producer_id'],
                         receiving_scope=self.config['receiving_scope'], resource_ref=target['resource_ref'],
                         incident_id=target['id'] + '-' + secrets.token_hex(12), operation_id='service-' + target['id'],
                         revision=1, phase='availability', problem_code='service_unavailable',
                         severity='error', processing_state='open', summary=summary,
                         action='Inspect the selected service and browser connection; recover explicitly and verify an actual operation. No models or automatic task retries.',
                         source_at=now, updated_at=now, work_preserved=True)
        raw = json.dumps(value, sort_keys=True)
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO incidents VALUES(?,?)', (target['id'], raw))
            self.db.execute('INSERT INTO outbox(content) VALUES(?)', (raw,))

    def tick(self, check=probe, now=None):
        now = int(time.time()) if now is None else now
        available = True
        try:
            self.flush()
        except Unavailable:
            available = False
        rows = []
        for target in self.config['targets']:
            try:
                reason = check(target)
                if reason not in REASONS:
                    reason = 'observation_unreadable'
            except Exception:
                reason = 'observation_unreadable'
            rows.append(dict(id=target['id'], state=reason))
            # Do not mutate uncertain revisions while acceptance is pending.
            if available:
                # Unknown observation never resolves a retained outage.
                self.transition(target, reason, now)
        if available:
            try:
                self.flush()
                self.client.call('PUT', '/v1/observers/' + self.config['observer_id'],
                                 dict(receiving_scope=self.config['receiving_scope'], source_at=now,
                                      source_state='healthy' if all(r['state'] != 'observation_unreadable' for r in rows) else 'observation_unreadable'))
            except Unavailable:
                available = False
        result = dict(checked_at=now, service_state='available' if available else 'unavailable', targets=rows)
        with self.db:
            self.db.execute('DELETE FROM status')
            self.db.execute('INSERT INTO status VALUES(?)', (json.dumps(result),))
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    monitor = None
    try:
        config = json.loads(private_text(args.config))
        monitor = Monitor(config)
        result = monitor.tick()
        print(json.dumps(result))
        return 0 if result['service_state'] == 'available' else 2
    except (OSError, ValueError, KeyError, TypeError):
        print(json.dumps({'service_state': 'unavailable', 'reason': 'configuration_or_observation_unavailable'}))
        return 2
    finally:
        if monitor:
            monitor.close()


if __name__ == '__main__':
    raise SystemExit(main())
