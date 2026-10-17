#!/usr/bin/env python3
"""Finite statvfs probe and durable Alerts producer; never cleans or runs models."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import time

from client import Client, Unavailable, private_text

GIB = 1024 ** 3


def sample(target):
    path = Path(target['path'])
    # A missing data mount must not silently turn into a healthy root probe.
    if not path.is_mount():
        raise OSError('selected filesystem is not mounted')
    v = os.statvfs(path)
    if v.f_blocks <= 0 or v.f_frsize <= 0:
        raise OSError('filesystem capacity unavailable')
    used = v.f_blocks - v.f_bfree
    available = max(0, v.f_bavail)
    return {'used_percent': 100 * used / max(1, used + available),
            'available_gib': available * v.f_frsize / GIB,
            'inode_percent': (100 * (v.f_files - v.f_ffree) /
                              max(1, v.f_files - v.f_ffree + v.f_favail))
                              if v.f_files else None}


def pressure(metrics, policy, previous='healthy'):
    used, free, inode = (metrics[k] for k in ('used_percent', 'available_gib', 'inode_percent'))
    inode = inode or 0
    def reached(level, margin=0):
        return (used >= policy[level + '_percent'] - margin or
                inode >= policy[level + '_inode_percent'] - margin or
                free <= policy[level + '_free_gib'] + (policy['recovery_gib'] if margin else 0))
    if reached('critical') or (previous == 'critical' and reached('critical', policy['recovery_percent'])):
        return 'critical'
    if reached('warning') or (previous in ('critical', 'warning') and reached('warning', policy['recovery_percent'])):
        return 'warning'
    return 'healthy'


class Monitor:
    def __init__(self, config, client=None):
        self.config = config
        targets = config['targets']
        if not 1 <= len(targets) <= 16:
            raise ValueError('bounded filesystem selection required')
        def identifier(value):
            if not isinstance(value, str) or not re.fullmatch('[A-Za-z0-9_-]{1,128}', value):
                raise ValueError('opaque identifier required')
        for key in ('producer_id', 'observer_id', 'receiving_scope'):
            identifier(config[key])
        seen = set()
        for target in targets:
            identifier(target['id']); identifier(target['resource_ref'])
            if len(target['id']) > 70:
                raise ValueError('target ID exceeds derived incident ID bound')
            if target['id'] in seen or not Path(target['path']).is_absolute():
                raise ValueError('unique target and absolute mount required')
            seen.add(target['id'])
        p = config['thresholds']
        for suffix in ('percent', 'inode_percent'):
            if not 0 < p['warning_' + suffix] < p['critical_' + suffix] <= 100:
                raise ValueError('ordered percentage thresholds required')
        if not 0 <= p['critical_free_gib'] < p['warning_free_gib'] or not 0 < p['recovery_percent'] < 10 or not 0 < p['recovery_gib'] < 20:
            raise ValueError('bounded free-space and hysteresis thresholds required')
        state = Path(config['state_dir'])
        state.mkdir(parents=True, exist_ok=True, mode=0o700)
        if state.is_symlink() or state.stat().st_uid != os.getuid() or state.stat().st_mode & 0o077:
            raise ValueError('owner-private state directory required')
        self.lock = open(state / 'monitor.lock', 'a+b')
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.db = sqlite3.connect(state / 'projection.sqlite')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS binding(value TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS incidents(key TEXT PRIMARY KEY, content TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS outbox(id INTEGER PRIMARY KEY, content TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS status(value TEXT)')
        selected = {k:config[k] for k in ('targets','thresholds','producer_id','observer_id','receiving_scope','base_url','token_file')}
        binding = hashlib.sha256(json.dumps(selected, sort_keys=True).encode()).hexdigest()
        prior = self.db.execute('SELECT value FROM binding').fetchone()
        if prior and prior[0] != binding:
            raise ValueError('monitor binding changed; retain journal and reconcile explicitly')
        with self.db:
            if not prior: self.db.execute('INSERT INTO binding VALUES(?)', (binding,))
        self.client = client or Client(config['base_url'], config['token_file'])

    def close(self):
        self.db.close(); self.lock.close()

    def flush(self):
        for seq, raw in self.db.execute('SELECT id,content FROM outbox ORDER BY id').fetchall():
            value = json.loads(raw)
            result = self.client.call('PUT', '/v1/incidents/' + value['producer_id'] + '/' + value['incident_id'], value)
            if any(result.get(k) != value[k] for k in ('producer_id','incident_id','revision')):
                raise Unavailable('acceptance mismatch')
            with self.db: self.db.execute('DELETE FROM outbox WHERE id=?', (seq,))

    def transition(self, key, target, level, metrics, now):
        row = self.db.execute('SELECT content FROM incidents WHERE key=?', (key,)).fetchone()
        prior = json.loads(row[0]) if row else None
        active = level != 'healthy'
        if (not active and (not prior or prior['processing_state'] == 'resolved')):
            return
        if prior and prior['processing_state'] == 'open' and active and prior['severity'] == level:
            return
        summary = (f"{target['id']}: filesystem observation unavailable." if metrics is None else
                   f"{target['id']}: {level}; {metrics['used_percent']:.1f}% used, "
                   f"{metrics['available_gib']:.1f} GiB available; inodes " +
                   (f"{metrics['inode_percent']:.1f}%." if metrics['inode_percent'] is not None else 'unreported.'))
        if prior and prior['processing_state'] == 'open':
            value = dict(prior, revision=prior['revision'] + 1, updated_at=max(now, prior['updated_at']),
                         processing_state='open' if active else 'resolved', summary=summary,
                         severity=level if active else 'info')
        else:
            value = dict(schema='daimon-alerts/v1', producer_id=self.config['producer_id'],
                incident_id=key+'-'+secrets.token_hex(12), operation_id='storage-'+key,
                revision=1, receiving_scope=self.config['receiving_scope'], phase='storage',
                resource_ref=target['resource_ref'], severity=level,
                problem_code='observation_unreadable' if metrics is None else 'storage_pressure',
                summary=summary, action='Inspect the selected filesystem and retained work; free or relocate verified disposable data. No automatic deletion is performed.',
                source_at=now, updated_at=now, processing_state='open', work_preserved=True)
        raw = json.dumps(value, sort_keys=True)
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO incidents VALUES(?,?)', (key, raw))
            self.db.execute('INSERT INTO outbox(content) VALUES(?)', (raw,))

    def tick(self, probe=sample):
        now = int(time.time())
        try:
            self.flush()
            available = True
        except Unavailable:
            available = False
        rows = []
        for target in self.config['targets']:
            key = target['id']
            row = self.db.execute('SELECT content FROM incidents WHERE key=?', (key,)).fetchone()
            prior = json.loads(row[0]) if row else {}
            try:
                metrics = probe(target)
                previous = prior.get('severity', 'healthy') if prior.get('processing_state') == 'open' else 'healthy'
                level = pressure(metrics, self.config['thresholds'], previous)
            except (OSError, ValueError, ZeroDivisionError):
                metrics, level = None, 'error'
            rows.append(dict(id=key, level=level, metrics=metrics))
            # Freeze uncertain projections. Bound the outbox; flush before the next observation.
            if available:
                if metrics is not None:
                    self.transition(key+'-probe', target, 'healthy', metrics, now)
                    self.transition(key, target, level, metrics, now)
                else:
                    # An unreadable filesystem never resolves retained pressure.
                    self.transition(key+'-probe', target, 'error', None, now)
        if available:
            try:
                self.flush()
                self.client.call('PUT', '/v1/observers/'+self.config['observer_id'],
                    dict(receiving_scope=self.config['receiving_scope'], source_at=now,
                         source_state='healthy' if all(r['metrics'] is not None for r in rows) else 'observation_unreadable'))
            except Unavailable:
                available = False
        status = dict(checked_at=now, service_state='available' if available else 'unavailable', targets=rows)
        with self.db:
            self.db.execute('DELETE FROM status')
            self.db.execute('INSERT INTO status VALUES(?)', (json.dumps(status),))
        return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    monitor = Monitor(json.loads(private_text(args.config)))
    try:
        status = monitor.tick()
        print(json.dumps(status))
        return 0 if status['service_state'] == 'available' and all(r['metrics'] is not None for r in status['targets']) else 1
    finally:
        monitor.close()


if __name__ == '__main__':
    raise SystemExit(main())
