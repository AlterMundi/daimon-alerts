#!/usr/bin/env python3
"""Finite critical-service observer. No models, page navigation or automatic repair."""
import argparse
import concurrent.futures
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
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
    if target['kind'] == 'http_health':
        try:
            value = read_json(target['base_url'].rstrip('/') + '/health')
            return 'healthy' if isinstance(value, dict) and value.get('ok') is True else 'health_unavailable'
        except (OSError, ValueError, TypeError):
            return 'health_unavailable'
    if target['kind'] == 'enrollment_registry':
        return target.get('_registry_state', 'observation_unreadable')
    if target['kind'] == 'incus_native':
        # Instance names come only from the operator-owned enrollment registry.
        # No command, home, credential or executable is supplied by its records.
        prefix = [target['incus_binary'], 'exec', target['instance'], '--']
        try:
            try:
                state = subprocess.run(prefix + ['systemctl', 'show',
                    'daimon-onboarding-codex.service', '-p', 'NRestarts', '--value'],
                    capture_output=True, text=True, timeout=15)
                if state.returncode == 0:
                    target['_counters'] = {'restarts': int(state.stdout.strip())}
            except (OSError, ValueError, subprocess.TimeoutExpired):
                pass
            result = subprocess.run(prefix + ['runuser', '-u', 'agent', '--', 'env',
                'HOME=/home/agent', 'CODEX_HOME=/home/agent/.codex',
                '/usr/local/bin/codex', 'app-server', 'daemon', 'version'],
                capture_output=True, text=True, timeout=15)
            if result.returncode:
                return 'native_unresponsive'
            value = json.loads(result.stdout)
            if value.get('status') == 'running' and isinstance(value.get('appServerVersion'), str):
                return 'healthy' if '_counters' in target else 'observation_unreadable'
            return 'native_unresponsive'
        except (OSError, ValueError, TypeError, AttributeError, subprocess.TimeoutExpired):
            return 'native_unresponsive'
    if target['kind'] == 'native_codex':
        try:
            command = ['systemctl'] + (['--user'] if target.get('scope') == 'user' else [])
            try:
                state = subprocess.run(command + ['show', target['unit'], '-p', 'NRestarts', '--value'],
                                       capture_output=True, text=True, timeout=3)
                if state.returncode == 0:
                    target['_counters'] = {'restarts': int(state.stdout.strip())}
            except (OSError,ValueError,subprocess.TimeoutExpired):
                pass
            result = subprocess.run([target['binary'], 'app-server', 'daemon', 'version'],
                                    capture_output=True, text=True, timeout=15)
            if result.returncode:
                return 'native_unresponsive'
            value = json.loads(result.stdout)
            if value.get('status') == 'running' and isinstance(value.get('appServerVersion'), str):
                return 'healthy' if '_counters' in target else 'observation_unreadable'
            return 'native_unresponsive'
        except (OSError, ValueError, TypeError, AttributeError, subprocess.TimeoutExpired):
            return 'native_unresponsive'
    if target['kind'] == 'host_memory':
        try:
            memory = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
            total = int(memory['MemTotal'].split()[0])
            available = int(memory['MemAvailable'].split()[0])
            events = dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
            pressure = dict(line.split(' ', 1) for line in Path('/proc/pressure/memory').read_text().splitlines())
            full = dict(field.split('=') for field in pressure['full'].split())
            stalled = float(full['avg60'])
            killed = int(events['oom_kill'])
            if total <= 0 or not 0 <= available <= total or killed < 0 or not 0 <= stalled <= 100:
                return 'observation_unreadable'
            target['_counters'] = {'oom_kill': killed, 'full_avg60_percent': stalled}
            return 'memory_reclaim_pressure' if stalled >= target['full_avg60_percent'] else 'healthy'
        except (OSError, ValueError, KeyError):
            return 'observation_unreadable'
    if target['kind'] == 'cgroup_memory':
        try:
            root = Path(target['cgroup_root'])
            events = dict(line.split() for line in (root / 'memory.events').read_text().splitlines())
            target['_counters'] = {'oom_kill': int(events['oom_kill'])}
            current = int((root / 'memory.current').read_text())
            maximum = (root / 'memory.max').read_text().strip()
            if current < 0 or maximum != 'max' and int(maximum) <= 0:
                return 'observation_unreadable'
            pressure = dict(line.split(' ', 1) for line in
                            (root / 'memory.pressure').read_text().splitlines())
            full = dict(field.split('=') for field in pressure['full'].split())
            stalled = float(full['avg60'])
            if not 0 <= stalled <= 100:
                return 'observation_unreadable'
            target['_counters']['full_avg60_percent'] = stalled
            return ('memory_reclaim_pressure' if stalled >= target['full_avg60_percent']
                    else 'healthy')
        except (OSError, ValueError, KeyError):
            return 'observation_unreadable'
    if target['kind'] == 'unit':
        command = ['systemctl'] + (['--user'] if target.get('scope', 'user') == 'user' else [])
        command += ['show', target['unit'], '--property=LoadState,ActiveState']
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=3)
            props = dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
            return 'healthy' if (result.returncode == 0 and props.get('LoadState') == 'loaded'
                                 and props.get('ActiveState') == 'active') else 'unit_inactive'
        except (OSError, subprocess.TimeoutExpired):
            return 'observation_unreadable'
    # An explicitly stopped on-demand browser is available to start, not healthy
    # as a live browser. Failed/unknown unit states retain ordinary alerting.
    if target.get('demand_unit'):
        try:
            result = subprocess.run(['systemctl', '--user', 'show', target['demand_unit'],
                                     '--property=LoadState,ActiveState,SubState'],
                                    capture_output=True, text=True, timeout=3)
            props = dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
            if result.returncode or props.get('LoadState') != 'loaded':
                return 'observation_unreadable'
            if props.get('ActiveState') == 'inactive' and props.get('SubState') == 'dead':
                return 'standby'
            if props.get('ActiveState') == 'failed':
                return 'unit_inactive'
            if props.get('ActiveState') != 'active':
                return 'observation_unreadable'
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


REASONS = {'healthy', 'standby', 'unit_inactive', 'health_unavailable', 'observation_unreadable', 'daemon_unavailable',
           'extension_disconnected', 'extension_unresponsive', 'native_unresponsive',
           'memory_reclaim_pressure', 'native_restarted', 'memory_oom', 'enrollment_unavailable'}


class BindingChanged(ValueError):
    pass


def binding_digest(config):
    selection = {k: config[k] for k in ('producer_id', 'observer_id', 'receiving_scope', 'targets', 'base_url', 'token_file')}
    return hashlib.sha256(json.dumps(selection, sort_keys=True).encode()).hexdigest()


class Monitor:
    @staticmethod
    def validate(config):
        for k in ('producer_id', 'observer_id', 'receiving_scope'):
            identifier(config[k])
        targets = config['targets']
        if not 1 <= len(targets) <= 16 or len({t['id'] for t in targets}) != len(targets):
            raise ValueError('bounded unique target selection required')
        for target in targets:
            identifier(target['id']); identifier(target['resource_ref'])
            if target['kind'] in ('webbridge', 'http_health'):
                local_url(target['base_url'])
            elif target['kind'] == 'native_codex':
                if (not Path(target['binary']).is_absolute()
                        or target.get('scope', 'system') not in ('user','system')
                        or not re.fullmatch('[A-Za-z0-9_.@:-]+\\.service',target['unit'])):
                    raise ValueError('explicit native binary and service required')
            elif target['kind'] == 'host_memory':
                if not 0 < target['full_avg60_percent'] <= 100:
                    raise ValueError('bounded host memory stall threshold required')
            elif target['kind'] == 'cgroup_memory':
                if (not Path(target['cgroup_root']).is_absolute()
                        or not 0 < target['full_avg60_percent'] <= 100):
                    raise ValueError('explicit cgroup and bounded stall threshold required')
            elif target['kind'] == 'enrollment_registry':
                if (not Path(target['registry']).is_absolute()
                        or not Path(target['incus_binary']).is_absolute()
                        or not 0 < target['full_avg60_percent'] <= 100):
                    raise ValueError('explicit enrollment registry and Incus required')
            elif target['kind'] == 'unit':
                if target.get('scope', 'user') not in ('user', 'system'):
                    raise ValueError('explicit user/system unit scope required')
                if not re.fullmatch('[A-Za-z0-9_.@:-]+\\.service', target['unit']):
                    raise ValueError('explicit service unit required')
            else:
                raise ValueError('unsupported health probe')
        # Demand policy changes observation, not target/producer identity. Keep
        # the existing incident journal and pending revisions across adoption.
        demand = config.get('demand_units', {})
        if not isinstance(demand, dict):
            raise ValueError('explicit demand unit mapping required')
        by_id = {t['id']: t for t in targets}
        for target_id, unit in demand.items():
            if (target_id not in by_id or by_id[target_id]['kind'] != 'webbridge'
                    or not isinstance(unit, str)
                    or not re.fullmatch(r'[A-Za-z0-9_.@:-]+\.service', unit)):
                raise ValueError('on-demand policy requires an existing browser and user unit')
    def __init__(self, config, client=None):
        self.validate(config)
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
                             'CREATE TABLE IF NOT EXISTS runtime_counters(id TEXT PRIMARY KEY, content TEXT);'
                             'CREATE TABLE IF NOT EXISTS status(value TEXT);'
                             'CREATE TABLE IF NOT EXISTS memory_recovery(id TEXT PRIMARY KEY, since INTEGER, sampled INTEGER);')
        binding = binding_digest(config)
        prior = self.db.execute('SELECT value FROM binding').fetchone()
        if prior and prior[0] != binding:
            self.db.close(); self.lock.close()
            raise BindingChanged('changed observer binding requires explicit reconciliation')
        with self.db:
            if not prior:
                self.db.execute('INSERT INTO binding VALUES(?)', (binding,))
        self.client = client or Client(config['base_url'], config['token_file'])

    def relocate(self, replacement, now=None):
        """Reconcile an explicit physical move, preserving exact pending revisions."""
        self.validate(replacement)
        if {k:v for k,v in self.config.items() if k != 'targets'} != {
                k:v for k,v in replacement.items() if k != 'targets'}:
            raise ValueError('relocation changes targets only, never authority or journal')
        old = {t['id']:t for t in self.config['targets']}
        new = {t['id']:t for t in replacement['targets']}
        memory_kinds = {'host_memory', 'cgroup_memory'}
        if (old.keys() != new.keys() or any((old[k]['kind'] != new[k]['kind']
                and {old[k]['kind'],new[k]['kind']} != memory_kinds)
                or old[k]['kind'] == 'enrollment_registry' for k in old)):
            raise ValueError('relocation preserves fixed probe identities and kinds')
        changed = [k for k in old if old[k] != new[k]]
        if not changed:
            raise ValueError('explicit changed placement required')
        now = int(time.time()) if now is None else now
        # Failure here leaves original publications and binding intact. A later
        # retry sends the retained exact projections, never fabricated recovery.
        self.flush()
        with self.db:
            for key in changed:
                row = self.db.execute('SELECT content FROM incidents WHERE id=?', (key,)).fetchone()
                if not row:
                    continue
                value = json.loads(row[0])
                if value['processing_state'] == 'resolved':
                    continue
                if value['resource_ref'] != old[key]['resource_ref']:
                    raise ValueError('relocation cannot rewrite incident provenance')
                value.update(revision=value['revision']+1, updated_at=max(now, value['updated_at']),
                             processing_state='resolved', severity='info',
                             summary=key+': old placement observation retired.',
                             action='Physical relocation retired this observation. Receiving health must be observed separately; source work and incident history remain retained.')
                raw = json.dumps(value, sort_keys=True)
                self.db.execute('UPDATE incidents SET content=? WHERE id=?', (raw,key))
                self.db.execute('INSERT INTO outbox(content) VALUES(?)', (raw,))
        self.flush()
        prior, selected = binding_digest(self.config), binding_digest(replacement)
        with self.db:
            self.db.execute('CREATE TABLE IF NOT EXISTS relocations(seq INTEGER PRIMARY KEY, prior TEXT, selected TEXT, content TEXT)')
            history = {'source_at': now, 'previous_targets': list(old.values()),
                       'receiving_targets': list(new.values()), 'previous_counters': {},
                       'previous_memory_recovery': {}, 'retained_incidents': {}}
            for key in changed:
                for table, field in [('runtime_counters','previous_counters'), ('incidents','retained_incidents')]:
                    row = self.db.execute('SELECT content FROM '+table+' WHERE id=?', (key,)).fetchone()
                    if row:
                        history[field][key] = json.loads(row[0])
                row = self.db.execute('SELECT since,sampled FROM memory_recovery WHERE id=?', (key,)).fetchone()
                if row:
                    history['previous_memory_recovery'][key] = list(row)
                self.db.execute('DELETE FROM runtime_counters WHERE id=?', (key,))
                self.db.execute('DELETE FROM memory_recovery WHERE id=?', (key,))
            self.db.execute('INSERT INTO relocations(prior,selected,content) VALUES(?,?,?)',
                            (prior,selected,json.dumps(history,sort_keys=True)))
            # Bounded local placement/counter audit; published incident history
            # and unresolved/uncertain work are not removed by this retention.
            self.db.execute('DELETE FROM relocations WHERE seq NOT IN (SELECT seq FROM relocations ORDER BY seq DESC LIMIT 32)')
            self.db.execute('UPDATE binding SET value=? WHERE value=?', (selected,prior))
        self.config = replacement
        return dict(relocated_targets=changed, receiving_health_verified=False)

    def close(self):
        self.db.close(); self.lock.close()

    def retire_enrollment(self, name, now=None, *, remote=False):
        """Explicitly retire an old placement, not its work or another body."""
        identifier(name)
        registries = [t for t in self.config['targets'] if t['kind'] == 'enrollment_registry']
        if len(registries) != 1:
            raise ValueError('one explicit enrollment registry required')
        target = registries[0]
        root = Path(target['registry'])
        # Reuse enrollment validation before touching the selected record.
        enrollments(target)
        now = int(time.time()) if now is None else now
        with (root / 'lock').open('a+b') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            filename = ('remote-' if remote else '') + name + '.json'
            path = root / filename
            archive = root / (filename + '.retired')
            if path.exists():
                if archive.exists():
                    if private_text(path, 4096) != private_text(archive, 4096):
                        raise ValueError('prior retirement requires explicit reconciliation')
                    # A legacy enrollment reconciler can recreate its exact old
                    # record. The durable retirement remains authoritative.
                    path.unlink()
                # Only this registry record leaves the active *.json selection.
                # Archive first: after a crash the same command resumes resolution.
                else:
                    path.rename(archive)
                fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            elif not archive.exists():
                raise ValueError('selected enrollment does not exist')
            expected_resource = (json.loads(private_text(archive,4096))['resource_ref']
                                 if remote else 'container-dm-'+name)
            # Do not supersede a pending/uncertain publication. Exact flush is
            # idempotent; a service outage leaves the archive and outbox resumable.
            self.flush()
            resolved = []
            with self.db:
                for suffix in (('availability',) if remote else ('native', 'memory')):
                    key = name + '-' + suffix
                    prior = self.db.execute('SELECT content FROM incidents WHERE id=?', (key,)).fetchone()
                    if not prior:
                        continue
                    value = json.loads(prior[0])
                    if value['resource_ref'] != expected_resource:
                        raise ValueError('retirement cannot rebind incident provenance')
                    if value['processing_state'] == 'resolved':
                        continue
                    value.update(revision=value['revision'] + 1,
                                 updated_at=max(now, value['updated_at']),
                                 processing_state='resolved', severity='info',
                                 summary=key + ': old placement observation retired.',
                                 action='This placement observation was explicitly retired. This resolves its observation only; use the receiving host monitor for current health. Source work and incident history remain retained.')
                    raw = json.dumps(value, sort_keys=True)
                    self.db.execute('UPDATE incidents SET content=? WHERE id=?', (raw, key))
                    self.db.execute('INSERT INTO outbox(content) VALUES(?)', (raw,))
                    resolved.append(value['incident_id'])
            self.flush()
        return dict(**{('retired_remote' if remote else 'retired_enrollment'):name}, resolved_incidents=resolved,
                    source_work_recovered=False)

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
        if prior and prior['processing_state'] == 'open' and prior['resource_ref'] != target['resource_ref']:
            raise ValueError('changed resource requires explicit observation retirement')
        active = reason not in ('healthy', 'standby')
        if not active and (not prior or prior['processing_state'] == 'resolved'):
            return
        summary = target['id'] + ': ' + reason.replace('_', ' ') + '.'
        severity = 'warning' if reason in ('memory_reclaim_pressure','native_restarted') else 'error'
        if prior and prior['processing_state'] == 'open':
            if active and prior['summary'] == summary:
                return
            value = dict(prior, revision=prior['revision'] + 1, updated_at=max(now, prior['updated_at']),
                         summary=summary, processing_state='open' if active else 'resolved',
                         severity=severity if active else 'info')
        else:
            value = dict(schema='daimon-alerts/v1', producer_id=self.config['producer_id'],
                         receiving_scope=self.config['receiving_scope'], resource_ref=target['resource_ref'],
                         incident_id=target['id'] + '-' + secrets.token_hex(12), operation_id='service-' + target['id'],
                         revision=1, phase='availability', problem_code=(
                             'worker_stalled' if target['kind'] in ('cgroup_memory','host_memory')
                             else 'service_unavailable'),
                         severity=severity, processing_state='open', summary=summary,
                         action=('Inspect the selected resource and native control health. The native supervisor recovers a departed daemon while retaining unresponsive work. No model requests, conversation replay or automatic memory resizing.'
                                 if target['kind'] in ('native_codex', 'incus_native', 'cgroup_memory', 'host_memory') else
                                 'Inspect the selected service and browser connection; recover explicitly and verify an actual operation. No models or automatic task retries.'),
                         source_at=now, updated_at=now, work_preserved=True)
        raw = json.dumps(value, sort_keys=True)
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO incidents VALUES(?,?)', (target['id'], raw))
            self.db.execute('INSERT INTO outbox(content) VALUES(?)', (raw,))

    def memory_resolution_ready(self, target, reason, counters, now):
        """Retain an open outage until lower PSI is sustained across real samples."""
        if target['kind'] not in ('cgroup_memory','host_memory'):
            return True
        prior = self.db.execute('SELECT content FROM incidents WHERE id=?',
                                (target['id'],)).fetchone()
        if not prior or json.loads(prior[0])['processing_state'] != 'open':
            with self.db:
                self.db.execute('DELETE FROM memory_recovery WHERE id=?', (target['id'],))
            return True
        stalled = (counters or {}).get('full_avg60_percent')
        eligible = (reason == 'healthy' and isinstance(stalled, (int, float))
                    and 0 <= stalled < target['full_avg60_percent'] / 2)
        with self.db:
            row = self.db.execute('SELECT since,sampled FROM memory_recovery WHERE id=?',
                                  (target['id'],)).fetchone()
            if not eligible:
                self.db.execute('DELETE FROM memory_recovery WHERE id=?', (target['id'],))
                return reason != 'healthy'
            # A restart retains progress; an observation gap or clock reversal
            # cannot masquerade as continuously observed recovery.
            since = row[0] if row and 0 <= now - row[1] <= 90 else now
            self.db.execute('INSERT OR REPLACE INTO memory_recovery VALUES(?,?,?)',
                            (target['id'], since, now))
        return now - since >= 120

    def tick(self, check=probe, now=None):
        live_clock = now is None
        now = int(time.time()) if now is None else now
        available = True
        try:
            self.flush()
        except Unavailable:
            available = False
        rows = []
        targets = []
        for target in self.config['targets']:
            if target['kind'] != 'enrollment_registry':
                targets.append(target)
                continue
            registry = dict(target, _registry_state='healthy')
            try:
                targets.extend(enrollments(target))
                if any(Path(target['registry']).glob('.failure-*')):
                    registry['_registry_state'] = 'enrollment_unavailable'
            except (OSError, ValueError, KeyError, TypeError):
                registry['_registry_state'] = 'observation_unreadable'
            targets.append(registry)
        # Probe independent hosted bodies concurrently, with a fixed upper bound.
        # SQLite/counter/incident changes remain serialized in the existing tick.
        def observe(target):
            selected = dict(target)
            demand_unit = self.config.get('demand_units', {}).get(target['id'])
            if demand_unit:
                selected['demand_unit'] = demand_unit
            try:
                return check(selected), selected.get('_counters')
            except Exception:
                return 'observation_unreadable', None
        def observations():
            nonlocal available
            native = [t for t in targets if t['kind'] == 'incus_native']
            for target in targets:
                if target['kind'] != 'incus_native':
                    yield target, observe(target)
            if not native:
                return
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                pending = {pool.submit(observe, t): t for t in native}
                heartbeat_at = time.monotonic() + 10
                while pending:
                    done, _ = concurrent.futures.wait(pending,
                        timeout=max(0, heartbeat_at - time.monotonic()),
                        return_when=concurrent.futures.FIRST_COMPLETED)
                    if time.monotonic() >= heartbeat_at:
                        if available:
                            try:
                                self.client.call('PUT', '/v1/observers/' + self.config['observer_id'],
                                    dict(receiving_scope=self.config['receiving_scope'],
                                         source_at=int(time.time()) if live_clock else now,
                                         source_state='observation_unreadable'))
                            except Unavailable:
                                available = False
                        heartbeat_at = time.monotonic() + 10
                    for future in done:
                        yield pending.pop(future), future.result()
        for target, (reason, counters) in observations():
            try:
                if counters is not None:
                    prior = self.db.execute('SELECT content FROM runtime_counters WHERE id=?',
                                            (target['id'],)).fetchone()
                    prior = json.loads(prior[0]) if prior else {}
                    if counters.get('oom_kill',0) > prior.get('oom_kill',counters.get('oom_kill',0)):
                        reason = 'memory_oom'
                    elif (reason == 'healthy' and counters.get('restarts',0)
                          > prior.get('restarts',counters.get('restarts',0))):
                        reason = 'native_restarted'
                if reason not in REASONS:
                    reason = 'observation_unreadable'
            except Exception:
                reason = 'observation_unreadable'
            observed_at = int(time.time()) if live_clock else now
            resolution_ready = (self.memory_resolution_ready(target, reason, counters, observed_at)
                                if available else True)
            rows.append(dict(id=target['id'], state=reason if resolution_ready else 'recovery_pending'))
            # Do not mutate uncertain revisions while acceptance is pending.
            if available:
                # Unknown observation never resolves a retained outage.
                if resolution_ready:
                    self.transition(target, reason, observed_at)
                # Retain the incident/outbox before consuming the counter edge.
                # Service outage or a crash must not erase an observed restart.
                if counters is not None:
                    with self.db:
                        self.db.execute('INSERT OR REPLACE INTO runtime_counters VALUES(?,?)',
                                        (target['id'],json.dumps(counters)))
                # Do not hold a fast body's incident behind another guest's
                # slow control RPC. Acceptance/outbox retain their normal rules.
                try:
                    self.flush()
                except Unavailable:
                    available = False
        if available:
            try:
                self.flush()
                self.client.call('PUT', '/v1/observers/' + self.config['observer_id'],
                                 dict(receiving_scope=self.config['receiving_scope'], source_at=int(time.time()) if live_clock else now,
                                      source_state='healthy' if all(r['state'] != 'observation_unreadable' for r in rows) else 'observation_unreadable'))
            except Unavailable:
                available = False
        result = dict(checked_at=now, service_state='available' if available else 'unavailable', targets=rows)
        with self.db:
            self.db.execute('DELETE FROM status')
            self.db.execute('INSERT INTO status VALUES(?)', (json.dumps(result),))
        return result


def enrollments(target):
    """Expand one fixed operator registry, without rebinding retained incidents."""
    root = Path(target['registry'])
    info = root.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('private receiving registry required')
    files = sorted(root.glob('*.json'))
    if len(files) > 16:
        raise ValueError('bounded hosted resource registry required')
    targets = []
    for path in files:
        record = json.loads(private_text(path, 4096))
        if record.get('schema') == 'cluster-runtime-health-remote/v1':
            if set(record) != {'schema','name','resource_ref','plan_digest','base_url'}:
                raise ValueError('closed remote observation record required')
            name = record['name']; identifier(name); identifier(record['resource_ref'])
            if (path.name != 'remote-'+name+'.json' or not isinstance(record['plan_digest'],str)
                    or not re.fullmatch('[0-9a-f]{64}',record['plan_digest'])):
                raise ValueError('qualified remote observation required')
            local_url(record['base_url'])
            retired=path.with_name(path.name+'.retired')
            if retired.exists():
                if json.loads(private_text(retired,4096)) != record:
                    raise ValueError('retired remote observation requires explicit reconciliation')
                continue
            targets.append(dict(id=name+'-availability',resource_ref=record['resource_ref'],
                                kind='http_health',base_url=record['base_url']))
            continue
        if set(record) != {'schema','name','instance','resource_ref','plan_digest'}:
            raise ValueError('closed enrollment record required')
        name = record['name']; identifier(name)
        instance = 'dm-' + name
        if (record['schema'] != 'cluster-runtime-health-enrollment/v1'
                or record['instance'] != instance or path.name != name + '.json'
                or record['resource_ref'] != 'container-' + instance
                or not isinstance(record['plan_digest'], str)
                or not re.fullmatch('[0-9a-f]{64}', record['plan_digest'])):
            raise ValueError('qualified receiving enrollment required')
        retired = path.with_name(path.name + '.retired')
        if retired.exists():
            if json.loads(private_text(retired, 4096)) != record:
                raise ValueError('retired placement requires explicit reconciliation')
            continue
        resource = record['resource_ref']; identifier(resource)
        for suffix in ('native','memory'):
            item = dict(id=name+'-'+suffix, resource_ref=resource)
            identifier(item['id'])
            if suffix == 'native':
                item.update(kind='incus_native', instance=instance, incus_binary=target['incus_binary'])
            else:
                item.update(kind='cgroup_memory',
                    cgroup_root='/sys/fs/cgroup/lxc.payload.'+instance,
                    full_avg60_percent=target['full_avg60_percent'])
            targets.append(item)
    return targets


def relocate_monitor(config, replacement, client=None, now=None):
    """Resume the same move after an acknowledged binding switch or lost reply."""
    if {k:v for k,v in config.items() if k != 'targets'} != {
            k:v for k,v in replacement.items() if k != 'targets'}:
        raise ValueError('relocation changes targets only, never authority or journal')
    monitor = None
    try:
        try:
            monitor = Monitor(config, client)
        except BindingChanged:
            monitor = Monitor(replacement, client)
            table = monitor.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='relocations'").fetchone()
            row = monitor.db.execute('SELECT prior,selected FROM relocations ORDER BY seq DESC LIMIT 1').fetchone() if table else None
            if row != (binding_digest(config), binding_digest(replacement)):
                raise ValueError('retained relocation receipt required')
            return dict(already_relocated=True, receiving_health_verified=False)
        return monitor.relocate(replacement, now=now)
    finally:
        if monitor:
            monitor.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--retire-enrollment', metavar='NAME',
                        help='Explicitly archive one old container enrollment and resolve only its observations')
    action.add_argument('--relocate-to', type=Path, metavar='PRIVATE_CONFIG',
                        help='Explicitly reconcile changed physical targets, preserving the existing journal and authority')
    action.add_argument('--retire-remote',metavar='NAME',
                        help='Retire one remote availability record, preserving exact pending publications')
    args = parser.parse_args()
    os.umask(0o077)
    monitor = None
    try:
        config = json.loads(private_text(args.config))
        if args.relocate_to:
            print(json.dumps(relocate_monitor(config, json.loads(private_text(args.relocate_to)))))
            return 0
        monitor = Monitor(config)
        if args.retire_remote:
            print(json.dumps(monitor.retire_enrollment(args.retire_remote,remote=True)))
            return 0
        if args.retire_enrollment:
            print(json.dumps(monitor.retire_enrollment(args.retire_enrollment)))
            return 0
        result = monitor.tick()
        print(json.dumps(result))
        return 0 if result['service_state'] == 'available' else 2
    except (OSError, ValueError, KeyError, TypeError, Unavailable):
        print(json.dumps({'service_state': 'unavailable', 'reason': 'configuration_or_observation_unavailable'}))
        return 2
    finally:
        if monitor:
            monitor.close()


if __name__ == '__main__':
    raise SystemExit(main())
