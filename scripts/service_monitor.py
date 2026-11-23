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


MATRIX_RECEIVING_STATES = {
    'matrix_local_capability_expiring', 'matrix_listener_unavailable',
    'matrix_probe_contended', 'matrix_roster_contended', 'matrix_roster_invalid',
    'matrix_roster_unavailable', 'matrix_status_socket_missing',
    'matrix_status_socket_refused', 'matrix_status_socket_timeout',
}


def matrix_receiving(command):
    # The operator-selected adapter authenticates the current application,
    # not merely its systemd process. It returns no speech or private state.
    try:
        result = subprocess.run(command, capture_output=True,
                                timeout=12, text=True)
        if result.returncode or len(result.stdout) > 4096:
            return 'observation_unreadable'
        value = json.loads(result.stdout)
        if (set(value) == {'schema', 'state'}
                and value['schema'] == 'dm.monitor.matrix-receiving/v2'
                and value['state'] in MATRIX_RECEIVING_STATES):
            return value['state']
        if (set(value) != {'schema', 'ready'}
                or value['schema'] != 'dm.monitor.matrix-receiving/v1'
                or type(value['ready']) is not bool):
            return 'observation_unreadable'
        return 'healthy' if value['ready'] else 'matrix_reader_unavailable'
    except (OSError, ValueError, TypeError, subprocess.TimeoutExpired):
        return 'observation_unreadable'


def scheduled_job(target):
    """Observe the selected timer and its real finite-job outcome, not inactivity."""
    command = ['systemctl'] + (['--user'] if target['scope'] == 'user' else [])
    def properties(unit):
        result = subprocess.run(command + ['show', unit,
            '--property=LoadState,ActiveState,UnitFileState,Result,ExecMainStatus,ExecMainStartTimestampMonotonic,ExecMainExitTimestampMonotonic,Wants,Requires,After,Triggers'],
            capture_output=True, text=True, timeout=3)
        if result.returncode or len(result.stdout) > 4096:
            raise ValueError('bounded unit observation required')
        return dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
    try:
        timer, job = properties(target['timer']), properties(target['unit'])
        if (timer.get('LoadState') != 'loaded' or timer.get('ActiveState') != 'active'
                or timer.get('UnitFileState') != 'enabled' or job.get('LoadState') != 'loaded'):
            return 'unit_inactive'
        if target.get('trigger_unit'):
            trigger=properties(target['trigger_unit'])
            # The timer's matching service must really pull in and wait for the
            # observed dependency. A configured name alone proves no schedule.
            dependencies=(trigger.get('Wants','')+' '+trigger.get('Requires','')).split()
            if (target['trigger_unit'] not in timer.get('Triggers','').split()
                    or trigger.get('LoadState')!='loaded' or target['unit'] not in dependencies
                    or target['unit'] not in trigger.get('After','').split()):
                return 'unit_inactive'
        uptime = float(Path('/proc/uptime').read_text().split()[0])
        if job.get('ActiveState') in ('active', 'activating'):
            start = int(job.get('ExecMainStartTimestampMonotonic', '0')) / 1e6
            return ('healthy' if start > 0 and 0 <= uptime-start <= target['max_run_seconds']
                    else 'health_unavailable')
        end = int(job.get('ExecMainExitTimestampMonotonic', '0')) / 1e6
        return ('healthy' if job.get('ActiveState') == 'inactive'
                and job.get('Result') == 'success' and job.get('ExecMainStatus') == '0'
                and end > 0 and 0 <= uptime-end <= target['max_age_seconds']
                else 'health_unavailable')
    except (OSError, ValueError, KeyError, subprocess.TimeoutExpired):
        return 'observation_unreadable'


def https_group(target):
    if target.get('enabled',True) is False:
        return 'standby'
    # A fixed deployment's small endpoint set shares one observation/incident.
    # This does not turn an endpoint failure into a physical-host diagnosis.
    def check(endpoint):
        try:
            value=read_json(endpoint['url'])
            return isinstance(value,dict) and value.get('ok') is True
        except (OSError,ValueError,TypeError):
            return False
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        return 'healthy' if all(list(pool.map(check,target['endpoints']))) else 'health_unavailable'


CONVERSATION_ACCESS_STATES = {
    'ready', 'receiver_binding_changed', 'alerts_binding_changed',
    'unexpected_access_response', 'unexpected_entry_response', 'transport_unavailable',
}


def conversation_access(target):
    """Read the owning App probe's closed, fixed-roster observation."""
    target.pop('_conversation_receivers', None)
    try:
        command = target['command']
        result = subprocess.run(command, capture_output=True, timeout=16, text=True)
        if result.returncode not in (0, 1) or len(result.stdout) > 4096:
            return 'observation_unreadable'
        value = json.loads(result.stdout)
        if (set(value) != {'schema', 'scope', 'receivers'}
                or value['schema'] != 'daimonapp.conversation-access-observation/v1'
                or value['scope'] != 'anonymous_public_entry'
                or not isinstance(value['receivers'], list)):
            return 'observation_unreadable'
        rows = value['receivers']
        if ([row.get('origin') for row in rows] != command[2:]
                or any(row.get('state') not in CONVERSATION_ACCESS_STATES for row in rows)):
            return 'observation_unreadable'
        states = {row['state'] for row in rows}
        ready = states == {'ready'}
        if ready != (result.returncode == 0):
            return 'observation_unreadable'
        target['_conversation_receivers'] = [
            {'id': urllib.parse.urlsplit(row['origin']).hostname.removesuffix('.dmn.homes'),
             'state': row['state']}
            for row in rows
        ]
        if ready:
            return 'healthy'
        if states & {'receiver_binding_changed', 'alerts_binding_changed'}:
            return 'conversation_access_needs_requalification'
        return 'conversation_entry_unavailable'
    except (OSError, ValueError, TypeError, KeyError, AttributeError, subprocess.TimeoutExpired):
        return 'observation_unreadable'


def probe(target):
    """Return only a closed health code; never expose response/tab/process data."""
    if target['kind'] == 'conversation_access':
        return conversation_access(target)
    if target['kind'] == 'provider_auth':
        try:
            result = subprocess.run(target['command'], capture_output=True,
                                    timeout=24, text=True)
            if result.returncode or len(result.stdout) > 4096:
                return 'observation_unreadable'
            value = json.loads(result.stdout)
            if (set(value) != {'schema', 'state'}
                    or value['schema'] != 'dm.monitor.provider-auth/v1'
                    or value['state'] not in {'healthy', 'provider_login_required',
                                             'provider_unavailable'}):
                return 'observation_unreadable'
            return value['state']
        except (OSError, ValueError, TypeError, subprocess.TimeoutExpired):
            return 'observation_unreadable'
    if target['kind'] == 'https_health_group':
        return https_group(target)
    if target['kind'] == 'scheduled_job':
        return scheduled_job(target)
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
            if (result.returncode != 0 or props.get('LoadState') != 'loaded'
                    or props.get('ActiveState') != 'active'):
                return 'unit_inactive'
            return matrix_receiving(target['receiving_command']) if 'receiving_command' in target else 'healthy'
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
           'provider_login_required', 'provider_unavailable',
           'conversation_access_needs_requalification', 'conversation_entry_unavailable',
           'extension_disconnected', 'extension_unresponsive', 'native_unresponsive',
           'matrix_reader_unavailable', *MATRIX_RECEIVING_STATES,
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
        app_checks = sum(t.get('kind') == 'conversation_access' for t in targets)
        if (app_checks > 1 or not 1 <= len(targets) <= 16 + app_checks
                or len({t['id'] for t in targets}) != len(targets)):
            raise ValueError('bounded unique target selection required')
        if sum(len(t['endpoints']) for t in targets if t.get('kind')=='https_health_group' and isinstance(t.get('endpoints'),list))>4:
            raise ValueError('at most four external HTTPS health endpoints per observer')
        for target in targets:
            identifier(target['id']); identifier(target['resource_ref'])
            if target['kind'] == 'provider_auth':
                command = target.get('command')
                if (set(target) != {'id', 'resource_ref', 'kind', 'command'}
                        or not isinstance(command, list) or not 1 <= len(command) <= 24
                        or any(not isinstance(arg, str) or not arg or len(arg) > 4096 for arg in command)
                        or not Path(command[0]).is_absolute()):
                    raise ValueError('explicit bounded provider health adapter required')
            elif target['kind'] == 'conversation_access':
                command = target.get('command')
                if (set(target) != {'id', 'resource_ref', 'kind', 'command'}
                        or not isinstance(command, list) or not 3 <= len(command) <= 10
                        or any(not isinstance(arg, str) or not arg or len(arg) > 2048 for arg in command)
                        or not Path(command[0]).is_absolute() or not Path(command[1]).is_absolute()
                        or len(set(command[2:])) != len(command[2:])
                        or any(not re.fullmatch(r'https://[a-z0-9-]{1,63}\.dmn\.homes', origin)
                               for origin in command[2:])):
                    raise ValueError('select one owning Conversation access probe and fixed Home roster')
            elif target['kind'] in ('webbridge', 'http_health'):
                local_url(target['base_url'])
            elif target['kind']=='https_health_group':
                entries=target.get('endpoints')
                if (not {'id','resource_ref','kind','endpoints'}<=set(target)
                        or set(target)-{'id','resource_ref','kind','endpoints','enabled'}
                        or type(target.get('enabled',True)) is not bool
                        or not isinstance(entries,list) or not 1<=len(entries)<=4):
                    raise ValueError('bounded explicit HTTPS endpoint group required')
                ids=set();urls=set()
                for entry in entries:
                    if not isinstance(entry,dict) or set(entry)!={'id','url'}:
                        raise ValueError('closed HTTPS health endpoint required')
                    identifier(entry['id'])
                    url=entry['url']
                    if not isinstance(url,str) or len(url)>2048:
                        raise ValueError('bounded HTTPS health URL required')
                    u=urllib.parse.urlsplit(url)
                    _=u.port  # Validate malformed/out-of-range ports before probing.
                    if (u.scheme!='https' or not u.hostname or u.username or u.password
                            or u.query or u.fragment or not u.path.endswith('/health')
                            or any(c.isspace() for c in url)):
                        raise ValueError('explicit credential-free HTTPS health required')
                    if entry['id'] in ids or url in urls:
                        raise ValueError('unique HTTPS endpoints required')
                    ids.add(entry['id']);urls.add(url)
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
            elif target['kind'] == 'scheduled_job':
                trigger=target.get('trigger_unit',target['unit'])
                if (target.get('scope') not in ('user', 'system')
                        or not re.fullmatch(r'[A-Za-z0-9_.@:-]+\.service', target['unit'])
                        or not isinstance(trigger,str) or not re.fullmatch(r'[A-Za-z0-9_.@:-]+\.service', trigger)
                        or target['timer'] != trigger[:-8]+'.timer'
                        or type(target['max_age_seconds']) is not int
                        or not 60 <= target['max_age_seconds'] <= 172800
                        or type(target['max_run_seconds']) is not int
                        or not 10 <= target['max_run_seconds'] <= 3600):
                    raise ValueError('explicit bounded scheduled service/timer required')
            elif target['kind'] == 'unit':
                if target.get('scope', 'user') not in ('user', 'system'):
                    raise ValueError('explicit user/system unit scope required')
                if not re.fullmatch('[A-Za-z0-9_.@:-]+\\.service', target['unit']):
                    raise ValueError('explicit service unit required')
                if 'receiving_command' in target:
                    command = target['receiving_command']
                    if (not isinstance(command, list) or not 1 <= len(command) <= 8
                            or any(not isinstance(arg, str) or not arg or len(arg) > 4096 for arg in command)
                            or not Path(command[0]).is_absolute()):
                        raise ValueError('explicit bounded Matrix health adapter required')
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

    def extend(self, replacement):
        """Explicitly add fixed targets without rewriting any retained incident."""
        self.validate(replacement)
        if {k:v for k,v in self.config.items() if k != 'targets'} != {
                k:v for k,v in replacement.items() if k != 'targets'}:
            raise ValueError('extension preserves authority and journal')
        old = {t['id']:t for t in self.config['targets']}
        new = {t['id']:t for t in replacement['targets']}
        if not old.keys() < new.keys() or any(new[k] != v for k,v in old.items()):
            raise ValueError('extension only adds targets, never changes old targets')
        self.flush()
        prior, selected = binding_digest(self.config), binding_digest(replacement)
        with self.db:
            self.db.execute('CREATE TABLE IF NOT EXISTS extensions(seq INTEGER PRIMARY KEY, prior TEXT, selected TEXT)')
            self.db.execute('INSERT INTO extensions(prior,selected) VALUES(?,?)', (prior, selected))
            self.db.execute('DELETE FROM extensions WHERE seq NOT IN (SELECT seq FROM extensions ORDER BY seq DESC LIMIT 32)')
            self.db.execute('UPDATE binding SET value=?', (selected,))
        self.config = replacement
        return dict(added_targets=sorted(new.keys()-old.keys()), receiving_health_verified=False)

    def relocate(self, replacement, now=None, *, reader_only=False):
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
        if reader_only:
            for key in changed:
                if (old[key]['kind'] != 'unit' or 'receiving_command' not in old[key]
                        or 'receiving_command' not in new[key]
                        or {k:v for k,v in old[key].items() if k != 'receiving_command'} !=
                           {k:v for k,v in new[key].items() if k != 'receiving_command'}):
                    raise ValueError('reader refresh changes only the existing Matrix health adapter')
                if probe(new[key]) != 'healthy':
                    raise ValueError('replacement Matrix health adapter unavailable')
            # Software refresh is not placement retirement or incident recovery.
            # Keep pending incident versions and counters byte-for-byte. The
            # next ordinary observation owns any real higher-revision resolution.
            prior, selected = binding_digest(self.config), binding_digest(replacement)
            with self.db:
                self.db.execute('CREATE TABLE IF NOT EXISTS relocations(seq INTEGER PRIMARY KEY, prior TEXT, selected TEXT, content TEXT)')
                self.db.execute('INSERT INTO relocations(prior,selected,content) VALUES(?,?,?)',
                    (prior,selected,json.dumps(dict(reader_only=True,previous_targets=list(old.values()),
                                                   receiving_targets=list(new.values())),sort_keys=True)))
                self.db.execute('DELETE FROM relocations WHERE seq NOT IN (SELECT seq FROM relocations ORDER BY seq DESC LIMIT 32)')
                self.db.execute('UPDATE binding SET value=? WHERE value=?',(selected,prior))
            self.config=replacement
            return dict(refreshed_readers=changed, receiving_health_verified=True, incidents_changed=False)
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

    def flush(self, incident_id=None):
        # A finite owner repair publishes only its retained incident; ordinary
        # observer flushes keep their existing complete ordered outbox behavior.
        query = 'SELECT seq,content FROM outbox'
        params = ()
        if incident_id is not None:
            query += " WHERE json_extract(content,'$.incident_id')=?"
            params = (incident_id,)
        for seq, raw in self.db.execute(query + ' ORDER BY seq', params).fetchall():
            value = json.loads(raw)
            result = self.client.call('PUT', '/v1/incidents/' + value['producer_id'] + '/' + value['incident_id'], value)
            if any(result.get(k) != value[k] for k in ('producer_id', 'incident_id', 'revision')):
                raise Unavailable('acceptance mismatch')
            with self.db:
                self.db.execute('DELETE FROM outbox WHERE seq=?', (seq,))

    def reader_repair_state(self, target_id, incident_id):
        if not self.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='owner_repairs'").fetchone():
            return 'no_matching_owner_repair_recorded'
        row = self.db.execute('SELECT content FROM owner_repairs WHERE target=?', (target_id,)).fetchone()
        repair = json.loads(row[0]) if row else {}
        if repair.get('incident_id') != incident_id:
            return 'no_matching_owner_repair_recorded'
        return ('start_acknowledged' if repair.get('state') == 'start_acknowledged'
                else 'claimed_outcome_unconfirmed')

    def heartbeat_report(self):
        """Project retained incidents for an explicitly selected owner reader.

        This is navigation, not recovery authority or an acknowledgment. Keep
        exact incident revisions and acceptance uncertainty; never expose probe
        commands, endpoints, credentials or the rest of the private config.
        """
        fields = ('producer_id', 'incident_id', 'revision', 'operation_id',
                  'resource_ref', 'phase', 'problem_code', 'processing_state',
                  'severity', 'source_at', 'updated_at', 'summary', 'action',
                  'work_preserved')
        rows = self.db.execute("SELECT content FROM incidents ORDER BY "
            "json_extract(content,'$.processing_state')='open' DESC, "
            "json_extract(content,'$.updated_at') DESC,id LIMIT 33").fetchall()
        incidents = []
        has_observations = self.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='reader_incident_observations'"
        ).fetchone()
        authenticated_targets = {
            target['id'] for target in self.config['targets']
            if target['kind'] == 'unit' and 'receiving_command' in target
        }
        for (raw,) in rows[:32]:
            value = json.loads(raw)
            pending = self.db.execute("SELECT 1 FROM outbox WHERE "
                "json_extract(content,'$.incident_id')=? AND "
                "json_extract(content,'$.revision')<=? LIMIT 1",
                (value['incident_id'], value['revision'])).fetchone()
            incident = {**{key: value[key] for key in fields},
                        'acceptance': 'pending' if pending else 'accepted'}
            target_id = value['operation_id'].removeprefix('service-')
            if (target_id in authenticated_targets
                    and value['processing_state'] == 'resolved'
                    and value['summary'] == target_id + ': healthy.'):
                # This is the successful authenticated check used by the observer
                # transition. A start or causal ownership needs its own receipt.
                incident['resolution_evidence'] = {
                    'authenticated_reader': 'verified',
                    'observed_at': value['updated_at'],
                    'scope': 'selected_unit_receiving_probe',
                }
                observation = self.db.execute(
                    'SELECT opening_reason, repair_state FROM reader_incident_observations '
                    'WHERE incident=? AND target=?',
                    (value['incident_id'], target_id),
                ).fetchone() if has_observations else None
                incident['resolution_evidence']['opening_observation'] = (
                    observation[0] if observation and observation[0] in REASONS else 'not_retained'
                )
                incident['resolution_evidence']['owner_repair'] = (
                    observation[1] if observation and observation[1] is not None
                    else self.reader_repair_state(target_id, value['incident_id'])
                )
            incidents.append(incident)
        status = self.db.execute('SELECT value FROM status').fetchone()
        status = json.loads(status[0]) if status else {}
        report = dict(schema='daimon-alerts.heartbeat-report/v1',
            producer_id=self.config['producer_id'],
            checked_at=status.get('checked_at'),
            service_state=status.get('service_state', 'unobserved'),
            incidents=incidents, more_incidents=len(rows) > 32)
        raw = (json.dumps(report, sort_keys=True) + '\n').encode()
        if len(raw) > 65536:
            raise ValueError('bounded heartbeat report required')
        # Keep the model-readable projection separate from the private journal
        # and configuration. A reader can grant this directory read-only while
        # atomic replacement keeps producing fresh files beneath that rule.
        directory = Path(self.config['state_dir']) / 'heartbeat'
        directory.mkdir(mode=0o700, exist_ok=True)
        info = directory.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077):
            raise ValueError('owner-private report directory required')
        pending = directory / 'heartbeat-report.json.pending'
        destination = directory / 'heartbeat-report.json'
        fd = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_nlink != 1 or info.st_mode & 0o077):
                raise ValueError('owner-private heartbeat report required')
            os.ftruncate(fd, 0)
            with os.fdopen(fd, 'wb') as stream:
                fd = -1
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(pending, destination)
            descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            if fd != -1:
                os.close(fd)
        return report

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
        unavailable = [row['id'] for row in target.get('_conversation_receivers', ())
                       if row['state'] != 'ready']
        if unavailable:
            homes = ', '.join(unavailable[:2])
            if len(unavailable) > 2:
                homes += f' and {len(unavailable)-2} more'
            summary = f'Conversation access unavailable on {homes}.'
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
                         action=('Provider login must be renewed with the existing selected account when required. Verify authenticated native provider access after recovery. A running process or public health page is insufficient. Never replay failed messages or replace accounts automatically.'
                                 if target['kind'] == 'provider_auth' else
                                 'Check the per-Home Conversation access results. For a changed receiving binding, use the owning App requalification and verify access again. A public entry check does not sign in as a human.'
                                 if target['kind'] == 'conversation_access' else
                                 'Inspect the selected resource and native control health. The native supervisor recovers a departed daemon while retaining unresponsive work. No model requests, conversation replay or automatic memory resizing.'
                                 if target['kind'] in ('native_codex', 'incus_native', 'cgroup_memory', 'host_memory') else
                                 'Inspect the selected service and browser connection; recover explicitly and verify an actual operation. No models or automatic task retries.'),
                         source_at=now, updated_at=now, work_preserved=True)
        raw = json.dumps(value, sort_keys=True)
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO incidents VALUES(?,?)', (target['id'], raw))
            self.db.execute('INSERT INTO outbox(content) VALUES(?)', (raw,))
            if (active and value['revision'] == 1 and target['kind'] == 'unit'
                    and 'receiving_command' in target):
                self.db.execute(
                    'CREATE TABLE IF NOT EXISTS reader_incident_observations('
                    'seq INTEGER PRIMARY KEY, incident TEXT UNIQUE, target TEXT, opening_reason TEXT, '
                    'authenticated_at INTEGER, repair_state TEXT)'
                )
                self.db.execute(
                    'INSERT INTO reader_incident_observations(incident,target,opening_reason) VALUES(?,?,?)',
                    (value['incident_id'], target['id'], reason),
                )
                self.db.execute(
                    "DELETE FROM reader_incident_observations WHERE seq NOT IN ("
                    "SELECT seq FROM reader_incident_observations ORDER BY seq DESC LIMIT 128)"
                    " AND incident NOT IN (SELECT json_extract(content,'$.incident_id') FROM incidents)"
                )
            if (reason == 'healthy' and target['kind'] == 'unit' and 'receiving_command' in target
                    and self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                                        "AND name='reader_incident_observations'").fetchone()):
                self.db.execute(
                    'UPDATE reader_incident_observations SET authenticated_at=?, repair_state=? '
                    'WHERE incident=? AND target=?',
                    (value['updated_at'], self.reader_repair_state(target['id'], value['incident_id']),
                     value['incident_id'], target['id']),
                )

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
                return check(selected), selected
            except Exception:
                return 'observation_unreadable', target
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
        for target, (reason, observed) in observations():
            counters = observed.get('_counters')
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
            row = dict(id=target['id'], state=reason if resolution_ready else 'recovery_pending')
            if '_conversation_receivers' in observed:
                row['receivers'] = observed['_conversation_receivers']
            rows.append(row)
            # Do not mutate uncertain revisions while acceptance is pending.
            if available:
                # Unknown observation never resolves a retained outage.
                if resolution_ready:
                    self.transition(observed, reason, observed_at)
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


def relocate_monitor(config, replacement, client=None, now=None, *, reader_only=False):
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
            row = monitor.db.execute('SELECT prior,selected,content FROM relocations ORDER BY seq DESC LIMIT 1').fetchone() if table else None
            if (row is None or row[:2] != (binding_digest(config), binding_digest(replacement))
                    or bool(json.loads(row[2]).get('reader_only',False)) != reader_only):
                raise ValueError('retained relocation receipt required')
            return dict(already_relocated=True, receiving_health_verified=False, reader_only=reader_only)
        return monitor.relocate(replacement, now=now, reader_only=reader_only)
    finally:
        if monitor:
            monitor.close()


def extend_monitor(config, replacement, client=None):
    monitor = None
    try:
        try:
            monitor = Monitor(config, client)
        except BindingChanged:
            monitor = Monitor(replacement, client)
            table = monitor.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='extensions'").fetchone()
            row = monitor.db.execute('SELECT prior,selected FROM extensions ORDER BY seq DESC LIMIT 1').fetchone() if table else None
            if row != (binding_digest(config), binding_digest(replacement)):
                raise ValueError('retained target-extension receipt required')
            return dict(already_extended=True, receiving_health_verified=False)
        return monitor.extend(replacement)
    finally:
        if monitor:
            monitor.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--heartbeat-report', action='store_true',
                        help='Write one bounded owner-private incident report after the ordinary observation')
    parser.add_argument('--owner-repair-selection', type=Path,
                        help='Run the existing fixed owner repair immediately after observation; no model')
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--retire-enrollment', metavar='NAME',
                        help='Explicitly archive one old container enrollment and resolve only its observations')
    action.add_argument('--relocate-to', type=Path, metavar='PRIVATE_CONFIG',
                        help='Explicitly reconcile changed physical targets, preserving the existing journal and authority')
    action.add_argument('--refresh-reader-to', type=Path, metavar='PRIVATE_CONFIG',
                        help='Select a healthy replacement Matrix health adapter without retiring its incident')
    action.add_argument('--retire-remote',metavar='NAME',
                        help='Retire one remote availability record, preserving exact pending publications')
    action.add_argument('--extend-to', type=Path, metavar='PRIVATE_CONFIG',
                        help='Explicitly add targets, preserving current observations and authority')
    args = parser.parse_args()
    os.umask(0o077)
    monitor = None
    try:
        config = json.loads(private_text(args.config))
        if args.refresh_reader_to:
            print(json.dumps(relocate_monitor(config, json.loads(private_text(args.refresh_reader_to)),reader_only=True)))
            return 0
        if args.extend_to:
            print(json.dumps(extend_monitor(config, json.loads(private_text(args.extend_to)))))
            return 0
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
        if args.owner_repair_selection:
            from owner_repair import reconcile_observed
            selection = json.loads(private_text(args.owner_repair_selection, 4096))
            result['owner_repair'] = reconcile_observed(monitor, selection)
        if args.heartbeat_report:
            monitor.heartbeat_report()
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
