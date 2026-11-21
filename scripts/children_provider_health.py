#!/usr/bin/env python3
"""Model-free provider authentication observation for the existing four guests.

Run as the existing host operator through its maintained SSH route. The ordinary
service observer calls this finite adapter; it installs no schedule. One cached
projection bounds provider reads to four per five minutes, with one replaceable
file. --fresh is an explicit recovery verification, not a retry of human input.
"""
import argparse
import concurrent.futures
import fcntl
import json
import os
from pathlib import Path
import subprocess
import time

NAMES = ('pixin', 'calipso', 'c-137', 'noodles')
STATES = {'healthy', 'provider_login_required', 'provider_unavailable'}
WORKER = r'''
import importlib.util,json,pathlib,queue,subprocess,time
native=None
state='provider_unavailable'
try:
    pid=json.loads(pathlib.Path('/home/agent/.codex/app-server-daemon/daemon.pid').read_text())['pid']
    binary=str(pathlib.Path('/proc',str(pid),'exe').resolve(strict=True))
    webpid=subprocess.check_output(['systemctl','show','children-conversation-web.service','-p','MainPID','--value'],text=True,timeout=2).strip()
    source=pathlib.Path('/proc',webpid,'cwd').resolve(strict=True)/'scripts/activity_indicator.py'
    spec=importlib.util.spec_from_file_location('selected_native_observer',source)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    native=module.Native(binary)
    def read(method,params):
        native.sequence+=1;number=native.sequence
        native.send({'id':number,'method':method,'params':params})
        end=time.monotonic()+4
        while time.monotonic()<end:
            value=native.messages.get(timeout=max(.01,end-time.monotonic()))
            if value.get('id')==number:return value
        raise TimeoutError()
    account=read('account/read',{'refreshToken':False})
    if 'result' in account and account['result'].get('account') is None:
        state='provider_login_required'
    else:
        result=read('account/rateLimits/read',{})
        if isinstance(result.get('result'),dict) and isinstance(result['result'].get('rateLimits'),dict):
            state='healthy'
        elif 'error' in result:
            # Provider error text is inspected in memory only, never persisted.
            error=json.dumps(result['error']).lower()
            if any(code in error for code in ('401','unauthorized','refresh token','invalid_grant')):
                state='provider_login_required'
except Exception:
    pass
finally:
    if native is not None:native.close()
print(json.dumps({'state':state}))
'''


def check(name):
    try:
        result = subprocess.run(['/usr/bin/incus', 'exec', 'dm-' + name, '--',
            'runuser', '-u', 'agent', '--', 'env', 'HOME=/home/agent',
            'CODEX_HOME=/home/agent/.codex', 'python3', '-B', '-'],
            input=WORKER, capture_output=True, text=True, timeout=19)
        if result.returncode == 0 and len(result.stdout) <= 256:
            value = json.loads(result.stdout)
            if set(value) == {'state'} and value['state'] in STATES:
                return value['state']
    except (OSError, ValueError, TypeError, subprocess.TimeoutExpired):
        pass
    return 'provider_unavailable'


def aggregate(states):
    if len(states) != 4 or any(value not in STATES for value in states):
        return 'provider_unavailable'
    if 'provider_login_required' in states:
        return 'provider_login_required'
    return 'healthy' if all(value == 'healthy' for value in states) else 'provider_unavailable'


def observe(directory, fresh=False):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = directory.lstat()
    if directory.is_symlink() or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('private operator-owned state required')
    with (directory / 'probe.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        now = time.time()
        path = directory / 'current.json'
        if not fresh and path.exists():
            cached = json.loads(path.read_text())
            if (0 <= now - cached['checked_at'] < 300
                    and set(cached['states']) == set(NAMES)
                    and all(v in STATES for v in cached['states'].values())):
                return aggregate(list(cached['states'].values()))
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            states = dict(zip(NAMES, pool.map(check, NAMES)))
        value = dict(checked_at=now, states=states, model_inputs=0)
        pending = directory / 'current.json.pending'
        pending.write_text(json.dumps(value, sort_keys=True) + '\n')
        os.replace(pending, path)
        return aggregate(list(states.values()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--fresh', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    try:
        state = observe(args.state, args.fresh)
    except (OSError, ValueError, KeyError, TypeError):
        state = 'provider_unavailable'
    print(json.dumps(dict(schema='dm.monitor.provider-auth/v1', state=state)))


if __name__ == '__main__':
    main()
