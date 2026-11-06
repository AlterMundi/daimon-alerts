#!/usr/bin/env python3
"""Authenticated, model-free application reader readiness for service_monitor.

Operator configuration supplies existing status authority, never new grants.
One replaceable read request is retained per local probe, not one per tick.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import time


def ready(status, server, required, now_ms):
    from daimon_matrix.identity import validity_contains, validate_validity
    from daimon_matrix.canonical import canonical_bytes
    loaded = status['loaded_runtime']
    application = status['loaded_application']
    gate = status['receiving']['channels']['tribe']
    roster = status['receiving']['rosters']['tribe']
    if (gate.get('controlled') is not True or gate.get('phase') != 'select'
            or loaded['local_origin'] != server or gate['process_id'] != loaded['process_id']):
        return False
    readers = gate['readers']
    coordinates = sorted((r['being_ref'], r['embodiment_id']) for r in readers)
    if coordinates != sorted(tuple(r) for r in required):
        return False
    audience = {(r['being_ref'], r['embodiment_id'], r['credential_id']) for r in roster['audience']}
    epochs = set(roster['required_authority_epochs'])
    for reader in readers:
        if (reader.get('schema') != 'dm.receiving.reader/v2'
                or not validity_contains(validate_validity(reader['validity']), now_ms)
                or not epochs <= set(reader['authority_epochs'])
                or not {'dm.tribe.conversation/v1', 'dm.tribe.direct-conversation/v1'} <= set(reader['wire_read'])
                or (reader['being_ref'], reader['embodiment_id'], reader['credential_id']) not in audience):
            return False
    own = [r for r in readers if r['embodiment_id'] == server['embodiment_id']]
    if len(own) != 1 or own[0]['loaded_view_sha256'] != application['loaded_view_sha256']:
        return False
    startup = loaded['qualified_startup']
    selection = dict(receiving_code=startup['code_digest'], matrix_sdk=startup['matrix_wheel_sha256'],
                     runtime_bundle_sha256=loaded['runtime_bundle_sha256'],
                     application_sha256=application['application_sha256'],
                     publication_sha256=application['publication_sha256'],
                     visibility_installation_sha256=application['visibility']['installation_sha256'])
    return own[0]['receiving_selection_sha256'] == hashlib.sha256(canonical_bytes(selection)).hexdigest()


def inspect(config):
    sys.path.insert(0, config['operator'])
    from clusterctl.owner_process import ConnectedProcessObserver
    from clusterctl.onboarding_release import regular
    from daimon_matrix.client import ClientConfig, LocalClient
    from daimon_matrix.operator_capabilities import host_capability_profile
    root = Path(config['root'])
    profile = host_capability_profile('status')
    directory = root / profile['client_directory']
    cfg = ClientConfig.load(directory / profile['client_config_filename'],
                           bytearray(regular(directory / profile['client_key_filename'], uid=os.getuid())))
    state = Path(config['state'])
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = state.lstat()
    if state.is_symlink() or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('private probe state required')
    with (state / 'lock').open('a+b') as lock, ConnectedProcessObserver() as observer:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # Establish the actual peer before selecting the retained read token.
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(2)
            connection.connect(config['socket'])
            with observer(connection):
                process = observer.process
        client = LocalClient(Path(config['socket']), cfg, connection_observer=observer,
                             timeout_seconds=8, deadline_ns=time.monotonic_ns()+8_000_000_000)
        binding = dict(process=process, server=dict(cfg.expected_server),
                       capability=cfg.capability.descriptor, runtime_id=cfg.runtime_id)
        path = state / 'status-request.json'
        previous = json.loads(regular(path, uid=os.getuid())) if path.exists() else None
        if previous is not None and previous['binding'] == binding:
            request = previous['request']
        else:
            request = client.prepare('runtime.status', {})
            raw = json.dumps(dict(binding=binding, request=request)).encode()
            if len(raw) > 65536:
                raise ValueError('bounded status request required')
            temporary = state / 'status-request.tmp'
            fd = os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(raw); stream.flush(); os.fsync(stream.fileno())
            os.replace(temporary, path)
        if request['method'] != 'runtime.status' or request['params'] != {}:
            raise ValueError('status-only request required')
        response = client.send(request)
        if response.get('ok') is not True:
            raise ValueError('authenticated status unavailable')
        if response['result']['loaded_runtime']['process_id'] != process['pid']:
            raise ValueError('process changed')
        return ready(response['result'], response['server'], config['required_readers'], time.time_ns()//1_000_000)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        config = json.loads(args.config.read_text())
        result = inspect(config)
    except Exception:
        # No daemon exception, private path, credential or source text escapes.
        return 1
    print(json.dumps(dict(schema='dm.monitor.matrix-receiving/v1', ready=result)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
