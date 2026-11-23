#!/usr/bin/env python3
"""Authenticated, model-free application reader readiness for service_monitor.

Operator configuration supplies existing status authority, never new grants.
One replaceable read request is retained per local probe, not one per tick.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import socket
import stat
import sys
import time


class LocalHealthFailure(ValueError):
    pass


def check_local_health(config, descriptor, pid, now_ms, *, proc=Path('/proc')):
    expiry = descriptor.get('not_after_ms')
    if expiry is not None and expiry - now_ms <= 86400000:
        raise LocalHealthFailure('matrix_local_capability_expiring')
    listener = config.get('listener')
    if listener is None:
        return
    if (set(listener) != {'host', 'port'} or listener['host'] != '127.0.0.1'
            or type(listener['port']) is not int or not 1 <= listener['port'] <= 65535):
        raise ValueError('explicit IPv4 loopback listener required')
    # An ingress proxy can keep accepting TCP while its actual daemon is gone.
    # Match the authenticated daemon's own listening fd, not the proxy's port.
    fds = list((proc / str(pid) / 'fd').iterdir())
    if len(fds) > 4096:
        raise ValueError('bounded process descriptors required')
    sockets = set()
    for path in fds:
        try:
            sockets.add(os.readlink(path))
        except FileNotFoundError:
            continue  # A concurrent close is normal; an absent listener fails below.
    raw = (proc / str(pid) / 'net/tcp').read_bytes()
    if len(raw) > 262144:
        raise ValueError('bounded socket inventory required')
    address = f'0100007F:{listener["port"]:04X}'
    if not any(len(row) > 9 and row[1] == address and row[3] == '0A'
               and 'socket:[' + row[9] + ']' in sockets
               for row in (line.split() for line in raw.decode().splitlines()[1:])):
        raise LocalHealthFailure('matrix_listener_unavailable')


def reader_coverage(readers, required, audience):
    """Keep protected bodies and cover new members without reviving siblings."""
    coordinates = {(r['being_ref'], r['embodiment_id']) for r in readers}
    return (len(coordinates) == len(readers)
            and {tuple(row) for row in required} <= coordinates
            and {r['being_ref'] for r in audience} <= {row[0] for row in coordinates})



def roster_unavailable(status):
    """Recognize only the native public unavailable contract, not malformed data."""
    roster = status.get('receiving', {}).get('rosters', {}).get('tribe', {})
    if (isinstance(roster, dict) and set(roster) == {'schema', 'error', 'retryable'}
            and roster['schema'] == 'dm.runtime.tribe-roster-unavailable/v1'
            and roster['error'] in {'reader_roster_unavailable', 'reader_roster_invalid'}
            and type(roster['retryable']) is bool):
        return roster
    return None

def readiness_problem(status, server, required, now_ms):
    from daimon_matrix.identity import validity_contains, validate_validity
    loaded = status['loaded_runtime']
    gate = status['receiving']['channels']['tribe']
    roster = status['receiving']['rosters']['tribe']
    if gate.get('controlled') is not True or gate.get('phase') != 'select':
        return 'matrix_gate_not_selected'
    if loaded['local_origin'] != server or gate['process_id'] != loaded['process_id']:
        return 'matrix_runtime_binding_changed'
    if roster_unavailable(status) is not None:
        return 'matrix_roster_unavailable'
    readers = gate['readers']
    if not reader_coverage(readers, required, roster['audience']):
        return 'matrix_reader_coverage_incomplete'
    audience = {(r['being_ref'], r['embodiment_id'], r['credential_id']) for r in roster['audience']}
    epochs = set(roster['required_authority_epochs'])
    for reader in readers:
        if (reader.get('schema') != 'dm.receiving.reader/v2'
                or not validity_contains(validate_validity(reader['validity']), now_ms)
                or not epochs <= set(reader['authority_epochs'])
                or not {'dm.tribe.conversation/v1', 'dm.tribe.direct-conversation/v1'} <= set(reader['wire_read'])
                or (reader['being_ref'], reader['embodiment_id'], reader['credential_id']) not in audience):
            return 'matrix_reader_authority_unavailable'
    own = [r for r in readers if r['embodiment_id'] == server['embodiment_id']]
    # The native gate binds this live process and authenticated reader contract.
    # Preserve the installed compatible-startup recovery: software/view hashes
    # describe the retained selection, not necessarily today's package bytes.
    return None if len(own) == 1 else 'matrix_reader_coverage_incomplete'


def ready(status, server, required, now_ms):
    return readiness_problem(status, server, required, now_ms) is None


def retain_failure(config, diagnostic, error):
    """Keep one bounded private cause classification, never exception text."""
    stages = {'sdk_import', 'status_authority', 'probe_state', 'peer_connect',
        'status_request', 'status_send', 'response_authentication', 'listener_check',
        'process_verify', 'reader_check'}
    try:
        root = Path(config['state'])
        info = root.lstat()
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077):
            return
        kind = ('probe_busy' if isinstance(error, BlockingIOError) else
            'status_timeout' if isinstance(error, TimeoutError) else
            'local_transport' if isinstance(error, OSError) else 'verification_failed')
        value = dict(schema='dm.monitor.matrix-probe-failure/v1',
            observed_at_ms=time.time_ns()//1_000_000, kind=kind,
            stage=diagnostic.get('stage') if diagnostic.get('stage') in stages else 'unknown')
        if diagnostic.get('refusal_code') in {'verification_failed', 'permission_denied',
                'invalid_params', 'busy', 'deadline_exceeded'}:
            value['refusal_code'] = diagnostic['refusal_code']
        error_type = type(error).__name__
        if error_type in {'KeyError', 'ValueError', 'TypeError', 'AttributeError',
                'BlockingIOError', 'TimeoutError', 'ConnectionRefusedError', 'FileNotFoundError',
                'PermissionError', 'RuntimeError'}:
            value['exception_type'] = error_type
        if isinstance(error, KeyError) and len(error.args) == 1 and error.args[0] in {
                'loaded_runtime', 'local_origin', 'process_id', 'receiving', 'channels',
                'tribe', 'rosters', 'readers', 'audience', 'required_authority_epochs',
                'authority_epochs', 'schema', 'being_ref', 'embodiment_id', 'credential_id',
                'validity', 'wire_read', 'not_before_ms', 'not_after_ms'}:
            value['contract_field'] = error.args[0]
        if diagnostic.get('roster_error') in {'reader_roster_unavailable', 'reader_roster_invalid'}:
            value['roster_error'] = diagnostic['roster_error']
            if type(diagnostic.get('retryable')) is bool:
                value['retryable'] = diagnostic['retryable']
            if diagnostic.get('status_reads') in range(1, 6):
                value['status_reads'] = diagnostic['status_reads']
            if type(diagnostic.get('recovered_within_read')) is bool:
                value['recovered_within_read'] = diagnostic['recovered_within_read']
        raw = json.dumps(value).encode()
        temporary = root/'probe-failure.tmp'
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as stream:
            opened = os.fstat(stream.fileno())
            if (not stat.S_ISREG(opened.st_mode) or opened.st_uid != os.getuid()
                    or opened.st_nlink != 1 or opened.st_mode & 0o077):
                return
            stream.truncate(0)
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, root/'probe-failure.json')
    except (OSError, ValueError, TypeError, KeyError):
        pass  # Diagnostics must not replace the original readiness failure.


def read_status(client, request, process, diagnostic):
    backoff = (0.1, 0.3, 0.7, 1.2)
    for attempt in range(len(backoff) + 1):
        diagnostic['stage'] = 'status_send'
        response = client.send(request)
        diagnostic['stage'] = 'response_authentication'
        if response.get('ok') is not True:
            failure = response.get('error')
            if isinstance(failure, dict):
                diagnostic['refusal_code'] = failure.get('code')
            raise ValueError('authenticated status unavailable')
        if response['result']['loaded_runtime']['process_id'] != process['pid']:
            raise ValueError('process changed')
        unavailable = roster_unavailable(response['result'])
        if unavailable is None:
            break
        diagnostic.update(stage='reader_check', roster_error=unavailable['error'],
            retryable=unavailable['retryable'], status_reads=attempt+1,
            recovered_within_read=False)
        if (attempt < len(backoff) and unavailable['error'] == 'reader_roster_unavailable'
                and unavailable['retryable'] is True):
            # The same client keeps its original absolute eight-second
            # deadline. Repeat only this authenticated read, never input,
            # a native action, service control or provider/model request.
            time.sleep(backoff[attempt])
            continue
        break
    return response, attempt+1


def acquire_probe_lock(lock):
    # An overlapping operator read is not evidence that Matrix is down.
    deadline = time.monotonic() + 1.5
    while True:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise LocalHealthFailure('matrix_probe_contended') from None
            time.sleep(min(0.05, remaining))


def observe_peer(address, observer):
    # Establish the actual peer before selecting the retained read token.
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(2)
        try:
            connection.connect(address)
        except FileNotFoundError:
            raise LocalHealthFailure('matrix_status_socket_missing') from None
        except ConnectionRefusedError:
            raise LocalHealthFailure('matrix_status_socket_refused') from None
        except TimeoutError:
            raise LocalHealthFailure('matrix_status_socket_timeout') from None
        with observer(connection):
            return observer.process


def inspect(config, diagnostic=None):
    diagnostic = {} if diagnostic is None else diagnostic
    diagnostic['stage'] = 'sdk_import'
    sys.path.insert(0, config['operator'])
    from clusterctl.owner_process import ConnectedProcessObserver
    from clusterctl.onboarding_release import regular
    from daimon_matrix.client import ClientConfig, LocalClient
    from daimon_matrix.operator_capabilities import host_capability_profile
    diagnostic['stage'] = 'status_authority'
    root = Path(config['root'])
    profile = host_capability_profile('status')
    directory = root / profile['client_directory']
    cfg = ClientConfig.load(directory / profile['client_config_filename'],
                           bytearray(regular(directory / profile['client_key_filename'], uid=os.getuid())))
    diagnostic['stage'] = 'probe_state'
    state = Path(config['state'])
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = state.lstat()
    if state.is_symlink() or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError('private probe state required')
    with (state / 'lock').open('a+b') as lock, ConnectedProcessObserver() as observer:
        acquire_probe_lock(lock)
        diagnostic['stage'] = 'peer_connect'
        process = observe_peer(config['socket'], observer)
        client = LocalClient(Path(config['socket']), cfg, connection_observer=observer,
                             timeout_seconds=8, deadline_ns=time.monotonic_ns()+8_000_000_000)
        binding = dict(process=process, server=dict(cfg.expected_server),
                       capability=cfg.capability.descriptor, runtime_id=cfg.runtime_id, view='receiving')
        diagnostic['stage'] = 'status_request'
        path = state / 'status-request.json'
        previous = json.loads(regular(path, uid=os.getuid())) if path.exists() else None
        if previous is not None and previous['binding'] == binding:
            request = previous['request']
        else:
            request = client.prepare('runtime.status', {'view':'receiving'})
            raw = json.dumps(dict(binding=binding, request=request)).encode()
            if len(raw) > 65536:
                raise ValueError('bounded status request required')
            temporary = state / 'status-request.tmp'
            fd = os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(raw); stream.flush(); os.fsync(stream.fileno())
            os.replace(temporary, path)
        if request['method'] != 'runtime.status' or request['params'] != {'view':'receiving'}:
            raise ValueError('status-only request required')
        response, status_reads = read_status(client, request, process, diagnostic)
        diagnostic['stage'] = 'listener_check'
        check_local_health(config, cfg.capability.descriptor, process['pid'], time.time_ns()//1_000_000)
        diagnostic['stage'] = 'process_verify'
        observer.verify()
        diagnostic['stage'] = 'reader_check'
        problem = readiness_problem(response['result'], response['server'],
                                    config['required_readers'], time.time_ns()//1_000_000)
        result = problem is None
        if 'roster_error' in diagnostic:
            diagnostic['status_reads'] = status_reads
            diagnostic['recovered_within_read'] = result
            retain_failure(config, diagnostic, ValueError())
        if problem and roster_unavailable(response['result']) is None:
            raise LocalHealthFailure(problem)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    config, diagnostic = {}, {}
    try:
        config = json.loads(args.config.read_text())
        result = inspect(config, diagnostic)
    except LocalHealthFailure as failure:
        print(json.dumps(dict(schema='dm.monitor.matrix-receiving/v2', state=str(failure))))
        return 0
    except Exception as error:
        # No daemon exception, private path, credential or source text escapes.
        retain_failure(config, diagnostic, error)
        return 1
    if not result and diagnostic.get('roster_error'):
        state = {
            'reader_roster_invalid': 'matrix_roster_invalid',
            'reader_roster_unavailable': (
                'matrix_roster_contended' if diagnostic.get('retryable')
                else 'matrix_roster_unavailable'
            ),
        }[diagnostic['roster_error']]
        print(json.dumps(dict(schema='dm.monitor.matrix-receiving/v2', state=state)))
    else:
        print(json.dumps(dict(schema='dm.monitor.matrix-receiving/v1', ready=result)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
