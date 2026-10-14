"""Bounded synchronous HMK capture supervision, independent of its source inbox.

Persist intent before invoking the existing capture API. Never stores source,
payloads, paths, exception text or credentials in an attempt projection.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import time
import uuid

SCHEMA='hmk.capture-attempt/v1'
EVENTS={'Stop','SessionEnd','SessionStart','UserPromptSubmit','PostToolUse','PreCompact','PostCompact','Interrupt'}
REASONS={'hook_timeout','hook_uncertain','handler_failed','receipt_unavailable','invalid_payload','binding_unreadable','binding_changed',
         'transcript_unavailable','source_exceeds_capture_limit','source_changed_during_capture','source_header_incomplete',
         'session_scope_unverified','transcript_missing','capture_deferred'}

def atomic(path,value):
    fd,name=tempfile.mkstemp(dir=path.parent,prefix='.capture-')
    try:
        with os.fdopen(fd,'w') as stream:
            json.dump(value,stream);stream.flush();os.fsync(stream.fileno())
        os.replace(name,path)
        directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:
        if os.path.exists(name):os.unlink(name)

def read(path):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd) as stream:
        info=os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode&0o077:
            raise ValueError('private regular capture record required')
        raw=stream.read(16385)
        if len(raw)>16384:raise ValueError('bounded capture record required')
        return json.loads(raw)

def attempt_record(path,expected_binding_hash):
    a=read(path)
    if (not isinstance(a,dict) or a.get('schema')!=SCHEMA or a.get('binding_sha256')!=expected_binding_hash or
            not isinstance(a.get('attempt_id'),str) or not re.fullmatch('[a-f0-9]{32}',a['attempt_id']) or
            path.name!='attempt-'+a['attempt_id']+'.json' or
            a.get('state') not in ('started','failed','complete','skipped','resolved') or
            a.get('reason') not in REASONS|{None} or a.get('event') not in EVENTS|{None}):
        raise ValueError('capture receipt mismatch')
    for key in ('started_ns','updated_ns','deadline_ns'):
        if type(a.get(key)) is not int or a[key]<0:raise ValueError('invalid capture timestamp')
    if (a['updated_ns']<a['started_ns'] or a['deadline_ns']<a['started_ns'] or
            a['deadline_ns']-a['started_ns']>32_000_000_000 or a['updated_ns']>time.time_ns()+60_000_000_000):
        raise ValueError('invalid capture timestamp')
    if a.get('source_ref') is not None and (not isinstance(a['source_ref'],str) or not re.fullmatch('[a-f0-9]{64}',a['source_ref'])):
        raise ValueError('invalid capture source reference')
    return a

def directory(path):
    path=Path(path).absolute()
    if any(p.is_symlink() for p in (path,*path.parents)):raise ValueError('private capture destination required')
    path.mkdir(parents=True,exist_ok=True,mode=0o700)
    info=path.stat()
    if info.st_uid!=os.getuid() or info.st_mode&0o077:raise ValueError('private capture destination required')
    return path

def begin(root,binding_hash,timeout):
    if not re.fullmatch('[a-f0-9]{64}',binding_hash) or not 0.1<=timeout<=30:
        raise ValueError('selected binding and bounded deadline required')
    with open(root/'capture.lock','a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        owner=root/'owner.json'
        if owner.exists():
            if read(owner)!=dict(binding_sha256=binding_hash):raise ValueError('capture binding changed')
        else:atomic(owner,dict(binding_sha256=binding_hash))
        paths=sorted(root.glob('attempt-*.json'),key=lambda p:p.name)
        # Keep unresolved attempts. A full ledger fails visibly instead of losing gaps.
        for path in paths:
            if len(paths)<128:break
            prior=read(path)
            if prior['state'] in ('complete','skipped') or (prior['state']=='resolved' and time.time_ns()-prior['updated_ns']>86400_000_000_000):
                path.unlink();paths.remove(path)
        if len(paths)>=128:raise ValueError('capture supervision capacity requires attention')
        now=time.time_ns();attempt=dict(schema=SCHEMA,attempt_id=uuid.uuid4().hex,binding_sha256=binding_hash,
            started_ns=now,updated_ns=now,deadline_ns=now+int((timeout+2)*1e9),state='started',reason=None,event=None,source_ref=None)
        path=root/('attempt-'+attempt['attempt_id']+'.json');atomic(path,attempt)
        return path,attempt

def initialize(root,binding_hash):
    if not re.fullmatch('[a-f0-9]{64}',binding_hash):raise ValueError('selected binding digest required')
    root=directory(root)
    with open(root/'capture.lock','a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        owner=root/'owner.json'
        expected=dict(binding_sha256=binding_hash)
        if owner.exists():
            if read(owner)!=expected:raise ValueError('capture binding changed')
        else:atomic(owner,expected)

def child(args):
    # Import only the existing selected HMK handler, not memory/models or a daemon.
    sys.path.insert(0,str(Path(args.handler_source)/'scripts'))
    from codex_hook_capture import capture
    raw=Path(args.binding).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=args.binding_sha256:raise ValueError('capture binding changed')
    binding=json.loads(raw);payload=json.load(sys.stdin)
    receipt=capture(binding,payload)
    safe=dict(capture_complete=receipt['capture_complete'],skipped=bool(receipt.get('skipped')),
              reason=receipt.get('error') if receipt.get('error') in REASONS else 'capture_deferred')
    atomic(Path(args.child_receipt),safe)
    print(json.dumps({} if safe['capture_complete'] or safe['skipped'] else
        {'systemMessage':'HMK source capture deferred: '+safe['reason']}))

def invoke(args):
    root=directory(args.state_dir);path,attempt=begin(root,args.binding_sha256,args.timeout_seconds)
    output={};receipt_path=root/('.result-'+attempt['attempt_id']+'.json')
    try:
        try:binding_bytes=Path(args.binding).read_bytes()
        except OSError:
            attempt.update(state='failed',reason='binding_unreadable');return finish(path,attempt)
        if hashlib.sha256(binding_bytes).hexdigest()!=args.binding_sha256:
            attempt.update(state='failed',reason='binding_changed');return finish(path,attempt)
        raw=sys.stdin.buffer.read(65537)
        try:
            if len(raw)>65536:raise ValueError()
            payload=json.loads(raw);binding=json.loads(binding_bytes)
            if not isinstance(payload,dict):raise ValueError()
        except (ValueError,UnicodeError):
            attempt.update(state='failed',reason='invalid_payload');return finish(path,attempt)
        # Unselected events carry no retained session metadata.
        if payload.get('cwd') in binding.get('allowed_cwds',[]):
            event=payload.get('hook_event_name');sid=payload.get('session_id')
            attempt['event']=event if event in EVENTS else None
            if isinstance(sid,str) and 0<len(sid)<=128:
                attempt['source_ref']=hashlib.sha256(sid.encode()).hexdigest()
        atomic(path,attempt)
        command=[sys.executable,'-B',str(Path(__file__).absolute()),'--child','--binding',args.binding,
                 '--binding-sha256',args.binding_sha256,'--handler-source',args.handler_source,'--child-receipt',str(receipt_path)]
        try:
            result=subprocess.run(command,input=raw,capture_output=True,timeout=args.timeout_seconds)
        except subprocess.TimeoutExpired:
            attempt.update(state='failed',reason='hook_timeout');return finish(path,attempt)
        if result.returncode:
            attempt.update(state='failed',reason='handler_failed');return finish(path,attempt)
        try:
            receipt=read(receipt_path);output=json.loads(result.stdout)
            if not isinstance(output,dict):raise ValueError()
        except (OSError,ValueError):
            attempt.update(state='failed',reason='receipt_unavailable');return finish(path,attempt)
        attempt.update(state='complete' if receipt['capture_complete'] else 'skipped' if receipt['skipped'] else 'failed',
            reason=None if receipt['capture_complete'] or receipt['skipped'] else receipt['reason'])
        return finish(path,attempt,output)
    finally:
        receipt_path.unlink(missing_ok=True)

def finish(path,attempt,output=None):
    attempt['updated_ns']=time.time_ns();atomic(path,attempt)
    if output is None:output={'systemMessage':'HMK source capture not confirmed: '+attempt['reason']}
    print(json.dumps(output));return 0

def safe_projection(root,expected_binding_hash):
    """Read-only gap projection; a killed supervisor leaves durable uncertainty."""
    root=Path(root)
    if read(root/'owner.json')!=dict(binding_sha256=expected_binding_hash):raise ValueError('capture binding mismatch')
    paths=sorted(root.glob('attempt-*.json'))
    if len(paths)>128:raise ValueError('capture ledger capacity exceeded')
    items=[]
    for path in paths:
        try:a=attempt_record(path,expected_binding_hash)
        except FileNotFoundError:continue # Concurrent pruning of terminal success.
        if a['state'] in ('complete','skipped'):continue
        if a['state']=='started' and time.time_ns()<a['deadline_ns']:continue
        if a['state'] not in ('started','failed','resolved'):raise ValueError('unsupported capture state')
        reason=a['reason'] if a['state']!='started' else 'hook_uncertain'
        if reason not in REASONS|{'hook_uncertain'}:raise ValueError('unsupported capture reason')
        resolved=a['state']=='resolved'
        items.append(dict(incident_id='capture-'+a['attempt_id'],operation_id='capture-'+a['attempt_id'],phase='capture',
            severity='info' if resolved else 'error',problem_code='timeout' if reason=='hook_timeout' else
                'dispatch_unresolved' if reason=='hook_uncertain' else 'observation_unreadable',
            summary='Source capture restored after explicit recovery.' if resolved else 'Source capture not confirmed: '+reason.replace('_',' ')+'.',
            action='Inspect the native capture hook and retained attempt; restore source capture explicitly. Do not assume the source was preserved.',
            source_at=a['started_ns']//1_000_000_000,updated_at=(a['deadline_ns'] if a['state']=='started' else a['updated_ns'])//1_000_000_000,
            processing_state='resolved' if resolved else 'open',work_preserved=resolved))
    return dict(items=items)

def resolve(root,attempt_id,recovery_id):
    root=directory(root)
    if not all(re.fullmatch('[a-f0-9]{32}',x) for x in (attempt_id,recovery_id)):raise ValueError('retained attempt required')
    with open(root/'capture.lock','a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        path=root/('attempt-'+attempt_id+'.json');owner=read(root/'owner.json')
        old=attempt_record(path,owner['binding_sha256']);recovery=attempt_record(root/('attempt-'+recovery_id+'.json'),owner['binding_sha256'])
        if old['state']=='resolved' and old.get('recovery_attempt_id')==recovery_id:return
        if (old['state'] not in ('failed','started') or recovery['state']!='complete' or
                not old['source_ref'] or old['source_ref']!=recovery['source_ref'] or old['event']!=recovery['event'] or
                old['binding_sha256']!=recovery['binding_sha256'] or recovery['started_ns']<=old['started_ns'] or
                (old['state']=='started' and recovery['started_ns']<=old['deadline_ns'])):
            raise ValueError('confirmed matching source recovery required')
        if old['state']=='started':old['reason']='hook_uncertain'
        old.update(state='resolved',updated_ns=time.time_ns(),recovery_attempt_id=recovery_id);atomic(path,old)

def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--child',action='store_true');p.add_argument('--binding');p.add_argument('--handler-source');p.add_argument('--child-receipt')
    p.add_argument('--state-dir');p.add_argument('--binding-sha256');p.add_argument('--timeout-seconds',type=float,default=1.5)
    p.add_argument('--project',action='store_true');p.add_argument('--resolve-attempt');p.add_argument('--recovery-attempt')
    p.add_argument('--initialize',action='store_true')
    args=p.parse_args()
    try:
        if args.child:child(args);return 0
        if args.initialize:
            initialize(args.state_dir,args.binding_sha256)
            print(json.dumps({'supervision_initialized':True,'capture_invoked':False}));return 0
        if args.project:
            print(json.dumps(safe_projection(args.state_dir,args.binding_sha256)));return 0
        if args.resolve_attempt or args.recovery_attempt:
            resolve(args.state_dir,args.resolve_attempt,args.recovery_attempt)
            print(json.dumps({'capture_gap_resolved':True,'processing_invoked':False}));return 0
        return invoke(args)
    except Exception:
        print(json.dumps({'systemMessage':'HMK capture supervision unavailable; source preservation is unconfirmed.'}))
        return 1 if args.child or args.initialize or args.project or args.resolve_attempt or args.recovery_attempt else 0

if __name__=='__main__':raise SystemExit(main())
