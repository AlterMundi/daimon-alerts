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
import select
import stat
import subprocess
import sys
import tempfile
import time
import uuid

SCHEMA='hmk.capture-attempt/v1'
EVENTS={'Stop','SessionEnd','SessionStart','UserPromptSubmit','PostToolUse','PreCompact','PostCompact','Interrupt'}
EXCLUSIONS={'unselected_working_directory','unselected_session_working_directory','session_precedes_selected_boundary'}
REASONS={'hook_timeout','input_timeout','hook_uncertain','handler_failed','receipt_unavailable','invalid_payload','binding_unreadable','binding_changed',
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
    if a.get('exclusion_reason') is not None and a['exclusion_reason'] not in EXCLUSIONS:
        raise ValueError('invalid capture exclusion')
    if a.get('resolution_kind') not in (None,'source_captured','source_excluded'):
        raise ValueError('invalid capture resolution')
    if a.get('resolution_kind')=='source_excluded' and (a['state']!='resolved' or a.get('exclusion_reason') not in EXCLUSIONS):
        raise ValueError('verified scope exclusion required')
    return a

def directory(path):
    path=Path(path).absolute()
    if any(p.is_symlink() for p in (path,*path.parents)):raise ValueError('private capture destination required')
    path.mkdir(parents=True,exist_ok=True,mode=0o700)
    info=path.stat()
    if info.st_uid!=os.getuid() or info.st_mode&0o077:raise ValueError('private capture destination required')
    return path

def begin(root,binding_hash,timeout,*,event=None,source_ref=None):
    if not re.fullmatch('[a-f0-9]{64}',binding_hash) or not 0.1<=timeout<=30:
        raise ValueError('selected binding and bounded deadline required')
    if event not in EVENTS|{None} or (source_ref is not None and not re.fullmatch('[a-f0-9]{64}',source_ref)):
        raise ValueError('validated capture identity required')
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
            started_ns=now,updated_ns=now,deadline_ns=now+int((timeout+2)*1e9),state='started',reason=None,event=event,source_ref=source_ref)
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
    if safe['skipped'] and receipt.get('reason') in EXCLUSIONS:
        safe['exclusion_reason']=receipt['reason']
    atomic(Path(args.child_receipt),safe)
    print(json.dumps({} if safe['capture_complete'] or safe['skipped'] else
        {'systemMessage':'HMK source capture deferred: '+safe['reason']}))

def input_bytes(fd,timeout):
    """Bounded native input, including a producer that never closes its pipe."""
    deadline=time.monotonic()+min(timeout,1.0);raw=bytearray()
    while len(raw)<=65536:
        remaining=deadline-time.monotonic()
        if remaining<=0 or not select.select([fd],[],[],remaining)[0]:raise TimeoutError()
        part=os.read(fd,65537-len(raw))
        if not part:return bytes(raw)
        raw.extend(part)
    return bytes(raw)

def prepare_input(args):
    """Read metadata only; no capture/import happens before the durable intent."""
    try:binding_bytes=Path(args.binding).read_bytes()
    except OSError:return b'',None,None,'binding_unreadable'
    if hashlib.sha256(binding_bytes).hexdigest()!=args.binding_sha256:
        return b'',None,None,'binding_changed'
    try:raw=input_bytes(sys.stdin.fileno(),args.timeout_seconds)
    except TimeoutError:return b'',None,None,'input_timeout'
    try:
        if len(raw)>65536:raise ValueError()
        payload=json.loads(raw);binding=json.loads(binding_bytes)
        if (not isinstance(payload,dict) or not isinstance(binding,dict) or
                not isinstance(binding.get('allowed_cwds'),list)):raise ValueError()
    except (ValueError,UnicodeError):return raw,None,None,'invalid_payload'
    event=source_ref=None
    # Do not retain metadata from unselected working directories.
    if payload.get('cwd') in binding.get('allowed_cwds',[]):
        candidate=payload.get('hook_event_name');sid=payload.get('session_id')
        event=candidate if isinstance(candidate,str) and candidate in EVENTS else None
        if isinstance(sid,str) and 0<len(sid)<=128:
            source_ref=hashlib.sha256(sid.encode()).hexdigest()
    return raw,event,source_ref,None

def invoke(args):
    entered=time.monotonic()
    root=directory(args.state_dir)
    raw,event,source_ref,error=prepare_input(args)
    # Codex runs terminal hooks synchronously and kills their process group at
    # three seconds. Include input/intent overhead, and retain time to commit
    # the failure receipt instead of advertising an impossible 30-second child.
    terminal=event in {'SessionEnd','Interrupt'}
    child_timeout=min(args.timeout_seconds,1.5) if terminal else args.timeout_seconds
    path,attempt=begin(root,args.binding_sha256,child_timeout,event=event,source_ref=source_ref)
    output={};receipt_path=root/('.result-'+attempt['attempt_id']+'.json')
    try:
        if error:
            attempt.update(state='failed',reason=error);return finish(path,attempt)
        command=[sys.executable,'-B',str(Path(__file__).absolute()),'--child','--binding',args.binding,
                 '--binding-sha256',args.binding_sha256,'--handler-source',args.handler_source,'--child-receipt',str(receipt_path)]
        remaining=min(child_timeout,2.5-(time.monotonic()-entered)) if terminal else child_timeout
        if remaining<=0:
            attempt.update(state='failed',reason='hook_timeout');return finish(path,attempt)
        try:
            result=subprocess.run(command,input=raw,capture_output=True,timeout=remaining)
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
        if attempt['state']=='skipped' and receipt.get('exclusion_reason') in EXCLUSIONS:
            attempt['exclusion_reason']=receipt['exclusion_reason']
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
        excluded=resolved and a.get('resolution_kind')=='source_excluded'
        items.append(dict(incident_id='capture-'+a['attempt_id'],operation_id='capture-'+a['attempt_id'],phase='capture',
            severity='info' if resolved else 'error',problem_code='timeout' if reason in ('hook_timeout','input_timeout') else
                'dispatch_unresolved' if reason=='hook_uncertain' else 'observation_unreadable',
            summary='Source excluded by the unchanged capture binding; no source bytes admitted.' if excluded else 'Source capture restored after explicit recovery.' if resolved else 'Source capture not confirmed: '+reason.replace('_',' ')+'.',
            action='Preserve the verified exclusion evidence; no source intake or paid retry is required.' if excluded else 'Inspect the native capture hook and retained attempt; restore source capture explicitly. Do not assume the source was preserved.',
            source_at=a['started_ns']//1_000_000_000,updated_at=(a['deadline_ns'] if a['state']=='started' else a['updated_ns'])//1_000_000_000,
            processing_state='resolved' if resolved else 'open',work_preserved=resolved and not excluded))
    return dict(items=items)

def resolve(root,attempt_id,recovery_id,*,excluded=False):
    root=directory(root)
    if not all(re.fullmatch('[a-f0-9]{32}',x) for x in (attempt_id,recovery_id)):raise ValueError('retained attempt required')
    with open(root/'capture.lock','a+b') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        path=root/('attempt-'+attempt_id+'.json');owner=read(root/'owner.json')
        old=attempt_record(path,owner['binding_sha256']);recovery=attempt_record(root/('attempt-'+recovery_id+'.json'),owner['binding_sha256'])
        kind='source_excluded' if excluded else 'source_captured'
        if old['state']=='resolved' and old.get('recovery_attempt_id')==recovery_id and old.get('resolution_kind','source_captured')==kind:return
        valid_recovery=(recovery['state']=='skipped' and recovery.get('exclusion_reason') in EXCLUSIONS) if excluded else recovery['state']=='complete'
        if (old['state'] not in ('failed','started') or not valid_recovery or
                not old['source_ref'] or old['source_ref']!=recovery['source_ref'] or old['event']!=recovery['event'] or
                old['binding_sha256']!=recovery['binding_sha256'] or recovery['started_ns']<=old['started_ns'] or
                (old['state']=='started' and recovery['started_ns']<=old['deadline_ns'])):
            raise ValueError('confirmed matching source recovery required')
        if old['state']=='started':old['reason']='hook_uncertain'
        old.update(state='resolved',updated_ns=time.time_ns(),recovery_attempt_id=recovery_id,resolution_kind=kind)
        if excluded:old['exclusion_reason']=recovery['exclusion_reason']
        atomic(path,old)

def main():
    os.umask(0o077);p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--child',action='store_true');p.add_argument('--binding');p.add_argument('--handler-source');p.add_argument('--child-receipt')
    p.add_argument('--state-dir');p.add_argument('--binding-sha256');p.add_argument('--timeout-seconds',type=float,default=1.5)
    p.add_argument('--project',action='store_true');p.add_argument('--resolve-attempt');p.add_argument('--exclude-attempt');p.add_argument('--recovery-attempt')
    p.add_argument('--initialize',action='store_true')
    args=p.parse_args()
    try:
        if args.child:child(args);return 0
        if args.initialize:
            initialize(args.state_dir,args.binding_sha256)
            print(json.dumps({'supervision_initialized':True,'capture_invoked':False}));return 0
        if args.project:
            print(json.dumps(safe_projection(args.state_dir,args.binding_sha256)));return 0
        if args.resolve_attempt or args.exclude_attempt or args.recovery_attempt:
            if bool(args.resolve_attempt)==bool(args.exclude_attempt):raise ValueError('select one resolution kind')
            resolve(args.state_dir,args.resolve_attempt or args.exclude_attempt,args.recovery_attempt,excluded=bool(args.exclude_attempt))
            print(json.dumps({'capture_gap_resolved':True,'source_excluded':bool(args.exclude_attempt),'processing_invoked':False}));return 0
        return invoke(args)
    except Exception:
        print(json.dumps({'systemMessage':'HMK capture supervision unavailable; source preservation is unconfirmed.'}))
        return 1 if args.child or args.initialize or args.project or args.resolve_attempt or args.exclude_attempt or args.recovery_attempt else 0

if __name__=='__main__':raise SystemExit(main())
