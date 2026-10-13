"""Deterministic safe projection observer. Never invokes HMK processing or delivery."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import importlib
import json
import os
import re
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
from client import Client, Unavailable, private_text

CODES={'payment_required','quota_exhausted','authentication_failed','access_denied','rate_limited','service_unavailable','timeout','transport_error','invalid_request','response_incomplete','dispatch_unresolved','provider_error'}
PHASES={'capture','selection','organization','consolidation','index'}
ACTIONS={
    'worker_stopped':'Inspect the worker service and retained pending work; recovery is explicit.',
    'worker_stalled':'Inspect the retained worker dispatch before explicit recovery.',
    'observation_stale':'Restore the safe source observation route; processing state remains unknown.',
    'observation_unreadable':'Repair the safe observation route; preserve source journals.',
}

def atomic(path,value):
    import tempfile
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    fd,name=tempfile.mkstemp(dir=path.parent,prefix='.observation-')
    try:
        with os.fdopen(fd,'w') as stream:
            json.dump(value,stream); stream.flush(); os.fsync(stream.fileno())
        os.replace(name,path)
        fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    finally:
        if os.path.exists(name): os.unlink(name)

class ReadOnlyInbox:
    """Only the selected bound producer journal; never the canonical memory DB."""
    def __init__(self,path,binding): self.path=Path(path); self.binding=binding
    @contextmanager
    def connect(self):
        # Read-only URI deliberately refuses to create an absent journal.
        from urllib.parse import quote
        con=sqlite3.connect('file:'+quote(str(self.path.resolve()),safe='/')+'?mode=ro',uri=True,timeout=1)
        try:
            con.row_factory=sqlite3.Row
            rows=con.execute('SELECT value FROM binding').fetchall()
            if len(rows)!=1 or json.loads(rows[0][0])!=self.binding: raise ValueError('producer binding mismatch')
            yield con
        finally: con.close()

class Observer:
    def __init__(self,config):
        self.config=config; self.client=Client(config['base_url'],config['token_file'])
        self.state=Path(config['state_dir']); self.state.mkdir(mode=0o700,parents=True,exist_ok=True)
        if self.state.is_symlink() or self.state.stat().st_mode&0o077: raise ValueError('private observer state required')
        self.lock=open(self.state/'observer.lock','a+b'); fcntl.flock(self.lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        self.db=sqlite3.connect(self.state/'projection.sqlite'); self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS projections(id TEXT PRIMARY KEY,revision INTEGER,content TEXT,accepted INTEGER)')
        self.db.execute('CREATE TABLE IF NOT EXISTS binding(value TEXT PRIMARY KEY)')
        mode=config.get('worker_mode','continuous')
        scope=config.get('worker_systemd_scope','user')
        if mode not in ('continuous','oneshot') or scope not in ('user','system'):
            raise ValueError('unsupported worker supervision')
        unit=config.get('worker_unit')
        if unit and not re.fullmatch(r'[A-Za-z0-9_.@:-]+\.service',unit):
            raise ValueError('selected service unit required')
        if mode=='oneshot' and not unit:
            raise ValueError('oneshot supervision requires a service unit')
        daily=config.get('worker_daily_utc')
        if daily is not None:
            if mode!='oneshot' or not re.fullmatch(r'(?:[01][0-9]|2[0-3]):[0-5][0-9]',daily):
                raise ValueError('explicit daily UTC cadence required')
            grace=config.get('worker_cadence_grace_seconds')
            if type(grace) is not int or not 1<=grace<=86400:
                raise ValueError('bounded cadence grace required')
        selected={k:config.get(k) for k in ('producer_id','observer_id','receiving_scope','resource_ref','inbox','inbox_binding','worker_state_dir','worker_unit','base_url')}
        # Preserve old default bindings; changed semantics need a new state dir.
        if mode!='continuous': selected['worker_mode']=mode
        if scope!='user': selected['worker_systemd_scope']=scope
        if daily is not None:
            selected['worker_daily_utc']=daily
            selected['worker_cadence_grace_seconds']=config['worker_cadence_grace_seconds']
        binding=json.dumps(selected,sort_keys=True)
        prior=self.db.execute('SELECT value FROM binding').fetchone()
        if prior and prior[0]!=binding: raise ValueError('observer binding changed; explicit reconciliation required')
        with self.db: self.db.execute('INSERT OR IGNORE INTO binding VALUES(?)',(binding,))
        sys.path.insert(0,str(Path(config['hmk_source'])/'scripts'))
        self.provider=importlib.import_module('provider_status')
        if not hasattr(self.provider,'safe_projection'): raise ValueError('qualified safe HMK projection required')
        self.inbox=ReadOnlyInbox(config['inbox'],config['inbox_binding'])
    def project(self,fields):
        value=dict(schema='daimon-alerts/v1',producer_id=self.config['producer_id'],receiving_scope=self.config['receiving_scope'],resource_ref=self.config['resource_ref'],**fields)
        content=json.dumps(value,sort_keys=True,separators=(',',':'))
        row=self.db.execute('SELECT revision,content,accepted FROM projections WHERE id=?',(value['incident_id'],)).fetchone()
        if row and row[1]==content: revision=row[0]; accepted=bool(row[2])
        else:
            revision=row[0]+1 if row else 1; accepted=False
            with self.db: self.db.execute('INSERT INTO projections VALUES(?,?,?,0) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,content=excluded.content,accepted=0',(value['incident_id'],revision,content))
        if not accepted:
            value['revision']=revision
            result=self.client.call('PUT',f"/v1/incidents/{value['producer_id']}/{value['incident_id']}",value)
            if result.get('revision')!=revision or result.get('incident_id')!=value['incident_id']: raise Unavailable()
            with self.db: self.db.execute('UPDATE projections SET accepted=1 WHERE id=? AND revision=?',(value['incident_id'],revision))
        return revision
    def source_state(self):
        source=None
        self.observation_kind=None
        try:
            properties=None
            unit=self.config.get('worker_unit')
            if unit:
                command=['systemctl']
                if self.config.get('worker_systemd_scope','user')=='user': command.append('--user')
                command.extend(['show',unit,'--property=LoadState,Type,ActiveState,Result,ExecMainCode,ExecMainStatus'])
                observed=subprocess.run(command,capture_output=True,text=True,timeout=2)
                if observed.returncode and not observed.stdout: return 'observation_unreadable',source
                properties=dict(line.split('=',1) for line in observed.stdout.splitlines() if '=' in line)
            path=Path(self.config['worker_state_dir'])/'worker-status.json'
            if not path.exists():
                if properties and properties.get('LoadState')=='loaded' and properties.get('ActiveState')=='failed':
                    self.observation_kind='launch_failure'
                    return 'worker_stopped',source
                self.observation_kind='unobserved'
                return 'observation_unreadable',source
            raw=path.read_bytes()
            if len(raw)>16384: raise ValueError()
            worker=json.loads(raw)
            if worker.get('schema')!='hmk.worker-status/v1' or type(worker.get('updated_ns')) is not int or worker['updated_ns']<0: raise ValueError()
            source=worker['updated_ns']//1000000000
            if source>int(time.time())+60: raise ValueError()
            if unit:
                if properties.get('LoadState')!='loaded': return 'observation_unreadable',source
                active=properties.get('ActiveState')
                if self.config.get('worker_mode','continuous')=='oneshot':
                    if properties.get('Type')!='oneshot': return 'observation_unreadable',source
                    if active not in ('active','activating'):
                        # Successful inactive units may be garbage-collected and
                        # reloaded with code 0: the fresh HMK receipt is required.
                        # Exit 2 preserves pending work, not a lost worker.
                        success=(properties.get('Result')=='success' and
                                 properties.get('ExecMainStatus')=='0' and
                                 properties.get('ExecMainCode') in ('0','1'))
                        pending=(properties.get('Result')=='exit-code' and
                                 properties.get('ExecMainCode')=='1' and
                                 properties.get('ExecMainStatus')=='2' and
                                 worker.get('state')=='pending')
                        completed=((success or pending) and
                                   active in ('inactive','failed') and
                                   worker.get('state') in ('pending','idle'))
                        if not completed: return 'worker_stopped',source
                elif active not in ('active','activating'): return 'worker_stopped',source
            age=time.time()-source
            if worker.get('state')=='running' and age>self.config['stalled_seconds']: return 'worker_stalled',source
            if age>self.config['source_freshness_seconds']: return 'observation_stale',source
            if worker.get('state') not in ('running','pending','idle'): raise ValueError()
            daily=self.config.get('worker_daily_utc')
            if daily is not None:
                hour,minute=map(int,daily.split(':'))
                now=time.time(); scheduled=int(now)//86400*86400+hour*3600+minute*60
                if now<scheduled+self.config['worker_cadence_grace_seconds']: scheduled-=86400
                started=worker.get('started_ns')
                if type(started) is not int or started<0 or started>time.time_ns()+60_000_000_000:
                    return 'observation_unreadable',source
                if started//1_000_000_000<scheduled:
                    self.observation_kind='missed_invocation'
                    return 'observation_stale',source
            return 'healthy',source
        except (OSError,ValueError,subprocess.TimeoutExpired): return 'observation_unreadable',source
    def observation_incident(self,code,source):
        # Persist recurrence identity separately; ticks do not create new alerts.
        path=self.state/'source-incident.json'
        prior=json.loads(path.read_text()) if path.exists() else None
        if code=='healthy':
            if prior and prior['processing_state']=='open':
                prior.update(processing_state='resolved',updated_at=int(time.time()),summary='Safe worker observation restored.',severity='info')
                atomic(path,prior); self.project(prior)
            elif prior: self.project(prior)
            return
        detail=getattr(self,'observation_kind',None)
        summary={'unobserved':'HMK worker not started or unobserved: no first worker receipt.',
                 'launch_failure':'HMK worker launch failed before publishing a receipt.',
                 'missed_invocation':'HMK worker missed its selected daily invocation.'}.get(detail,
                 'Worker observation requires attention: '+code.replace('_',' ')+'.')
        if prior and prior['processing_state']=='open' and prior['problem_code']==code:
            if prior['summary']!=summary:
                prior.update(summary=summary,updated_at=int(time.time()))
                atomic(path,prior)
            self.project(prior); return
        if prior and prior['processing_state']=='open':
            prior.update(processing_state='resolved',updated_at=int(time.time()),summary='Observation failure replaced by a newer diagnosed state.',severity='info')
            self.project(prior)
        import uuid
        now=int(time.time())
        value=dict(incident_id='observation-'+uuid.uuid4().hex,operation_id=self.config['observer_id'],phase='observation',severity='error',problem_code=code,summary=summary,action=ACTIONS[code],source_at=now,updated_at=now,processing_state='open',work_preserved=True)
        atomic(path,value); self.project(value)
    def tick(self):
        code,source=self.source_state(); accepted=0; service='available'
        try:
            cursor=0
            # HMK initializes its own outbox at first invocation. A bound
            # journal without that table is expected before its first receipt.
            with self.inbox.connect() as con:
                outbox=bool(con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='operational_alerts'").fetchone())
            if not outbox and self.observation_kind not in ('unobserved','launch_failure'):
                raise ValueError('missing initialized operational outbox')
            # Bounded source window; next tick resumes with stable accepted rows.
            for _ in range(10):
                page=self.provider.safe_projection(self.inbox,after=cursor,limit=100) if outbox else dict(items=[],next_cursor=None)
                for record in page['items']:
                    self.project(record); accepted+=1
                cursor=page['next_cursor']
                if cursor is None: break
            else: raise ValueError('source projection capacity exceeded')
        except (OSError,ValueError,sqlite3.Error):
            code='observation_unreadable'; self.observation_kind=None
        except Unavailable: service='unavailable'
        try:
            if service=='available':
                self.observation_incident(code,source)
                self.client.call('PUT','/v1/observers/'+self.config['observer_id'],dict(receiving_scope=self.config['receiving_scope'],source_at=source,source_state=code))
        except Unavailable: service='unavailable'
        result=dict(schema='daimon-alerts.observer/v1',observed_at=int(time.time()),source_state=code,source_at=source,service_state=service,accepted_projections=accepted)
        if self.observation_kind is not None: result['observation_kind']=self.observation_kind
        atomic(self.state/'status.json',result)
        return result

def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser(); parser.add_argument('--config',required=True); parser.add_argument('--once',action='store_true'); args=parser.parse_args()
    try:
        config=json.loads(private_text(args.config)); observer=Observer(config)
        while True:
            result=observer.tick()
            if args.once: print(json.dumps(result)); return
            time.sleep(max(2,min(60,config.get('interval_seconds',10))))
    except Exception: raise SystemExit('safe observer failed; inspect private source/binding and retained state') from None
if __name__=='__main__': main()
