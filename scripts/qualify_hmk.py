"""Explicit fictional HMK credit failure/recovery using existing producer APIs.

Never selects a live memory binding or provider credential. No network/model calls.
The real receiving path is qualified separately by Telecodex's owning adapter.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import time


def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser(); parser.add_argument('operation',choices=['failure','recover','status']); parser.add_argument('--hmk-source',type=Path,required=True); parser.add_argument('--state-dir',type=Path,required=True); args=parser.parse_args()
    state=args.state_dir.resolve(); state.mkdir(parents=True,exist_ok=True,mode=0o700)
    if state.is_symlink() or state.stat().st_mode&0o077: raise ValueError('private qualification directory required')
    canonical=state/'fictional-corpus'; canonical.mkdir(mode=0o700,exist_ok=True)
    os.environ['HMK_AGENT_MEMORY_BASE']=str(canonical); os.environ['HMK_DB_PATH']=str(canonical/'library.sqlite')
    scripts=args.hmk_source.resolve()/'scripts';sys.path.insert(0,str(scripts))
    spec=importlib.util.spec_from_file_location('qualified_fictional_memory',scripts/'memoryctl.py')
    memory=importlib.util.module_from_spec(spec); spec.loader.exec_module(memory); memory.init_db()
    from capture_inbox import Inbox
    from portable_producer import process_once
    from provider_status import Alerts,ProviderProblem
    binding={'subject':'fictional:alerts-qualification','source_instance':'fictional:receiving-test'}
    inbox=Inbox(state/'capture.sqlite',binding)
    raw=(json.dumps({'type':'response_item','payload':{'type':'message','role':'user','content':[{'type':'input_text','text':'Fictional Jo demonstrated a clamp technique on June 3.'}]}})+'\n').encode()
    inbox.observe('fictional-session',{'hook_event_name':'Stop'},raw)
    def missing_credit(*_): raise ProviderProblem('fictional-provider','fictional-credit-request',{'code':'payment_required','http_status':402})
    def restored(request,_):
        sources=json.loads(request['messages'][1]['content'])['sources']
        return {'selection':dict(outcome='applied',reason='Fictional qualification event',records=[dict(key='jo',operation='add',shelf='episodes',title='Fictional Jo',source_ids=[sources[0]['id']])],links=[]),'dispatch':{'model':'fictional','calls':1}}
    result=None
    if args.operation=='recover':
        # Explicitly repair this driver's own invalid fictional title, retaining
        # failed attempts. This never changes a producer code/binding guard and
        # validates the corrected selection against the exact frozen sources.
        job_path=state/'processor/batch-1.json'
        if job_path.exists():
            job=json.loads(job_path.read_text())
            navigation=job.get('navigation',{})
            if navigation.get('halted_reason'):
                from portable_producer import _code_hash
                from selection_navigation import effective_sources
                from portable_capture import selected_decision
                from codex_coordinator import checkpoint
                if job['code_hash']!=_code_hash():
                    raise ValueError('pending code changed; select the matching source version')
                errors=[step.get('feedback',{}).get('selection_error') for step in navigation['steps']]
                if navigation['halted_reason']!='selection remains invalid after two corrections' or any(error!='title must be an exact recognition phrase in a selected source block' for error in errors):
                    raise ValueError('unrelated pending failure requires its owning recovery')
                proposal=restored(job['request'],None)
                selected_decision(proposal['selection'],effective_sources(job),navigation['canon'])
                backup=job_path.with_suffix('.invalid-fixture-before.json')
                if not backup.exists(): backup.write_bytes(job_path.read_bytes())
                job.setdefault('qualification_corrections',[]).append(dict(reason='fictional_driver_title_corrected',at_ns=time.time_ns()))
                navigation.pop('halted_reason')
                checkpoint(job_path,job)
    if args.operation!='status':
        result=process_once(memory,inbox,state/'processor',missing_credit if args.operation=='failure' else restored,binding)
    alerts=Alerts(inbox)
    status=alerts.status()
    with inbox.connect() as con:
        source=con.execute('SELECT delta FROM revisions ORDER BY id LIMIT 1').fetchone()[0]
    with memory.connect() as con: chapters=con.execute('SELECT COUNT(*) FROM chapters').fetchone()[0]
    # A finite producer receipt, explicitly not a heartbeat or live worker proof.
    worker=dict(schema='hmk.worker-status/v1',updated_ns=time.time_ns(),state='pending' if not status['healthy'] else 'idle')
    (state/'worker-status.json').write_text(json.dumps(worker))
    receipt=dict(schema='daimon-alerts.fictional-hmk-qualification/v1',operation=args.operation,processing_result=result.get('state') if result else None,source_preserved=source==raw,source_sha256=hashlib.sha256(source).hexdigest(),chapters=chapters,open_incidents=sum(row['state']=='open' for row in status['alerts']),producer_notice_states=[row['delivery'] for row in status['alerts']],real_provider_used=False)
    encoded=json.dumps(receipt,indent=2)+'\n'
    (state/f'{args.operation}-receipt.json').write_text(encoded)
    (state/f'{args.operation}-receipt-{time.time_ns()}.json').write_text(encoded)
    print(json.dumps(receipt))
if __name__=='__main__': main()
