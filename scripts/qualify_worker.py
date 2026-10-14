"""Controlled finite HMK launcher qualification; isolated fiction, no network.

Uses the installed worker/runtime/model receipt and explicit-retry APIs. Only the
provider transport and embedding computation are deterministic test doubles.
Never selects the real memory database, capture binding or provider credentials.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import sys
import time


def main():
    os.umask(0o077)
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation',choices=['failure','recover','replay'])
    parser.add_argument('--hmk-source',type=Path,required=True)
    parser.add_argument('--state-dir',type=Path,required=True)
    args=parser.parse_args()
    root=args.state_dir.absolute()
    if any(p.is_symlink() for p in (root,*root.parents)):
        raise ValueError('private qualification destination required')
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    if root.stat().st_mode&0o077: raise ValueError('private qualification state required')
    scripts=args.hmk_source.resolve()/'scripts';sys.path.insert(0,str(scripts))
    import memory_runtime
    import memory_models
    from capture_inbox import Inbox
    from codex_coordinator import checkpoint
    from provider_status import Alerts,ProviderProblem
    base=root/'fictional-memory';base.mkdir(mode=0o700,exist_ok=True)
    os.environ['HMK_AGENT_MEMORY_BASE']=str(base)
    os.environ['HMK_DB_PATH']=str(base/'library.sqlite')
    spec=importlib.util.spec_from_file_location('fixture_worker_memory',scripts/'memoryctl.py')
    memory=importlib.util.module_from_spec(spec);spec.loader.exec_module(memory)
    if not Path(memory.DB_PATH).exists(): memory.init_db()
    binding={'subject':'fictional:finite-alerts-test','source_instance':'fictional:compaii-worker-test'}
    inbox=Inbox(root/'capture.sqlite',binding)
    raw=(json.dumps({'type':'response_item','payload':{'type':'message','role':'user','content':[{'type':'input_text','text':'Fictional Jo demonstrated a clamp technique on June 3.'}]}})+'\n').encode()
    inbox.observe('fictional-worker-session',{'hook_event_name':'Stop'},raw)
    checkpoint(root/'binding.json',binding)
    checkpoint(root/'catalog.json',{'models':[]})
    profile=dict(schema='hmk.worker-profile/v1',database=str(memory.DB_PATH),memory_base=str(base),inbox=str(inbox.path),binding=str(root/'binding.json'),context=binding,state_dir=str(root/'processor'),source_scope='fictional',
        generation=dict(provider='deepseek',model='deepseek-flash',effort='low',output_token_limit=6000,credential_env='ALERTS_WORKER_TEST_CREDENTIAL'),
        review=dict(provider='openai-codex-native',model='gpt-6.1-sol',effort='low',catalog=str(root/'catalog.json')),
        budgets=dict(max_steps=6,capture_limit=32,index_limit=32,source_budget_bytes=64000,navigation_steps=4))
    checkpoint(root/'profile.json',profile)
    os.environ['ALERTS_WORKER_TEST_CREDENTIAL']='fictional-local-transport-only'
    counts_path=root/'transport-counts.json'
    counts=json.loads(counts_path.read_text()) if counts_path.exists() else dict(failed_dispatches=0,successful_dispatches=0,embedding_calls=0)
    def transport(path,directory,credential,provider):
        receipt=dict(request_sha256=path.stem,tool_calls_observed=[])
        if args.operation=='failure':
            receipt.update(state='failed',problem=dict(code='payment_required',http_status=402))
            counts['failed_dispatches']+=1
        else:
            request=json.loads(path.read_text());sources=json.loads(request['messages'][1]['content'])['sources']
            selection=dict(outcome='applied',reason='Fictional finite worker qualification',records=[dict(key='jo',operation='add',shelf='episodes',title='Fictional Jo',source_ids=[sources[0]['id']])],links=[])
            receipt.update(state='completed',content=json.dumps(selection),finish_reason='stop')
            counts['successful_dispatches']+=1
        (directory/'dispatches').mkdir(exist_ok=True)
        checkpoint(directory/'dispatches'/path.name,receipt)
        checkpoint(counts_path,counts)
        if receipt['state']=='failed': raise ProviderProblem(provider,path.stem,receipt['problem'])
    memory_models.text_api.dispatch=transport
    original_load=memory_runtime.load_memory
    def load(value):
        loaded=original_load(value)
        loaded.embeddings_runtime_config=lambda **kw:dict(provider='local',model='fictional',output_dimensionality=None)
        def embed(provider,texts,**kw):
            counts['embedding_calls']+=1;checkpoint(counts_path,counts)
            return [[1.0,0.0] for _ in texts]
        loaded.embed_texts=embed
        return loaded
    memory_runtime.load_memory=load
    if args.operation=='recover':
        for path in (root/'processor').rglob('requests/*.json'):
            receipt_path=path.parent.parent/'dispatches'/path.name
            if receipt_path.exists() and json.loads(receipt_path.read_text()).get('state')=='failed' and not (path.parent.parent/'retries'/path.name).exists():
                memory_runtime.retry(profile,path,'Controlled fictional credit restored; failed transport made no network request.')
    status=memory_runtime.run(profile)['status']
    with inbox.connect() as con:
        preserved=con.execute('SELECT delta FROM revisions ORDER BY id LIMIT 1').fetchone()[0]==raw
    with memory.connect() as con: chapters=con.execute('SELECT COUNT(*) FROM chapters').fetchone()[0]
    alerts=Alerts(inbox).status()
    result=dict(schema='daimon-alerts.finite-worker-qualification/v1',operation=args.operation,worker_state=status['state'],chapters=chapters,source_preserved=preserved,open_incidents=sum(x['state']=='open' for x in alerts['alerts']),counts=counts,real_provider_used=False,at_ns=time.time_ns())
    checkpoint(root/(args.operation+'-receipt.json'),result)
    print(json.dumps(result))
    return 0 if status['state']=='idle' else 2


if __name__=='__main__': raise SystemExit(main())
