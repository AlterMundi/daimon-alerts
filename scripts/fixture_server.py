"""Run fictional fixtures against the real runtime in disposable isolated state."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import urllib.request
import time

ROOT = Path(__file__).resolve().parents[1]
def configuration(directory):
    credentials = {name:secrets.token_urlsafe(32) for name in ('producer','receiver','denied')}
    actors=[]
    for name,token in credentials.items():
        actors.append(dict(actor_id='fictional-'+name,token_sha256=hashlib.sha256(token.encode()).hexdigest(),producers=['fictional-hmk'] if name=='producer' else [],scopes=['fictional-human'] if name!='denied' else ['other-human'],adapters=['fictional-telegram'] if name=='receiver' else [],grants=['status','publish','read'] if name=='producer' else ['status','read','acknowledge','deliver']))
    config=dict(database=str(directory/'state.sqlite'),listen='127.0.0.1:8791',freshness_seconds=120,max_incidents=1000,retention_seconds=604800,actors=actors,routes=[dict(receiving_scope='fictional-human',adapter_id='fictional-telegram')])
    path=directory/'config.json'; path.write_text(json.dumps(config)); path.chmod(0o600)
    env=directory/'client.env'; env.write_text('\n'.join(f'export ALERTS_{k.upper()}_TOKEN={v}' for k,v in credentials.items())+'\n'); env.chmod(0o600)
    return config,credentials

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--port',type=int,default=8791); args=parser.parse_args()
    subprocess.run(['cargo','build','--locked'],cwd=ROOT,check=True)
    with tempfile.TemporaryDirectory(prefix='daimon-alerts-fixture-') as tmp:
        directory=Path(tmp); config,credentials=configuration(directory); config['listen']=f'127.0.0.1:{args.port}'
        (directory/'config.json').write_text(json.dumps(config))
        process=subprocess.Popen([str(ROOT/'target/debug/daimon-alerts'),'serve',str(directory/'config.json')])
        try:
            for _ in range(100):
                try:
                    projection=json.loads((ROOT/'fixtures/open.json').read_text()); projection.update(source_at=int(time.time()),updated_at=int(time.time()))
                    request=urllib.request.Request(f'http://127.0.0.1:{args.port}/v1/incidents/fictional-hmk/credit-failure-001',data=json.dumps(projection).encode(),method='PUT',headers={'Authorization':'Bearer '+credentials['producer'],'Content-Type':'application/json'})
                    urllib.request.urlopen(request,timeout=2).close(); break
                except OSError: time.sleep(.05)
            else: raise RuntimeError('fixture server did not start')
            print(f'Fictional runtime ready. Credential references: {directory}/client.env',flush=True)
            process.wait()
        except KeyboardInterrupt: pass
        finally:
            process.terminate(); process.wait(timeout=5)
if __name__=='__main__': main()
