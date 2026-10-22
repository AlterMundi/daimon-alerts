"""Real process/HTTP/SQLite qualification; never sends a Telegram message."""
import concurrent.futures
import hashlib
import jsonschema
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request

ROOT=Path(__file__).resolve().parents[1]
SCHEMA=json.loads((ROOT/"schemas/daimon-alerts-v1.json").read_text())
sys.path.insert(0,str(ROOT/'scripts'))
from fixture_server import configuration

class Contract(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='alerts-contract-'); self.directory=Path(self.temp.name)
        config,self.tokens=configuration(self.directory)
        with socket.socket() as sock: sock.bind(('127.0.0.1',0)); port=sock.getsockname()[1]
        config['listen']=f'127.0.0.1:{port}'; config['freshness_seconds']=1
        # Two actors and processes exercise database concurrency, not only a mutex.
        other='other-actor-token-0000000000000000000000000'
        config['actors'].append(dict(config['actors'][1],actor_id='receiver-two',token_sha256=hashlib.sha256(other.encode()).hexdigest()))
        self.tokens['receiver-two']=other
        self.config=config; self.base=f'http://127.0.0.1:{port}'
        self.path=self.directory/'config.json'; self.path.write_text(json.dumps(config)); self.path.chmod(0o600)
        self.start()
        self.projection=json.loads((ROOT/'fixtures/open.json').read_text()); self.projection.update(source_at=int(time.time()),updated_at=int(time.time()))
        self.incident='/v1/incidents/fictional-hmk/credit-failure-001'
    def start(self):
        self.process=subprocess.Popen([str(ROOT/'target/debug/daimon-alerts'),'serve',str(self.path)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        # Readiness is not a disk/CPU benchmark. Keep one bounded wait for the
        # actual listener rather than a three-second loop that fails cold I/O.
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            try:
                if self.call('GET','/v1/status')[0]==200: return
            except OSError: time.sleep(.03)
            if self.process.poll() is not None:break
        self.process.terminate();self.process.communicate(timeout=5)
        raise RuntimeError('server did not start')
    def tearDown(self):
        if self.process.poll() is None: self.process.terminate()
        stdout,stderr=self.process.communicate(timeout=5)
        self.assertNotIn(b'Bearer',stdout+stderr)
        self.temp.cleanup()
    def call(self,method,path,body=None,actor='receiver',base=None,raw=None,content_type='application/json'):
        data=raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        req=urllib.request.Request((base or self.base)+path,data=data,method=method,headers={'Authorization':'Bearer '+self.tokens[actor],'Content-Type':content_type})
        try: response=urllib.request.urlopen(req,timeout=5)
        except urllib.error.HTTPError as error: response=error
        with response:
            value=json.loads(response.read())
            if response.status>=400: shape='Error'
            elif path.startswith('/v1/status') or path.startswith('/v1/observers'): shape='Status'
            elif '/acknowledgments/' in path: shape='Acknowledgment'
            elif path.endswith('/claim'): shape='Claim'
            elif path.endswith('/receipt'): shape='Delivery'
            elif path.startswith('/v1/deliveries'): shape='DeliveryPage'
            elif method=='PUT': shape='Acceptance'
            elif path.split('?')[0]=='/v1/incidents': shape='IncidentPage'
            else: shape='Incident'
            jsonschema.validate(value,dict(SCHEMA, **{'$ref':'#/$defs/'+shape}))
            return response.status,value
    def publish(self,value=None): return self.call('PUT',self.incident,value or self.projection,actor='producer')
    def pending(self): return self.call('GET','/v1/deliveries?state=pending')[1]['items']
    def test_revision_replay_resolution_and_ack(self):
        status,accept=self.publish(); self.assertEqual(status,201)
        status,replay=self.publish(); self.assertEqual(status,200); self.assertTrue(replay['idempotent']); self.assertEqual(accept['accepted_at'],replay['accepted_at'])
        conflict=dict(self.projection,summary='Changed content'); self.assertEqual(self.publish(conflict)[1]['error'],'revision_conflict')
        resolution=dict(self.projection,revision=3,processing_state='resolved'); self.assertEqual(self.publish(resolution)[0],200)
        self.assertEqual(self.publish()[1]['error'],'stale_revision')
        self.assertEqual(self.publish(dict(self.projection,revision=4))[1]['error'],'resolved_incident')
        self.assertEqual(len(self.pending()),2)
        path=self.incident+'/acknowledgments/request-one'
        first=self.call('PUT',path,{})
        self.assertEqual(first[0],201); self.assertEqual(self.call('PUT',path,{})[1],first[1])
        self.assertEqual(self.call('PUT',path,{},actor='receiver-two')[0],201)
        view=self.call('GET',self.incident)[1]; self.assertEqual(view['projection']['processing_state'],'resolved'); self.assertEqual(len(view['acknowledgments']),2)
    def test_claim_concurrency_restart_lost_reply_and_receipt(self):
        self.publish(); delivery=self.pending()[0]['delivery_id']; path=f'/v1/deliveries/{delivery}'
        # Bind a second independent service process to the same SQLite file.
        with socket.socket() as sock: sock.bind(('127.0.0.1',0)); port=sock.getsockname()[1]
        config=dict(self.config,listen=f'127.0.0.1:{port}')
        second_path=self.directory/'second.json'; second_path.write_text(json.dumps(config)); second_path.chmod(0o600)
        second=subprocess.Popen([str(ROOT/'target/debug/daimon-alerts'),'serve',str(second_path)],stderr=subprocess.PIPE)
        second_base=f'http://127.0.0.1:{port}'
        try:
            for _ in range(100):
                try: self.call('GET','/v1/status',base=second_base); break
                except OSError: time.sleep(.03)
            def claim(i): return self.call('POST',path+'/claim',{'request_id':f'attempt-{i}'},base=second_base if i%2 else self.base)
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool: results=list(pool.map(claim,range(32)))
            allowed=[r[1] for r in results if r[1].get('dispatch_allowed')]; self.assertEqual(len(allowed),1)
            request=allowed[0]['request_id']
            self.process.kill(); self.process.communicate(); self.start()
            self.assertFalse(self.call('POST',path+'/claim',{'request_id':request})[1]['dispatch_allowed'])
            receipt=dict(request_id=request,outcome='delivered',message_ref='message-001',destination_ref='human-001')
            self.assertEqual(self.call('PUT',path+'/receipt',receipt,actor='receiver-two')[1]['error'],'claim_conflict')
            final=self.call('PUT',path+'/receipt',receipt); self.assertEqual(final[0],200)
            self.assertEqual(self.call('PUT',path+'/receipt',receipt)[1],final[1])
            self.assertEqual(self.call('PUT',path+'/receipt',dict(receipt,outcome='failed'))[1]['error'],'receipt_conflict')
            self.assertEqual(self.call('GET',self.incident)[1]['projection']['processing_state'],'open')
        finally: second.terminate(); second.communicate(timeout=5)
    def test_denial_body_limits_filters_privacy(self):
        self.publish()
        self.assertEqual(self.call('GET',self.incident,actor='denied')[0],404)
        self.assertEqual(self.call('GET','/v1/incidents',actor='denied')[1]['items'],[])
        self.assertEqual(self.call('PUT',self.incident+'/acknowledgments/a',{},actor='denied')[0],404)
        self.assertEqual(self.call('PUT',self.incident,self.projection)[0],403)
        self.assertEqual(self.call('GET','/v1/deliveries?adapter_id=other')[0],403)
        for query in ('limit=0','limit=101','cursor=-1','state=bogus','unknown=x'):
            self.assertEqual(self.call('GET','/v1/deliveries?'+query)[0],400)
        self.assertEqual(self.call('PUT',self.incident,actor='producer',raw=b'x'*17000)[0],413)
        self.assertEqual(self.call('PUT',self.incident,actor='producer',raw=b'{')[0],400)
        self.assertEqual(self.call('PUT',self.incident,self.projection,actor='producer',content_type='text/plain')[0],415)
        bad=dict(self.projection,revision=2,raw_provider_error='private error'); self.assertEqual(self.publish(bad)[0],400)
        bad=dict(self.projection,revision=2,summary='token\nprivate'); self.assertEqual(self.publish(bad)[0],400)
        result=json.dumps(self.call('GET',self.incident)[1]); self.assertNotIn(self.tokens['producer'],result)
    def test_pagination_capacity_backup_and_freshness(self):
        for index in range(3):
            value=dict(self.projection,incident_id=f'incident-{index}')
            self.assertEqual(self.call('PUT',f'/v1/incidents/fictional-hmk/incident-{index}',value,actor='producer')[0],201)
        page=self.call('GET','/v1/incidents?limit=2')[1]; self.assertEqual(len(page['items']),2)
        second=self.call('GET','/v1/incidents?limit=2&cursor='+page['next_cursor'])[1]; self.assertEqual(len(second['items']),1); self.assertIsNone(second['next_cursor'])
        backup=self.directory/'backup.sqlite'
        subprocess.run([str(ROOT/'target/debug/daimon-alerts'),'backup',str(self.path),str(backup)],check=True)
        with sqlite3.connect(backup) as con: self.assertEqual(con.execute('pragma integrity_check').fetchone()[0],'ok'); self.assertEqual(con.execute('select count(*) from incidents').fetchone()[0],3)
        self.assertEqual(backup.stat().st_mode&0o077,0)
        time.sleep(2.1)
        self.assertEqual(self.call('GET','/v1/incidents')[1]['items'][0]['freshness'],'stale')
        self.process.terminate(); self.process.communicate(); self.config['max_incidents']=3; self.path.write_text(json.dumps(self.config)); self.start()
        self.assertEqual(self.publish()[1]['error'],'capacity_exceeded')
    def test_backup_restore_preserves_uncertain_dispatch(self):
        self.publish()
        delivery=self.pending()[0]['delivery_id']; claim_path=f'/v1/deliveries/{delivery}/claim'
        first=self.call('POST',claim_path,{'request_id':'retained-attempt'})
        self.assertTrue(first[1]['dispatch_allowed'])
        backup=self.directory/'restore.sqlite'
        subprocess.run([str(ROOT/'target/debug/daimon-alerts'),'backup',str(self.path),str(backup)],check=True)
        self.process.terminate(); self.process.communicate()
        self.config['database']=str(backup);self.path.write_text(json.dumps(self.config));self.start()
        self.assertFalse(self.call('POST',claim_path,{'request_id':'retained-attempt'})[1]['dispatch_allowed'])
        self.assertEqual(self.call('GET',self.incident)[1]['deliveries'][0]['state'],'uncertain')
        self.process.terminate(); self.process.communicate()
        with sqlite3.connect(backup) as con: con.execute('PRAGMA user_version=99')
        refused=subprocess.run([str(ROOT/'target/debug/daimon-alerts'),'serve',str(self.path)],capture_output=True,timeout=5)
        self.assertNotEqual(refused.returncode,0)
        self.assertNotIn(self.tokens['producer'].encode(),refused.stderr)

    def test_observer_death_and_source_failure(self):
        self.assertEqual(self.call('GET','/v1/status')[1]['state'],'unobserved')
        self.process.terminate(); self.process.communicate()
        actor=self.config['actors'][0]; actor['grants'].append('observe'); actor['producers'].append('fictional-observer'); self.path.write_text(json.dumps(self.config)); self.start()
        body=dict(receiving_scope='fictional-human',source_at=int(time.time()),source_state='healthy')
        self.assertEqual(self.call('PUT','/v1/observers/fictional-observer',body,actor='producer')[1]['state'],'healthy')
        time.sleep(2.1)
        self.assertEqual(self.call('GET','/v1/status')[1]['state'],'degraded')
        self.assertEqual(self.call('PUT','/v1/observers/fictional-observer',dict(body,source_state='observation_unreadable'),actor='producer')[1]['state'],'degraded')

    def test_explicit_observer_retirement_preserves_pending_work_and_uncertain_delivery(self):
        self.process.terminate();self.process.communicate()
        actor=self.config['actors'][0];actor['grants'].append('observe');actor['producers']+=['fictional-observer','retained-observer']
        self.path.write_text(json.dumps(self.config));self.start();self.publish()
        delivery=self.pending()[0]['delivery_id'];claim=f'/v1/deliveries/{delivery}/claim'
        self.assertTrue(self.call('POST',claim,{'request_id':'uncertain-before-retirement'})[1]['dispatch_allowed'])
        body=dict(receiving_scope='fictional-human',source_at=int(time.time()),source_state='healthy')
        for id in ('fictional-observer','retained-observer'):
            self.assertEqual(self.call('PUT','/v1/observers/'+id,body,actor='producer')[0],200)
        route='/v1/observers/fictional-observer/retire';value={'receiving_scope':'fictional-human'}
        self.assertEqual(self.call('POST',route,value)[0],403)
        self.assertEqual(self.call('POST',route,{'receiving_scope':'foreign'},actor='producer')[0],403)
        self.assertEqual(self.call('POST','/v1/observers/foreign-observer/retire',value,actor='producer')[0],403)
        self.assertEqual(self.call('POST',route,dict(value,raw_error='private'),actor='producer')[0],400)
        for _ in range(2):
            result=self.call('POST',route,value,actor='producer');self.assertEqual(result[0],200)
            self.assertEqual([o['observer_id'] for o in result[1]['observers']],['retained-observer'])
        self.process.terminate();self.process.communicate();self.start()
        self.assertEqual([o['observer_id'] for o in self.call('GET','/v1/status')[1]['observers']],['retained-observer'])
        self.assertEqual(self.call('GET',self.incident)[1]['projection']['processing_state'],'open')
        self.assertEqual(self.call('GET',self.incident)[1]['deliveries'][0]['state'],'uncertain')
        self.assertFalse(self.call('POST',claim,{'request_id':'uncertain-before-retirement'})[1]['dispatch_allowed'])

if __name__=='__main__':
    subprocess.run(['cargo','build','--locked'],cwd=ROOT,check=True)
    unittest.main(verbosity=2)
