"""Changing an operator registry keeps observations and incident identity durable."""
import json,sys,subprocess,tempfile,unittest,threading
from pathlib import Path
from unittest.mock import Mock,patch
sys.path.insert(0,str(Path(__file__).parents[1]/'scripts'))
from service_monitor import Monitor, enrollments, probe, Unavailable

class Hosted(unittest.TestCase):
 def test_relocated_retirement_preserves_other_bodies_history_and_exact_uncertain_resolution(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);root.chmod(0o700);registry=root/'registry';registry.mkdir(mode=0o700)
   for name in ('one','two'):
    p=registry/(name+'.json');p.write_text(json.dumps(dict(schema='cluster-runtime-health-enrollment/v1',name=name,instance='dm-'+name,resource_ref='container-dm-'+name,plan_digest='a'*64)));p.chmod(0o600)
   cfg=dict(producer_id='hosted',observer_id='observer',receiving_scope='operator',base_url='http://127.0.0.1:9',token_file=str(root/'token'),state_dir=str(root/'state'),targets=[dict(id='hosted',resource_ref='host',kind='enrollment_registry',registry=str(registry),incus_binary='/usr/bin/incus',full_avg60_percent=5)])
   sent=[];client=Mock()
   def accept(method,path,value):
    if '/incidents/' in path:sent.append(dict(value));return {k:value[k] for k in ('producer_id','incident_id','revision')}
    return {}
   client.call.side_effect=accept;monitor=Monitor(cfg,client)
   monitor.tick(lambda t:'native_unresponsive' if t['kind']=='incus_native' else 'healthy',now=100)
   before=dict(monitor.db.execute('SELECT id,content FROM incidents').fetchall())
   # A lost retirement response leaves exactly one pending resolution, archived
   # enrollment and unchanged other body's incident. Restart retries that revision.
   def uncertain(method,path,value):
    if value.get('processing_state')=='resolved':raise Unavailable('lost reply')
    return accept(method,path,value)
   client.call.side_effect=uncertain
   with self.assertRaises(Unavailable):monitor.retire_enrollment('one',now=101)
   pending=monitor.db.execute('SELECT content FROM outbox').fetchone()[0]
   self.assertEqual(json.loads(pending)['incident_id'],json.loads(before['one-native'])['incident_id'])
   self.assertEqual(monitor.db.execute('SELECT content FROM incidents WHERE id="two-native"').fetchone()[0],before['two-native'])
   self.assertTrue((registry/'one.json.retired').exists());self.assertFalse((registry/'one.json').exists())
   monitor.close();client.call.side_effect=accept;monitor=Monitor(cfg,client)
   try:
    monitor.retire_enrollment('one',now=102)
    self.assertEqual(sent[-1],json.loads(pending));self.assertIn('retired',sent[-1]['summary']);self.assertNotIn('healthy',sent[-1]['summary'])
    count=len(sent);monitor.retire_enrollment('one',now=103);self.assertEqual(len(sent),count)
    result=monitor.tick(lambda t:'native_unresponsive' if t['kind']=='incus_native' else 'healthy',now=104)
    self.assertEqual({t['id'] for t in result['targets']},{'two-native','two-memory','hosted'})
    self.assertEqual(monitor.db.execute('SELECT content FROM incidents WHERE id="two-native"').fetchone()[0],before['two-native'])
    with self.assertRaises(ValueError):monitor.retire_enrollment('missing',now=105)
    self.assertTrue((registry/'two.json').exists())
   finally:monitor.close()

 def test_new_enrollment_after_monitor_start_and_existing_incident_survives_restart(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);root.chmod(0o700);registry=root/'registry';registry.mkdir(mode=0o700)
   target=dict(id='hosted',resource_ref='host',kind='enrollment_registry',registry=str(registry),incus_binary='/usr/bin/incus',full_avg60_percent=5)
   cfg=dict(producer_id='hosted',observer_id='observer',receiving_scope='operator',base_url='http://127.0.0.1:9',token_file=str(root/'token'),state_dir=str(root/'state'),targets=[target])
   client=Mock();sent=[]
   def call(method,path,value):
    if '/incidents/' in path:sent.append(value);return {k:value[k] for k in ('producer_id','incident_id','revision')}
    return {}
   client.call.side_effect=call
   monitor=Monitor(cfg,client);self.assertEqual(len(monitor.tick(lambda t:'healthy',now=100)['targets']),1)
   def add(name):
    p=registry/(name+'.json');p.write_text(json.dumps(dict(schema='cluster-runtime-health-enrollment/v1',name=name,instance='dm-'+name,resource_ref='container-dm-'+name,plan_digest='a'*64)));p.chmod(0o600)
   add('first')
   monitor.tick(lambda t:'native_unresponsive' if t['id']=='first-native' else 'healthy',now=101)
   first=sent[-1]['incident_id'];monitor.close()
   add('second');monitor=Monitor(cfg,client)
   result=monitor.tick(lambda t:'native_unresponsive' if t['id']=='first-native' else 'healthy',now=102)
   self.assertEqual(len(result['targets']),5);self.assertEqual(len(sent),1)
   monitor.tick(lambda t:'healthy',now=103);self.assertEqual(sent[-1]['incident_id'],first);self.assertEqual(sent[-1]['processing_state'],'resolved');monitor.close()
 def test_registry_refuses_foreign_paths_permissions_and_resource_rebinding(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);root.chmod(0o700);p=root/'one.json';value=dict(schema='cluster-runtime-health-enrollment/v1',name='one',instance='dm-one',resource_ref='container-dm-one',plan_digest='a'*64)
   t=dict(registry=str(root),incus_binary='/usr/bin/incus',full_avg60_percent=5)
   p.write_text(json.dumps(value));p.chmod(0o600);self.assertEqual(len(enrollments(t)),2)
   for replacement in (dict(value,instance='dm-other'),dict(value,name='../outside'),dict(value,command=['bad'])):
    p.write_text(json.dumps(replacement))
    with self.assertRaises(ValueError):enrollments(t)
   p.write_text(json.dumps(value));p.chmod(0o644)
   with self.assertRaises(ValueError):enrollments(t)
 def test_incus_probe_only_reads_the_bound_native_and_never_starts_models(self):
  target=dict(kind='incus_native',incus_binary='/usr/bin/incus',instance='dm-one')
  with patch('service_monitor.subprocess.run',side_effect=[Mock(returncode=0,stdout='3\n'),Mock(returncode=0,stdout='{"status":"running","appServerVersion":"fixture"}')]) as run:
   self.assertEqual(probe(target),'healthy');calls=[c.args[0] for c in run.call_args_list];self.assertTrue(all(c[:4]==['/usr/bin/incus','exec','dm-one','--'] for c in calls));self.assertEqual(calls[-1][-4:],['/usr/local/bin/codex','app-server','daemon','version'])

 def test_counter_timeout_still_reads_actual_native(self):
  target=dict(kind='incus_native',incus_binary='/usr/bin/incus',instance='dm-one')
  with patch('service_monitor.subprocess.run',side_effect=[subprocess.TimeoutExpired('counter',15),Mock(returncode=0,stdout='{"status":"running","appServerVersion":"fixture"}')]) as run:
   self.assertEqual(probe(target),'observation_unreadable');self.assertEqual(run.call_count,2)

 def test_slow_guest_does_not_hold_memory_alert_and_partial_probe_is_not_healthy(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);root.chmod(0o700);registry=root/'registry';registry.mkdir(mode=0o700)
   p=registry/'one.json';p.write_text(json.dumps(dict(schema='cluster-runtime-health-enrollment/v1',name='one',instance='dm-one',resource_ref='container-dm-one',plan_digest='a'*64)));p.chmod(0o600)
   (registry/'.failure-overflow').write_text('{"reason":"enrollment_unavailable"}')
   target=dict(id='hosted',resource_ref='host',kind='enrollment_registry',registry=str(registry),incus_binary='/usr/bin/incus',full_avg60_percent=5)
   cfg=dict(producer_id='hosted',observer_id='observer',receiving_scope='operator',base_url='http://127.0.0.1:9',token_file=str(root/'token'),state_dir=str(root/'state'),targets=[target])
   memory_accepted=threading.Event();heartbeats=[];client=Mock()
   def call(method,path,value):
    if '/incidents/' in path:
     if 'memory' in value['operation_id']:memory_accepted.set()
     return {k:value[k] for k in ('producer_id','incident_id','revision')}
    heartbeats.append(value['source_state']);return {}
   client.call.side_effect=call
   monitor=Monitor(cfg,client)
   def check(t):
    if t['kind']=='enrollment_registry':return t['_registry_state']
    if t['kind']=='cgroup_memory':return 'memory_reclaim_pressure'
    self.assertTrue(memory_accepted.is_set());return 'healthy'
   # Exercise the heartbeat branch without a real ten-second stall.
   import concurrent.futures
   actual_wait=concurrent.futures.wait;first=[True];clock=[0]
   def bounded_wait(pending,**kwargs):
    if first[0]:first[0]=False;clock[0]=11;return set(),set(pending)
    return actual_wait(pending,**kwargs)
   with patch('service_monitor.concurrent.futures.wait',side_effect=bounded_wait), patch('service_monitor.time.monotonic',side_effect=lambda:clock[0]):result=monitor.tick(check,now=100)
   self.assertIn('observation_unreadable',heartbeats)
   self.assertTrue(any(t['state']=='enrollment_unavailable' for t in result['targets']))
   monitor.close()

 def test_heartbeat_deadline_survives_frequent_interleaved_completions(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);root.chmod(0o700);registry=root/'registry';registry.mkdir(mode=0o700)
   for name in ('one','two','three','four'):
    p=registry/(name+'.json');p.write_text(json.dumps(dict(schema='cluster-runtime-health-enrollment/v1',name=name,instance='dm-'+name,resource_ref='container-dm-'+name,plan_digest='a'*64)));p.chmod(0o600)
   target=dict(id='hosted',resource_ref='host',kind='enrollment_registry',registry=str(registry),incus_binary='/usr/bin/incus',full_avg60_percent=5)
   cfg=dict(producer_id='hosted',observer_id='observer',receiving_scope='operator',base_url='http://127.0.0.1:9',token_file=str(root/'token'),state_dir=str(root/'state'),targets=[target])
   client=Mock();observations=[];clock=[0]
   def call(method,path,value):
    if '/observers/' in path:observations.append((clock[0],value['source_state']))
    return {}
   client.call.side_effect=call;monitor=Monitor(cfg,client)
   def interleaved(pending,**kwargs):
    clock[0]+=9;future=next(iter(pending));return {future},set(pending)-{future}
   with patch('service_monitor.concurrent.futures.wait',side_effect=interleaved),patch('service_monitor.time.monotonic',side_effect=lambda:clock[0]):monitor.tick(lambda t:'healthy',now=100)
   self.assertEqual([at for at,state in observations if state=='observation_unreadable'],[18,36]);self.assertEqual(observations[-1][1],'healthy');monitor.close()

class UnavailableHeartbeat(unittest.TestCase):
 def test_unavailable_core_does_not_busy_spin_pending_native_probes(self):
  with tempfile.TemporaryDirectory() as temporary:
   root=Path(temporary);root.chmod(0o700)
   registry=root/'registry';registry.mkdir(mode=0o700)
   p=registry/'one.json';p.write_text(json.dumps(dict(schema='cluster-runtime-health-enrollment/v1',name='one',instance='dm-one',resource_ref='container-dm-one',plan_digest='a'*64)));p.chmod(0o600)
   cfg=dict(producer_id='hosted',observer_id='observer',receiving_scope='operator',base_url='http://127.0.0.1:9',token_file=str(root/'token'),state_dir=str(root/'state'),targets=[dict(id='hosted',resource_ref='host',kind='enrollment_registry',registry=str(registry),incus_binary='/usr/bin/incus',full_avg60_percent=5)])
   client=Mock();client.call.side_effect=Unavailable('offline');monitor=Monitor(cfg,client);clock=[0];timeouts=[]
   def wait(pending,**kwargs):
    timeouts.append(kwargs['timeout'])
    if len(timeouts)==1:clock[0]=11;return set(),set(pending)
    return set(pending),set()
   with patch('service_monitor.concurrent.futures.wait',side_effect=wait),patch('service_monitor.time.monotonic',side_effect=lambda:clock[0]):result=monitor.tick(lambda t:'healthy',now=100)
   self.assertEqual(timeouts,[10,10]);self.assertEqual(result['service_state'],'unavailable');monitor.close()

if __name__=='__main__':unittest.main()
