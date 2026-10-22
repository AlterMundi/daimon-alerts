"""Changing an operator registry keeps observations and incident identity durable."""
import json,sys,subprocess,tempfile,unittest,threading
from pathlib import Path
from unittest.mock import Mock,patch
sys.path.insert(0,str(Path(__file__).parents[1]/'scripts'))
from service_monitor import Monitor, enrollments, probe

class Hosted(unittest.TestCase):
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
   actual_wait=concurrent.futures.wait;first=[True]
   def bounded_wait(pending,**kwargs):
    if first[0]:first[0]=False;return set(),set(pending)
    return actual_wait(pending,**kwargs)
   with patch('service_monitor.concurrent.futures.wait',side_effect=bounded_wait):result=monitor.tick(check,now=100)
   self.assertIn('observation_unreadable',heartbeats)
   self.assertTrue(any(t['state']=='enrollment_unavailable' for t in result['targets']))
   monitor.close()

if __name__=='__main__':unittest.main()
