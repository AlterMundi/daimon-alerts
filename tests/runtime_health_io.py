"""Native control, cgroup stalls and durable restart/OOM edges without inference."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0,str(Path(__file__).parents[1]/'scripts'))
from service_monitor import Monitor, probe
from client import Unavailable


class Runtime(unittest.TestCase):
    def test_native_control_probe_never_starts_or_replays(self):
        target=dict(kind='native_codex',binary='/codex',unit='native.service')
        for response,expected in [('{"status":"running","appServerVersion":"fixture"}','healthy'),
                                  ('{"status":"stopped"}','native_unresponsive'),
                                  ('[]','native_unresponsive')]:
            with patch('service_monitor.subprocess.run',side_effect=[
                    Mock(returncode=0,stdout='3\n'),Mock(returncode=0,stdout=response)]) as run:
                self.assertEqual(probe(dict(target)),expected)
                self.assertEqual(run.call_args.args[0],['/codex','app-server','daemon','version'])
        with patch('service_monitor.subprocess.run',side_effect=[
                Mock(returncode=0,stdout='3\n'),subprocess.TimeoutExpired('version',15)]):
            self.assertEqual(probe(target),'native_unresponsive')

    def test_nearly_full_reclaimable_cache_is_not_itself_an_outage(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            (root/'memory.current').write_text('99')
            (root/'memory.max').write_text('100')
            (root/'memory.events').write_text('oom_kill 0\n')
            target=dict(kind='cgroup_memory',cgroup_root=str(root),full_avg60_percent=5)
            for stalled,expected in [(0,'healthy'),(12,'memory_reclaim_pressure')]:
                (root/'memory.pressure').write_text(f'full avg10=0 avg60={stalled} avg300=0 total=0\n')
                self.assertEqual(probe(target),expected)
            (root/'memory.events').unlink()
            self.assertEqual(probe(target),'observation_unreadable')

    def test_restart_edge_survives_monitor_restart_and_unavailable_delivery(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);client=Mock();sent=[]
            def publish(method,path,value):
                if '/incidents/' in path:
                    sent.append(value)
                    return {k:value[k] for k in ('producer_id','incident_id','revision')}
                return {}
            client.call.side_effect=publish
            config=dict(producer_id='runtime',observer_id='observer',receiving_scope='human',
                        base_url='http://127.0.0.1:9',token_file=str(root/'token'),
                        state_dir=str(root/'state'),targets=[dict(id='native',resource_ref='body',
                        kind='native_codex',binary='/codex',unit='native.service')])
            count=[31]
            def check(target):target['_counters']={'restarts':count[0]};return 'healthy'
            monitor=Monitor(config,client)
            monitor.tick(check,now=100);monitor.close()
            monitor=Monitor(config,client)
            try:
                count[0]=32;monitor.tick(check,now=101)
                self.assertIn('native restarted',sent[-1]['summary'])
                original=sent[-1]['incident_id']
                monitor.tick(check,now=102)
                self.assertEqual(sent[-1]['incident_id'],original)
                self.assertEqual(sent[-1]['processing_state'],'resolved')
                # A queued accepted-state projection blocks consuming a new edge.
                monitor.db.execute('INSERT INTO outbox(content) VALUES(?)',(json.dumps(sent[-1]),))
                monitor.db.commit();client.call.side_effect=Unavailable()
                count[0]=33;monitor.tick(check,now=103)
                self.assertEqual(json.loads(monitor.db.execute('SELECT content FROM runtime_counters').fetchone()[0])['restarts'],32)
                client.call.side_effect=publish;monitor.tick(check,now=104)
                self.assertNotEqual(sent[-1]['incident_id'],original)
                self.assertEqual(sent[-1]['processing_state'],'open')
            finally:monitor.close()

    def test_unreadable_first_or_later_target_never_consumes_another_counter(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);client=Mock()
            client.call.side_effect=lambda method,path,v: {k:v[k] for k in
                ('producer_id','incident_id','revision')} if '/incidents/' in path else {}
            cfg=dict(producer_id='runtime',observer_id='observer',receiving_scope='human',
                base_url='http://127.0.0.1:9',token_file=str(root/'token'),state_dir=str(root/'state'),
                targets=[dict(id=n,resource_ref='body',kind='cgroup_memory',cgroup_root=str(root),
                              full_avg60_percent=5) for n in ('first','later')])
            monitor=Monitor(cfg,client)
            try:
                for failed in ('first','later'):
                    def check(target):
                        if target['id']==failed:raise OSError()
                        target['_counters']={'oom_kill':3};return 'healthy'
                    monitor.tick(check,now=100)
                    row=json.loads(monitor.db.execute('SELECT content FROM incidents WHERE id=?',(failed,)).fetchone()[0])
                    self.assertIn('observation unreadable',row['summary'])
                self.assertEqual(monitor.db.execute('SELECT count(*) FROM runtime_counters').fetchone()[0],2)
            finally:monitor.close()

    def test_short_oom_is_retained_even_when_pressure_has_already_recovered(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);client=Mock()
            client.call.side_effect=lambda method,path,v: {k:v[k] for k in
                ('producer_id','incident_id','revision')} if '/incidents/' in path else {}
            cfg=dict(producer_id='runtime',observer_id='observer',receiving_scope='human',
                     base_url='http://127.0.0.1:9',token_file=str(root/'token'),state_dir=str(root/'state'),
                     targets=[dict(id='memory',resource_ref='body',kind='cgroup_memory',
                                   cgroup_root=str(root),full_avg60_percent=5)])
            for name,text in {'memory.current':'30','memory.max':'100',
                              'memory.pressure':'full avg60=0\n','memory.events':'oom_kill 7\n'}.items():
                (root/name).write_text(text)
            monitor=Monitor(cfg,client)
            try:
                monitor.tick(now=100)
                self.assertEqual(monitor.db.execute('SELECT count(*) FROM incidents').fetchone()[0],0)
                (root/'memory.events').write_text('oom_kill 8\n');monitor.tick(now=101)
                row=json.loads(monitor.db.execute('SELECT content FROM incidents').fetchone()[0])
                self.assertIn('memory oom',row['summary'])
                monitor.tick(now=102)
                row=json.loads(monitor.db.execute('SELECT content FROM incidents').fetchone()[0])
                self.assertEqual(row['processing_state'],'resolved')
                monitor.tick(lambda _: (_ for _ in ()).throw(OSError()),now=103)
                row=json.loads(monitor.db.execute('SELECT content FROM incidents').fetchone()[0])
                self.assertEqual(row['processing_state'],'open')
            finally:monitor.close()


if __name__=='__main__':unittest.main()
