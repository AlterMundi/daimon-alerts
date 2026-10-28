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
    def test_full_os_memory_uses_global_psi_and_oom_not_cache_occupancy(self):
        files={'/proc/meminfo':'MemTotal: 1000 kB\nMemAvailable: 1 kB\n',
               '/proc/vmstat':'oom_kill 7\n',
               '/proc/pressure/memory':'some avg60=0\nfull avg60=0\n'}
        target=dict(kind='host_memory',full_avg60_percent=5)
        def read(path):return files[str(path)]
        with patch.object(Path,'read_text',read):
            self.assertEqual(probe(target),'healthy')
            self.assertEqual(target['_counters']['oom_kill'],7)
            files['/proc/pressure/memory']='full avg60=6\n'
            self.assertEqual(probe(target),'memory_reclaim_pressure')
            files['/proc/meminfo']='MemTotal: 0 kB\nMemAvailable: 0 kB\n'
            self.assertEqual(probe(target),'observation_unreadable')
        with patch.object(Path,'read_text',side_effect=OSError()):
            self.assertEqual(probe(target),'observation_unreadable')

    def test_cgroup_to_whole_os_move_retains_old_oom_and_starts_new_baseline(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);client=Mock();sent=[]
            def publish(method,path,value):
                if '/incidents/' in path:
                    sent.append(dict(value));return {k:value[k] for k in ('producer_id','incident_id','revision')}
                return {}
            client.call.side_effect=publish
            cfg=dict(producer_id='runtime',observer_id='observer',receiving_scope='human',
                     base_url='http://127.0.0.1:9',token_file=str(root/'token'),state_dir=str(root/'state'),
                     targets=[dict(id='memory',resource_ref='container',kind='cgroup_memory',
                                   cgroup_root=str(root),full_avg60_percent=5)])
            monitor=Monitor(cfg,client)
            def prior(target):target['_counters']={'oom_kill':2,'full_avg60_percent':8};return 'memory_reclaim_pressure'
            monitor.tick(prior,now=10)
            original=sent[-1]['incident_id']
            new=dict(cfg,targets=[dict(id='memory',resource_ref='full-vm',kind='host_memory',full_avg60_percent=5)])
            try:
                monitor.relocate(new,now=11)
                self.assertEqual(sent[-1]['incident_id'],original)
                self.assertEqual(sent[-1]['processing_state'],'resolved')
                def current(target):target['_counters']={'oom_kill':9,'full_avg60_percent':0};return 'healthy'
                monitor.tick(current,now=12)
                self.assertEqual(len(sent),2)  # Lifetime9 is a new baseline, not7 fresh OOMs.
                def pressure(target):target['_counters']={'oom_kill':10,'full_avg60_percent':8};return 'memory_reclaim_pressure'
                monitor.tick(pressure,now=13)
                self.assertEqual(sent[-1]['problem_code'],'worker_stalled')
                self.assertIn('memory oom',sent[-1]['summary'])
                for time_value in (14,74):monitor.tick(current,now=time_value)
                self.assertEqual(sent[-1]['processing_state'],'open')
                monitor.tick(current,now=134)
                self.assertEqual(sent[-1]['processing_state'],'resolved')
            finally:monitor.close()

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

    def test_memory_flapping_retains_incident_until_sustained_low_psi(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); client = Mock(); sent = []
            def publish(method, path, value):
                if '/incidents/' in path:
                    sent.append(dict(value))
                    return {k: value[k] for k in ('producer_id','incident_id','revision')}
                return {}
            client.call.side_effect = publish
            config = dict(producer_id='runtime', observer_id='observer', receiving_scope='human',
                          base_url='http://127.0.0.1:9', token_file=str(root/'token'),
                          state_dir=str(root/'state'), targets=[dict(id='memory', resource_ref='body',
                          kind='cgroup_memory', cgroup_root=str(root), full_avg60_percent=5)])
            metric = [6]
            def check(target):
                target['_counters'] = {'oom_kill': 0, 'full_avg60_percent': metric[0]}
                return 'memory_reclaim_pressure' if metric[0] >= 5 else 'healthy'
            monitor = Monitor(config, client)
            monitor.tick(check, now=100)
            incident = sent[-1]['incident_id']
            metric[0] = 4; monitor.tick(check, now=110)
            metric[0] = 6; monitor.tick(check, now=120)
            self.assertEqual(len(sent), 1)
            metric[0] = 1; monitor.tick(check, now=130)
            monitor.close()
            monitor = Monitor(config, client)
            try:
                monitor.tick(check, now=190)  # Same retained recovery across restart.
                self.assertEqual(len(sent), 1)
                self.assertEqual(monitor.db.execute('SELECT since FROM memory_recovery').fetchone()[0], 130)
                monitor.tick(lambda _: 'observation_unreadable', now=200)
                metric[0] = 1
                monitor.tick(check, now=210)
                monitor.tick(check, now=310)  # Missing observations reset the window.
                monitor.tick(check, now=370)
                self.assertEqual(sent[-1]['processing_state'], 'open')
                monitor.tick(check, now=430)
                self.assertEqual(sent[-1]['incident_id'], incident)
                self.assertEqual(sent[-1]['processing_state'], 'resolved')
                metric[0] = 8; monitor.tick(check, now=440)
                self.assertNotEqual(sent[-1]['incident_id'], incident)
            finally:
                monitor.close()

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
                for sample in (102, 162, 222):
                    monitor.tick(now=sample)
                row=json.loads(monitor.db.execute('SELECT content FROM incidents').fetchone()[0])
                self.assertEqual(row['processing_state'],'resolved')
                monitor.tick(lambda _: (_ for _ in ()).throw(OSError()),now=223)
                row=json.loads(monitor.db.execute('SELECT content FROM incidents').fetchone()[0])
                self.assertEqual(row['processing_state'],'open')
            finally:monitor.close()


if __name__=='__main__':unittest.main()
