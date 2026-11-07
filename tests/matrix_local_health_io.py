import importlib.util
import os
from pathlib import Path
import socket
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('matrix_receiving_health', Path(__file__).parents[1]/'scripts/matrix_receiving_health.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class LocalHealthTests(unittest.TestCase):
    def test_live_owned_listener(self):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0)); listener.listen()
            m.check_local_health({'listener':{'host':'127.0.0.1','port':listener.getsockname()[1]}},
                {'not_after_ms': 200000000},os.getpid(),0)
    def test_proxy_socket_is_not_daemon_ownership(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); (root/'123/fd').mkdir(parents=True); (root/'123/net').mkdir()
            (root/'123/fd/4').symlink_to('socket:[999]')
            (root/'123/net/tcp').write_text('header\n0: 0100007F:221C 00000000:0000 0A 0:0 00:0 0 1000 0 555\n')
            with self.assertRaisesRegex(m.LocalHealthFailure,'matrix_listener_unavailable'):
                m.check_local_health({'listener':{'host':'127.0.0.1','port':8732}}, {},123,0,proc=root)
    def test_expiring_and_expired_roles(self):
        for expiry in (0,86400000):
            with self.assertRaisesRegex(m.LocalHealthFailure,'matrix_local_capability_expiring'):
                m.check_local_health({}, {'not_after_ms':expiry},os.getpid(),0)
    def test_live_and_until_revoked_roles(self):
        for descriptor in ({'not_after_ms':86400001},{'validity':{'mode':'until-revoked','not_before_ms':0}}):
            m.check_local_health({},descriptor,os.getpid(),0)
    def test_no_nonlocal_or_inferred_listener(self):
        for listener in ({'host':'10.0.0.1','port':8732},{'host':'127.0.0.1','port':True}):
            with self.assertRaises(ValueError):m.check_local_health({'listener':listener},{},os.getpid(),0)

if __name__=='__main__':unittest.main()
