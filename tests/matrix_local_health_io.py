import importlib.util
import os
import json
import io
import contextlib
from unittest.mock import patch
from pathlib import Path
import socket
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('matrix_receiving_health', Path(__file__).parents[1]/'scripts/matrix_receiving_health.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class LocalHealthTests(unittest.TestCase):
    def test_new_member_extends_protected_reader_vector(self):
        original=[dict(being_ref='first',embodiment_id='first-body')]
        newcomer=dict(being_ref='new',embodiment_id='new-body')
        required=[['first','first-body']]
        self.assertTrue(m.reader_coverage(original+[newcomer],required,original+[newcomer]))
        self.assertFalse(m.reader_coverage(original,required,original+[newcomer]))
        self.assertFalse(m.reader_coverage([newcomer],required,original+[newcomer]))

    def test_unselected_sibling_is_not_a_distinct_being_requirement(self):
        original=[dict(being_ref='first',embodiment_id='first-body')]
        sibling=dict(being_ref='first',embodiment_id='archived-body')
        self.assertTrue(m.reader_coverage(original,[['first','first-body']],original+[sibling]))
        self.assertFalse(m.reader_coverage([sibling],[['first','first-body']],original+[sibling]))
        self.assertFalse(m.reader_coverage(original+original,[['first','first-body']],original))

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


class SafeFailureTests(unittest.TestCase):
    def test_failure_is_bounded_redacted_and_replaced_not_appended(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            secret='private credential and source text'
            m.retain_failure({'state':tmp}, {'stage':'status_send','refusal_code':secret},
                TimeoutError(secret))
            first=json.loads((root/'probe-failure.json').read_text())
            self.assertEqual(first['kind'],'status_timeout')
            self.assertEqual(first['stage'],'status_send')
            self.assertNotIn(secret,(root/'probe-failure.json').read_text())
            m.retain_failure({'state':tmp}, {'stage':'peer_connect'}, ConnectionRefusedError(secret))
            second=json.loads((root/'probe-failure.json').read_text())
            self.assertEqual(second['kind'],'local_transport')
            self.assertEqual([p.name for p in root.iterdir()],['probe-failure.json'])
            self.assertLess((root/'probe-failure.json').stat().st_size,4096)
            self.assertEqual((root/'probe-failure.json').stat().st_mode & 0o777,0o600)

    def test_contract_shape_failure_records_only_allowlisted_field_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            m.retain_failure({'state':tmp},{'stage':'reader_check'},KeyError('rosters'))
            value=json.loads((root/'probe-failure.json').read_text())
            self.assertEqual(value['exception_type'],'KeyError')
            self.assertEqual(value['contract_field'],'rosters')
            m.retain_failure({'state':tmp},{'stage':'reader_check'},KeyError('private payload'))
            value=json.loads((root/'probe-failure.json').read_text())
            self.assertNotIn('contract_field',value)
            self.assertNotIn('private payload',(root/'probe-failure.json').read_text())

    def test_untrusted_stage_and_unsafe_destination_are_not_disclosed_or_followed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            target=root/'untouched'
            target.write_text('preserve')
            (root/'probe-failure.tmp').symlink_to(target)
            m.retain_failure({'state':tmp},{'stage':'private source'}, ValueError('private payload'))
            self.assertEqual(target.read_text(),'preserve')
            self.assertFalse((root/'probe-failure.json').exists())
            (root/'probe-failure.tmp').unlink()
            root.chmod(0o755)
            m.retain_failure({'state':tmp},{'stage':'status_send'},ValueError('private payload'))
            self.assertFalse((root/'probe-failure.json').exists())

    def test_main_keeps_existing_failure_exit_without_printing_private_exception(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            config=root/'config.json'
            config.write_text(json.dumps({'state':tmp}))
            def failed(_config, diagnostic):
                diagnostic['stage']='response_authentication'
                diagnostic['refusal_code']='verification_failed'
                raise ValueError('private payload')
            out=io.StringIO()
            with patch.object(m,'inspect',failed), patch.object(m.sys,'argv',
                    ['probe','--config',str(config)]), contextlib.redirect_stdout(out):
                self.assertEqual(m.main(),1)
            self.assertEqual(out.getvalue(),'')
            observed=json.loads((root/'probe-failure.json').read_text())
            self.assertEqual(observed['refusal_code'],'verification_failed')
            self.assertEqual(observed['stage'],'response_authentication')

if __name__=='__main__':unittest.main()
