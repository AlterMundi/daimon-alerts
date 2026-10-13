"""The published fixture command starts HTTP and terminates its child on SIGTERM."""
import selectors
import signal
import socket
import subprocess
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]
class FixtureCommand(unittest.TestCase):
    def test_start_and_sigterm_do_not_leave_a_listener(self):
        with socket.socket() as stream:
            stream.bind(('127.0.0.1',0)); port=stream.getsockname()[1]
        process=subprocess.Popen(['python3',str(ROOT/'scripts/fixture_server.py'),'--port',str(port)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout,selectors.EVENT_READ)
                self.assertTrue(selector.select(timeout=15),'fixture startup was not observed')
            self.assertTrue(process.stdout.readline().startswith('Fictional runtime ready.'))
            process.send_signal(signal.SIGTERM)
            process.communicate(timeout=5)
            self.assertEqual(process.returncode,0)
            with socket.socket() as stream:
                stream.settimeout(1)
                self.assertNotEqual(stream.connect_ex(('127.0.0.1',port)),0)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM); process.communicate(timeout=5)
if __name__=='__main__': unittest.main(verbosity=2)
