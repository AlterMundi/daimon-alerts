"""The independent embedding probe retains one incident across retries."""
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from observer import Observer


class EmbeddingObservation(unittest.TestCase):
    def test_recurrence_and_recovery(self):
        with TemporaryDirectory() as directory:
            observer = object.__new__(Observer)
            observer.state = Path(directory)
            observer.embedding_probe = ["/test/probe"]
            observer.config = {"observer_id": "dmk-test"}
            publications = []
            observer.project = lambda value: publications.append(dict(value))
            with patch("observer.subprocess.run") as probe, patch("observer.time.time") as clock:
                probe.side_effect = [
                    subprocess.CompletedProcess([], 1),
                    subprocess.CompletedProcess([], 1),
                    subprocess.CompletedProcess([], 0),
                    subprocess.TimeoutExpired([], 15),
                ]
                clock.return_value = 1000
                observer.embedding_incident()
                original = publications[-1]
                clock.return_value = 1299
                observer.embedding_incident()
                self.assertEqual(probe.call_count, 1)
                clock.return_value = 1300
                observer.embedding_incident()
                self.assertEqual(publications[-1], original)
                clock.return_value = 1600
                observer.embedding_incident()
                self.assertEqual(publications[-1]["processing_state"], "resolved")
                self.assertEqual(publications[-1]["incident_id"], original["incident_id"])
                clock.return_value = 1900
                observer.embedding_incident()
                self.assertEqual(publications[-1]["processing_state"], "open")
                self.assertNotEqual(publications[-1]["incident_id"], original["incident_id"])
                sample = json.loads((observer.state / "embedding-probe.json").read_text())
                self.assertEqual(sample["checked_at"], 1900)
                self.assertFalse(sample["healthy"])


if __name__ == "__main__":
    unittest.main()
