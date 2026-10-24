"""Tests for OS1-5 offline outbox and lossless replay."""
import os
import sys
import tempfile
import unittest
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from edge.teams.os1_building import s5_wan_outage as m  # noqa: E402
from edge.storage import Storage  # noqa: E402


class TestS5WanOutage(unittest.TestCase):
    def test_events_queue_offline_and_drain_after_reconnect(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "test.db")
            ok, lines = m.run(event_count=37, batch_size=8, db_path=db_path)
            self.assertTrue(ok, "\n".join(lines))
            db = Storage(db_path, str(ROOT / "sql" / "schema.sql"), init=False)
            report = db.conservation_report()
            self.assertEqual(report, {"events": 37, "sink": 37, "pending": 0, "conserved": True})

    def test_invalid_batch_size_rejected(self):
        with self.assertRaises(ValueError):
            m.run(event_count=10, batch_size=0)


if __name__ == "__main__":
    unittest.main()
