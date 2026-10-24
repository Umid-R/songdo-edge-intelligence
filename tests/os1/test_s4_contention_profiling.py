"""Tests for OS1-4 /proc resource sampling and SQLite persistence."""
import os
import sys
import tempfile
import unittest
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from edge.teams.os1_building import s4_contention_profiling as m  # noqa: E402
from edge.storage import Storage  # noqa: E402


class TestS4ContentionProfiling(unittest.TestCase):
    def test_samples_rss_and_context_switches(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "test.db")
            ok, lines = m.run(samples=3, interval_s=0.01, db_path=db_path, pid=os.getpid())
            self.assertTrue(ok, "\n".join(lines))
            db = Storage(db_path, str(ROOT / "sql" / "schema.sql"), init=False)
            rows = db.list_health()
            self.assertEqual(len(rows), 1)
            self.assertGreater(rows[0]["rss_kb"], 0)
            self.assertGreaterEqual(rows[0]["ctx_voluntary"], 0)
            self.assertGreaterEqual(rows[0]["ctx_involuntary"], 0)

    def test_invalid_sample_count_rejected(self):
        with self.assertRaises(ValueError):
            m.run(samples=0, interval_s=0)


if __name__ == "__main__":
    unittest.main()
