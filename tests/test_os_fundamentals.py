"""Automated OS concurrency and durability test suite (course README, Quickstart Step 3).

    python tests/test_os_fundamentals.py          # from the repository root (python3 on Ubuntu)

Verifies, in the order the README lists them:
  1. SQLite WAL mode & atomic outbox enqueue
  2. Bounded queue backpressure (drop_oldest policy under load)
  3. Classic race condition demonstration and Mutex resolution
  4. Deadlock detection with timeout recovery
  5. Online SQLite database backup integrity check

Each check uses the shared modules (src/edge/storage.py, src/edge/ipc.py) or the scenario that
owns the concept (OS1-2 race condition, OS5-3 deadlock), so this file tests the real code.
"""
import os
import queue
import sqlite3
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge.ipc import BoundedQueue                                     # noqa: E402
from edge.models import SensorEvent                                   # noqa: E402
from edge.storage import Storage                                      # noqa: E402
from edge.teams.os1_building import s2_race_condition as race          # noqa: E402
from edge.teams.os5_reliable import s3_deadlock_timeout as deadlock    # noqa: E402

SCHEMA = os.path.join(ROOT, "sql", "schema.sql")


def _event(value=1.0):
    return SensorEvent(node_id="EDGE_NODE_TEST", sensor="TEST", value=value)


class Test1WalAndAtomicOutbox(unittest.TestCase):
    """1. SQLite WAL mode & atomic outbox enqueue."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.st = Storage(os.path.join(self.tmp.name, "edge.db"), SCHEMA)

    def tearDown(self):
        self.tmp.cleanup()

    def test_journal_mode_is_wal(self):
        self.assertEqual(self.st.journal_mode(), "wal")

    def test_event_and_outbox_row_written_together(self):
        for i in range(25):
            self.assertTrue(self.st.insert_event_with_outbox(_event(i)))
        self.assertEqual(self.st.counts()["events"], 25)
        self.assertEqual(self.st.outbox_pending(), 25)       # one outbox row per event, no more, no less

    def test_duplicate_event_is_ignored(self):
        e = _event()
        self.assertTrue(self.st.insert_event_with_outbox(e))
        self.assertFalse(self.st.insert_event_with_outbox(e))  # same event_id: idempotent
        self.assertEqual(self.st.counts()["events"], 1)
        self.assertEqual(self.st.outbox_pending(), 1)

    def test_failed_transaction_leaves_nothing(self):
        with self.assertRaises(RuntimeError):
            with self.st.tx() as c:
                c.execute("INSERT INTO kv (key, value) VALUES ('half', 'written')")
                raise RuntimeError("crash in the middle of a transaction")
        self.assertIsNone(self.st.kv_get("half"))             # rolled back as a whole

    def test_drain_conserves_every_event(self):
        for i in range(120):
            self.st.insert_event_with_outbox(_event(i))
        while self.st.drain_outbox_to_sink(50):
            pass
        r = self.st.conservation_report()
        self.assertEqual((r["events"], r["sink"], r["pending"], r["conserved"]), (120, 120, 0, True))


class Test2BoundedQueueBackpressure(unittest.TestCase):
    """2. Bounded queue backpressure (drop_oldest under load)."""

    def test_queue_never_exceeds_500_and_drops_oldest(self):
        q = BoundedQueue(maxsize=500)
        for i in range(700):                 # producer much faster than the (absent) consumer
            q.put(i)
        time.sleep(0.5)                      # let the feeder thread flush into the pipe
        items = []
        while True:
            try:
                items.append(q.get(timeout=0.3))
            except queue.Empty:
                break
        self.assertEqual(len(items), 500)    # capped: memory stays bounded
        self.assertEqual(q.dropped, 200)     # exactly the overflow was dropped
        self.assertEqual(items[0], 200)      # the OLDEST 200 went
        self.assertEqual(items[-1], 699)     # the newest reading is kept


class Test3RaceConditionAndMutex(unittest.TestCase):
    """3. Race condition demonstration (80 increments) and Mutex resolution."""

    def test_threads_lose_updates_without_mutex(self):
        ok, lines = race.run(workers=80, processes=False)
        self.assertTrue(ok, "\n".join(lines))   # unprotected < 80, with Mutex == 80

    def test_real_processes_lose_updates_without_lock(self):
        expected, res = race.run_processes(total=80, procs=4)
        self.assertEqual(expected, 80)
        self.assertLess(res["unprotected"], 80)
        self.assertEqual(res["locked"], 80)


class Test4DeadlockTimeoutRecovery(unittest.TestCase):
    """4. Deadlock detection with timeout recovery (acquire(timeout=0.2))."""

    def test_detected_then_recovered(self):
        ok, lines = deadlock.run()
        self.assertTrue(ok, "\n".join(lines))   # naive deadlocks and is detected; timeout + ordered finish


class Test5OnlineBackupIntegrity(unittest.TestCase):
    """5. Online SQLite backup integrity check."""

    def test_backup_is_complete_and_passes_integrity_check(self):
        with tempfile.TemporaryDirectory() as tmp:
            st = Storage(os.path.join(tmp, "edge.db"), SCHEMA)
            for i in range(50):
                st.insert_event_with_outbox(_event(i))
            dest = os.path.join(tmp, "backups", "edge_backup.db")
            self.assertTrue(st.backup(dest))                  # taken while the source DB is open
            conn = sqlite3.connect(dest)
            try:
                self.assertEqual(conn.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            finally:
                conn.close()
            copy = Storage(dest, SCHEMA, init=False)
            self.assertEqual(copy.counts()["events"], 50)
            self.assertEqual(copy.outbox_pending(), 50)


if __name__ == "__main__":
    unittest.main(verbosity=2)
