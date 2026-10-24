"""Core tests. Run: PYTHONPATH=src python -m unittest discover -s tests -v"""
import os
import queue
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge.ipc import BoundedQueue, StopFlag, UnixSocketServer, unix_request   # noqa: E402
from edge.models import EdgeDecision, SensorEvent                    # noqa: E402
from edge.storage import Storage                                     # noqa: E402
from edge.decision import RuleEngine, dispatch                            # noqa: E402

SCHEMA = os.path.join(ROOT, "sql", "schema.sql")


def ev(value=1.0, sensor="S"):
    return SensorEvent(node_id="N", sensor=sensor, value=value)


class TestBoundedQueue(unittest.TestCase):
    def test_cap_and_drop_oldest(self):
        q = BoundedQueue(maxsize=500)
        for i in range(700):
            q.put(i)
        time.sleep(0.5)
        items = []
        while True:
            try:
                items.append(q.get(timeout=0.3))
            except queue.Empty:
                break
        self.assertEqual(len(items), 500)          # never above the cap
        self.assertEqual(q.dropped, 200)
        self.assertEqual(items[0], 200)            # oldest 200 were dropped
        self.assertEqual(items[-1], 699)           # newest kept


def _waiter(flag, ready):
    ready.set()
    flag.wait(30)


class TestStopFlag(unittest.TestCase):
    def test_set_wait_is_set(self):
        flag = StopFlag()
        self.assertFalse(flag.is_set())
        self.assertFalse(flag.wait(0.05))          # times out, not set
        flag.set()
        self.assertTrue(flag.is_set())
        self.assertTrue(flag.wait(0.05))

    def test_set_does_not_hang_after_a_waiter_is_sigkilled(self):
        """Regression: with multiprocessing.Event this deadlocked forever."""
        import multiprocessing as mp
        import signal
        import threading
        ctx = mp.get_context("spawn")
        flag, ready = StopFlag(ctx), ctx.Event()
        p = ctx.Process(target=_waiter, args=(flag, ready))
        p.start()
        self.assertTrue(ready.wait(10))
        time.sleep(0.3)                            # the child is now inside flag.wait()
        os.kill(p.pid, signal.SIGKILL)
        p.join(5)
        t = threading.Thread(target=flag.set, daemon=True)
        t.start()
        t.join(3)
        self.assertFalse(t.is_alive(), "StopFlag.set() hung after a waiter was SIGKILLed")
        self.assertTrue(flag.is_set())


class TestStorage(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.s = Storage(os.path.join(self.tmp.name, "t.db"), SCHEMA)

    def tearDown(self):
        self.tmp.cleanup()

    def test_wal_mode(self):
        self.assertEqual(self.s.journal_mode(), "wal")

    def test_event_and_outbox_are_atomic(self):
        e = ev()
        self.assertTrue(self.s.insert_event_with_outbox(e))
        self.assertFalse(self.s.insert_event_with_outbox(e))     # duplicate ignored
        self.assertEqual(self.s.counts()["events"], 1)
        self.assertEqual(self.s.outbox_pending(), 1)

    def test_rollback_leaves_nothing(self):
        with self.assertRaises(RuntimeError):
            with self.s.tx() as c:
                c.execute("INSERT INTO kv (key,value) VALUES ('a','b')")
                raise RuntimeError("boom")
        self.assertIsNone(self.s.kv_get("a"))

    def test_drain_is_conserving_and_idempotent(self):
        for i in range(120):
            self.s.insert_event_with_outbox(ev(i))
        self.assertEqual(self.s.drain_outbox_to_sink(50), 50)
        self.assertEqual(self.s.drain_outbox_to_sink(100), 70)
        self.assertEqual(self.s.drain_outbox_to_sink(100), 0)
        r = self.s.conservation_report()
        self.assertEqual((r["events"], r["sink"], r["pending"], r["conserved"]), (120, 120, 0, True))

    def test_wan_flag_shared_via_db(self):
        self.assertTrue(self.s.wan_online())
        self.s.set_wan_online(False)
        self.assertFalse(Storage(self.s.db_path, SCHEMA, init=False).wan_online())

    def test_online_backup_integrity(self):
        self.s.insert_event_with_outbox(ev())
        dest = os.path.join(self.tmp.name, "bk", "b.db")
        self.assertTrue(self.s.backup(dest))
        self.assertEqual(Storage(dest, SCHEMA, init=False).counts()["events"], 1)


class TestDecision(unittest.TestCase):
    RULES = [{"name": "r", "sensor": "CO2", "op": ">", "threshold": 1000,
              "actuator": "DAMPER", "command": 100, "ttl_s": 15.0}]

    def test_rule_fires_only_above_threshold(self):
        eng = RuleEngine(self.RULES)
        self.assertEqual(len(eng.evaluate(ev(1200, "CO2"))), 1)
        self.assertEqual(len(eng.evaluate(ev(800, "CO2"))), 0)
        self.assertEqual(len(eng.evaluate(ev(1200, "OTHER"))), 0)

    def test_monotonic_ttl_expiry(self):
        d = RuleEngine(self.RULES).evaluate(ev(1200, "CO2"), now_mono=100.0)[0]
        self.assertEqual(d.expires_mono, 115.0)
        self.assertEqual(dispatch(d, now_mono=110.0), "DISPATCHED")
        self.assertEqual(dispatch(d, now_mono=115.1), "EXPIRED")


class TestUnixSocket(unittest.TestCase):
    def test_roundtrip(self):
        with tempfile.TemporaryDirectory() as t:
            path = os.path.join(t, "s.sock")
            srv = UnixSocketServer(path, lambda m: {"echo": m})
            srv.start()
            try:
                self.assertEqual(unix_request(path, {"a": 1}), {"echo": {"a": 1}})
            finally:
                srv.stop()


if __name__ == "__main__":
    unittest.main()
