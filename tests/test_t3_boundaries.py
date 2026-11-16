"""T3 (final-term assessment): hit the 0.8 m and TTL boundaries under writer contention.

Uses the shared node's own path (RuleEngine with the obstacle_estop rule from config/os2.yaml ->
Storage.commit_event -> Storage.apply_intent) while three other processes keep writing to the SAME
SQLite database. A 50 ms control loop on absolute monotonic deadlines feeds LiDAR values right at the
boundary (0.81, 0.80, 0.79 m). Every tick records its monotonic time, the gap to the previous tick,
the decision and, for every command, its age when it was applied.

Safety semantics checked:
    - strictly below 0.8 m latches the e-stop; exactly 0.80 m and above do not
    - a command is applied only while younger than its 500 ms TTL; at or past the TTL it is EXPIRED
      (exact boundary checked with monotonic offsets just inside and exactly at the TTL)
    - under contention every applied command was younger than the TTL, and ticks kept running
With SONGDO_EVIDENCE_DIR set, the monotonic trace is kept as T3_boundaries_under_contention.json.
"""
import multiprocessing as mp
import os
import sqlite3
import sys
import tempfile
import time
import unittest

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, os.path.join(ROOT, "tests"))
import evidence                                    # noqa: E402
from edge.decision import RuleEngine, boot_epoch   # noqa: E402
from edge.models import SensorEvent                # noqa: E402
from edge.storage import Storage                   # noqa: E402

PERIOD = 0.050
TICKS = 120
VALUES = [1.50, 0.81, 0.80, 0.79, 2.00, 0.80, 0.799, 1.20]   # 0.79 and 0.799 must latch, 0.80 / 0.81 not
WRITERS = 3


def writer(db, stop, counter):
    """Contention: small write transactions on the same database file as fast as possible."""
    c = sqlite3.connect(db, timeout=10)
    c.execute("PRAGMA busy_timeout=10000")
    n = 0
    while not stop.is_set():
        with c:
            c.execute("INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                      (f"load-{os.getpid()}-{n % 50}", "x" * 200))
        n += 1
    with counter.get_lock():
        counter.value += n
    c.close()


def estop_rule():
    with open(os.path.join(ROOT, "config", "os2.yaml"), encoding="utf-8") as f:
        rules = yaml.safe_load(f)["rules"]
    return [r for r in rules if r["name"] == "obstacle_estop"][0]


class TestT3Boundaries(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db = os.path.join(self.tmp.name, "t3.db")
        self.st = Storage(self.db, os.path.join(ROOT, "sql", "schema.sql"))
        self.rule = estop_rule()
        self.engine = RuleEngine([self.rule], deterministic_ids=True)
        self.epoch = boot_epoch()

    def tearDown(self):
        self.st.close()
        self.tmp.cleanup()

    def test_thresholds_and_ttl_under_writer_contention(self):
        self.assertEqual((self.rule["op"], self.rule["threshold"], self.rule["ttl_s"]), ("<", 0.8, 0.5))
        ctx = mp.get_context("spawn")
        stop, counter = ctx.Event(), ctx.Value("q", 0)
        procs = [ctx.Process(target=writer, args=(self.db, stop, counter)) for _ in range(WRITERS)]
        for p in procs:
            p.start()
        time.sleep(0.5)                                    # writers are running
        trace, prev = [], None
        t_next = time.monotonic()
        try:
            for i in range(TICKS):
                t_next += PERIOD
                now = time.monotonic()
                value = VALUES[i % len(VALUES)]
                ev = SensorEvent(node_id="T3", sensor="LIDAR_PROXIMITY_M", value=value, unit="m", event_id=f"t3-{i:04d}")
                ds = self.engine.evaluate(ev, now_mono=now)
                self.st.commit_event(ev, ds)
                row = {"tick": i, "mono": round(now, 6), "gap_ms": None if prev is None else round((now - prev) * 1000, 2),
                       "value_m": value, "commands": len(ds), "status": None, "command_age_ms": None}
                for d in ds:
                    pending = [r for r in self.st.pending_intents() if r["decision_id"] == d.decision_id][0]
                    t_apply = time.monotonic()
                    row["status"], _ = self.st.apply_intent(pending, self.epoch, now_mono=t_apply)
                    row["command_age_ms"] = round((t_apply - d.issued_mono) * 1000, 3)
                trace.append(row)
                prev = now
                time.sleep(max(0.0, t_next - time.monotonic()))
        finally:
            stop.set()
            for p in procs:
                p.join(10)
        written = counter.value

        # ---- 0.8 m boundary (strict "<") ----
        for r in trace:
            if r["value_m"] < 0.8:
                self.assertEqual(r["commands"], 1, r)
            else:
                self.assertEqual(r["commands"], 0, r)          # 0.80 exactly does NOT latch
        latched = [r for r in trace if r["commands"]]
        self.assertTrue(latched)
        # ---- TTL under contention: applied => younger than TTL; otherwise expired ----
        ttl_ms = self.rule["ttl_s"] * 1000
        for r in latched:
            if r["status"] == "DISPATCHED":
                self.assertLess(r["command_age_ms"], ttl_ms, r)
            else:
                self.assertEqual(r["status"], "EXPIRED", r)
                self.assertGreaterEqual(r["command_age_ms"], ttl_ms, r)
        gaps = [r["gap_ms"] for r in trace if r["gap_ms"] is not None]
        self.assertLess(max(gaps), 1000.0, "control loop stalled for a second under contention")
        self.assertGreater(written, TICKS, "the writers did not actually contend")

        # ---- exact TTL boundary with monotonic offsets ----
        ev = SensorEvent(node_id="T3", sensor="LIDAR_PROXIMITY_M", value=0.5, unit="m", event_id="ttl-inside")
        d = self.engine.evaluate(ev, now_mono=1000.0)[0]
        self.st.commit_event(ev, [d])
        r = [x for x in self.st.pending_intents() if x["decision_id"] == d.decision_id][0]
        inside = self.st.apply_intent(r, self.epoch, now_mono=1000.0 + 0.5 - 0.001)[0]
        ev2 = SensorEvent(node_id="T3", sensor="LIDAR_PROXIMITY_M", value=0.5, unit="m", event_id="ttl-at")
        d2 = self.engine.evaluate(ev2, now_mono=2000.0)[0]
        self.st.commit_event(ev2, [d2])
        r2 = [x for x in self.st.pending_intents() if x["decision_id"] == d2.decision_id][0]
        at = self.st.apply_intent(r2, self.epoch, now_mono=2000.0 + 0.5)[0]
        self.assertEqual((inside, at), ("DISPATCHED", "EXPIRED"))
        rep = self.st.recovery_report()
        self.assertTrue(rep["ok"], rep)                      # receipts = dispatched, nothing duplicated

        ages = [r["command_age_ms"] for r in latched]
        summary = {"ticks": len(trace), "period_ms": PERIOD * 1000, "writers": WRITERS, "rows_written_by_writers": written,
                   "max_gap_ms": max(gaps), "mean_gap_ms": round(sum(gaps) / len(gaps), 2),
                   "latched": len(latched), "dispatched": sum(r["status"] == "DISPATCHED" for r in latched),
                   "expired": sum(r["status"] == "EXPIRED" for r in latched),
                   "max_command_age_ms": max(ages), "ttl_ms": ttl_ms,
                   "ttl_boundary": {"applied_at_ttl_minus_1ms": inside, "applied_at_ttl": at}}
        print(f"\nT3: {summary}")
        evidence.save("T3_boundaries_under_contention.json", {
            "test": "T3", "rule": self.rule, "clock": "time.monotonic()", "summary": summary, "trace": trace})


if __name__ == "__main__":
    unittest.main()
