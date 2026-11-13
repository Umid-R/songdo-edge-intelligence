"""T1 (final-term assessment): kill the shared node's Processor at every crash point and check the
exit rule: every admitted event and every command intent has exactly one durable disposition,
and every applied command has exactly one receipt (no lost events, no duplicate effects).

The Processor really dies (os._exit(137), same as SIGKILL: no cleanup runs) at:
    after_dequeue  - the event is off the queue but not committed
    after_commit   - event + outbox + intents committed, nothing applied
    after_apply    - one command applied and its receipt committed
then a fresh Processor process is started on the same database.
"""
import multiprocessing as mp
import os
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from edge.decision import RuleEngine, boot_epoch          # noqa: E402
from edge.ipc import STOP, BoundedQueue, StopFlag          # noqa: E402
from edge.models import SensorEvent                        # noqa: E402
from edge.storage import Storage                           # noqa: E402
from edge.workers import processor_main                    # noqa: E402

N_EVENTS = 40
RULE = {"name": "co2_vent", "sensor": "CO2", "op": ">", "threshold": 1000,
        "actuator": "HVAC_DAMPER_PCT", "command": 100, "ttl_s": 60.0}


def make_cfg(tmp):
    return {"node": {"id": "TEST"}, "rules": [RULE],
            "paths": {"db": os.path.join(tmp, "edge.db"), "schema": os.path.join(ROOT, "sql", "schema.sql")},
            "processor": {"sweep_interval_s": 0.2, "sweep_age_s": 0.0}}


def events():
    # every 4th reading is above 1000 ppm -> 10 commands
    return [SensorEvent(node_id="TEST", sensor="CO2", value=1200.0 if i % 4 == 0 else 600.0, unit="ppm",
                        event_id=f"ev{i:03d}") for i in range(N_EVENTS)]


def run_processor(cfg, items, env=None, timeout=20):
    """Start processor_main in its own process, feed items, then STOP. Returns the exit code."""
    ctx = mp.get_context("spawn")
    q, stop = BoundedQueue(500, ctx), StopFlag(ctx)
    old = dict(os.environ)
    os.environ.update(env or {})
    try:
        p = ctx.Process(target=processor_main, args=(cfg, q, stop))
        p.start()
    finally:
        os.environ.clear()
        os.environ.update(old)
    for it in items:
        q.put(it)
    if not env:                               # normal run: stop after the queue is drained
        q.put(STOP)
        stop.set()
    p.join(timeout)                           # crash run: it dies by itself at the crash point
    if p.is_alive():
        p.kill()
        p.join()
    return p.exitcode


class TestCrashRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.cfg = make_cfg(self.tmp.name)
        self.st = Storage(self.cfg["paths"]["db"], self.cfg["paths"]["schema"])
        self.evs = events()
        for ev in self.evs:                   # SensorGen's job: admit durably, then queue
            self.st.admit(ev)

    def tearDown(self):
        self.st.close()
        self.tmp.cleanup()

    def crash_then_restart(self, point):
        mark = os.path.join(self.tmp.name, "crashed")
        code = run_processor(self.cfg, [e.to_dict() for e in self.evs],
                             env={"EDGE_CRASH_AT": point, "EDGE_CRASH_MARK": mark})
        self.assertEqual(code, 137, f"processor should have crashed at {point}")
        self.assertTrue(os.path.exists(mark))
        before = self.st.recovery_report()
        # restarted Processor: the queue content died with the old process, only SQLite is left
        self.assertEqual(run_processor(self.cfg, []), 0)
        return before, self.st.recovery_report()

    def check_exit_rule(self, rep):
        self.assertEqual(rep["admitted"], N_EVENTS)
        self.assertEqual(rep["unprocessed"], 0, rep)              # nothing admitted left behind
        self.assertEqual(rep["admitted_but_lost"], 0, rep)
        self.assertEqual(self.st.counts()["events"], N_EVENTS)
        self.assertEqual(self.st.outbox_pending(), N_EVENTS)      # every event still goes upstream
        self.assertEqual(sum(rep["intents"].values()), N_EVENTS // 4, rep)   # one intent per hot reading
        self.assertEqual(rep["intents"].get("PENDING", 0), 0, rep)           # every intent terminal
        self.assertEqual(rep["duplicate_intents"], 0)
        self.assertEqual(rep["receipts"], rep["dispatched"])     # applied <=> receipt
        self.assertEqual(rep["orphan_receipts"], 0)
        self.assertTrue(rep["ok"], rep)

    def test_kill_after_dequeue(self):
        before, after = self.crash_then_restart("after_dequeue")
        self.assertEqual(before["unprocessed"], N_EVENTS)        # died before the first commit
        self.check_exit_rule(after)

    def test_kill_after_commit_before_dispatch(self):
        before, after = self.crash_then_restart("after_commit")
        self.assertEqual(before["intents"].get("PENDING", 0), 1)  # committed, never applied
        self.assertEqual(before["receipts"], 0)
        self.check_exit_rule(after)
        self.assertEqual(after["dispatched"], N_EVENTS // 4)      # replayed and applied, TTL 60 s

    def test_kill_after_dispatch_no_duplicate_effect(self):
        before, after = self.crash_then_restart("after_apply")
        self.assertEqual(before["receipts"], 1)
        self.check_exit_rule(after)
        rows = self.st._read("SELECT decision_id, COUNT(*) AS n FROM actuator_receipts GROUP BY 1 HAVING n > 1")
        self.assertEqual(rows, [])                                # no command applied twice

    def test_replay_is_idempotent(self):
        """Process every event twice (queue + sweep race): still one event, one intent each."""
        self.assertEqual(run_processor(self.cfg, [e.to_dict() for e in self.evs] * 2), 0)
        self.check_exit_rule(self.st.recovery_report())


class TestIntentGuards(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.st = Storage(os.path.join(self.tmp.name, "g.db"), os.path.join(ROOT, "sql", "schema.sql"))
        self.engine = RuleEngine([RULE], deterministic_ids=True)

    def tearDown(self):
        self.st.close()
        self.tmp.cleanup()

    def commit(self, eid, now, age=0.0):
        ev = SensorEvent(node_id="T", sensor="CO2", value=1500.0, event_id=eid)
        ds = self.engine.evaluate(ev, now_mono=now, age_s=age)
        self.st.commit_event(ev, ds)
        return ds[0]

    def pending(self, did):
        return [r for r in self.st.pending_intents() if r["decision_id"] == did][0]

    def test_deterministic_id(self):
        a = self.commit("same", 100.0)
        b = self.engine.evaluate(SensorEvent(node_id="T", sensor="CO2", value=1500.0, event_id="same"))[0]
        self.assertEqual(a.decision_id, b.decision_id)

    def test_ttl_expired_on_monotonic_clock(self):
        d = self.commit("e1", now=100.0)
        self.assertEqual(self.st.apply_intent(self.pending(d.decision_id), boot_epoch(), now_mono=161.0)[0],
                         "EXPIRED")
        self.assertEqual(self.st.actuator_states(), [])

    def test_replayed_old_reading_is_born_expired(self):
        d = self.commit("old", now=100.0, age=120.0)            # reading is 2 minutes old
        self.assertEqual(self.st.apply_intent(self.pending(d.decision_id), boot_epoch(), now_mono=100.0)[0],
                         "EXPIRED")

    def test_old_boot_epoch_rejected(self):
        d = self.commit("e2", now=100.0)
        self.assertEqual(self.st.apply_intent(self.pending(d.decision_id), "another-boot", now_mono=100.0),
                         ("EXPIRED", "issued in another boot epoch"))

    def test_lower_sequence_superseded_and_apply_idempotent(self):
        d1 = self.commit("a", now=100.0)
        d2 = self.commit("b", now=100.0)
        r1, r2 = self.pending(d1.decision_id), self.pending(d2.decision_id)
        self.assertEqual(self.st.apply_intent(r2, boot_epoch(), now_mono=101.0)[0], "DISPATCHED")
        self.assertEqual(self.st.apply_intent(r1, boot_epoch(), now_mono=101.0)[0], "SUPERSEDED")
        self.assertEqual(self.st.apply_intent(r2, boot_epoch(), now_mono=101.0)[0], "DISPATCHED")  # no-op
        self.assertEqual(self.st.recovery_report()["receipts"], 1)
        self.assertEqual(self.st.actuator_states()[0]["last_seq"], r2["seq"])


if __name__ == "__main__":
    unittest.main()
