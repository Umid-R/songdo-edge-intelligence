"""OS3-5: Offline Variable Speed Sign Dispatch & Event Replay

Mission : The WAN is cut while pedestrians surge at the crosswalk. The node must still set
          the speed sign to 30 km/h locally, then replay every stored event when the WAN returns.
Concept : Local decisions never wait for the network. The command carries a monotonic-clock
          TTL, so a stale command is never executed late. Replay is safe because the upstream
          deduplicates on event_id.

Owner   : (write names here)
Test    : tests/os3/test_s5_speed_sign_replay.py
Run     : PYTHONPATH=src python -m edge.teams.os3_mobility.s5_speed_sign_replay [--duration 600]
          (--duration is the length of the WAN outage in seconds; default 3, use 600 for the
          10-minute signature run. The live node version: python scripts/os3_live_demo.py)

Rule: config/os3.yaml (PED_CROSSWALK_COUNT > 10 -> VARIABLE_SPEED_SIGN_KMH = 30). Its TTL
there is 60 s (ASSUMED); this scenario shortens it to 1 s so the sign visibly reverts to
50 km/h during a short run.
"""
import argparse
import os
import random
import sys
import tempfile
import time

from edge.config import load_config
from edge.models import EdgeDecision, SensorEvent
from edge.storage import Storage
from edge.decision import RuleEngine, dispatch

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))))
NODE_ID = "EDGE_NODE_OS3_MOBILITY"
NORMAL_KMH = 50.0
SCENARIO_TTL_S = 1.0


class SpeedSign:
    """Simulated variable speed sign. Shows the last command while it is fresh, else 50."""

    def __init__(self, normal=NORMAL_KMH):
        self.normal = normal
        self._active = None
        self.executed = 0
        self.rejected = 0

    def dispatch(self, decision):
        status = dispatch(decision)                # core actuator: rejects expired commands
        if status == "DISPATCHED":
            self._active = decision
            self.executed += 1
        else:
            self.rejected += 1
        return status

    def effective_kmh(self):
        if self._active is not None and self._active.is_fresh():
            return self._active.command_value
        return self.normal


def _count(st, sql):
    with st.tx() as c:
        return c.execute(sql).fetchone()[0]


def run(duration=3.0):
    """Returns (ok, report_lines)."""
    cfg = load_config(os.path.join(ROOT, "config", "os3.yaml"))
    rules = [dict(r, ttl_s=SCENARIO_TTL_S) for r in cfg["rules"]]
    engine = RuleEngine(rules)
    rng = random.Random(7)
    sign = SpeedSign()
    generated = 0
    sign_seen = set()

    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(os.path.join(tmp, "os3_s5.db"), cfg["paths"]["schema"])

        def sync_tick():
            """What the SyncWorker does: drain only while the WAN is up."""
            if st.wan_online():
                return st.drain_outbox_to_sink(50)
            return None

        def run_phase(seconds, surge_from=None, surge_to=None):
            nonlocal generated
            start = last_sync = time.monotonic()
            failures = 0
            while (now := time.monotonic()) - start < seconds:
                elapsed = now - start
                surging = surge_from is not None and surge_from <= elapsed < surge_to
                if generated % 2:
                    sensor, unit = "PED_CROSSWALK_COUNT", "count"
                    value = float(rng.randint(12, 20) if surging else rng.randint(0, 5))
                else:
                    sensor, unit = "RADAR_SPEED", "km_h"
                    value = max(0.0, rng.gauss(45, 8))
                ev = SensorEvent(NODE_ID, sensor, round(value, 2), unit, received_at=time.time())
                st.insert_event_with_outbox(ev)
                generated += 1
                for d in engine.evaluate(ev):
                    st.insert_decision(d)
                    st.set_decision_status(d.decision_id, sign.dispatch(d))
                sign_seen.add(sign.effective_kmh())
                if now - last_sync >= 0.25:
                    if sync_tick() is None:
                        failures += 1
                    last_sync = now
                time.sleep(0.01)
            return failures

        run_phase(1.0)                                         # online, normal traffic
        sink_online = st.counts()["sink"]

        st.set_wan_online(False)                               # ---- WAN cut ----
        pending_before = st.outbox_pending()
        decisions_before = st.counts()["decisions"]
        sign_seen.clear()
        failures = run_phase(duration, surge_from=duration / 3, surge_to=2 * duration / 3)
        pending_after = st.outbox_pending()
        outage_decisions = st.counts()["decisions"] - decisions_before
        got_30 = 30.0 in sign_seen
        sink_during = st.counts()["sink"] - sink_online
        time.sleep(SCENARIO_TTL_S + 0.1)                       # no more refreshes
        reverted = sign.effective_kmh() == NORMAL_KMH

        st.set_wan_online(True)                                # ---- WAN back ----
        # Simulate a crash between "delivered" and "marked sent": 5 rows reach the sink twice.
        with st.tx() as c:
            rows = c.execute("SELECT event_id, payload FROM outbox WHERE sent_at IS NULL "
                             "ORDER BY id LIMIT 5").fetchall()
            resent = [(r["event_id"], r["payload"]) for r in rows]
            first = [c.execute("INSERT OR IGNORE INTO upstream_sink VALUES (?,?,?)",
                               (e, p, time.time())).rowcount for e, p in resent]
        with st.tx() as c:
            again = [c.execute("INSERT OR IGNORE INTO upstream_sink VALUES (?,?,?)",
                               (e, p, time.time())).rowcount for e, p in resent]
        duplicates_ignored = sum(1 for n in again if n == 0)
        while sync_tick():                                     # replay until the outbox is empty
            pass

        stale = EdgeDecision(event_id="x", rule="stale_test", actuator="VARIABLE_SPEED_SIGN_KMH",
                             command_value=30, issued_mono=time.monotonic() - 5,
                             expires_mono=time.monotonic() - 1)
        stale_rejected = sign.dispatch(stale) == "EXPIRED"

        stored = st.counts()["events"]
        outbox_total = _count(st, "SELECT COUNT(*) FROM outbox")
        pending_final = st.outbox_pending()
        report = st.conservation_report()

    ok = (outage_decisions > 0 and got_30 and reverted
          and sink_during == 0 and failures > 0 and pending_after > pending_before
          and pending_final == 0 and report["conserved"]
          and report["sink"] == generated == stored == outbox_total
          and len(resent) == 5 and duplicates_ignored == 5 and stale_rejected)
    return ok, [
        f"outage lasted {duration:.0f}s; local decisions during outage: {outage_decisions}; "
        f"sign showed 30 km/h: {got_30}; reverted to {NORMAL_KMH:.0f} after the TTL: {reverted}",
        f"outbox pending {pending_before} -> {pending_after} during outage; "
        f"{failures} sync ticks skipped (WAN down); upstream got {sink_during} events",
        f"after reconnect: generated {generated}, stored {stored}, outbox rows {outbox_total}, "
        f"upstream {report['sink']}, still pending {pending_final}, conserved {report['conserved']}",
        f"re-sent {len(resent)} rows on purpose, upstream ignored {duplicates_ignored} duplicates",
        f"expired command rejected by the actuator: {stale_rejected}",
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=3.0, help="WAN outage seconds (600 = full demo)")
    args = ap.parse_args()
    ok, lines = run(args.duration)
    print(f"[{'PASS' if ok else 'FAIL'}] OS3-5 Offline speed sign dispatch & event replay")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
