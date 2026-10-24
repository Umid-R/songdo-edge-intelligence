"""OS5-4: Disaster Flood Relief Sluice Gate Actuation

Mission : Open the backup sluice gate when the canal water level exceeds the critical 2.8 m.
Action  : dispatch BACKUP_SLUICE_GATE_OPEN (rule and TTL come from config/os5.yaml).
Concept : Strict threshold (> 2.8 m, so exactly 2.8 does NOT open), command TTL on the MONOTONIC
          clock (a stale open command is rejected), and hysteresis so the gate does not chatter.

Owner   : Teshaboev Umidillo
Test    : tests/os5/test_s4_sluice_gate.py
Run     : PYTHONPATH=src python -m edge.teams.os5_reliable.s4_sluice_gate

Design choices that are OURS (the handbook does not give them; explain them in the defense):
  * the gate closes again at or below 2.5 m (ASSUMED hysteresis; between 2.5 and 2.8 it keeps its state)
  * the command TTL is 60 s (ASSUMED in config/os5.yaml)
Water levels are SIMULATED (a made-up flood hydrograph), not real MEIS data.
"""
import os
import sys
import tempfile
import time

from edge.config import load_config
from edge.models import SensorEvent
from edge.storage import Storage
from edge.decision import RuleEngine
from edge.teams.os5_reliable._common import ROOT, gate_next_state

ACTUATOR = "BACKUP_SLUICE_GATE_OPEN"
LEVELS = [1.5, 1.8, 2.2, 2.6, 2.79, 2.8, 2.81, 3.0, 3.2, 3.1, 2.9, 2.7, 2.6, 2.5, 2.4]


class SluiceGate:
    def __init__(self):
        self.state = "CLOSED"
        self.transitions = []

    def submit(self, decision, now_mono=None):
        """Actuate only while the command is fresh. Returns DISPATCHED or EXPIRED."""
        if not decision.is_fresh(now_mono):
            return "EXPIRED"
        if decision.actuator == ACTUATOR:
            self.state = "OPEN"
        return "DISPATCHED"

    def on_level(self, level, index=None):
        new = gate_next_state(self.state, level)
        if new == "CLOSED" and self.state == "OPEN":
            self.state = "CLOSED"
            self.transitions.append((index, "CLOSED"))


def run():
    """Returns (ok, report_lines)."""
    cfg = load_config(os.path.join(ROOT, "config", "os5.yaml"))
    rule = cfg["rules"][0]
    engine = RuleEngine(cfg["rules"])
    node = cfg["node"]["id"]
    checks = []

    def check(name, cond):
        checks.append((name, bool(cond)))

    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(os.path.join(tmp, "os5_sluice.db"), cfg["paths"]["schema"])
        gate = SluiceGate()
        states, statuses, latencies = [], [], []
        for i, level in enumerate(LEVELS):
            now = 100.0 + i                                   # injected monotonic clock, 1 s per reading
            ev = SensorEvent(node_id=node, sensor="SLUICE_WATER_LEVEL_M", value=level, unit="m")
            st.insert_event_with_outbox(ev)
            t0 = time.perf_counter()
            decisions = engine.evaluate(ev, now_mono=now)
            for d in decisions:
                status = gate.submit(d, now_mono=now)
                st.insert_decision(d)
                st.set_decision_status(d.decision_id, status)
                statuses.append(status)
                if gate.state == "OPEN" and not gate.transitions:
                    gate.transitions.append((i, "OPEN"))
            latencies.append((time.perf_counter() - t0) * 1000)
            gate.on_level(level, i)
            states.append(gate.state)

        opened = states.index("OPEN") if "OPEN" in states else None
        closed = next((k for k in range(opened or 0, len(states)) if states[k] == "CLOSED"), None) if opened is not None else None
        check("2.80 m does NOT open (threshold is strictly above 2.8)", states[5] == "CLOSED" and LEVELS[5] == 2.8)
        check("2.81 m opens the gate", opened == 6 and LEVELS[6] == 2.81)
        check("gate stays open while the level is still above 2.5 m (hysteresis)", states[6:13] == ["OPEN"] * 7)
        check("gate closes at 2.5 m", closed == 13)
        n_over = sum(1 for v in LEVELS if v > 2.8)
        check(f"one decision per reading above 2.8 m ({n_over})", st.counts()["decisions"] == n_over == 5)
        check("all of them dispatched (fresh)", statuses == ["DISPATCHED"] * n_over)
        check("rule/TTL from config: threshold 2.8, TTL 60 s, actuator " + ACTUATOR,
              rule["threshold"] == 2.8 and rule["ttl_s"] == 60.0 and rule["actuator"] == ACTUATOR)
        check("every reading evaluated in under 10 ms", max(latencies) < 10)

        # stale command: issued at t=500, applied 61 s later -> rejected, gate stays closed
        g2 = SluiceGate()
        ev = SensorEvent(node_id=node, sensor="SLUICE_WATER_LEVEL_M", value=3.0, unit="m")
        st.insert_event_with_outbox(ev)
        stale = engine.evaluate(ev, now_mono=500.0)[0]
        check("command 61 s old is rejected (EXPIRED)", g2.submit(stale, now_mono=561.0) == "EXPIRED")
        check("...and the gate did not open on a stale command", g2.state == "CLOSED")
        check("command 59 s old is still accepted", SluiceGate().submit(stale, now_mono=559.0) == "DISPATCHED")

        # real clock, no injection (short TTL so the test is quick)
        short = [dict(cfg["rules"][0], ttl_s=0.3)]
        fast = RuleEngine(short).evaluate(ev)[0]
        g3 = SluiceGate()
        check("fresh on the real clock", g3.submit(fast) == "DISPATCHED")
        time.sleep(0.35)
        check("expired after 0.35 s on the real clock", SluiceGate().submit(fast) == "EXPIRED")

    failed = [n for n, ok in checks if not ok]
    return not failed, [
        f"{len(checks) - len(failed)}/{len(checks)} checks passed; hydrograph {LEVELS[0]} -> {max(LEVELS)} -> {LEVELS[-1]} m",
        f"gate states per reading: {' '.join('O' if s == 'OPEN' else '.' for s in states)}  (O = open)",
        f"opened at reading {opened} ({LEVELS[opened]} m), closed at reading {closed} ({LEVELS[closed]} m); "
        f"slowest evaluation {max(latencies):.3f} ms",
    ] + [f"FAILED: {n}" for n in failed]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS5-4 Disaster flood relief sluice gate actuation")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
