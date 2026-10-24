"""OS4-3: Dust Suppression Mist Cannon Relay Actuation

Mission : Activate the high-pressure mist cannon when PM2.5 exceeds the Korea "Bad" threshold
          (> 50 ug/m3).
Action  : dispatch MIST_CANNON_RELAY with a 30-second TTL (rule and TTL come from config/os4.yaml).
Concept : Strict threshold (50.0 does NOT trigger, 50.1 does) and a command TTL on the MONOTONIC clock:
          a stale command is rejected. The TTL is also used as a LEASE: the relay stays on only until the
          last accepted command expires, so if the node dies or the air clears, the cannon switches itself
          off (fail-safe) and keeps running only while fresh commands keep arriving.

Owner   : Nazarbek
Test    : tests/os4/test_s3_mist_cannon.py
Run     : PYTHONPATH=src python -m edge.teams.os4_iot.s3_mist_cannon

The lease behaviour is OUR design choice (the handbook only gives the 50 ug/m3 rule and the 30 s TTL).
PM2.5 values are SIMULATED, 1 reading per second on an injected monotonic clock so the run is instant.
"""
import os
import sys
import tempfile
import time

from edge.config import load_config
from edge.models import SensorEvent
from edge.storage import Storage
from edge.decision import RuleEngine

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
ACTUATOR = "MIST_CANNON_RELAY"
PM25 = [20, 35, 49.9, 50.0, 50.1, 80, 120, 90, 60] + [30] * 40     # clean air after the dust episode


class MistRelay:
    def __init__(self):
        self.on_until = float("-inf")

    def submit(self, decision, now_mono=None):
        """Accept only fresh commands; an accepted command powers the relay until it expires."""
        if not decision.is_fresh(now_mono):
            return "EXPIRED"
        self.on_until = max(self.on_until, decision.expires_mono)
        return "DISPATCHED"

    def is_on(self, now_mono):
        return now_mono < self.on_until


def run():
    """Returns (ok, report_lines)."""
    cfg = load_config(os.path.join(ROOT, "config", "os4.yaml"))
    rule = cfg["rules"][0]
    engine = RuleEngine(cfg["rules"])
    node = cfg["node"]["id"]
    checks = []

    def check(name, cond):
        checks.append((name, bool(cond)))

    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(os.path.join(tmp, "os4_mist.db"), cfg["paths"]["schema"])
        relay, states, statuses, latencies = MistRelay(), [], [], []
        for i, pm in enumerate(PM25):
            now = 100.0 + i
            ev = SensorEvent(node_id=node, sensor="PM25_UG_M3", value=float(pm), unit="ug_m3")
            st.insert_event_with_outbox(ev)
            t0 = time.perf_counter()
            for d in engine.evaluate(ev, now_mono=now):
                status = relay.submit(d, now_mono=now)
                st.insert_decision(d)
                st.set_decision_status(d.decision_id, status)
                statuses.append(status)
            latencies.append((time.perf_counter() - t0) * 1000)
            states.append(relay.is_on(now))
        on_idx = states.index(True)
        off_idx = next(k for k in range(on_idx, len(states)) if not states[k])

        check("49.9 and 50.0 ug/m3 do not trigger (threshold is strictly above 50)", not any(states[:4]))
        check("50.1 ug/m3 switches the relay on", on_idx == 4 and PM25[4] == 50.1)
        check("5 readings above 50 -> 5 decisions, all dispatched",
              st.counts()["decisions"] == 5 and statuses == ["DISPATCHED"] * 5)
        check("relay stays on after the air clears, only until the last command expires (lease)",
              all(states[4:38]) and off_idx == 38)
        check("rule from config: PM25 > 50 -> " + ACTUATOR + ", TTL 30 s",
              rule["threshold"] == 50 and rule["actuator"] == ACTUATOR and rule["ttl_s"] == 30.0)
        check("every reading evaluated in under 10 ms", max(latencies) < 10)

        ev = SensorEvent(node_id=node, sensor="PM25_UG_M3", value=90.0, unit="ug_m3")
        st.insert_event_with_outbox(ev)
        cmd = engine.evaluate(ev, now_mono=500.0)[0]
        check("command 29.9 s old is accepted", MistRelay().submit(cmd, now_mono=529.9) == "DISPATCHED")
        stale_relay = MistRelay()
        check("command exactly 30.0 s old is rejected", stale_relay.submit(cmd, now_mono=530.0) == "EXPIRED")
        check("...and a rejected command never switches the relay on", not stale_relay.is_on(530.0))

        fast = RuleEngine([dict(rule, ttl_s=0.3)]).evaluate(ev)[0]       # real clock, short TTL
        check("fresh on the real clock", MistRelay().submit(fast) == "DISPATCHED")
        time.sleep(0.35)
        check("expired after 0.35 s on the real clock", MistRelay().submit(fast) == "EXPIRED")

    failed = [n for n, ok in checks if not ok]
    return not failed, [
        f"{len(checks) - len(failed)}/{len(checks)} checks passed",
        f"relay on at reading {on_idx} ({PM25[on_idx]} ug/m3), off at reading {off_idx} "
        f"(30 s after the last command); slowest evaluation {max(latencies):.3f} ms",
        "relay per reading: " + "".join("#" if s else "." for s in states[:46]),
    ] + [f"FAILED: {n}" for n in failed]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS4-3 Dust suppression mist cannon relay actuation")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
