"""OS2-3: Obstacle Proximity E-Stop & Actuator Command Expiry

Mission : Trigger an emergency stop immediately when LiDAR proximity < 0.8 m.
Concept : Every actuator command carries a validity window (TTL = 500 ms) measured on the
          MONOTONIC clock. The safety latch rejects any command that is older than that, and
          it also rejects motion commands while the e-stop is latched.

Owner   : Umid, Umrbek
Test    : tests/os2/test_s3_estop_ttl.py
Run     : PYTHONPATH=src python -m edge.teams.os2_robot.s3_estop_ttl

Design (ours, not specified by the handbook, so explain it in the defense):
  * The latch engages DIRECTLY from the sensor reading (on_proximity), not by waiting for a
    command. A late or lost e-stop command therefore can never leave the robot moving
    (fail-safe). The e-stop command from the rule engine is still logged and TTL-checked.
  * Releasing needs proximity >= 1.0 m (hysteresis, ASSUMED) so the latch does not flutter
    around 0.8 m. A zero-velocity command is always accepted: stopping is always safe.
Rule and TTL come from config/os2.yaml (LIDAR_PROXIMITY_M < 0.8 -> SAFETY_ESTOP_LATCH, 0.5 s).
"""
import os
import sys
import tempfile
import time

from edge.config import load_config
from edge.models import EdgeDecision, SensorEvent
from edge.storage import Storage
from edge.decision import RuleEngine

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

ACCEPTED = "ACCEPTED"
REJECTED_EXPIRED = "REJECTED_EXPIRED"
REJECTED_LATCHED = "REJECTED_LATCHED"


class SafetyLatch:
    def __init__(self, stop_below_m=0.8, clear_above_m=1.0):
        self.stop_below_m = stop_below_m
        self.clear_above_m = clear_above_m
        self.engaged = False
        self.engagements = 0

    def on_proximity(self, distance_m):
        """Engage when strictly below the threshold. True only on a NEW engagement."""
        if distance_m < self.stop_below_m and not self.engaged:
            self.engaged = True
            self.engagements += 1
            return True
        return False

    def try_release(self, distance_m):
        """Release only once the path is clear with margin (hysteresis)."""
        if self.engaged and distance_m >= self.clear_above_m:
            self.engaged = False
            return True
        return False

    def submit(self, decision, now_mono=None):
        """Validate an actuator command. Expired commands are always rejected."""
        if not decision.is_fresh(now_mono):
            return REJECTED_EXPIRED
        if decision.actuator == "SAFETY_ESTOP_LATCH":
            if not self.engaged:
                self.engaged = True
                self.engagements += 1
            return ACCEPTED
        if decision.actuator == "MOTOR_VEL" and self.engaged and decision.command_value != 0:
            return REJECTED_LATCHED
        return ACCEPTED


def motor_cmd(event_id, velocity, issued_mono, ttl_s=0.5):
    return EdgeDecision(event_id=event_id, rule="motion", actuator="MOTOR_VEL",
                        command_value=velocity, issued_mono=issued_mono,
                        expires_mono=issued_mono + ttl_s)


def run():
    """Returns (ok, report_lines)."""
    cfg = load_config(os.path.join(ROOT, "config", "os2.yaml"))
    rule = cfg["rules"][0]
    engine = RuleEngine(cfg["rules"])
    node = cfg["node"]["id"]
    checks = []

    def check(name, cond):
        checks.append((name, bool(cond)))

    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(os.path.join(tmp, "os2_estop.db"), cfg["paths"]["schema"])
        submitted = 0

        def log(event, decision, status):
            nonlocal submitted
            st.insert_decision(decision)
            st.set_decision_status(decision.decision_id, status)
            submitted += 1

        # 1) threshold is strict: 0.80 m does not stop the robot, 0.79 m does
        latch = SafetyLatch()
        check("0.80 m does not trigger", not latch.on_proximity(0.80) and not latch.engaged)
        ev = SensorEvent(node_id=node, sensor="LIDAR_PROXIMITY_M", value=0.79, unit="m")
        st.insert_event_with_outbox(ev)
        t0 = time.perf_counter()
        decisions = engine.evaluate(ev, now_mono=100.0)
        newly = latch.on_proximity(0.79)
        latency_ms = (time.perf_counter() - t0) * 1000
        check("0.79 m triggers the latch", newly and latch.engaged)
        check("reaction (rule + latch) under 10 ms", latency_ms < 10)

        # 2) the rule engine's e-stop command has the 500 ms validity window from the config
        estop = decisions[0]
        check("e-stop command TTL is 500 ms", abs((estop.expires_mono - estop.issued_mono) - rule["ttl_s"]) < 1e-9
              and rule["ttl_s"] == 0.5)
        log(ev, estop, latch.submit(estop, now_mono=100.1))
        check("fresh e-stop command accepted", latch.submit(estop, now_mono=100.1) == ACCEPTED)

        # 3) expiry on the monotonic clock (injected clock, deterministic)
        latch2 = SafetyLatch()
        cmd = motor_cmd(ev.event_id, 0.5, issued_mono=200.0)
        check("motion command 0.4 s old is accepted", latch2.submit(cmd, 200.4) == ACCEPTED)
        check("motion command 0.6 s old is rejected", latch2.submit(cmd, 200.6) == REJECTED_EXPIRED)
        check("motion command exactly 0.5 s old is rejected", latch2.submit(cmd, 200.5) == REJECTED_EXPIRED)
        log(ev, cmd, latch2.submit(cmd, 200.6))

        # 4) an expired e-stop command is rejected, yet the sensor path already latched (fail-safe)
        stale_estop = engine.evaluate(ev, now_mono=300.0)[0]
        latch3 = SafetyLatch()
        latch3.on_proximity(0.5)
        check("expired e-stop command rejected", latch3.submit(stale_estop, 300.7) == REJECTED_EXPIRED)
        check("...but the latch is still engaged", latch3.engaged)

        # 5) while latched: motion rejected, stop accepted; release needs hysteresis
        move = motor_cmd(ev.event_id, 0.5, issued_mono=400.0)
        halt = motor_cmd(ev.event_id, 0.0, issued_mono=400.0)
        check("motion rejected while latched", latch3.submit(move, 400.1) == REJECTED_LATCHED)
        check("zero-velocity (stop) always accepted", latch3.submit(halt, 400.1) == ACCEPTED)
        log(ev, move, REJECTED_LATCHED)
        check("0.9 m is not enough to release", not latch3.try_release(0.9) and latch3.engaged)
        check("1.0 m releases the latch", latch3.try_release(1.0) and not latch3.engaged)
        check("motion accepted again after release", latch3.submit(move, 400.2) == ACCEPTED)

        # 6) real clock, no injection: a command that waits 0.55 s must be refused
        latch4 = SafetyLatch()
        slow = motor_cmd(ev.event_id, 0.5, issued_mono=time.monotonic())
        check("fresh on the real clock", latch4.submit(slow) == ACCEPTED)
        time.sleep(0.55)
        check("rejected after 0.55 s on the real clock", latch4.submit(slow) == REJECTED_EXPIRED)
        log(ev, slow, REJECTED_EXPIRED)

        check("decisions persisted to SQLite", st.counts()["decisions"] == submitted)

    failed = [name for name, passed in checks if not passed]
    return not failed, [
        f"{len(checks) - len(failed)}/{len(checks)} checks passed; rule: {rule['sensor']} {rule['op']} "
        f"{rule['threshold']} -> {rule['actuator']}, TTL {rule['ttl_s']} s",
        f"sensor -> rule -> latch reaction: {latency_ms:.3f} ms",
    ] + [f"FAILED: {name}" for name in failed]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS2-3 Obstacle e-stop & actuator command expiry")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
