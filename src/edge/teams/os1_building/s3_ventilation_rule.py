"""OS1-3: Local Ventilation Rule & Damper Actuator Dispatch.

Owner   : Aidar, Arkar

Rule: CO2 > 1000 ppm -> HVAC_DAMPER_PCT = 100, command TTL = 15 seconds.
Use time.monotonic() for expiry because wall-clock time can jump.
"""
import os
import sys
import tempfile
import time
from pathlib import Path

from edge.models import EdgeDecision, SensorEvent
from edge.storage import Storage

ROOT = Path(__file__).resolve().parents[4]
SCHEMA = ROOT / "sql" / "schema.sql"
NODE_ID = "EDGE_NODE_OS1_CONVENSIA"
THRESHOLD_PPM = 1000.0
TTL_SECONDS = 15.0


def evaluate_co2(co2_ppm, event_id, now_mono=None, ttl_s=TTL_SECONDS):
    """Return a ventilation decision for high CO2, otherwise None."""
    if co2_ppm <= THRESHOLD_PPM:
        return None
    issued = time.monotonic() if now_mono is None else float(now_mono)
    return EdgeDecision(event_id=event_id, rule="co2_vent", actuator="HVAC_DAMPER_PCT",
                        command_value=100.0, issued_mono=issued,
                        expires_mono=issued + float(ttl_s))


def dispatch_if_fresh(decision, now_mono=None):
    """Simulate actuator dispatch only while the command TTL is still valid."""
    now = time.monotonic() if now_mono is None else float(now_mono)
    return "DISPATCHED" if decision.is_fresh(now) else "EXPIRED"


def run(co2_ppm=1200.0, db_path=None, now_mono=None):
    """Persist a sensor event and any resulting decision, then simulate dispatch."""
    temp_dir = tempfile.TemporaryDirectory() if db_path is None else None
    try:
        path = db_path or os.path.join(temp_dir.name, "os1_s3.db")
        db = Storage(path, str(SCHEMA))
        event = SensorEvent(NODE_ID, "INDOOR_CO2_PPM", float(co2_ppm), "ppm",
                            received_at=time.time())
        db.insert_event_with_outbox(event)
        decision = evaluate_co2(co2_ppm, event.event_id, now_mono=now_mono)
        if decision is None:
            return True, [f"CO2={co2_ppm:.1f} ppm: below/equal threshold; no damper command"]
        db.insert_decision(decision)
        status = dispatch_if_fresh(decision, now_mono=now_mono)
        db.set_decision_status(decision.decision_id, status)
        ok = (decision.command_value == 100.0 and
              abs(decision.expires_mono - decision.issued_mono - TTL_SECONDS) < 1e-9 and
              status == "DISPATCHED")
        return ok, [
            f"CO2={co2_ppm:.1f} ppm; threshold is strictly greater than {THRESHOLD_PPM:.0f} ppm",
            f"command: {decision.actuator}={decision.command_value:.0f}%",
            f"TTL: {TTL_SECONDS:.1f}s; status at dispatch: {status}",
            "expiry uses monotonic time so wall-clock adjustments do not revive stale commands",
        ]
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS1-3 Local ventilation rule & damper dispatch")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
