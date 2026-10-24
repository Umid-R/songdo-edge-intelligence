"""OS1-5: Signature WAN Outage & Offline Outbox Queueing.

Simulates a WAN outage without changing the real network interface. Sensor events
are atomically stored with outbox entries while WAN is down; after restoring the
mock WAN, the worker drains batches to the idempotent upstream sink.

Two parts:
  run()       fast functional simulation (instant).
  run_live()  REAL-TIME run at 10 Hz: online -> WAN cut -> restored, with a sync thread. The local CO2
              rule (config/os1.yaml: CO2 > 1000 ppm -> HVAC_DAMPER_PCT 100, TTL 15 s) keeps actuating
              during the outage, events pile up in the outbox, nothing is delivered while cut, and after
              the restore everything drains with zero loss and no duplicates.
              The handbook's signature demo is a 10-minute outage:
                  PYTHONPATH=src python -m edge.teams.os1_building.s5_wan_outage --outage 600
Readings are SIMULATED; the WAN is a flag in the database, not a real network interface.
"""
import argparse
import os
import sys
import tempfile
import threading
import time
from pathlib import Path

from edge.config import load_config
from edge.models import SensorEvent
from edge.storage import Storage
from edge.decision import RuleEngine, dispatch

ROOT = Path(__file__).resolve().parents[4]
SCHEMA = ROOT / "sql" / "schema.sql"
NODE_ID = "EDGE_NODE_OS1_CONVENSIA"
PERIOD_S = 0.1                       # live run: 10 readings per second


def run(event_count=120, batch_size=25, db_path=None):
    """Run a fast outage/recovery simulation; return (ok, report_lines)."""
    if event_count < 1 or batch_size < 1:
        raise ValueError("event_count and batch_size must be positive")
    temp_dir = tempfile.TemporaryDirectory() if db_path is None else None
    try:
        path = db_path or os.path.join(temp_dir.name, "os1_s5.db")
        db = Storage(path, str(SCHEMA))
        db.set_wan_online(False)
        for tick in range(event_count):
            event = SensorEvent(NODE_ID, "INDOOR_CO2_PPM", 600.0 + (tick % 500), "ppm",
                                observed_at=time.time() + tick, received_at=time.time())
            db.insert_event_with_outbox(event)

        queued_during_outage = db.outbox_pending()
        conservation_while_offline = db.conservation_report()
        # Restore the mock WAN and drain in bounded batches, as the sync worker does.
        db.set_wan_online(True)
        delivered = 0
        while db.outbox_pending() > 0:
            delivered += db.drain_outbox_to_sink(batch_size=batch_size)
        report = db.conservation_report()
        counts = db.counts()
        ok = (queued_during_outage == event_count and
              conservation_while_offline["conserved"] and
              delivered == event_count and report["conserved"] and
              report["sink"] == event_count and report["pending"] == 0 and
              counts["events"] == event_count)
        return ok, [
            f"mock WAN disconnected: queued {queued_during_outage}/{event_count} events locally",
            f"WAN restored: delivered {delivered} event(s) in batches of at most {batch_size}",
            f"conservation: events={report['events']}, upstream sink={report['sink']}, pending={report['pending']}",
            f"no data loss / no duplicate sink rows: {report['conserved'] and report['sink'] == event_count}",
            "fast functional simulation; the real-time outage run (run_live) follows",
        ]
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()


def _co2(i):
    """SIMULATED CO2 (ppm): normal 650, with a 3-reading spike to 1200 every 20 readings."""
    return 1200.0 if (i % 20) in (10, 11, 12) else 650.0


def _sync_loop(db, stop):
    """Upstream sync worker (thread): drains the outbox only while the WAN is online."""
    while not stop.is_set():
        if db.wan_online() and db.drain_outbox_to_sink(200):
            continue
        time.sleep(0.05)


def run_live(online_s=2.0, outage_s=5.0, restored_s=2.0):
    """Real-time online -> WAN cut -> restored run. Returns (ok, report_lines)."""
    cfg = load_config(str(ROOT / "config" / "os1.yaml"))
    engine = RuleEngine(cfg["rules"])
    phases = [("online", int(round(online_s / PERIOD_S))), ("WAN cut", int(round(outage_s / PERIOD_S))),
              ("restored", int(round(restored_s / PERIOD_S)))]
    per = {n: {"ticks": 0, "spikes": 0, "decisions": 0, "dispatched": 0} for n, _ in phases}
    snap, stamps = {}, []
    with tempfile.TemporaryDirectory() as tmp:
        db = Storage(os.path.join(tmp, "os1_live.db"), str(SCHEMA))
        db.set_wan_online(True)
        stop = threading.Event()
        syncer = threading.Thread(target=_sync_loop, args=(db, stop), daemon=True)
        syncer.start()
        i, deadline = 0, time.monotonic()
        for name, n_ticks in phases:
            if name == "WAN cut":
                db.set_wan_online(False)
            elif name == "restored":
                snap["end_pending"], snap["end_sink"] = db.outbox_pending(), db.counts()["sink"]
                db.set_wan_online(True)
            for k in range(n_ticks):
                delay = deadline - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                deadline += PERIOD_S
                stamps.append(time.monotonic())
                value = _co2(i)
                event = SensorEvent(NODE_ID, "INDOOR_CO2_PPM", value, "ppm",
                                    observed_at=time.time(), received_at=time.time())
                db.insert_event_with_outbox(event)          # local first: the network is never involved
                p = per[name]
                p["ticks"] += 1
                p["spikes"] += value > 1000
                for d in engine.evaluate(event):
                    db.insert_decision(d)
                    status = dispatch(d)
                    db.set_decision_status(d.decision_id, status)
                    p["decisions"] += 1
                    p["dispatched"] += status == "DISPATCHED"
                i += 1
                if name == "WAN cut" and k == min(10, n_ticks // 2):
                    snap["cut_pending"], snap["cut_sink"] = db.outbox_pending(), db.counts()["sink"]
        end = time.monotonic() + 15 + db.outbox_pending() / 200
        while db.outbox_pending() > 0 and time.monotonic() < end:
            time.sleep(0.05)
        stop.set()
        syncer.join(5)
        rep = db.conservation_report()
        conn = db._connect()
        try:
            distinct = conn.execute("SELECT COUNT(DISTINCT event_id) FROM upstream_sink").fetchone()[0]
        finally:
            conn.close()
    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    max_gap_ms = 1000 * max(gaps) if gaps else 0.0
    cut = per["WAN cut"]
    checks = {
        f"{cut['ticks']} readings kept arriving during the {outage_s:g} s WAN cut, longest gap "
        f"{max_gap_ms:.0f} ms (limit {PERIOD_S * 6000:.0f} ms)":
            cut["ticks"] == phases[1][1] and max_gap_ms < PERIOD_S * 6000,
        "local rule kept actuating in every phase (decisions == readings above 1000 ppm, all dispatched): "
        + ", ".join(f"{n}: {v['decisions']}/{v['spikes']}" for n, v in per.items()):
            sum(v["spikes"] for v in per.values()) > 0
            and all(v["decisions"] == v["spikes"] == v["dispatched"] for v in per.values()),
        f"outbox grew during the cut ({snap['cut_pending']} -> {snap['end_pending']} pending)":
            snap["end_pending"] > snap["cut_pending"],
        f"nothing delivered during the cut (sink stayed at {snap['cut_sink']})": snap["end_sink"] == snap["cut_sink"],
        f"after the restore the outbox drained to {rep['pending']} pending": rep["pending"] == 0,
        f"event conservation: {rep['events']} stored = {rep['sink']} delivered, {distinct} distinct (no duplicates)":
            rep["conserved"] and rep["events"] == rep["sink"] == distinct == len(stamps),
    }
    return all(checks.values()), [f"{'ok ' if v else 'FAILED '}{k}" for k, v in checks.items()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outage", type=float, default=5.0,
                    help="live WAN outage length in seconds (600 = the handbook's 10 minutes)")
    args = ap.parse_args()
    ok_fast, lines_fast = run()
    ok_live, lines_live = run_live(outage_s=args.outage)
    ok = ok_fast and ok_live
    print(f"[{'PASS' if ok else 'FAIL'}] OS1-5 WAN outage & offline outbox queueing")
    for line in lines_fast:
        print("      ", line)
    print(f"       real-time run ({args.outage:g} s outage):")
    for line in lines_live:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
