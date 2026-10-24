"""OS5-5: Compound Failure Survivability (WAN Cut + Worker Crash)

Mission : Survive a network disconnection and a worker crash at the SAME moment, during a flood.
Result  : The worker recovers locally; the outbox preserves every flood event; the WAN drains cleanly
          once the link is back.
Concept : Combines OS5-2 (durable inbox + disposable worker + supervisor) with the offline outbox:
            t0   : first readings of the flood are about to arrive
            t0+  : WAN cut and the Processor is SIGKILLed back to back
            ~0.3s: supervisor has restarted it; it catches up from the durable inbox, the sluice gate
                   opens locally (no cloud involved); events pile up in the outbox
            +3 s : WAN restored; the sync worker drains the outbox
          Verified: produced == stored == delivered, no duplicates, gate opened within the recovery
          bound, nothing delivered while the WAN was cut, gate closed again after the flood.

Owner   : (write names here)
Test    : tests/os5/test_s5_compound_failure.py
Run     : PYTHONPATH=src python -m edge.teams.os5_reliable.s5_compound_failure [--cut 3]
Readings are SIMULATED (see _common.flood_level); the WAN is a flag in the database, not a real network.
"""
import argparse
import multiprocessing as mp
import os
import sys
import tempfile
import threading
import time

from edge.config import load_config
from edge.ipc import StopFlag
from edge.storage import Storage
from edge.teams.os5_reliable import _common as c

GATE_OPEN_LIMIT_S = c.RECOVERY_LIMIT_S + 1.0


def run(n_events=240, interval_s=0.05, cut_s=3.0):
    """Returns (ok, report_lines). The cut and crash happen just before reading 100 (first flood reading)."""
    cfg = load_config(os.path.join(c.ROOT, "config", "os5.yaml"))
    ctx = mp.get_context("spawn")
    stop = StopFlag(ctx)
    sync_stop = threading.Event()
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "os5_compound.db")
        st = Storage(db, cfg["paths"]["schema"])
        c.init_inbox(st)
        st.set_wan_online(True)
        sup = c.RestartingSupervisor(c.processor_main, (db, cfg["paths"]["schema"], stop, cfg["rules"]), stop)
        prod = c.Producer(st, cfg["node"]["id"], n_events, interval_s)
        syncer = threading.Thread(target=c.sync_loop, args=(st, sync_stop), daemon=True)
        try:
            sup.start()
            syncer.start()
            prod.start()
            c.wait_until(lambda: prod.written >= 98, 30, sup)
            # ---- compound failure: both at once ----
            st.set_wan_online(False)
            cut_mono, cut_wall = time.monotonic(), time.time()
            crash = c.kill_and_recover(sup, st)
            c.wait_until(lambda: st.kv_get(c.GATE_KEY) == "OPEN", 15, sup)
            gate_open_mono = time.monotonic()
            gate_delay = gate_open_mono - prod.flood_mono
            sink_at_recovery = st.counts()["sink"]
            while time.monotonic() - cut_mono < cut_s:
                sup.wait(0.05)
                sup.poll()
            pending_at_restore, sink_at_restore = c.report(st)["pending"], st.counts()["sink"]
            st.set_wan_online(True)
            prod.join(30)
            c.wait_until(lambda: c.report(st)["inbox_done"] == n_events, 30, sup)
            c.wait_until(lambda: c.report(st)["pending"] == 0, 30 + n_events / 100, sup)
            zombies = c.zombie_children()
        except TimeoutError as e:
            return False, [f"timeout: {e}"]
        finally:
            sync_stop.set()
            sup.shutdown()
        rep = c.report(st)
        gate_final = st.kv_get(c.GATE_KEY)
        opens_after = st._read("SELECT COUNT(*) AS n FROM edge_decisions WHERE actuator=? AND created_at>=?",
                               ("BACKUP_SLUICE_GATE_OPEN", cut_wall))[0]["n"]
        log = [(r["seq"], r["state"]) for r in st._read("SELECT seq,state FROM os5_gate_log ORDER BY id")]

    checks = {
        f"worker recovered in {crash['recovery_s']:.2f} s after SIGKILL (limit {c.RECOVERY_LIMIT_S} s), "
        f"exit codes {sup.exit_codes}": crash["recovery_s"] < c.RECOVERY_LIMIT_S and sup.exit_codes == [-9],
        f"WAN cut and crash were simultaneous (cut {1000 * (crash['kill_mono'] - cut_mono):.0f} ms before kill)":
            0 <= crash["kill_mono"] - cut_mono < 0.1,
        f"flood gate opened locally {gate_delay:.2f} s after the first flood reading (limit {GATE_OPEN_LIMIT_S:.0f} s)":
            gate_delay < GATE_OPEN_LIMIT_S,
        f"{opens_after} gate-open decision(s) made AFTER the failure began": opens_after >= 1,
        f"nothing delivered while the WAN was cut (sink {sink_at_recovery} -> {sink_at_restore}), "
        f"outbox backlog {pending_at_restore}": sink_at_restore == sink_at_recovery and pending_at_restore > 0,
        f"after restore the outbox drained to {rep['pending']} pending": rep["pending"] == 0,
        f"event conservation: {rep['events']} stored = {rep['sink']} delivered = {n_events} produced, "
        f"{rep['distinct']} distinct": rep["events"] == rep["sink"] == rep["distinct"] == rep["outbox"] == n_events,
        f"gate history {log}: opened during the flood, closed after": gate_final == "CLOSED"
            and [s for _, s in log] == ["OPEN", "CLOSED"],
        "no zombie processes, supervisor did not give up": not zombies and not sup.gave_up,
    }
    return all(checks.values()), [f"{'ok ' if v else 'FAILED '}{k}" for k, v in checks.items()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cut", type=float, default=3.0, help="WAN outage length in seconds")
    args = ap.parse_args()
    ok, lines = run(cut_s=args.cut)
    print(f"[{'PASS' if ok else 'FAIL'}] OS5-5 Compound failure survivability (WAN cut + worker crash)")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
