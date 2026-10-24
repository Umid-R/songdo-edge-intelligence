"""OS5-2: Disposable Worker Kill & Preserved State Recovery

Mission : Kill the processor worker with SIGKILL (several times, at arbitrary moments) and verify the
          supervisor respawns it WITHOUT data loss.
Concept : Disposable worker + durable state. The worker keeps nothing that matters in memory: its
          input is a durable SQLite inbox and every row is handled in ONE transaction (see _common.py).
          The supervisor traps SIGCHLD, reaps the dead child with wait() (no zombie), and restarts it.

Owner   : Umidillo
Test    : tests/os5/test_s2_worker_kill_recovery.py
Run     : PYTHONPATH=src python -m edge.teams.os5_reliable.s2_worker_kill_recovery

Checks: after 3 SIGKILLs during a 400-reading burst: events stored == produced, all distinct, outbox
complete, every flood reading produced exactly one decision, the gate state equals a clean replay,
each recovery < 2.0 s, no zombies. Readings are SIMULATED.
"""
import multiprocessing as mp
import os
import sys
import tempfile

from edge.config import load_config
from edge.ipc import StopFlag
from edge.storage import Storage
from edge.teams.os5_reliable import _common as c


def run(n_events=400, interval_s=0.01, kills=3):
    """Returns (ok, report_lines)."""
    cfg = load_config(os.path.join(c.ROOT, "config", "os5.yaml"))
    ctx = mp.get_context("spawn")
    stop = StopFlag(ctx)
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "os5_kill.db")
        st = Storage(db, cfg["paths"]["schema"])
        c.init_inbox(st)
        sup = c.RestartingSupervisor(c.processor_main, (db, cfg["paths"]["schema"], stop, cfg["rules"]), stop)
        prod = c.Producer(st, cfg["node"]["id"], n_events, interval_s)
        results = []
        try:
            sup.start()
            prod.start()
            for k in range(kills):
                target = (k + 1) * n_events // (kills + 1)
                c.wait_until(lambda: prod.written >= target, 30, sup)
                results.append(c.kill_and_recover(sup, st))
            prod.join(30)
            c.wait_until(lambda: c.report(st)["inbox_done"] == n_events, 30, sup)
            zombies_running = c.zombie_children()
        except TimeoutError as e:
            return False, [f"timeout: {e}"]
        finally:
            sup.shutdown()
        zombies_after = c.zombie_children()
        rep = c.report(st)
        ids = {r["event_id"] for r in st._read("SELECT event_id FROM sensor_events")}
        flood_readings = sum(1 for i in range(n_events) if c.flood_level(i) > c.OPEN_ABOVE_M)
        one_per = st._read("SELECT COUNT(*) AS n, COUNT(DISTINCT event_id) AS d FROM edge_decisions")[0]
        gate = st.kv_get(c.GATE_KEY)
        expected_gate = "CLOSED"
        for i in range(n_events):
            expected_gate = c.gate_next_state(expected_gate, c.flood_level(i))

    recov = [r["recovery_s"] for r in results]
    checks = {
        f"zero loss: {rep['events']} stored = {n_events} produced, {rep['distinct']} distinct (no duplicates)":
            rep["events"] == rep["distinct"] == n_events and ids == {f"os5-{i:05d}" for i in range(n_events)},
        f"outbox complete: {rep['outbox']} rows for {n_events} events": rep["outbox"] == n_events,
        f"inbox fully processed: {rep['inbox_done']}/{rep['inbox']}": rep["inbox_done"] == rep["inbox"] == n_events,
        f"exactly one decision per flood reading ({flood_readings})":
            one_per["n"] == one_per["d"] == flood_readings,
        f"gate state after crashes = clean replay ({gate})": gate == expected_gate,
        f"{kills} SIGKILLs, {sup.restarts} restarts, exit codes {sup.exit_codes}":
            sup.restarts == kills and sup.exit_codes == [-9] * kills and not sup.gave_up,
        "SIGCHLD trapped by the handler": sup.sigchld_count >= kills,
        "recovery times " + ", ".join(f"{r:.2f}s" for r in recov) + f" (limit {c.RECOVERY_LIMIT_S} s)":
            all(r < c.RECOVERY_LIMIT_S for r in recov),
        "no zombie processes": not zombies_running and not zombies_after,
    }
    return all(checks.values()), [f"{'ok ' if v else 'FAILED '}{k}" for k, v in checks.items()]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS5-2 Disposable worker kill & preserved state recovery")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
