"""OS5-1: Health Check Architecture & Readiness State Machine

Mission : Implement LIVENESS vs READINESS probes for critical flood infrastructure.
Concept : A worker reports a periodic heartbeat to the process_health table.
            liveness  = "is it making progress?"  -> heartbeat is fresh AND the pid really exists
            readiness = "should it get work?"     -> the worker says READY (dependencies fine)
          They are different on purpose: a worker whose dependency is down is ALIVE but NOT READY
          (do not restart it, just stop sending work); a hung worker is NOT ALIVE although its pid
          still exists (restart it); a dead pid is DEAD.

State machine (reported by the worker):  STARTING --5 beats--> READY <--> NOT_READY
Probe verdicts (computed by the prober): STARTING, READY, NOT_READY, HUNG (stale heartbeat),
                                         DEAD (pid gone), NO_HEARTBEAT (never reported)

Owner   : Teshaboev Umidillo
Test    : tests/os5/test_s1_health_probes.py
Run     : PYTHONPATH=src python -m edge.teams.os5_reliable.s1_health_probes

The demo injects every transition for real: a dependency flag flip, SIGSTOP (hang), SIGCONT, and
SIGKILL. The "dependency" is SIMULATED (a shared flag); the heartbeat period is scaled down to
0.2 s so the demo is quick.
"""
import multiprocessing as mp
import os
import signal
import sys
import tempfile
import time

from edge.ipc import StopFlag
from edge.models import ProcessHealth
from edge.procfs import read_proc
from edge.storage import Storage
from edge.teams.os5_reliable._common import ROOT, wait_until

NAME = "os5-sensor-worker"
HEARTBEAT_S = 0.2
LIVENESS_TIMEOUT_S = 3 * HEARTBEAT_S     # three missed beats = not alive
WARMUP_BEATS = 5


def probe(row, now_wall, pid_exists, liveness_timeout=LIVENESS_TIMEOUT_S):
    """Pure function: heartbeat row (dict or None) -> {live, ready, state}."""
    if row is None:
        return {"live": False, "ready": False, "state": "NO_HEARTBEAT"}
    if not pid_exists:
        return {"live": False, "ready": False, "state": "DEAD"}
    if now_wall - row["updated_at"] > liveness_timeout:
        return {"live": False, "ready": False, "state": "HUNG"}
    return {"live": True, "ready": row["state"] == "READY", "state": row["state"]}


def health_worker(db_path, schema_path, stop, dep_ok):
    st = Storage(db_path, schema_path, init=False)
    beats = 0
    while not stop.is_set():
        beats += 1
        if beats <= WARMUP_BEATS:
            state = "STARTING"
        else:
            state = "READY" if dep_ok.value else "NOT_READY"
        m = read_proc(os.getpid()) or {}
        st.upsert_health(ProcessHealth(
            name=NAME, pid=os.getpid(), state=state, rss_kb=m.get("rss_kb", 0), cpu_pct=0.0,
            ctx_voluntary=m.get("ctx_voluntary", 0), ctx_involuntary=m.get("ctx_involuntary", 0)))
        time.sleep(HEARTBEAT_S)


def run():
    """Returns (ok, report_lines)."""
    ctx = mp.get_context("spawn")
    stop, dep_ok = StopFlag(ctx), ctx.RawValue("i", 1)
    history = []
    with tempfile.TemporaryDirectory() as tmp:
        schema = os.path.join(ROOT, "sql", "schema.sql")
        db = os.path.join(tmp, "os5_health.db")
        st = Storage(db, schema)
        p = ctx.Process(target=health_worker, args=(db, schema, stop, dep_ok), name=NAME)
        p.start()

        def verdict():
            rows = [r for r in st.list_health() if r["name"] == NAME]
            row = rows[0] if rows else None
            m = read_proc(row["pid"]) if row else None
            return probe(row, time.time(), m is not None and m["state"] != "Z")

        def expect(label, pred, timeout=8.0):
            v = wait_until(lambda: (lambda x: x if pred(x) else None)(verdict()), timeout)
            history.append((label, v))
            return v

        try:
            expect("worker starts: alive but not ready", lambda v: v["state"] == "STARTING" and v["live"] and not v["ready"])
            expect("warm-up done: alive and ready", lambda v: v["live"] and v["ready"])
            dep_ok.value = 0
            expect("dependency down: ALIVE but NOT READY", lambda v: v["state"] == "NOT_READY" and v["live"] and not v["ready"])
            dep_ok.value = 1
            expect("dependency back: ready again", lambda v: v["live"] and v["ready"])
            os.kill(p.pid, signal.SIGSTOP)                       # simulate a hang
            expect("SIGSTOP (hang): pid exists but NOT ALIVE", lambda v: v["state"] == "HUNG" and not v["live"])
            stopped_state = read_proc(p.pid)["state"]
            os.kill(p.pid, signal.SIGCONT)
            expect("SIGCONT: recovered, alive and ready", lambda v: v["live"] and v["ready"])
            os.kill(p.pid, signal.SIGKILL)
            p.join(5)
            expect("SIGKILL: DEAD", lambda v: v["state"] == "DEAD" and not v["live"])
        except TimeoutError:
            return False, [f"timeout after steps: {[h[0] for h in history]}"]
        finally:
            for sig in (signal.SIGCONT, signal.SIGKILL):
                if p.is_alive():
                    os.kill(p.pid, sig)
            stop.set()
            p.join(5)
        persisted = [r for r in st.list_health() if r["name"] == NAME]
    ok = len(history) == 7 and stopped_state in ("T", "t") and bool(persisted)
    lines = [f"ok {label}  -> live={v['live']} ready={v['ready']} state={v['state']}"
             for label, v in history]
    lines.append(f"hung worker really existed in /proc with state '{stopped_state}' (stopped), "
                 f"heartbeats persisted in process_health: {bool(persisted)}")
    return ok, lines


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS5-1 Health check architecture & readiness state machine")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
