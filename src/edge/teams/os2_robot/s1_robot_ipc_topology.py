"""OS2-1: Multi-Process Robot Architecture & IPC Topology

Mission : Separate the robot into three distinct Linux processes: LiDAR, Controller and
          SafetyMonitor, and show how they talk to each other.
Concept : Process isolation + two IPC mechanisms:
            LiDAR  --(BoundedQueue, drop_oldest)-->  Controller       sensor stream
            Controller <--(Unix domain socket)--> SafetyMonitor       request/response veto
          The SafetyMonitor is a separate process so that a bug in the Controller cannot
          disable it: it independently refuses any MOVE command while an obstacle is < 0.8 m.

Owner   : (write names here)
Test    : tests/os2/test_s1_robot_ipc_topology.py
Run     : PYTHONPATH=src python -m edge.teams.os2_robot.s1_robot_ipc_topology
Inspect : while it runs, `pstree -p -a <parent pid>` and `ss -xlp` show the same topology.

Fault injection (on purpose): the Controller deliberately sends a wrong MOVE command for
every second blocked reading. The test passes only if the SafetyMonitor vetoes each one.
All sensor values are SIMULATED (a fixed repeating sequence, so the run is deterministic).
"""
import multiprocessing as mp
import os
import queue
import sys
import tempfile
import time

from edge.ipc import STOP, BoundedQueue, UnixSocketServer, unix_request
from edge.procfs import read_proc

PERIOD_S = 0.05                 # LiDAR runs at 20 Hz
STOP_DISTANCE_M = 0.8
# Simulated LiDAR distances (m), repeated forever. 3 of every 10 readings are blocked (< 0.8).
PATTERN = [2.4, 1.9, 1.4, 0.6, 2.8, 1.1, 0.4, 2.2, 0.7, 3.0]


def lidar_main(q, stop, info_q, res_q):
    """Process 1: produce distance readings into the bounded queue."""
    info_q.put(("LiDAR", os.getpid(), os.getppid()))
    n = 0
    while not stop.is_set():
        q.put({"seq": n, "distance_m": PATTERN[n % len(PATTERN)], "t_mono": time.monotonic()})
        n += 1
        stop.wait(PERIOD_S)
    q.put(STOP)                                   # shutdown sentinel for the Controller
    res_q.put(("LiDAR", {"produced": n}))


def _ask(sock_path, msg):
    for _ in range(50):                           # the monitor may still be starting
        try:
            return unix_request(sock_path, msg, timeout=2.0)
        except (FileNotFoundError, ConnectionRefusedError):
            time.sleep(0.1)
    raise RuntimeError("SafetyMonitor unreachable")


def controller_main(q, stop, sock_path, info_q, res_q):
    """Process 2: turn readings into motion commands and get each one approved."""
    info_q.put(("Controller", os.getpid(), os.getppid()))
    processed = approved = denied = bad_sent = 0
    while True:
        try:
            item = q.get(timeout=0.3)
        except queue.Empty:
            if stop.is_set():
                break
            continue
        if item == STOP:
            break
        blocked = item["distance_m"] < STOP_DISTANCE_M
        if blocked and item["seq"] % 2 == 0:
            cmd = "MOVE"                          # INJECTED BUG: moves although blocked
            bad_sent += 1
        else:
            cmd = "STOP" if blocked else "MOVE"
        reply = _ask(sock_path, {"cmd": cmd, "distance_m": item["distance_m"], "seq": item["seq"]})
        processed += 1
        if reply["approved"]:
            approved += 1
        else:
            denied += 1
    res_q.put(("Controller", {"processed": processed, "approved": approved,
                              "denied": denied, "bad_sent": bad_sent}))


def safety_monitor_main(sock_path, stop, info_q, res_q):
    """Process 3: independent safety check, served over a Unix domain socket."""
    stats = {"requests": 0, "vetoes": 0}

    def handler(msg):
        stats["requests"] += 1
        if msg["cmd"] == "MOVE" and msg["distance_m"] < STOP_DISTANCE_M:
            stats["vetoes"] += 1
            return {"approved": False, "reason": "obstacle < 0.8 m"}
        return {"approved": True, "reason": "ok"}

    server = UnixSocketServer(sock_path, handler)
    server.start()
    info_q.put(("SafetyMonitor", os.getpid(), os.getppid()))
    stop.wait()
    server.stop()
    res_q.put(("SafetyMonitor", dict(stats)))


def _collect(q, n, timeout=10.0):
    out = {}
    deadline = time.monotonic() + timeout
    while len(out) < n and time.monotonic() < deadline:
        try:
            item = q.get(timeout=0.5)
        except queue.Empty:
            continue
        out[item[0]] = item[1:] if len(item) > 2 else item[1]
    return out


def run(duration=2.0):
    """Returns (ok, report_lines)."""
    ctx = mp.get_context("spawn")
    q = BoundedQueue(500, ctx)
    stop, mon_stop = ctx.Event(), ctx.Event()
    info_q, res_q = ctx.Queue(), ctx.Queue()

    with tempfile.TemporaryDirectory() as tmp:
        sock = os.path.join(tmp, "safety.sock")
        mon = ctx.Process(target=safety_monitor_main, args=(sock, mon_stop, info_q, res_q), name="SafetyMonitor")
        ctrl = ctx.Process(target=controller_main, args=(q, stop, sock, info_q, res_q), name="Controller")
        lidar = ctx.Process(target=lidar_main, args=(q, stop, info_q, res_q), name="LiDAR")

        mon.start()
        for _ in range(100):                      # wait until the socket is bound
            if os.path.exists(sock):
                break
            time.sleep(0.05)
        ctrl.start()
        lidar.start()

        time.sleep(duration / 2)
        # ---- topology evidence while everything is running ----
        procs = _collect(info_q, 3)               # name -> (pid, ppid)
        pids = {name: v[0] for name, v in procs.items()}
        ppids = {name: v[1] for name, v in procs.items()}
        with open("/proc/net/unix", encoding="utf-8") as f:
            socket_listed = sock in f.read()      # what `ss -xlp` reads
        alive = all(read_proc(p) is not None and read_proc(p)["state"] != "Z" for p in pids.values())
        time.sleep(duration / 2)

        # ---- ordered, clean shutdown ----
        stop.set()
        lidar.join(5)
        ctrl.join(5)
        mon_stop.set()
        mon.join(5)
        res = _collect(res_q, 3)

    me = os.getpid()
    distinct = len(set(pids.values())) == 3 and me not in pids.values()
    children = len(ppids) == 3 and all(pp == me for pp in ppids.values())
    exit_codes = [lidar.exitcode, ctrl.exitcode, mon.exitcode]
    reaped = all(read_proc(p) is None for p in pids.values())
    produced = res.get("LiDAR", {}).get("produced", -1)
    c = res.get("Controller", {})
    m = res.get("SafetyMonitor", {})
    flow_ok = c.get("processed", -1) + q.dropped == produced > 0
    veto_ok = c.get("bad_sent", 0) > 0 and m.get("vetoes") == c.get("bad_sent")

    ok = (distinct and children and socket_listed and alive and flow_ok and veto_ok
          and exit_codes == [0, 0, 0] and reaped)
    return ok, [
        f"3 separate processes: LiDAR pid {pids.get('LiDAR')}, Controller pid {pids.get('Controller')}, "
        f"SafetyMonitor pid {pids.get('SafetyMonitor')} (all distinct, all children of pid {me}: {distinct and children})",
        f"Unix socket {os.path.basename(sock)} listed in /proc/net/unix while running (like `ss -xlp`): {socket_listed}",
        f"LiDAR produced {produced} readings -> BoundedQueue -> Controller processed "
        f"{c.get('processed')} (dropped {q.dropped})",
        f"Controller sent {c.get('bad_sent')} wrong MOVE commands (injected bug); "
        f"SafetyMonitor vetoed {m.get('vetoes')} of {m.get('requests')} requests",
        f"clean shutdown: exit codes {exit_codes}, all pids reaped (no zombies): {reaped}",
    ]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS2-1 Multi-process robot architecture & IPC topology")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
