"""OS5-3: Deadlock Detection & Mutex Timeout Recovery

Mission : Prevent a permanent hang during INVERTED resource acquisition.
Concept : Two workers need two locks. Worker 0 takes L1 then L2, worker 1 takes L2 then L1. If each
          grabs its first lock before the other grabs its second, neither can ever continue: deadlock
          (circular wait). Three variants are run with real processes and multiprocessing.Lock:
            naive    : blocking acquire()               -> DEADLOCK, detected by a watchdog, then killed
            timeout  : acquire(timeout=0.2) + release the first lock + back off + retry -> recovers
            ordered  : both take L1 then L2 (global lock ordering) -> deadlock cannot happen

Owner   : Teshaboev Umidillo
Test    : tests/os5/test_s3_deadlock_timeout.py
Run     : PYTHONPATH=src python -m edge.teams.os5_reliable.s3_deadlock_timeout

Detection = both processes alive, both stuck holding exactly one lock, and no progress change over 0.3 s
(wait channel from /proc/<pid>/wchan is printed as extra evidence when the kernel exposes it).
The retry back-off is deliberately asymmetric (10 ms vs 60 ms) so the two workers cannot livelock.
The inverted-order window is forced on purpose so the demo is deterministic.
"""
import multiprocessing as mp
import os
import signal
import sys
import time

ACQUIRE_TIMEOUT_S = 0.2
MAX_ATTEMPTS = 20


def _wait_flag(flags, other, timeout=2.0):
    end = time.monotonic() + timeout
    while not flags[other] and time.monotonic() < end:
        time.sleep(0.002)


def _worker(me, first, second, locks, flags, progress, stats, mode, backoff_s):
    other = 1 - me
    if mode == "naive":
        locks[first].acquire()
        progress[me] = 1
        flags[me] = 1
        _wait_flag(flags, other)                 # force the inverted-order window
        locks[second].acquire()                  # blocks forever if the other worker holds it
        progress[me] = 2
        locks[second].release()
        locks[first].release()
    elif mode == "timeout":
        for attempt in range(1, MAX_ATTEMPTS + 1):
            stats[2 + me] = attempt
            locks[first].acquire()
            progress[me] = 1
            if attempt == 1:
                flags[me] = 1
                _wait_flag(flags, other)
            if locks[second].acquire(timeout=ACQUIRE_TIMEOUT_S):
                progress[me] = 2
                locks[second].release()
                locks[first].release()
                return
            stats[me] += 1                       # timed out: give up the lock we hold, back off, retry
            locks[first].release()
            progress[me] = 0
            time.sleep(backoff_s)
    else:                                        # ordered: always L1 then L2
        locks[0].acquire()
        locks[1].acquire()
        progress[me] = 2
        locks[1].release()
        locks[0].release()


def _wchan(pid):
    try:
        with open(f"/proc/{pid}/wchan", encoding="utf-8") as f:
            return f.read().strip() or "-"
    except OSError:
        return "?"


def run_mode(ctx, mode, watch_s=1.0):
    locks = [ctx.Lock(), ctx.Lock()]
    flags, progress, stats = ctx.RawArray("i", 2), ctx.RawArray("i", 2), ctx.RawArray("i", 4)
    orders = [(0, 1), (0, 1)] if mode == "ordered" else [(0, 1), (1, 0)]
    backoffs = [0.01, 0.06]
    procs = [ctx.Process(target=_worker, args=(i, orders[i][0], orders[i][1], locks, flags, progress,
                                               stats, mode, backoffs[i])) for i in range(2)]
    t0 = time.monotonic()
    for p in procs:
        p.start()
    limit = watch_s if mode == "naive" else 15.0
    while time.monotonic() - t0 < limit and not all(v == 2 for v in progress):
        time.sleep(0.02)
    finished = all(v == 2 for v in progress)
    elapsed = time.monotonic() - t0
    out = {"mode": mode, "finished": finished, "elapsed_s": elapsed, "timeouts": stats[0] + stats[1],
           "attempts": (stats[2], stats[3]), "deadlock": False, "wchan": []}
    if not finished:
        snap1 = tuple(progress)
        time.sleep(0.3)
        out["deadlock"] = (all(p.is_alive() for p in procs) and snap1 == tuple(progress)
                           and tuple(progress) == (1, 1))
        out["wchan"] = [_wchan(p.pid) for p in procs]
    for p in procs:
        if not finished and p.is_alive():
            os.kill(p.pid, signal.SIGKILL)       # watchdog recovery, ONLY when a deadlock was detected
        p.join(5)
    out["exit_codes"] = [p.exitcode for p in procs]
    return out


def run():
    """Returns (ok, report_lines)."""
    ctx = mp.get_context("spawn")
    naive, timeout, ordered = (run_mode(ctx, m) for m in ("naive", "timeout", "ordered"))
    checks = {
        f"naive: DEADLOCK detected (both stuck holding one lock, no progress; wchan {naive['wchan']}), "
        f"killed by the watchdog {naive['exit_codes']}":
            not naive["finished"] and naive["deadlock"] and naive["exit_codes"] == [-9, -9],
        f"timeout: both finished in {timeout['elapsed_s']:.2f} s after {timeout['timeouts']} lock timeout(s), "
        f"attempts {timeout['attempts']}":
            timeout["finished"] and timeout["timeouts"] >= 1 and timeout["exit_codes"] == [0, 0]
            and timeout["elapsed_s"] < 5,
        f"ordered: both finished in {ordered['elapsed_s']:.2f} s with {ordered['timeouts']} timeouts (no deadlock possible)":
            ordered["finished"] and ordered["timeouts"] == 0 and ordered["exit_codes"] == [0, 0],
    }
    return all(checks.values()), [f"{'ok ' if v else 'FAILED '}{k}" for k, v in checks.items()]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS5-3 Deadlock detection & mutex timeout recovery")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
