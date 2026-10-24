"""OS2-2: Periodic Monotonic Deadlines & Jitter Measurement

Mission : Run a 50 ms control loop and measure how late/early each tick is under the Linux
          scheduler (CFS), with and without CPU contention.
Concept : Jitter J[n] = |dt[n] - T| with T = 50 ms and dt[n] = t[n] - t[n-1], where every
          timestamp comes from the MONOTONIC clock (immune to NTP / wall-clock jumps).
          Two loop styles are compared:
            naive     : work(); sleep(T)           -> period = T + work time, drift grows forever
            absolute  : sleep until next_deadline; next_deadline += T   -> no cumulative drift

Owner   : Umid, Umrbek
Test    : tests/os2/test_s2_jitter.py
Run     : PYTHONPATH=src python -m edge.teams.os2_robot.s2_jitter [--ticks 100]

The "control work" is simulated with a 5 ms busy loop. Thresholds in the test are generous
on purpose (a VM can be noisy); the printed numbers are what you explain in the defense.
"""
import argparse
import multiprocessing as mp
import os
import sys
import time

PERIOD_S = 0.05
WORK_S = 0.005
POLICIES = {getattr(os, n): n for n in ("SCHED_OTHER", "SCHED_BATCH", "SCHED_IDLE", "SCHED_FIFO", "SCHED_RR")
            if hasattr(os, n)}


def busy(seconds):
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        pass


def run_loop(n_ticks, absolute=True, period=PERIOD_S, work=WORK_S):
    """Run n_ticks control cycles. Returns the monotonic timestamp of every tick (n+1 values)."""
    stamps = [time.monotonic()]
    next_deadline = stamps[0] + period
    for _ in range(n_ticks):
        if absolute:
            delay = next_deadline - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            next_deadline += period
        else:
            time.sleep(period)
        stamps.append(time.monotonic())
        busy(work)                                # the control computation
    return stamps


def stats(stamps, period=PERIOD_S):
    """Jitter statistics in milliseconds. drift = how far the last tick is from n*T."""
    dts = [b - a for a, b in zip(stamps, stamps[1:])]
    jit = sorted(abs(dt - period) for dt in dts)
    n = len(jit)
    return {
        "ticks": n,
        "mean_ms": 1000 * sum(jit) / n,
        "p95_ms": 1000 * jit[int(0.95 * (n - 1))],
        "max_ms": 1000 * jit[-1],
        "drift_ms": 1000 * ((stamps[-1] - stamps[0]) - n * period),
    }


def _burn(stop):
    while not stop.is_set():
        for _ in range(200000):
            pass


def run_loaded(n_ticks):
    """Same absolute-deadline loop while every CPU is kept busy by other processes."""
    ctx = mp.get_context("spawn")
    stop = ctx.Event()
    workers = [ctx.Process(target=_burn, args=(stop,)) for _ in range(min(max(os.cpu_count() or 2, 2), 8))]
    for w in workers:
        w.start()
    time.sleep(0.3)
    try:
        return run_loop(n_ticks, absolute=True), len(workers)
    finally:
        stop.set()
        for w in workers:
            w.join(5)


def _fmt(label, s):
    return (f"{label}: mean {s['mean_ms']:.2f} ms, p95 {s['p95_ms']:.2f} ms, max {s['max_ms']:.2f} ms, "
            f"drift after {s['ticks']} ticks {s['drift_ms']:+.1f} ms")


def run(ticks=60):
    """Returns (ok, report_lines)."""
    a = stats(run_loop(ticks, absolute=True))
    nv = stats(run_loop(ticks, absolute=False))
    loaded_stamps, n_burners = run_loaded(max(ticks * 2 // 3, 20))
    ld = stats(loaded_stamps)

    absolute_ok = a["mean_ms"] < PERIOD_S * 500 and abs(a["drift_ms"]) < PERIOD_S * 1000
    naive_drifts = nv["drift_ms"] >= 0.5 * ticks * WORK_S * 1000 and nv["drift_ms"] > 3 * abs(a["drift_ms"])
    loaded_ok = ld["max_ms"] < 1000
    ok = absolute_ok and naive_drifts and loaded_ok
    policy = POLICIES.get(os.sched_getscheduler(0), "unknown")
    return ok, [
        f"target period T = {PERIOD_S * 1000:.0f} ms, simulated control work {WORK_S * 1000:.0f} ms, "
        f"scheduler policy {policy} (the CFS class; kernels 6.6+ use its successor EEVDF), nice {os.nice(0)}",
        _fmt("absolute deadlines     ", a),
        _fmt("naive sleep(T)         ", nv) + "  <- period is T + work, drift keeps growing",
        _fmt(f"absolute + {n_burners} CPU burners", ld) + "  <- CFS time-slices us against the burners",
        f"absolute loop keeps the schedule (no cumulative drift): {absolute_ok}; "
        f"naive loop drifts: {naive_drifts}; loaded run completed: {loaded_ok}",
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticks", type=int, default=60, help="ticks per loop (60 ticks = 3 s)")
    args = ap.parse_args()
    ok, lines = run(args.ticks)
    print(f"[{'PASS' if ok else 'FAIL'}] OS2-2 Periodic monotonic deadlines & jitter")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
