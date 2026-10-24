"""OS1-2: Competing Occupancy Writers & Race Condition Demonstration.

Owner   : Aidar, Arkar

Two demonstrations of the same lost-update bug (counter = counter + 1 is LOAD, ADD, STORE):

1. Threads, deterministic: the unsafe version deliberately makes all 80 workers read the same old
   value before writing (a barrier forces the bad interleaving), so the result is always exactly 1.
   The safe version protects the read-modify-write with a Mutex (threading.Lock).
2. Processes, real concurrency: 4 separate processes increment a counter in shared memory 20 times
   each (80 total), with a 0.5 ms gap between LOAD and STORE so the other processes get in between.
   Without a lock updates are lost: the processes run in lock-step (a start barrier makes them begin
   together), so in each of the 20 rounds all four read the same value and only one increment
   survives. The result is typically 20 (always far below 80). With multiprocessing.Lock the result is
   exactly 80. The gap and the barrier are deliberate: they make the lost update reproducible.
"""
import multiprocessing as mp
import sys
import threading
import time

GAP_S = 0.0005          # window between LOAD and STORE in the process demo


def _proc_worker(counter, lock, start, n):
    start.wait()
    for _ in range(n):
        if lock is None:
            old = counter.value
            time.sleep(GAP_S)
            counter.value = old + 1
        else:
            with lock:                       # the lock covers LOAD, ADD and STORE
                old = counter.value
                time.sleep(GAP_S)
                counter.value = old + 1


def run_processes(total=80, procs=4):
    """Real-process race. Returns (expected, {'unprotected': n, 'locked': n})."""
    ctx = mp.get_context("spawn")
    per = total // procs
    results = {}
    for name, use_lock in (("unprotected", False), ("locked", True)):
        counter = ctx.RawValue("i", 0)
        lock = ctx.Lock() if use_lock else None
        start = ctx.Barrier(procs)
        workers = [ctx.Process(target=_proc_worker, args=(counter, lock, start, per)) for _ in range(procs)]
        for p in workers:
            p.start()
        for p in workers:
            p.join(30)
        results[name] = counter.value
    return per * procs, results


def run(workers=80, processes=True):
    """Return (ok, report_lines) for a lost-update demonstration (threads, then real processes)."""
    if workers < 2:
        raise ValueError("workers must be at least 2 to demonstrate competing writers")

    # Unsafe: force a race window with a barrier after every worker reads the value.
    unsafe = {"value": 0}
    barrier = threading.Barrier(workers)

    def unsafe_increment():
        old_value = unsafe["value"]
        barrier.wait()
        unsafe["value"] = old_value + 1

    threads = [threading.Thread(target=unsafe_increment) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    unsafe_result = unsafe["value"]

    # Safe: lock covers the complete read-modify-write operation.
    safe = {"value": 0}
    mutex = threading.Lock()

    def safe_increment():
        with mutex:
            safe["value"] += 1

    threads = [threading.Thread(target=safe_increment) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    safe_result = safe["value"]

    ok = unsafe_result < workers and safe_result == workers
    lines = [
        f"concurrent writers: {workers}; expected final counter: {workers}",
        f"without Mutex: {unsafe_result} (lost {workers - unsafe_result} update(s))",
        f"with Mutex: {safe_result} (lost 0 updates)",
        "Mutex protects the entire read-modify-write operation, not just one line",
    ]
    if processes:
        expected, res = run_processes()
        proc_ok = res["unprotected"] < expected and res["locked"] == expected
        ok = ok and proc_ok
        lines += [
            f"real processes (4 x 20 increments, shared memory, widened LOAD/STORE gap): "
            f"without Lock {res['unprotected']}/{expected} (lost {expected - res['unprotected']}), "
            f"with multiprocessing.Lock {res['locked']}/{expected}",
            f"process demo correct (unprotected < {expected}, locked == {expected}): {proc_ok}",
        ]
    return ok, lines


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS1-2 Competing occupancy writers & race condition")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
