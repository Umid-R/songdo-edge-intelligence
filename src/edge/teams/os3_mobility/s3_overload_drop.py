"""OS3-3: Overload Experiment & Drop Policy Verification

Mission : Flood the node faster than it can process and prove that memory stays bounded.
Concept : A bounded queue with drop_oldest keeps the freshest data and never blocks the
          producer; an unbounded queue would grow RSS until the OOM killer steps in.

Owner   : (write names here)
Test    : tests/os3/test_s3_overload_drop.py
Run     : PYTHONPATH=src python -m edge.teams.os3_mobility.s3_overload_drop

Uses the shared core BoundedQueue (a real multiprocessing queue), with a real second
process as the slow consumer.
"""
import multiprocessing as mp
import queue
import sys
import time

from edge.ipc import STOP, BoundedQueue


def _slow_consumer(q, out, delay_s):
    """A real process that handles one item per delay_s."""
    seen = []
    while True:
        try:
            item = q.get(timeout=2.0)
        except queue.Empty:
            break
        if item == STOP:
            break
        seen.append(item)
        time.sleep(delay_s)
    out.put(seen)


def run(burst=5000, cap=500):
    """Returns (ok, report_lines)."""
    # 1) burst with no consumer: size capped, exactly burst-cap dropped, oldest dropped first
    q = BoundedQueue(maxsize=cap)
    t0 = time.perf_counter()
    for i in range(burst):
        q.put(i)
    dt = time.perf_counter() - t0
    time.sleep(0.5)                                  # let the feeder thread flush
    size = q.qsize()
    oldest = q.get(timeout=2)
    burst_ok = size == cap and q.dropped == burst - cap and oldest == burst - cap

    # 2) fast producer + slow real consumer process: bounded, newest data survives
    ctx = mp.get_context("spawn")
    q2 = BoundedQueue(maxsize=100, ctx=ctx)
    out = ctx.Queue()
    consumer = ctx.Process(target=_slow_consumer, args=(q2, out, 0.002))
    consumer.start()
    total = 3000
    peak = 0
    t0 = time.perf_counter()
    for i in range(total):
        q2.put(i)
        peak = max(peak, q2.qsize())
    produce_s = time.perf_counter() - t0
    time.sleep(0.3)
    q2.put(STOP)
    seen = out.get(timeout=30)
    consumer.join(5)
    delivered = len(seen)
    in_order = seen == sorted(seen)
    newest_kept = bool(seen) and seen[-1] == total - 1
    overload_ok = (peak <= 100 and q2.dropped > 0 and in_order and newest_kept
                   and delivered + q2.dropped == total and produce_s < 5)

    ok = burst_ok and overload_ok
    return ok, [
        f"burst of {burst} into a {cap}-slot queue: size {size}, dropped {q.dropped}, "
        f"oldest surviving item {oldest}, {burst} puts took {dt * 1000:.0f} ms (never blocked)",
        f"{total} items vs a slow consumer process: peak queue {peak} (cap 100), "
        f"delivered {delivered}, dropped {q2.dropped}, delivered+dropped={delivered + q2.dropped}",
        f"order preserved: {in_order}; newest item delivered: {newest_kept}",
    ]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS3-3 Overload & drop-oldest policy")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
