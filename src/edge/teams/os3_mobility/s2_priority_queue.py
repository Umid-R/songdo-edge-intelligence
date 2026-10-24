"""OS3-2: Priority-Aware Bounded Queue Processing

Mission : Prioritize pedestrian surge alarms over background sensor logging.
Concept : Bounded queue prevents buffer bloat and guarantees low-latency processing.

Owner   : (write names here)
Test    : tests/os3/test_s2_priority_queue.py
Run     : PYTHONPATH=src python -m edge.teams.os3_mobility.s2_priority_queue

The shared core BoundedQueue has one level (drop oldest). This file adds a two-level
queue on top of the same idea, without touching the shared src/edge/ipc.py: HIGH (alarms) and LOW
(background logs) share ONE capacity. get() serves HIGH first. When the queue is full the
oldest LOW item is dropped, so an alarm is never lost to background logging.
"""
import sys
import threading
import time
from collections import deque


class PriorityBoundedQueue:
    HIGH, LOW = 0, 1

    def __init__(self, maxsize=500):
        self.maxsize = maxsize
        self._q = {self.HIGH: deque(), self.LOW: deque()}
        self._cond = threading.Condition()
        self.dropped = {self.HIGH: 0, self.LOW: 0}

    def _size(self):
        return len(self._q[self.HIGH]) + len(self._q[self.LOW])

    def put(self, item, priority=LOW):
        """Never blocks. Returns False if the new item itself was dropped."""
        with self._cond:
            if self._size() >= self.maxsize:
                if priority == self.LOW and not self._q[self.LOW]:
                    self.dropped[self.LOW] += 1       # only alarms queued: the new log loses
                    return False
                victim = self.LOW if self._q[self.LOW] else self.HIGH
                self._q[victim].popleft()
                self.dropped[victim] += 1
            self._q[priority].append(item)
            self._cond.notify()
            return True

    def get(self, timeout=None):
        """HIGH first, then LOW (FIFO inside a level). None on timeout."""
        with self._cond:
            if not self._size():
                self._cond.wait(timeout)
            for p in (self.HIGH, self.LOW):
                if self._q[p]:
                    return self._q[p].popleft()
            return None

    def qsize(self):
        with self._cond:
            return self._size()


def run():
    """Returns (ok, report_lines)."""
    # 1) alarm overtakes 600 logs that overflowed a 500-slot queue
    q = PriorityBoundedQueue(maxsize=500)
    for i in range(600):
        q.put(("log", i), PriorityBoundedQueue.LOW)
    q.put(("ALARM", "pedestrian_surge"), PriorityBoundedQueue.HIGH)
    first = q.get(timeout=0.1)
    second = q.get(timeout=0.1)
    overtook = (first == ("ALARM", "pedestrian_surge") and second == ("log", 101)
                and q.dropped[q.HIGH] == 0 and q.dropped[q.LOW] == 101 and q.qsize() == 498)

    # 2) flood of logs from another thread: the alarm's wait stays tiny
    q2 = PriorityBoundedQueue(maxsize=500)
    stop = threading.Event()

    def flood():
        i = 0
        while not stop.is_set():
            q2.put(("log", i), q2.LOW)
            i += 1

    t = threading.Thread(target=flood, daemon=True)
    t.start()
    time.sleep(0.2)
    t0 = time.perf_counter()
    q2.put(("ALARM", "x"), q2.HIGH)
    got = q2.get(timeout=1)
    wait_ms = (time.perf_counter() - t0) * 1000
    stop.set()
    t.join(1)
    under_flood = got == ("ALARM", "x") and q2.dropped[q2.HIGH] == 0 and wait_ms < 50

    # 3) only alarms in the queue: a new log is the one that is dropped
    q3 = PriorityBoundedQueue(maxsize=3)
    for i in range(3):
        q3.put(("ALARM", i), q3.HIGH)
    kept_alarms = (q3.put(("log", 0), q3.LOW) is False and q3.dropped[q3.LOW] == 1
                   and q3.qsize() == 3)

    ok = overtook and under_flood and kept_alarms
    return ok, [
        f"first item served: {first} (the alarm, although it arrived last)",
        f"next item: {second} (oldest surviving log); dropped logs {q.dropped[q.LOW]}, "
        f"dropped alarms {q.dropped[q.HIGH]}",
        f"alarm served in {wait_ms:.2f} ms while another thread flooded logs: {under_flood}",
        f"full queue of alarms rejects a new log instead of dropping an alarm: {kept_alarms}",
    ]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS3-2 Priority-aware bounded queue")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
