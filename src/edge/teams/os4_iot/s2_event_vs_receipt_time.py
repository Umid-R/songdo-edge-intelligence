"""OS4-2: Event-Time vs. Receipt-Time Handling

Mission : Resolve out-of-order delivery from delayed sensor nodes.
Concept : Every reading carries TWO timestamps: observed_at (when the sensor measured it, event time)
          and received_at (when the gateway got it, receipt time). They must stay independent. If you
          aggregate by receipt time, a delayed reading lands in the wrong window and the result is wrong.
          Aggregating by event time gives the right answer; a watermark decides when a 1 s window is
          "closed", and readings that arrive after that are LATE: they are still stored and they CORRECT
          the already-emitted window.

Owner   : Nazarbek
Test    : tests/os4/test_s2_event_vs_receipt_time.py
Run     : PYTHONPATH=src python -m edge.teams.os4_iot.s2_event_vs_receipt_time

Data is SIMULATED and deterministic (seeded): 40 readings, one every 0.25 s, 30% delayed by 1 to 3 s.
Arrival order is the order of received_at. Allowed lateness (watermark delay) is 1.5 s.
"""
import os
import random
import sys
import tempfile
from collections import defaultdict

from edge.models import SensorEvent
from edge.storage import Storage

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
BASE = 1_700_000_000.0               # simulated epoch start
WINDOW_S = 1.0
ALLOWED_LATENESS_S = 1.5


def make_stream(n=40, period=0.25, seed=7):
    """(i, observed_at, received_at, pm25) tuples, in sensor order."""
    rng = random.Random(seed)
    out = []
    for i in range(n):
        observed = BASE + i * period
        delay = rng.uniform(1.0, 3.0) if rng.random() < 0.3 else rng.choice([0.01, 0.02, 0.03])
        out.append((i, observed, observed + delay, 20.0 + (i % 12) * 2.5))
    return out


def window_of(t):
    return int((t - BASE) // WINDOW_S)


def window_means(pairs):
    acc = defaultdict(lambda: [0.0, 0])
    for t, v in pairs:
        acc[window_of(t)][0] += v
        acc[window_of(t)][1] += 1
    return {w: s / c for w, (s, c) in acc.items()}


class EventTimeWindower:
    """Streaming aggregation by event time with a watermark = max observed - allowed lateness."""

    def __init__(self, allowed_lateness=ALLOWED_LATENESS_S):
        self.allowed = allowed_lateness
        self.sums = defaultdict(lambda: [0.0, 0])
        self.closed, self.first_emit = set(), {}
        self.max_obs = float("-inf")
        self.late = 0

    def _mean(self, w):
        s, c = self.sums[w]
        return s / c

    def add(self, observed, value):
        w = window_of(observed)
        if w in self.closed:
            self.late += 1                           # arrived after its window was emitted
        self.sums[w][0] += value
        self.sums[w][1] += 1
        self.max_obs = max(self.max_obs, observed)
        watermark = self.max_obs - self.allowed
        for w2 in sorted(self.sums):
            if w2 not in self.closed and BASE + (w2 + 1) * WINDOW_S <= watermark:
                self.closed.add(w2)
                self.first_emit[w2] = self._mean(w2)

    def finish(self):
        for w in self.sums:
            if w not in self.closed:
                self.closed.add(w)
                self.first_emit[w] = self._mean(w)
        return {w: self._mean(w) for w in self.sums}


def run():
    """Returns (ok, report_lines)."""
    stream = make_stream()
    arrival = sorted(stream, key=lambda e: e[2])                      # delivery order
    truth = window_means([(o, v) for _, o, _, v in stream])           # what really happened
    inversions = sum(1 for a, b in zip(arrival, arrival[1:]) if b[1] < a[1])

    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(os.path.join(tmp, "os4_time.db"), os.path.join(ROOT, "sql", "schema.sql"))
        windower = EventTimeWindower()
        for i, observed, received, value in arrival:                  # insert in ARRIVAL order
            ev = SensorEvent(node_id="EDGE_NODE_OS4_IOT_HUB", sensor="PM25_UG_M3", value=value, unit="ug_m3",
                             observed_at=observed, received_at=received, event_id=f"os4-time-{i:03d}")
            st.insert_event_with_outbox(ev)
            windower.add(observed, value)
        final = windower.finish()
        rows = st._read("SELECT event_id,value,observed_at,received_at FROM sensor_events")
        by_event = window_means([(r["observed_at"], r["value"]) for r in rows])
        by_receipt = window_means([(r["received_at"], r["value"]) for r in rows])
        preserved = {r["event_id"]: (r["observed_at"], r["received_at"]) for r in rows} == \
                    {f"os4-time-{i:03d}": (o, rc) for i, o, rc, _ in stream}
    wrong_windows = [w for w in truth if abs(by_receipt.get(w, -1) - truth[w]) > 1e-9]
    corrected = [w for w in final if abs(windower.first_emit[w] - final[w]) > 1e-9]
    checks = {
        "observed_at and received_at both stored exactly as sent (independent columns)": preserved,
        f"delivery is out of order ({inversions} inversions in arrival order)": inversions >= 5,
        f"aggregating by EVENT time matches the ground truth in all {len(truth)} windows":
            set(by_event) == set(truth) and all(abs(by_event[w] - truth[w]) < 1e-9 for w in truth),
        f"aggregating by RECEIPT time is wrong in {len(wrong_windows)} window(s) (the bug this avoids)":
            len(wrong_windows) >= 1,
        f"{windower.late} readings arrived after their window closed (late > {ALLOWED_LATENESS_S} s)":
            windower.late >= 1,
        f"{len(corrected)} already-emitted window(s) were corrected by late data; final == ground truth":
            len(corrected) >= 1 and all(abs(final[w] - truth[w]) < 1e-9 for w in truth),
    }
    return all(checks.values()), [f"{'ok ' if v else 'FAILED '}{k}" for k, v in checks.items()]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS4-2 Event-time vs. receipt-time handling")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
