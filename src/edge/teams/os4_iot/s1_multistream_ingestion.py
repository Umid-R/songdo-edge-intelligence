"""OS4-1: Heterogeneous Multi-Stream Sensor Ingestion

Mission : Aggregate PM2.5, ambient noise and illumination, which arrive at different, asynchronous rates.
Concept : Polling and NON-BLOCKING I/O across diverse sensor channels. Each sensor is its own process
          writing newline-delimited JSON to a pipe. ONE ingestion loop multiplexes all three pipes with
          selectors (epoll), reads whatever bytes are available with os.read() on non-blocking file
          descriptors, and reassembles frames that arrive in partial pieces (FrameBuffer). A slow
          sensor can never stall a fast one, and no thread per sensor is needed.

Owner   : (write names here)
Test    : tests/os4/test_s1_multistream_ingestion.py
Run     : PYTHONPATH=src python -m edge.teams.os4_iot.s1_multistream_ingestion

Rates (SIMULATED values and rates): PM2.5 2 Hz, noise 10 Hz, illuminance 4 Hz. Events are stored
atomically with their outbox row. Noise is aggregated as Leq (energy average, 10*log10(mean(10^(L/10)))),
not as a plain mean, because decibels are logarithmic.
"""
import argparse
import json
import math
import os
import selectors
import subprocess
import sys
import tempfile
import time

from edge.models import SensorEvent
from edge.storage import Storage

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
NODE_ID = "EDGE_NODE_OS4_IOT_HUB"
# name, unit, rate (Hz), base value, amplitude  (all SIMULATED)
STREAMS = [
    ("PM25_UG_M3", "ug_m3", 2.0, 30.0, 8.0),
    ("NOISE_LEQ_DBA", "dBA", 10.0, 55.0, 4.0),
    ("ILLUMINANCE_LUX", "lux", 4.0, 800.0, 150.0),
]


class FrameBuffer:
    """Reassemble newline-delimited frames from arbitrary byte chunks (a read may stop mid-frame)."""

    def __init__(self):
        self._buf = b""

    def feed(self, chunk):
        self._buf += chunk
        *lines, self._buf = self._buf.split(b"\n")
        return [line for line in lines if line]


def leq(levels_db):
    """Equivalent continuous level: energy average of decibel values."""
    return 10 * math.log10(sum(10 ** (v / 10) for v in levels_db) / len(levels_db))


def emit(name, duration_s):
    """Sensor process body: print one JSON frame per period to stdout (the pipe)."""
    _, unit, rate, base, amp = next(s for s in STREAMS if s[0] == name)
    t_end = time.monotonic() + duration_s
    deadline, n = time.monotonic(), 0
    while time.monotonic() < t_end:
        frame = {"sensor": name, "unit": unit, "seq": n, "observed_at": time.time(),
                 "value": round(base + amp * math.sin(n / 5.0), 2)}
        print(json.dumps(frame), flush=True)
        n += 1
        deadline += 1.0 / rate
        time.sleep(max(0.0, deadline - time.monotonic()))


def ingest(duration_s, st):
    """One loop, three non-blocking pipes. Returns per-sensor stats."""
    env = dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src"))
    sel = selectors.DefaultSelector()
    procs, bufs = [], {}
    stats = {s[0]: {"frames": 0, "partial_reads": 0, "lat_sum": 0.0} for s in STREAMS}
    nonblocking = []
    for name, *_ in STREAMS:
        p = subprocess.Popen([sys.executable, "-m", "edge.teams.os4_iot.s1_multistream_ingestion",
                              "--emit", name, "--duration", str(duration_s)],
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=env)
        os.set_blocking(p.stdout.fileno(), False)
        nonblocking.append(not os.get_blocking(p.stdout.fileno()))
        sel.register(p.stdout, selectors.EVENT_READ, name)
        procs.append(p)
        bufs[name] = FrameBuffer()
    open_streams, polls = len(procs), 0
    deadline = time.monotonic() + duration_s + 5
    while open_streams and time.monotonic() < deadline:
        polls += 1
        for key, _ in sel.select(timeout=0.05):
            try:
                chunk = os.read(key.fd, 4096)
            except BlockingIOError:
                continue
            if not chunk:                                   # EOF: sensor exited
                sel.unregister(key.fileobj)
                open_streams -= 1
                continue
            frames = bufs[key.data].feed(chunk)
            if not chunk.endswith(b"\n"):
                stats[key.data]["partial_reads"] += 1
            for line in frames:
                f = json.loads(line)
                ev = SensorEvent(node_id=NODE_ID, sensor=f["sensor"], value=f["value"], unit=f["unit"],
                                 observed_at=f["observed_at"])
                ev.received_at = time.time()
                st.insert_event_with_outbox(ev)
                stats[key.data]["frames"] += 1
                stats[key.data]["lat_sum"] += ev.received_at - ev.observed_at
    for p in procs:
        try:
            p.wait(5)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
        p.stdout.close()
    sel.close()
    return stats, [p.returncode for p in procs], nonblocking, polls


def window_stats(rows, base):
    """1-second windows by event time: PM2.5 and lux as mean, noise as Leq."""
    out = {}
    for r in rows:
        w = int((r["observed_at"] - base) // 1.0)
        out.setdefault(w, {}).setdefault(r["sensor"], []).append(r["value"])
    return {w: {s: (leq(v) if s == "NOISE_LEQ_DBA" else sum(v) / len(v)) for s, v in d.items()}
            for w, d in out.items()}


def run(duration=3.0):
    """Returns (ok, report_lines)."""
    # frame reassembly with a frame split across three reads
    fb = FrameBuffer()
    frame = b'{"sensor":"X","value":1}\n'
    got = fb.feed(frame[:5]) + fb.feed(frame[5:14]) + fb.feed(frame[14:] + b'{"sensor":"Y"')
    reassembled = got == [b'{"sensor":"X","value":1}'] and fb.feed(b',"value":2}\n') == [b'{"sensor":"Y","value":2}']

    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(os.path.join(tmp, "os4_ingest.db"), os.path.join(ROOT, "sql", "schema.sql"))
        stats, codes, nonblocking, polls = ingest(duration, st)
        rows = st._read("SELECT sensor,value,observed_at,received_at FROM sensor_events ORDER BY observed_at")
        pending = st.outbox_pending()
        n_events = st.counts()["events"]
    parsed = sum(s["frames"] for s in stats.values())
    expected = {n: math.ceil(rate * duration) for n, _, rate, _, _ in STREAMS}
    counts_ok = all(abs(stats[n]["frames"] - expected[n]) <= 1 for n in expected)
    lat_ms = 1000 * sum(s["lat_sum"] for s in stats.values()) / max(parsed, 1)
    win = window_stats(rows, rows[0]["observed_at"]) if rows else {}
    first = next(iter(sorted(win)), None)
    checks = {
        "frame reassembly: a frame split across 3 reads comes out whole": reassembled,
        "all pipes were non-blocking (os.set_blocking(fd, False))": all(nonblocking),
        "frames per sensor " + ", ".join(f"{n}={stats[n]['frames']} (expected ~{expected[n]})" for n in expected):
            counts_ok,
        "fast sensor not starved by the slow one (noise 10 Hz vs PM2.5 2 Hz)":
            stats["NOISE_LEQ_DBA"]["frames"] >= 3 * stats["PM25_UG_M3"]["frames"],
        f"{parsed} frames parsed = {n_events} events stored = {pending} outbox rows": parsed == n_events == pending > 0,
        f"mean ingestion latency {lat_ms:.1f} ms, one loop made {polls} polls": lat_ms < 500,
        f"sensor processes exited cleanly {codes}": codes == [0, 0, 0],
        "1 s aggregates computed for all three sensors": first is not None and len(win[first]) == 3,
    }
    lines = [f"{'ok ' if v else 'FAILED '}{k}" for k, v in checks.items()]
    if first is not None:
        w = win[first]
        lines.append(f"first 1-second window: PM2.5 mean {w['PM25_UG_M3']:.2f} ug/m3, "
                     f"noise Leq {w['NOISE_LEQ_DBA']:.2f} dBA, illuminance mean {w['ILLUMINANCE_LUX']:.0f} lux")
    return all(checks.values()), lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--emit", help="(internal) run as a sensor process for this sensor name")
    ap.add_argument("--duration", type=float, default=3.0)
    args = ap.parse_args()
    if args.emit:
        emit(args.emit, args.duration)
        return
    ok, lines = run(args.duration)
    print(f"[{'PASS' if ok else 'FAIL'}] OS4-1 Heterogeneous multi-stream sensor ingestion")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
