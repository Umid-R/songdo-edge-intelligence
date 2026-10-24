"""OS4-5: WAN Outage Backlog Drain & AirKorea Baseline Benchmark

Mission : Drain the PM2.5 telemetry queued during a WAN outage and cross-check it against the AirKorea
          baseline of Songdo Station 131233.
Verify  : 100% delivery acknowledgement to the downstream Urban Data Platform.
Concept : 6 simulated hours (1 reading per minute): hours 0-1 online, the WAN is cut for hours 2-4, then
          restored. While cut, readings pile up in the durable outbox and nothing is delivered. After the
          restore the backlog drains in batches. Delivery is at-least-once with an IDEMPOTENT receiver:
          to prove it, the first batch is delivered but the node "crashes" before recording the ack, so
          the same batch is sent again; the platform answers ACK_DUPLICATE and keeps exactly one copy.

Owner   : Nazarbek
Test    : tests/os4/test_s5_backlog_drain.py
Run     : PYTHONPATH=src python -m edge.teams.os4_iot.s5_backlog_drain

BASELINE: the real AirKorea file is NOT in the repo (no internet access when this was written). Put an
hourly PM2.5 export of station 131233 in data/airkorea_131233_pm25.csv (columns hour,pm25; the first 6
rows are used, matched by hour order). If it is missing, the scenario falls back to a clearly labelled
SIMULATED baseline. With the simulated one this checks the pipeline (hour alignment, units, delivery),
NOT real-world sensor accuracy: say so in the defense. The tolerance of 10 ug/m3 is ASSUMED.
"""
import csv
import json
import math
import os
import sys
import tempfile
import time

from edge.config import load_config
from edge.models import SensorEvent
from edge.storage import Storage

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
BASELINE_FILE = os.path.join(ROOT, "data", "airkorea_131233_pm25.csv")
BASE = 1_700_000_000.0
HOURS, PER_HOUR = 6, 60
TRUTH = [28.0, 35.0, 62.0, 75.0, 48.0, 30.0]              # SIMULATED hourly PM2.5 means (ug/m3)
SIM_OFFSETS = [1.5, -0.8, 2.0, -1.2, 0.5, 1.0]            # SIMULATED station-vs-sensor differences
CUT_AT, RESTORE_AT = 120, 300                              # reading indexes
TOLERANCE = 10.0                                           # ASSUMED, ug/m3


def gen_events(node_id):
    out = []
    for k in range(HOURS * PER_HOUR):
        value = round(TRUTH[k // PER_HOUR] + 3 * math.sin(2 * math.pi * (k % PER_HOUR) / PER_HOUR), 4)
        out.append(SensorEvent(node_id=node_id, sensor="PM25_UG_M3", value=value, unit="ug_m3",
                               observed_at=BASE + 60.0 * k, event_id=f"os4-drain-{k:04d}"))
    return out


def load_baseline(path=BASELINE_FILE):
    """(six hourly values, label). Uses the cached AirKorea file when present, else a SIMULATED baseline."""
    try:
        with open(path, encoding="utf-8", newline="") as f:
            vals = []
            for row in csv.reader(f):
                try:
                    vals.append(float(row[1]))
                except (ValueError, IndexError):
                    continue                                   # header or junk row
        if len(vals) >= HOURS:
            return vals[:HOURS], f"cached AirKorea station 131233 file ({os.path.basename(path)})"
    except OSError:
        pass
    return [t + o for t, o in zip(TRUTH, SIM_OFFSETS)], "SIMULATED baseline (data/airkorea_131233_pm25.csv not found)"


def deliver(st, limit=50, crash_before_mark=False):
    """Send up to `limit` outbox rows to the platform; the platform replies ACK_NEW or ACK_DUPLICATE per
    event. The outbox row is marked sent only after the acks arrive (unless we 'crash' before that)."""
    rows = st._read("SELECT id,event_id,payload FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT ?", (limit,))
    acks = {"new": 0, "dup": 0}
    if not rows:
        return acks
    with st.tx() as c:
        for r in rows:
            cur = c.execute("INSERT OR IGNORE INTO upstream_sink (event_id,payload,received_at) VALUES (?,?,?)",
                            (r["event_id"], r["payload"], time.time()))
            acks["new" if cur.rowcount else "dup"] += 1
    if not crash_before_mark:
        with st.tx() as c:
            now = time.time()
            for r in rows:
                c.execute("UPDATE outbox SET sent_at=? WHERE id=?", (now, r["id"]))
    return acks


def hourly_means(pairs):
    acc = {}
    for t, v in pairs:
        acc.setdefault(int((t - BASE) // 3600), []).append(v)
    return [sum(acc[h]) / len(acc[h]) for h in sorted(acc)]


def run():
    """Returns (ok, report_lines)."""
    cfg = load_config(os.path.join(ROOT, "config", "os4.yaml"))
    events = gen_events(cfg["node"]["id"])
    totals = {"new": 0, "dup": 0}
    with tempfile.TemporaryDirectory() as tmp:
        st = Storage(os.path.join(tmp, "os4_drain.db"), cfg["paths"]["schema"])
        st.set_wan_online(True)
        snap = {}
        crashed = False
        for k, ev in enumerate(events):
            if k == CUT_AT:
                st.set_wan_online(False)
            if k == RESTORE_AT:
                snap["pending_at_restore"], snap["sink_at_restore"] = st.outbox_pending(), st.counts()["sink"]
                st.set_wan_online(True)
            st.insert_event_with_outbox(ev)
            if st.wan_online():
                a = deliver(st, 50, crash_before_mark=(k == RESTORE_AT and not crashed))
                crashed = crashed or k == RESTORE_AT
                totals["new"] += a["new"]
                totals["dup"] += a["dup"]
        while st.outbox_pending():
            a = deliver(st, 50)
            totals["new"] += a["new"]
            totals["dup"] += a["dup"]
        rep = st.conservation_report()
        sink_rows = st._read("SELECT payload FROM upstream_sink")
        edge_rows = st._read("SELECT value,observed_at FROM sensor_events")
    platform = hourly_means([(json.loads(r["payload"])["observed_at"], json.loads(r["payload"])["value"])
                             for r in sink_rows])
    edge = hourly_means([(r["observed_at"], r["value"]) for r in edge_rows])
    baseline, label = load_baseline()
    mae = sum(abs(p - b) for p, b in zip(platform, baseline)) / HOURS
    bias = sum(p - b for p, b in zip(platform, baseline)) / HOURS
    n = len(events)
    checks = {
        f"WAN cut for readings {CUT_AT}-{RESTORE_AT - 1}: nothing delivered meanwhile "
        f"(sink stayed at {snap['sink_at_restore']}), backlog {snap['pending_at_restore']} queued":
            snap["sink_at_restore"] == CUT_AT and snap["pending_at_restore"] == RESTORE_AT - CUT_AT,
        f"100% acknowledged: {totals['new']} ACK_NEW for {n} readings, {rep['sink']} stored by the platform":
            totals["new"] == rep["sink"] == rep["events"] == n,
        f"idempotent receiver: {totals['dup']} re-sent events answered ACK_DUPLICATE, no second copy stored":
            totals["dup"] > 0 and rep["sink"] == n,
        f"outbox drained, event conservation holds ({rep['pending']} pending)": rep["pending"] == 0 and rep["conserved"],
        "platform hourly means identical to the edge's own data (delivery altered nothing)":
            len(platform) == HOURS and all(abs(p - e) < 1e-9 for p, e in zip(platform, edge)),
        f"cross-check vs baseline: MAE {mae:.2f} ug/m3, bias {bias:+.2f} (tolerance {TOLERANCE})": mae <= TOLERANCE,
    }
    lines = [f"{'ok ' if v else 'FAILED '}{k}" for k, v in checks.items()]
    lines.append(f"baseline used: {label}")
    lines.append("hourly PM2.5 platform vs baseline: " + ", ".join(f"{p:.1f}/{b:.1f}" for p, b in zip(platform, baseline)))
    return all(checks.values()), lines


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS4-5 WAN outage backlog drain & AirKorea baseline benchmark")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
