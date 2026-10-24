"""OS3-1: Incheon Bus Stop Context & Vehicle Influx Stream

Mission : Load the real bus stop and route context around the Techno-Park intersection,
          then store a stream of vehicle and pedestrian readings locally.
Concept : Context data is read-only reference data; the live stream goes through the
          shared atomic "event + outbox" write, so nothing is lost if the WAN is down.

Owner   : Arlen, Onill
Test    : tests/os3/test_s1_bus_stop_context.py
Run     : PYTHONPATH=src python -m edge.teams.os3_mobility.s1_bus_stop_context

Data    : data/bus_stops_techno_park.json, built by scripts/build_bus_stops.py from the real
          Incheon BIS route-stop CSV (data.go.kr, 2026-06-30). The CSV has NO coordinates, so
          stops are selected by exact name. Bus ARRIVAL TIMES ARE SIMULATED (a bus is at the
          stop 5 ticks out of every 40); only the stop and route context is real.
"""
import json
import os
import random
import sys
import tempfile
import time

from edge.models import SensorEvent
from edge.storage import Storage

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))))
BUS_STOPS = os.path.join(ROOT, "data", "bus_stops_techno_park.json")
SCHEMA = os.path.join(ROOT, "sql", "schema.sql")
NODE_ID = "EDGE_NODE_OS3_MOBILITY"


def load_bus_context(path=BUS_STOPS):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def run(ticks=300, seed=42):
    """Returns (ok, report_lines)."""
    doc = load_bus_context()
    stops = doc["stops"]
    n_routes = len({r["route_id"] for s in stops for r in s["routes"]})
    names = sorted({s["stop_name"] for s in stops})

    rng = random.Random(seed)
    base = time.time()
    ped_bus, ped_other = [], []
    with tempfile.TemporaryDirectory() as tmp:
        db = Storage(os.path.join(tmp, "os3_s1.db"), SCHEMA)
        for t in range(ticks):
            bus_here = (t % 40) < 5                       # simulated timing
            speed = max(0.0, rng.gauss(45, 8))
            ped = rng.randint(0, 5) + (8 if bus_here else 0)
            for sensor, value, unit in (("RADAR_SPEED", speed, "km_h"),
                                        ("PED_CROSSWALK_COUNT", float(ped), "count")):
                db.insert_event_with_outbox(SensorEvent(
                    NODE_ID, sensor, round(value, 2), unit, observed_at=base + t,
                    received_at=time.time()))
            (ped_bus if bus_here else ped_other).append(ped)
        stored = db.counts()["events"]
        pending = db.outbox_pending()
    mean_bus = sum(ped_bus) / len(ped_bus)
    mean_other = sum(ped_other) / len(ped_other)

    ok = (len(stops) > 0 and doc.get("sample") is False and stored == 2 * ticks
          and pending == stored and mean_bus > mean_other + 3)
    return ok, [
        f"bus stop context: {len(stops)} real Incheon BIS stops ({', '.join(names)}), "
        f"{n_routes} routes, data of {doc['reference_date']}",
        "bus arrival times are simulated; only the stop and route context is real",
        f"events stored: {stored} (expected {2 * ticks}); outbox rows: {pending}",
        f"mean pedestrians while a bus is at the stop: {mean_bus:.1f}, otherwise: {mean_other:.1f}",
    ]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS3-1 Bus stop context & vehicle stream")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
