"""OS1-1: Local Room Sensor Ingestion & Cached Inventory.

Reads indoor CO2 / temperature readings from several rooms, loads a cached VWorld building
GeoJSON, and for every reading records WHICH BUILDING its room is in (point-in-polygon against the
cached geometry). The readings themselves use the shared atomic event+outbox API; the building
context goes into its own table, os1_sensor_context (one row per event).

Inventory file, in this order (the report always says which one was used, nothing is hidden):
  1. the inventory_path argument, if given (a missing file is then an ERROR, not a silent fallback)
  2. data/vworld_buildings.geojson          the real VWorld export (not in the repo: add it)
  3. data/vworld_buildings_SAMPLE.geojson   hand-made footprints, NOT real VWorld data

Room positions are SIMULATED coordinates placed around Songdo Convensia. Never makes a network
request.
"""
import json
import os
import random
import sys
import tempfile
import time
from pathlib import Path

from edge.models import SensorEvent
from edge.storage import Storage

ROOT = Path(__file__).resolve().parents[4]
SCHEMA = ROOT / "sql" / "schema.sql"
DEFAULT_INVENTORY = ROOT / "data" / "vworld_buildings.geojson"
SAMPLE_INVENTORY = ROOT / "data" / "vworld_buildings_SAMPLE.geojson"
NODE_ID = "EDGE_NODE_OS1_CONVENSIA"
# room id, longitude, latitude (SIMULATED sensor placement); R-OUT is deliberately outside every building
ROOMS = [("R-101", 126.6534, 37.3888), ("R-102", 126.6541, 37.3892),
         ("R-201", 126.6560, 37.3888), ("R-OUT", 126.6600, 37.3900)]
# what the SAMPLE file must produce (used only when the SAMPLE file is the inventory)
SAMPLE_EXPECTED = {"R-101": "B-CONVENSIA", "R-102": "B-CONVENSIA", "R-201": "B-OFFICE", "R-OUT": None}


def load_cached_inventory(path=DEFAULT_INVENTORY):
    """Load a cached GeoJSON FeatureCollection; never makes a network request.

    Returns a list of features. A missing cache returns an empty list (callers must report that).
    Invalid JSON/GeoJSON raises ValueError instead of silently hiding corruption.
    """
    path = Path(path)
    if not path.exists():
        return []
    try:
        with path.open(encoding="utf-8") as f:
            doc = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid cached GeoJSON: {path}") from exc
    if doc.get("type") != "FeatureCollection" or not isinstance(doc.get("features"), list):
        raise ValueError("Cached inventory must be a GeoJSON FeatureCollection")
    return doc["features"]


def resolve_inventory(inventory_path=None):
    """Return (path, label). An explicit path is never replaced by a fallback."""
    if inventory_path is not None:
        return Path(inventory_path), f"explicit file {Path(inventory_path).name}"
    if DEFAULT_INVENTORY.exists():
        return DEFAULT_INVENTORY, "real cached VWorld export (data/vworld_buildings.geojson)"
    return SAMPLE_INVENTORY, "SAMPLE footprints (data/vworld_buildings_SAMPLE.geojson), NOT real VWorld data"


def match_building(room_id, features):
    """Match a room identifier to a feature's common ID properties, if present."""
    target = str(room_id)
    for feature in features:
        props = feature.get("properties") or {}
        identifiers = (props.get("room_id"), props.get("building_id"), props.get("id"),
                       feature.get("id"))
        if target in {str(x) for x in identifiers if x is not None}:
            return feature
    return None


def _ring_contains(lon, lat, ring):
    """Ray casting: is the point inside this closed ring of [lon, lat] vertices?"""
    inside, j = False, len(ring) - 1
    for i in range(len(ring)):
        xi, yi, xj, yj = ring[i][0], ring[i][1], ring[j][0], ring[j][1]
        if (yi > lat) != (yj > lat) and lon < (xj - xi) * (lat - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def polygon_contains(lon, lat, rings):
    """GeoJSON polygon = [exterior, *holes]: inside the exterior and outside every hole."""
    if not rings or not _ring_contains(lon, lat, rings[0]):
        return False
    return not any(_ring_contains(lon, lat, hole) for hole in rings[1:])


def feature_contains(feature, lon, lat):
    geom = feature.get("geometry") or {}
    if geom.get("type") == "Polygon":
        return polygon_contains(lon, lat, geom["coordinates"])
    if geom.get("type") == "MultiPolygon":
        return any(polygon_contains(lon, lat, poly) for poly in geom["coordinates"])
    return False


def locate_building(lon, lat, features):
    """The first building whose footprint contains the point, or None."""
    for feature in features:
        if feature_contains(feature, lon, lat):
            return feature
    return None


def building_id(feature):
    if feature is None:
        return None
    props = feature.get("properties") or {}
    return props.get("building_id") or feature.get("id") or props.get("id")


def building_name(feature):
    if feature is None:
        return None
    props = feature.get("properties") or {}
    return props.get("name") or props.get("building_name") or str(building_id(feature))


def run(ticks=20, seed=7, db_path=None, inventory_path=None):
    """Run a deterministic ingestion + building-mapping demonstration; return (ok, lines)."""
    if ticks < 1:
        raise ValueError("ticks must be at least 1")
    path, source = resolve_inventory(inventory_path)
    features = load_cached_inventory(path)
    if not features:
        return False, [f"NO INVENTORY: {path} is missing or empty, readings cannot be mapped to buildings",
                       f"inventory source requested: {source}"]
    rng = random.Random(seed)
    base = time.time()
    room_building = {room: locate_building(lon, lat, features) for room, lon, lat in ROOMS}
    temp_dir = tempfile.TemporaryDirectory() if db_path is None else None
    try:
        db_file = db_path or os.path.join(temp_dir.name, "os1_s1.db")
        db = Storage(db_file, str(SCHEMA))
        with db.tx() as c:
            c.execute("CREATE TABLE IF NOT EXISTS os1_sensor_context (event_id TEXT PRIMARY KEY "
                      "REFERENCES sensor_events(event_id), room_id TEXT NOT NULL, building_id TEXT, "
                      "building_name TEXT, lon REAL, lat REAL)")
        stored_ok = 0
        for tick in range(ticks):
            for room, lon, lat in ROOMS:
                co2 = max(350.0, rng.gauss(650.0, 55.0))
                temperature = rng.gauss(22.0, 1.2)
                for sensor, value, unit in (("INDOOR_CO2_PPM", co2, "ppm"),
                                            ("ROOM_TEMPERATURE_C", temperature, "C")):
                    event = SensorEvent(NODE_ID, sensor, round(value, 2), unit,
                                        observed_at=base + tick, received_at=time.time())
                    if db.insert_event_with_outbox(event):
                        stored_ok += 1
                        b = room_building[room]
                        with db.tx() as c:
                            c.execute("INSERT OR REPLACE INTO os1_sensor_context "
                                      "(event_id,room_id,building_id,building_name,lon,lat) VALUES (?,?,?,?,?,?)",
                                      (event.event_id, room, building_id(b), building_name(b), lon, lat))
        expected = len(ROOMS) * 2 * ticks
        counts, pending = db.counts(), db.outbox_pending()
        ctx = db._read("SELECT room_id, building_id, COUNT(*) AS n FROM os1_sensor_context "
                       "GROUP BY room_id, building_id ORDER BY room_id")
        ctx_rows = sum(r["n"] for r in ctx)
        mapped_rooms = {r["room_id"]: r["building_id"] for r in ctx}
        mapped_events = sum(r["n"] for r in ctx if r["building_id"] is not None)
        is_sample = Path(path).resolve() == SAMPLE_INVENTORY.resolve()
        sample_ok = (mapped_rooms == SAMPLE_EXPECTED) if is_sample else True
        ok = (stored_ok == expected and counts["events"] == expected and pending == expected
              and ctx_rows == expected and mapped_events > 0 and sample_ok)
        per_room = ", ".join(f"{r}->{b or 'UNMAPPED'}" for r, b in sorted(mapped_rooms.items()))
        return ok, [
            f"inventory: {source}; {len(features)} building feature(s)",
            f"room -> building by point-in-polygon: {per_room}",
            f"events persisted: {counts['events']}; outbox pending: {pending}",
            f"building context rows: {ctx_rows} (one per event), {mapped_events} mapped to a building, "
            f"{ctx_rows - mapped_events} outside every known footprint",
            "event and outbox rows are written atomically by shared Storage; "
            "the context row is a second short transaction (derivable, so it can be rebuilt)",
        ]
    finally:
        if temp_dir is not None:
            temp_dir.cleanup()


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS1-1 Local room sensor ingestion & cached inventory")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
