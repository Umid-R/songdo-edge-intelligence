"""Shared parking-lot model for OS6 (extra theme): layout, SQLite schema and helpers, 2D frames.

Lot: three rows (zones A, B, C) of 8 bays = 24 bays. The entrance is on the left, so the bay
nearest the entrance is column 01; within a column zone A comes first.

2D visualisation: every scenario prints frames as single lines "@VIZ {json}". The web console
(OS6 tab) reads them from the run log and draws the lot; the same frames are also written to
<run_dir>/os6_frames_<scenario>.json as a run artifact.
"""
import json
import os
import sqlite3
import time
from contextlib import contextmanager

ZONES = ("A", "B", "C")
COLS = 8
SLOTS = [f"{z}{c:02d}" for z in ZONES for c in range(1, COLS + 1)]
CAPACITY = len(SLOTS)
FREE, OCCUPIED, BOOKED = "FREE", "OCCUPIED", "BOOKED"
CODE = {FREE: "F", OCCUPIED: "O", BOOKED: "B"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS os6_slots (
    slot_id      TEXT PRIMARY KEY,
    zone         TEXT NOT NULL,
    col          INTEGER NOT NULL,
    state        TEXT NOT NULL CHECK (state IN ('FREE','OCCUPIED','BOOKED')),
    booking_id   TEXT,
    zone_tick    INTEGER NOT NULL DEFAULT 0,      -- last sensor tick of this bay's zone
    version      INTEGER NOT NULL DEFAULT 1,
    updated_mono REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS os6_bookings (
    booking_id   TEXT PRIMARY KEY,
    request_id   TEXT NOT NULL UNIQUE,           -- idempotency key chosen by the driver's app
    driver       TEXT NOT NULL,
    slot_id      TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('HELD','ARRIVED','EXPIRED')),
    created_mono REAL NOT NULL,
    hold_expires REAL NOT NULL                    -- monotonic deadline of the hold
);
"""


def entrance_order():
    """Bays sorted nearest-to-entrance first."""
    return sorted(SLOTS, key=lambda s: (int(s[1:]), ZONES.index(s[0])))


def connect(path):
    conn = sqlite3.connect(path, timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


@contextmanager
def tx(conn, mode="IMMEDIATE"):
    """All-or-nothing transaction. IMMEDIATE takes the write lock up front."""
    conn.execute(f"BEGIN {mode}")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise


def init_lot(path, states):
    """Fresh lot. states: {slot_id: FREE | OCCUPIED | BOOKED}."""
    conn = connect(path)
    conn.executescript(SCHEMA)
    with tx(conn) as c:
        c.execute("DELETE FROM os6_slots")
        c.execute("DELETE FROM os6_bookings")
        for s in SLOTS:
            c.execute("INSERT INTO os6_slots (slot_id, zone, col, state, updated_mono) VALUES (?,?,?,?,?)",
                      (s, s[0], int(s[1:]), states[s], time.monotonic()))
    return conn


def snapshot(conn):
    """One consistent read (a deferred read transaction = one WAL snapshot)."""
    with tx(conn, "DEFERRED") as c:
        rows = c.execute("SELECT slot_id, state, zone_tick, booking_id FROM os6_slots").fetchall()
    return {r["slot_id"]: dict(r) for r in rows}


def slot_string(snap):
    return "".join(CODE[snap[s]["state"]] for s in SLOTS)


def is_available(conn, slot_id):
    """The availability query: a bay is available only if it is FREE (not occupied, not held)."""
    row = conn.execute("SELECT state FROM os6_slots WHERE slot_id=?", (slot_id,)).fetchone()
    if row is None:
        raise KeyError(slot_id)
    return row["state"] == FREE


def nearest_free(conn):
    free = {r["slot_id"] for r in conn.execute("SELECT slot_id FROM os6_slots WHERE state='FREE'")}
    return next((s for s in entrance_order() if s in free), None)


class Viz:
    """2D frames for the console: printed as '@VIZ {json}' and kept as a run artifact."""

    def __init__(self, scenario):
        self.scenario = scenario
        self.frames = []
        self.t0 = time.monotonic()

    def emit(self, phase, slots, msg="", **extra):
        frame = {"s": self.scenario, "n": len(self.frames), "t": round(time.monotonic() - self.t0, 3),
                 "phase": phase, "slots": slots, "msg": msg, **extra}
        self.frames.append(frame)
        print("@VIZ " + json.dumps(frame, separators=(",", ":")), flush=True)

    def save(self):
        run_dir = os.environ.get("SONGDO_RUN_DIR")
        if not run_dir or not os.path.isdir(run_dir):
            return None
        path = os.path.join(run_dir, f"os6_frames_{self.scenario}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"layout": {"zones": ZONES, "cols": COLS, "slots": SLOTS}, "frames": self.frames}, f)
        return path


class Checks:
    def __init__(self):
        self.items = []

    def __call__(self, name, ok):
        self.items.append((name, bool(ok)))
        return bool(ok)

    @property
    def failed(self):
        return [n for n, ok in self.items if not ok]

    def summary(self):
        return f"{len(self.items) - len(self.failed)}/{len(self.items)} checks passed"
