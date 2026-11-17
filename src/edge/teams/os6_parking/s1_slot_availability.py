"""OS6-1: Parking Slot Availability (is a bay free?)

Mission : Bay sensors in three zones report occupancy; the display board and the drivers' app ask
          "is bay X available?" and "which free bay is nearest the entrance?" while the sensors
          keep writing. Every answer must match the real lot.
Concept : - one sensor process per zone (A, B, C) and a separate display-board reader process
          - debounce: a state change is committed only after 2 identical readings in a row, so a
            one-tick sensor glitch (a shadow, a person walking past) never flips a bay
          - atomicity + snapshot isolation: a zone's 8 bays are updated in ONE transaction and the
            reader reads in ONE WAL snapshot, so it never sees a half-updated zone (torn read).
            The naive phase writes bay by bay (autocommit) and the reader catches torn zones.

Owner   : Team A (extra theme OS6)
Test    : tests/os6/test_s1_slot_availability.py
Run     : PYTHONPATH=src python -m edge.teams.os6_parking.s1_slot_availability
"""
import multiprocessing as mp
import os
import random
import sys
import tempfile
import time

from edge.teams.os6_parking._common import (FREE, OCCUPIED, SLOTS, ZONES, Checks, Viz, connect,
                                            entrance_order, init_lot, is_available, nearest_free,
                                            slot_string, snapshot, tx)

TICKS = 48            # sensor ticks per phase
TICK_S = 0.04         # pause between ticks
BAY_WRITE_S = 0.001   # time to write one bay (widens the torn-read window in both phases)
DEBOUNCE = 2          # identical readings needed before a change is committed
FRAME_S = 0.08        # how often the console frame is sampled
PHASE_LIMIT_S = 60    # a phase normally takes ~3 s
BOARD_S = 0.001       # display-board refresh period (a reader that never pauses starves the writers)


def make_trace(seed):
    """Ground truth per bay (segments >= 4 ticks, last 5 ticks stable) and noisy sensor readings
    (single-tick glitches only where the truth is stable). Returns truth, raw, glitches per zone."""
    rng = random.Random(seed)
    truth, raw, glitches = {}, {}, dict.fromkeys(ZONES, 0)
    for s in SLOTS:
        seq, state = [], rng.random() < 0.55
        while len(seq) < TICKS:
            seq += [state] * rng.randint(4, 14)
            state = not state
        seq = seq[:TICKS]
        for t in range(TICKS - 4, TICKS):
            seq[t] = seq[TICKS - 5]
        noisy = list(seq)
        for t in range(2, TICKS - 1):
            stable = seq[t - 2] == seq[t - 1] == seq[t] == seq[t + 1]
            if stable and noisy[t - 1] == seq[t - 1] and rng.random() < 0.07:
                noisy[t] = not seq[t]
                glitches[s[0]] += 1
        truth[s], raw[s] = seq, noisy
    return truth, raw, glitches


def _zone_sensor(db, zone, raw, atomic, start, out_q):
    """One zone's sensor gateway: debounce the readings, write the zone's 8 bays every tick."""
    conn = connect(db)
    bays = [s for s in SLOTS if s[0] == zone]
    committed = {s: raw[s][0] for s in bays}
    pending = dict.fromkeys(bays, 0)
    changes = filtered = 0
    start.wait()
    for t in range(1, TICKS):
        for s in bays:
            if raw[s][t] == committed[s]:
                if pending[s]:
                    filtered += 1                  # a one-tick glitch was absorbed
                pending[s] = 0
            else:
                pending[s] += 1
                if pending[s] >= DEBOUNCE:
                    committed[s], pending[s] = raw[s][t], 0
                    changes += 1
        rows = [(OCCUPIED if committed[s] else FREE, t, time.monotonic(), s) for s in bays]
        sql = "UPDATE os6_slots SET state=?, zone_tick=?, updated_mono=?, version=version+1 WHERE slot_id=?"
        if atomic:
            with tx(conn) as c:                     # the whole zone in one transaction
                for r in rows:
                    c.execute(sql, r)
                    time.sleep(BAY_WRITE_S)
        else:
            for r in rows:                          # bay by bay, each its own autocommit
                conn.execute(sql, r)
                time.sleep(BAY_WRITE_S)
        time.sleep(TICK_S)
    out_q.put(("sensor", zone, changes, filtered))


def torn_zones(snap):
    """Zones whose bays carry different sensor ticks inside one snapshot = half-updated zone."""
    return [z for z in ZONES if len({snap[s]["zone_tick"] for s in SLOTS if s[0] == z}) > 1]


def _display_board(db, stop, out_q):
    """Reads the lot as fast as it can, the way a display board or the app would."""
    conn = connect(db)
    reads = torn = bad_totals = 0
    worst_ms = 0.0
    while not stop.is_set():
        t0 = time.perf_counter()
        snap = snapshot(conn)
        worst_ms = max(worst_ms, (time.perf_counter() - t0) * 1000)
        reads += 1
        states = [v["state"] for v in snap.values()]
        if len(states) != len(SLOTS) or states.count(FREE) + states.count(OCCUPIED) != len(SLOTS):
            bad_totals += 1
        if torn_zones(snap):
            torn += 1
        time.sleep(BOARD_S)
    out_q.put(("board", reads, torn, bad_totals, worst_ms))


def run_phase(path, truth, raw, atomic, viz):
    ctx = mp.get_context("spawn")
    phase = "atomic" if atomic else "naive"
    conn = init_lot(path, {s: OCCUPIED if truth[s][0] else FREE for s in SLOTS})
    start, stop, q = ctx.Event(), ctx.Event(), ctx.Queue()
    sensors = [ctx.Process(target=_zone_sensor, args=(path, z, raw, atomic, start, q)) for z in ZONES]
    board = ctx.Process(target=_display_board, args=(path, stop, q))
    for p in sensors + [board]:
        p.start()
    viz.emit(phase, slot_string(snapshot(conn)),
             "naive: each bay is its own transaction" if not atomic
             else "atomic: each zone is one transaction", tick=0, torn=[])
    start.set()
    seen_torn = 0
    deadline = time.monotonic() + PHASE_LIMIT_S
    while any(p.is_alive() for p in sensors):
        if time.monotonic() > deadline:            # fail fast instead of hanging the run
            for p in sensors + [board]:
                p.terminate()
            raise RuntimeError(f"{phase} phase did not finish within {PHASE_LIMIT_S} s")
        time.sleep(FRAME_S)
        snap = snapshot(conn)
        tz = torn_zones(snap)
        seen_torn += bool(tz)
        ticks = {z: max(snap[s]["zone_tick"] for s in SLOTS if s[0] == z) for z in ZONES}
        viz.emit(phase, slot_string(snap), ("torn read: zone " + ", ".join(tz) + " half-updated") if tz else "",
                 tick=min(ticks.values()), torn=tz)
    for p in sensors:
        p.join(30)
    stop.set()
    board.join(30)
    results = [q.get(timeout=10) for _ in range(len(ZONES) + 1)]
    sensors_out = {r[1]: r[2:] for r in results if r[0] == "sensor"}
    board_out = next(r[1:] for r in results if r[0] == "board")
    return conn, sensors_out, board_out, seen_torn


def run(seed=None):
    """Returns (ok, report_lines)."""
    seed = int(seed if seed is not None else os.environ.get("SONGDO_SEED", "6"))
    truth, raw, glitches = make_trace(seed)
    viz = Viz("OS6-1")
    ck = Checks()
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        _, _, (n_reads, n_torn, n_bad, _), _ = run_phase(os.path.join(tmp, "naive.db"), truth, raw, False, viz)
        conn, sensors, (a_reads, a_torn, a_bad, a_ms), _ = run_phase(
            os.path.join(tmp, "atomic.db"), truth, raw, True, viz)

        final = snapshot(conn)
        want = {s: OCCUPIED if truth[s][-1] else FREE for s in SLOTS}
        n_free = sum(v == FREE for v in want.values())
        ck("naive bay-by-bay writes: the board caught half-updated zones (torn reads)", n_torn > 0)
        ck("atomic zone transactions: 0 torn reads", a_torn == 0)
        ck("board read continuously during the atomic phase (> 200 snapshots)", a_reads > 200)
        ck("every snapshot adds up to 24 bays (free + occupied)", n_bad == 0 and a_bad == 0)
        ck("final lot state equals the ground truth for all 24 bays",
           all(final[s]["state"] == want[s] for s in SLOTS))
        real = {z: sum(sum(a != b for a, b in zip(truth[s], truth[s][1:])) for s in SLOTS if s[0] == z)
                for z in ZONES}
        ck("each real arrival / departure committed exactly once (debounce)",
           all(sensors[z][0] == real[z] for z in ZONES))
        ck(f"all {sum(glitches.values())} one-tick sensor glitches filtered, none committed",
           sum(glitches.values()) > 0 and all(sensors[z][1] == glitches[z] for z in ZONES))

        answers = {s: is_available(conn, s) for s in SLOTS}
        ck("is_available() correct for 24/24 bays", all(answers[s] == (want[s] == FREE) for s in SLOTS))
        near = nearest_free(conn)
        ck(f"nearest free bay to the entrance = {near}",
           near == next((s for s in entrance_order() if want[s] == FREE), None))
        ck("snapshot read under 250 ms while sensors write", a_ms < 250)
        probe = [s for s in entrance_order()][:3]
        viz.emit("query", slot_string(final), f"{n_free} of {len(SLOTS)} bays free · nearest to entrance: {near}",
                 nearest=near, free=n_free, queries={s: answers[s] for s in probe})
        viz.save()

    return not ck.failed, [
        f"{ck.summary()}; seed {seed}; {len(ZONES)} sensor processes + 1 display-board reader",
        f"naive phase: {n_reads} snapshots, {n_torn} torn (half-updated zone)",
        f"atomic phase: {a_reads} snapshots, {a_torn} torn; slowest snapshot {a_ms:.1f} ms",
        f"real changes committed {sum(s[0] for s in sensors.values())}, glitches filtered "
        f"{sum(s[1] for s in sensors.values())}/{sum(glitches.values())}",
        f"final: {n_free}/{len(SLOTS)} free; nearest free bay {near}; "
        + ", ".join(f"{s} {'free' if answers[s] else 'taken'}" for s in probe),
    ] + [f"FAILED: {n}" for n in ck.failed]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS6-1 Parking slot availability")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
