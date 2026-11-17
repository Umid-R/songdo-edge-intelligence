"""OS6-2: Parking Slot Booking (reserve a bay, no double booking)

Mission : 16 drivers press "Book" in the app at the same moment, but only 10 bays are free.
          Each free bay goes to exactly one driver, and nobody else is turned away. A booking is a
          hold with a deadline: a no-show's bay goes back to the pool and the waitlist gets it.
Concept : - naive check-then-act (SELECT a free bay, then UPDATE it) is a race: drivers who read
            the same free bay all "get" it (double booking)
          - fix: optimistic compare-and-set in one transaction
            (UPDATE ... SET state='BOOKED' WHERE slot_id=? AND state='FREE'); a loser sees
            rowcount 0, re-reads and retries with the next bay
          - idempotency key: re-sending the same request returns the same booking
          - hold TTL on the monotonic clock + a reaper process: an unused hold expires and the
            bay is freed; a late check-in on an expired hold is refused

Owner   : Team A (extra theme OS6)
Test    : tests/os6/test_s2_slot_booking.py
Run     : PYTHONPATH=src python -m edge.teams.os6_parking.s2_slot_booking
"""
import multiprocessing as mp
import os
import random
import sys
import tempfile
import time

from edge.teams.os6_parking._common import (BOOKED, FREE, OCCUPIED, SLOTS, Checks, Viz, connect, init_lot,
                                            slot_string, snapshot, tx)

N_DRIVERS = 16
N_FREE = 10
N_ARRIVE = 6          # of the 10 who get a bay, 6 arrive in time and 4 do not show up
HOLD_S = 2.5          # booking hold on the monotonic clock
RACE_S = 0.003        # time between reading a free bay and claiming it (the race window)
REAP_S = 0.05         # reaper period
DRIVERS = [f"D{i:02d}" for i in range(1, N_DRIVERS + 1)]


def initial_lot(seed):
    rng = random.Random(seed)
    free = set(rng.sample(SLOTS, N_FREE))
    return {s: FREE if s in free else OCCUPIED for s in SLOTS}


FREE_SQL = "SELECT slot_id FROM os6_slots WHERE state='FREE' ORDER BY col, zone"   # nearest first


# ---------------- naive: check-then-act ----------------
def _naive_driver(db, driver, barrier, q):
    conn = connect(db)
    barrier.wait(60)
    row = conn.execute(FREE_SQL).fetchone()
    if row is None:
        q.put((driver, "FULL", None, None, 0))
        return
    slot = row["slot_id"]
    time.sleep(RACE_S)                                # someone else may book it right now
    bid, now = f"bk-req-{driver}", time.monotonic()
    conn.execute("INSERT INTO os6_bookings VALUES (?,?,?,?,'HELD',?,?)",
                 (bid, f"req-{driver}", driver, slot, now, now + HOLD_S))
    conn.execute("UPDATE os6_slots SET state='BOOKED', booking_id=? WHERE slot_id=?", (bid, slot))
    q.put((driver, "BOOKED", slot, bid, 0))


# ---------------- safe: compare-and-set + idempotency key ----------------
def book(conn, driver, request_id, hold_s=HOLD_S, race_s=RACE_S):
    """Returns (result, slot_id, booking_id, retries). result: BOOKED | DUPLICATE | FULL."""
    retries = 0
    while True:
        candidates = [r["slot_id"] for r in conn.execute(FREE_SQL)]
        time.sleep(race_s)
        with tx(conn) as c:
            old = c.execute("SELECT booking_id, slot_id FROM os6_bookings WHERE request_id=?",
                            (request_id,)).fetchone()
            if old:
                return "DUPLICATE", old["slot_id"], old["booking_id"], retries
            if not candidates:
                return "FULL", None, None, retries
            slot, bid, now = candidates[0], f"bk-{request_id}", time.monotonic()
            won = c.execute("UPDATE os6_slots SET state='BOOKED', booking_id=?, version=version+1 "
                            "WHERE slot_id=? AND state='FREE'", (bid, slot)).rowcount == 1
            if won:
                c.execute("INSERT INTO os6_bookings VALUES (?,?,?,?,'HELD',?,?)",
                          (bid, request_id, driver, slot, now, now + hold_s))
                return "BOOKED", slot, bid, retries
        retries += 1                                  # lost the bay to another driver: re-read, retry


def check_in(conn, booking_id):
    """Driver arrives: only a HELD booking inside its hold deadline turns the bay OCCUPIED."""
    with tx(conn) as c:
        ok = c.execute("UPDATE os6_bookings SET status='ARRIVED' WHERE booking_id=? AND status='HELD' "
                       "AND hold_expires > ?", (booking_id, time.monotonic())).rowcount == 1
        if ok:
            c.execute("UPDATE os6_slots SET state='OCCUPIED', version=version+1 WHERE booking_id=?",
                      (booking_id,))
    return ok


def _safe_driver(db, driver, barrier, q):
    conn = connect(db)
    barrier.wait(60)
    q.put((driver, *book(conn, driver, f"req-{driver}")))


def _reaper(db, stop, q):
    """Expiry sweeper: HELD bookings past their monotonic deadline are expired and the bay freed."""
    conn = connect(db)
    released = []
    while not stop.is_set():
        now = time.monotonic()
        with tx(conn) as c:
            for r in c.execute("SELECT booking_id, slot_id, driver, hold_expires FROM os6_bookings "
                               "WHERE status='HELD' AND hold_expires <= ?", (now,)).fetchall():
                c.execute("UPDATE os6_bookings SET status='EXPIRED' WHERE booking_id=?", (r["booking_id"],))
                c.execute("UPDATE os6_slots SET state='FREE', booking_id=NULL, version=version+1 "
                          "WHERE slot_id=? AND booking_id=?", (r["slot_id"], r["booking_id"]))
                released.append((r["driver"], r["slot_id"], round(now - r["hold_expires"], 3)))
        time.sleep(REAP_S)
    q.put(("reaper", released))


# ---------------- helpers ----------------
def owners(conn):
    """{slot: [drivers with an active booking on it]} and {slot: seconds left on the hold}."""
    now, own, left = time.monotonic(), {}, {}
    for r in conn.execute("SELECT slot_id, driver, status, hold_expires FROM os6_bookings "
                          "WHERE status IN ('HELD','ARRIVED') ORDER BY driver"):
        own.setdefault(r["slot_id"], []).append(r["driver"])
        if r["status"] == "HELD":
            left[r["slot_id"]] = round(max(0.0, r["hold_expires"] - now), 2)
    return own, left


def frame(viz, conn, phase, msg="", **extra):
    own, left = owners(conn)
    dup = sorted(s for s, d in own.items() if len(d) > 1)
    viz.emit(phase, slot_string(snapshot(conn)), msg, owners=own, holds=left, hold_s=HOLD_S, dup=dup, **extra)


def race(ctx, db, target, conn, viz, phase):
    barrier, q = ctx.Barrier(N_DRIVERS), ctx.Queue()
    ps = [ctx.Process(target=target, args=(db, d, barrier, q)) for d in DRIVERS]
    for p in ps:
        p.start()
    out = [q.get(timeout=60) for _ in ps]
    for p in ps:
        p.join(10)
    out.sort()
    frame(viz, conn, phase, f"{N_DRIVERS} drivers pressed Book at once", results={d: r for d, r, *_ in out})
    return out


def run(seed=None):
    """Returns (ok, report_lines)."""
    seed = int(seed if seed is not None else os.environ.get("SONGDO_SEED", "6"))
    ctx = mp.get_context("spawn")
    lot = initial_lot(seed)
    free0 = {s for s in SLOTS if lot[s] == FREE}
    viz = Viz("OS6-2")
    ck = Checks()
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        # ---- 1. naive check-then-act ----
        db = os.path.join(tmp, "naive.db")
        conn = init_lot(db, lot)
        frame(viz, conn, "naive", f"{N_FREE} free bays, {N_DRIVERS} drivers (check-then-act, no guard)")
        naive = race(ctx, db, _naive_driver, conn, viz, "naive")
        own, _ = owners(conn)
        n_booked = sum(r == "BOOKED" for _, r, *_ in naive)
        dup_bays = {s: d for s, d in own.items() if len(d) > 1}
        frame(viz, conn, "naive", f"double booking: {len(dup_bays)} bay{'s' if len(dup_bays) != 1 else ''} promised to 2+ drivers "
              f"({n_booked} confirmations for {N_FREE} bays)")

        # ---- 2. safe compare-and-set, with the reaper running ----
        db = os.path.join(tmp, "safe.db")
        conn = init_lot(db, lot)
        stop, rq = ctx.Event(), ctx.Queue()
        reaper = ctx.Process(target=_reaper, args=(db, stop, rq))
        reaper.start()
        frame(viz, conn, "booking", "compare-and-set: UPDATE ... WHERE state='FREE' in one transaction")
        safe = race(ctx, db, _safe_driver, conn, viz, "booking")
        got = {d: (slot, bid) for d, r, slot, bid, _ in safe if r == "BOOKED"}
        full = [d for d, r, *_ in safe if r == "FULL"]
        retries = sum(x[4] for x in safe)
        own, _ = owners(conn)
        snap = snapshot(conn)
        ck("naive check-then-act double-booked at least one bay", len(dup_bays) > 0 or n_booked > N_FREE)
        ck("compare-and-set: every bay has at most one active booking", all(len(d) == 1 for d in own.values()))
        ck(f"exactly {N_FREE} drivers booked ({N_FREE} free bays), {N_DRIVERS - N_FREE} told 'lot full'",
           len(got) == N_FREE and len(full) == N_DRIVERS - N_FREE)
        ck("booked bays are exactly the bays that were free", {s for s, _ in got.values()} == free0)
        ck("occupied bays were never booked",
           all(snap[s]["state"] == OCCUPIED and snap[s]["booking_id"] is None for s in SLOTS if s not in free0))
        ck(f"lost races detected and retried ({retries} retries)", retries > 0)
        frame(viz, conn, "booking", f"{len(got)} booked, {len(full)} lot full, {retries} lost races retried",
              results={d: r for d, r, *_ in safe})

        # ---- 3. duplicate request (app retry after a timeout) ----
        d0 = sorted(got)[0]
        n_rows = conn.execute("SELECT COUNT(*) FROM os6_bookings").fetchone()[0]
        res, slot, bid, _ = book(connect(db), d0, f"req-{d0}")
        ck(f"re-sent request of {d0} returns the same booking, no new row",
           res == "DUPLICATE" and (slot, bid) == got[d0]
           and conn.execute("SELECT COUNT(*) FROM os6_bookings").fetchone()[0] == n_rows)
        frame(viz, conn, "duplicate", f"{d0} re-sent its request: same booking {bid} on {slot}", focus=[slot])

        # ---- 4. arrivals inside the hold, no-shows expire ----
        arrive, noshow = sorted(got)[:N_ARRIVE], sorted(got)[N_ARRIVE:]
        arrived_ok = []
        for d in arrive:
            arrived_ok.append(check_in(conn, got[d][1]))
            frame(viz, conn, "arrival", f"{d} arrived at {got[d][0]}: checked in, bay OCCUPIED", focus=[got[d][0]])
            time.sleep(0.05)
        ck(f"{N_ARRIVE} drivers checked in inside the {HOLD_S} s hold", all(arrived_ok))
        deadline = time.monotonic() + HOLD_S + 10
        while time.monotonic() < deadline:
            held = conn.execute("SELECT COUNT(*) FROM os6_bookings WHERE status='HELD'").fetchone()[0]
            frame(viz, conn, "hold", f"waiting for no-shows {', '.join(noshow)}: {held} holds left")
            if held == 0:
                break
            time.sleep(0.25)
        st = {r["driver"]: r["status"] for r in conn.execute("SELECT driver, status FROM os6_bookings")}
        snap = snapshot(conn)
        ck(f"{len(noshow)} no-show holds expired and their bays are FREE again",
           all(st[d] == "EXPIRED" and snap[got[d][0]]["state"] == FREE for d in noshow))
        ck("arrived bookings did not expire", all(st[d] == "ARRIVED" for d in arrive))
        frame(viz, conn, "expired", f"holds of {', '.join(noshow)} expired: {len(noshow)} bays back in the pool",
              focus=[got[d][0] for d in noshow])

        # ---- 5. waitlist gets the freed bays; late check-in refused ----
        wait = {}
        for d in full:
            r, slot, _, _ = book(conn, d, f"req-{d}-retry")
            wait[d] = (r, slot)
            frame(viz, conn, "waitlist", f"waitlist {d}: " + (f"booked {slot}" if r == "BOOKED" else "still full"),
                  focus=[slot] if slot else [])
        n_wait = sum(r == "BOOKED" for r, _ in wait.values())
        ck(f"waitlist: {len(noshow)} of {len(full)} drivers got the freed bays, the rest still 'full'",
           n_wait == len(noshow) and {s for r, s in wait.values() if r == "BOOKED"} == {got[d][0] for d in noshow})
        late = noshow[0]
        refused = not check_in(conn, got[late][1])
        ck(f"late check-in of {late} on its expired hold is refused", refused)
        frame(viz, conn, "late", f"{late} arrived late: hold expired, check-in refused", focus=[got[late][0]])

        stop.set()
        reaper.join(10)
        released = rq.get(timeout=10)[1]
        ck("reaper released only after the hold deadline (monotonic clock)",
           len(released) == len(noshow) and all(late_s >= 0 for _, _, late_s in released))
        snap = snapshot(conn)
        active = {r["slot_id"]: r["status"] for r in conn.execute(
            "SELECT slot_id, status FROM os6_bookings WHERE status IN ('HELD','ARRIVED')")}
        ck("final lot consistent: every BOOKED bay has one HELD booking, 24 bays in total",
           len(snap) == len(SLOTS) and all(active.get(s) == "HELD" for s in SLOTS if snap[s]["state"] == BOOKED)
           and sum(v == "HELD" for v in active.values()) == sum(snap[s]["state"] == BOOKED for s in SLOTS))
        n_free = sum(snap[s]["state"] == FREE for s in SLOTS)
        frame(viz, conn, "final", f"done: {sum(snap[s]['state'] == OCCUPIED for s in SLOTS)} occupied, "
              f"{sum(snap[s]['state'] == BOOKED for s in SLOTS)} booked (held), {n_free} free")
        viz.save()

    return not ck.failed, [
        f"{ck.summary()}; seed {seed}; {N_DRIVERS} driver processes, {N_FREE} free bays, hold {HOLD_S} s",
        f"naive: {n_booked} confirmations for {N_FREE} bays, double-booked bays: "
        + (", ".join(f"{s} ({'/'.join(d)})" for s, d in sorted(dup_bays.items())) or "none"),
        f"compare-and-set: {len(got)} booked, {len(full)} full, {retries} lost races retried",
        f"arrived {', '.join(arrive)}; no-shows {', '.join(noshow)} expired "
        f"(reaper lag {max((x[2] for x in released), default=0) * 1000:.0f} ms max)",
        f"waitlist: " + ", ".join(f"{d} {r.lower()}{' ' + s if s else ''}" for d, (r, s) in wait.items()),
    ] + [f"FAILED: {n}" for n in ck.failed]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS6-2 Parking slot booking")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
