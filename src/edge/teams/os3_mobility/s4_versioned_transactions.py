"""OS3-4: Transactional State Update & Version Guarding

Mission : Several processes update the same piece of node state (for example the speed
          sign setting). No update may be lost.
Concept : A blind read-modify-write loses updates. A version column plus compare-and-swap
          (UPDATE ... WHERE version = <what I read>) inside a transaction makes the loser
          notice and retry.

Owner   : (write names here)
Test    : tests/os3/test_s4_versioned_transactions.py
Run     : PYTHONPATH=src python -m edge.teams.os3_mobility.s4_versioned_transactions

The versioned table is created here (CREATE TABLE IF NOT EXISTS) so the shared
sql/schema.sql is not touched.
"""
import multiprocessing as mp
import os
import sys
import tempfile
import time

from edge.storage import Storage

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))))
SCHEMA = os.path.join(ROOT, "sql", "schema.sql")


def ensure_table(st):
    with st.tx() as c:
        c.execute("CREATE TABLE IF NOT EXISTS os3_state ("
                  "key TEXT PRIMARY KEY, value TEXT NOT NULL, version INTEGER NOT NULL DEFAULT 1)")


def get_state(st, key):
    """Returns (value, version) or (None, 0)."""
    with st.tx() as c:
        row = c.execute("SELECT value, version FROM os3_state WHERE key=?", (key,)).fetchone()
    return (row["value"], row["version"]) if row else (None, 0)


def set_state(st, key, value):
    """Blind write: no version check. This is the buggy way."""
    with st.tx() as c:
        c.execute("INSERT INTO os3_state (key,value,version) VALUES (?,?,1) "
                  "ON CONFLICT(key) DO UPDATE SET value=excluded.value, version=version+1",
                  (key, str(value)))


def cas_state(st, key, new_value, expected_version):
    """Update only if the row is still at the version we read. True on success."""
    with st.tx() as c:
        cur = c.execute("UPDATE os3_state SET value=?, version=version+1 "
                        "WHERE key=? AND version=?", (str(new_value), key, expected_version))
        return cur.rowcount == 1


def _naive_worker(db_path, key, n):
    st = Storage(db_path, SCHEMA, init=False)
    for _ in range(n):
        val, _ver = get_state(st, key)
        time.sleep(0.0005)                         # widen the race window
        set_state(st, key, int(val) + 1)           # overwrites what others wrote meanwhile


def _cas_worker(db_path, key, n, retries_q):
    st = Storage(db_path, SCHEMA, init=False)
    retries = 0
    for _ in range(n):
        while True:
            val, ver = get_state(st, key)
            time.sleep(0.0005)
            if cas_state(st, key, int(val) + 1, ver):
                break
            retries += 1                           # someone else won: re-read and retry
    retries_q.put(retries)


def run(n_procs=4, per=25):
    """Returns (ok, report_lines)."""
    ctx = mp.get_context("spawn")
    total = n_procs * per
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "os3_s4.db")
        st = Storage(path, SCHEMA)
        ensure_table(st)
        set_state(st, "naive", 0)
        set_state(st, "cas", 0)
        _, v0 = get_state(st, "cas")

        ps = [ctx.Process(target=_naive_worker, args=(path, "naive", per)) for _ in range(n_procs)]
        [p.start() for p in ps]
        [p.join() for p in ps]
        naive = int(get_state(st, "naive")[0])

        q = ctx.Queue()
        ps = [ctx.Process(target=_cas_worker, args=(path, "cas", per, q)) for _ in range(n_procs)]
        [p.start() for p in ps]
        retries = sum(q.get(timeout=60) for _ in range(n_procs))
        [p.join() for p in ps]
        val, ver = get_state(st, "cas")

        # a stale writer is refused: it still holds the version it read before
        stale_refused = not cas_state(st, "cas", "999", ver - 1)
        unchanged = get_state(st, "cas")[0] == val

    ok = (naive < total and int(val) == total and ver == v0 + total
          and stale_refused and unchanged)
    return ok, [
        f"{n_procs} processes x {per} increments = {total} expected",
        f"blind read-modify-write ended at {naive} (lost {total - naive} updates)",
        f"version-guarded ended at {val}, version {ver}, after {retries} retries",
        f"stale writer (old version) refused and state unchanged: {stale_refused and unchanged}",
    ]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS3-4 Transactional state update & version guard")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
