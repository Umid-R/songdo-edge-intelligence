"""OS4-4: Scheduled Database Backup via systemd Timer / Cron

Mission : Automate online database backup with flock overlap prevention.
Concept : SQLite ONLINE backup API: a consistent snapshot while writers keep writing (WAL mode), then
          PRAGMA integrity_check on the copy, then retention (keep the newest N). flock -n makes a second
          run that overlaps the first one exit immediately instead of piling up ("zero corrupt locks").

Owner   : Nazarbek
Test    : tests/os4/test_s4_scheduled_backup.py
Run     : PYTHONPATH=src python -m edge.teams.os4_iot.s4_scheduled_backup        (the scenario)
          bash scripts/backup.sh config/os4.yaml                                  (one real backup)

Pieces: this module (backup + verify + retention), scripts/backup.sh (flock), and the schedule:
systemd/songdo-backup@.service + songdo-backup@.timer (OnCalendar every 15 min, Persistent=true), or the
cron line inside the timer file. The units are TEMPLATES: they were checked as text, NOT run on systemd.

Bug fixed on the way: the old scripts/backup.sh used /tmp/songdo_backup.lock inside the script, the same
file as the handbook's outer `flock -n /tmp/songdo_backup.lock -c ./scripts/backup.sh`, so under that
documented usage the inner lock always failed and the backup never ran. The script now uses its own lock
file; the scenario proves both usages work.
"""
import argparse
import multiprocessing as mp
import os
import sqlite3
import subprocess
import sys
import tempfile
import time

import yaml

from edge.config import load_config
from edge.models import SensorEvent
from edge.storage import Storage

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
HANDBOOK_LOCK = "/tmp/songdo_backup.lock"


def backup_once(db_path, schema_path, dest_dir, keep=5):
    """Online backup + integrity check + retention. Returns a result dict."""
    st = Storage(db_path, schema_path, init=False)
    os.makedirs(dest_dir, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S") + f"_{int(time.time() * 1000) % 1000:03d}"
    dest = os.path.join(dest_dir, f"songdo_edge_{stamp}.db")
    ok = st.backup(dest)
    con = sqlite3.connect(dest)
    try:
        rows = con.execute("SELECT COUNT(*) FROM sensor_events").fetchone()[0]
        outbox = con.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
    finally:
        con.close()
    existing = sorted(f for f in os.listdir(dest_dir) if f.startswith("songdo_edge_") and f.endswith(".db"))
    removed = existing[:-keep] if keep > 0 else []
    for f in removed:
        os.remove(os.path.join(dest_dir, f))
    return {"path": dest, "ok": ok, "rows": rows, "outbox": outbox,
            "kept": len(existing) - len(removed), "removed": removed}


def _writer(db_path, schema_path, stop, written):
    st = Storage(db_path, schema_path, init=False)
    while not stop.value:
        st.insert_event_with_outbox(SensorEvent(node_id="N", sensor="PM25_UG_M3", value=30.0, unit="ug_m3"))
        written.value += 1


def _unit_has(path, *needles):
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
    except OSError:
        return False
    return all(n in text for n in needles)


def run():
    """Returns (ok, report_lines)."""
    schema = os.path.join(ROOT, "sql", "schema.sql")
    script = os.path.join(ROOT, "scripts", "backup.sh")
    env = dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src"))
    checks = {}
    with tempfile.TemporaryDirectory() as tmp:
        db, bdir, sdir = (os.path.join(tmp, n) for n in ("os4_backup.db", "backups", "script_backups"))
        st = Storage(db, schema)
        for _ in range(200):
            st.insert_event_with_outbox(SensorEvent(node_id="N", sensor="PM25_UG_M3", value=30.0, unit="ug_m3"))

        # 1) online backups while a writer process keeps inserting
        ctx = mp.get_context("spawn")
        stop, written = ctx.RawValue("i", 0), ctx.RawValue("i", 0)
        w = ctx.Process(target=_writer, args=(db, schema, stop, written))
        w.start()
        while written.value < 50:
            time.sleep(0.02)
        snaps = []
        for _ in range(3):
            time.sleep(0.15)
            snaps.append(backup_once(db, schema, bdir, keep=10))
        stop.value = 1
        w.join(10)
        live = st.counts()["events"]
        checks[f"3 online backups during {written.value} concurrent writes: all passed integrity_check"] = \
            all(s["ok"] for s in snaps)
        checks["each backup is a consistent snapshot (events == outbox rows inside it)"] = \
            all(s["rows"] == s["outbox"] for s in snaps)
        checks[f"snapshots grow with the live data ({[s['rows'] for s in snaps]} <= {live} live)"] = \
            all(200 <= s["rows"] <= live for s in snaps) and snaps[0]["rows"] <= snaps[-1]["rows"]

        # 2) retention
        rdir = os.path.join(tmp, "retention")
        res = [backup_once(db, schema, rdir, keep=5) for _ in range(7)]
        left = sorted(os.listdir(rdir))
        checks["retention: 7 backups with keep=5 leaves the newest 5"] = \
            len(left) == 5 and os.path.basename(res[-1]["path"]) in left

        # 3) real script, overlapping runs: B starts while A still holds the lock
        cfg_dir = os.path.join(tmp, "config")
        os.makedirs(cfg_dir)
        cfg_path = os.path.join(cfg_dir, f"os4_scenario_{os.getpid()}.yaml")
        with open(cfg_path, "w", encoding="utf-8") as f:
            yaml.safe_dump({"paths": {"db": db, "backups": sdir, "schema": schema}}, f)
        a = subprocess.Popen(["bash", script, cfg_path], env=dict(env, BACKUP_TEST_HOLD_S="1.5"),
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        time.sleep(0.6)
        b = subprocess.run(["bash", script, cfg_path], env=env, capture_output=True, text=True, timeout=30)
        a_out = a.communicate(timeout=30)[0]
        n_after_overlap = len(os.listdir(sdir)) if os.path.isdir(sdir) else 0
        checks["overlap: run B while A holds the lock -> B skips ('already running'), exit code 0"] = \
            "already running, skipping" in b.stdout and b.returncode == 0
        checks["...and A still completed its backup (exactly 1 file, OK)"] = \
            a.returncode == 0 and " OK " in a_out and n_after_overlap == 1

        # 4) handbook-style OUTER flock with the handbook lock file must still produce a backup
        c = subprocess.run(["flock", "-n", HANDBOOK_LOCK, "bash", script, cfg_path], env=env,
                           capture_output=True, text=True, timeout=30)
        checks["handbook usage `flock -n /tmp/songdo_backup.lock scripts/backup.sh` really backs up"] = \
            c.returncode == 0 and " OK " in c.stdout and len(os.listdir(sdir)) == 2

    svc = os.path.join(ROOT, "systemd", "songdo-backup@.service")
    tmr = os.path.join(ROOT, "systemd", "songdo-backup@.timer")
    checks["systemd service unit (text check): oneshot + outer flock -n + scripts/backup.sh"] = \
        _unit_has(svc, "Type=oneshot", "/usr/bin/flock -n", "scripts/backup.sh")
    checks["systemd timer unit (text check): OnCalendar + Persistent=true + timers.target"] = \
        _unit_has(tmr, "OnCalendar=", "Persistent=true", "timers.target")
    return all(checks.values()), [f"{'ok ' if v else 'FAILED '}{k}" for k, v in checks.items()] + [
        "note: the systemd units were verified as text only, not executed by a real systemd"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="do ONE backup of the database in --config and exit")
    ap.add_argument("--config", default=os.path.join(ROOT, "config", "os4.yaml"))
    ap.add_argument("--keep", type=int, default=5)
    args = ap.parse_args()
    if args.once:
        cfg = load_config(args.config)
        r = backup_once(cfg["paths"]["db"], cfg["paths"]["schema"], cfg["paths"]["backups"], args.keep)
        print(f"backup {r['path']} {'OK' if r['ok'] else 'INTEGRITY FAILED'} rows={r['rows']} kept={r['kept']}")
        sys.exit(0 if r["ok"] else 1)
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS4-4 Scheduled database backup (flock + online backup API)")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
