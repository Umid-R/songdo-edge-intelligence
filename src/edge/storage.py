"""SQLite storage: WAL mode, atomic transactions, durable outbox, online backup."""
import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager


class Storage:
    def __init__(self, db_path, schema_path, init=True):
        self.db_path = db_path
        self.schema_path = schema_path
        self._local = threading.local()          # one cached connection per thread (see _conn)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        if init:
            self.init_schema()

    # ---- connection helpers ----
    # _connect() opens a NEW connection (the caller closes it). tx() and _read() reuse one cached
    # connection per thread and per process instead: opening and closing a connection for every
    # write costs about 2 ms and, when the last connection to a WAL database closes, SQLite runs
    # a checkpoint inside close(), which stalled the 50 ms control loops by up to ~70 ms per write.
    # Cached per thread (sqlite3 connections must not cross threads) and per PID (a forked child
    # must never reuse its parent's connection).
    def _conn(self):
        loc = self._local
        if getattr(loc, "pid", None) != os.getpid() or getattr(loc, "conn", None) is None:
            loc.conn = self._connect()
            loc.pid = os.getpid()
        return loc.conn

    def __getstate__(self):                      # picklable for multiprocessing: never ship a connection
        state = self.__dict__.copy()
        state.pop("_local", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._local = threading.local()

    def close(self):
        """Close this thread's cached connection (optional; it also closes when the object is freed)."""
        conn = getattr(self._local, "conn", None)
        if conn is not None and getattr(self._local, "pid", None) == os.getpid():
            conn.close()
        self._local.conn = None

    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=5.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    @contextmanager
    def tx(self):
        """All-or-nothing transaction."""
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    def _read(self, sql, params=()):
        return [dict(r) for r in self._conn().execute(sql, params).fetchall()]

    def init_schema(self):
        with open(self.schema_path, encoding="utf-8") as f:
            script = f.read()
        conn = self._connect()
        try:
            conn.executescript(script)
            self._migrate(conn)
        finally:
            conn.close()

    # Columns added after the midterm. CREATE TABLE IF NOT EXISTS does not touch an existing
    # table, so a database created by the old schema gets them here.
    _DECISION_COLUMNS = {"rule_version": "TEXT", "boot_epoch": "TEXT", "seq": "INTEGER",
                         "reason": "TEXT", "applied_at": "REAL"}

    def _migrate(self, conn):
        have = {r[1] for r in conn.execute("PRAGMA table_info(edge_decisions)")}
        for col, typ in self._DECISION_COLUMNS.items():
            if col not in have:
                conn.execute(f"ALTER TABLE edge_decisions ADD COLUMN {col} {typ}")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_status ON edge_decisions(status, seq)")

    def journal_mode(self):
        return self._read("PRAGMA journal_mode")[0]["journal_mode"]

    # ---- events + outbox ----
    def insert_event_with_outbox(self, event):
        """Store the event AND enqueue it in the outbox atomically.
        Returns False if the event_id was already stored (duplicate ignored)."""
        with self.tx() as c:
            cur = c.execute(
                "INSERT OR IGNORE INTO sensor_events "
                "(event_id,node_id,sensor,value,unit,observed_at,received_at) VALUES (?,?,?,?,?,?,?)",
                (event.event_id, event.node_id, event.sensor, event.value,
                 event.unit, event.observed_at, event.received_at))
            if cur.rowcount == 0:
                return False
            c.execute("INSERT INTO outbox (event_id,payload,enqueued_at) VALUES (?,?,?)",
                      (event.event_id, json.dumps(event.to_dict()), time.time()))
            return True

    def outbox_pending(self):
        return self._read("SELECT COUNT(*) AS n FROM outbox WHERE sent_at IS NULL")[0]["n"]

    def drain_outbox_to_sink(self, batch_size=50):
        """Deliver one batch to the (mock) upstream and mark it sent, in ONE transaction.
        The sink's PRIMARY KEY makes redelivery idempotent. Returns rows delivered."""
        with self.tx() as c:
            rows = c.execute("SELECT id,event_id,payload FROM outbox "
                             "WHERE sent_at IS NULL ORDER BY id LIMIT ?", (batch_size,)).fetchall()
            now = time.time()
            for r in rows:
                c.execute("INSERT OR IGNORE INTO upstream_sink (event_id,payload,received_at) VALUES (?,?,?)",
                          (r["event_id"], r["payload"], now))
                c.execute("UPDATE outbox SET sent_at=? WHERE id=?", (now, r["id"]))
            return len(rows)

    def conservation_report(self):
        """Event conservation: every stored event is either in the sink or still pending."""
        q = lambda sql: self._read(sql)[0]["n"]
        events = q("SELECT COUNT(*) AS n FROM sensor_events")
        sink = q("SELECT COUNT(*) AS n FROM upstream_sink")
        pending = q("SELECT COUNT(*) AS n FROM outbox WHERE sent_at IS NULL")
        return {"events": events, "sink": sink, "pending": pending,
                "conserved": events == sink + pending}

    # ---- durable admission -> one-transaction commit -> replayable command intents ----
    # The shared node's Processor uses these. The crash windows of the midterm version
    # (dequeue -> commit -> decision -> dispatch, each a separate step) are closed like this:
    #   1. admit():        SensorGen writes the reading to `ingress` before it is queued.
    #   2. commit_event(): event + outbox row + every command intent + "ingress processed"
    #                      in ONE transaction (all or nothing).
    #   3. apply_intent(): TTL / boot-epoch / sequence check, then decision status + actuator
    #                      state + receipt in ONE transaction.
    # After a SIGKILL anywhere, the restarted Processor replays unprocessed ingress rows and
    # PENDING intents; deterministic decision IDs and the receipt table make both idempotent.
    def admit(self, event):
        """Durably accept a reading. Returns False if this event_id was already admitted."""
        with self.tx() as c:
            cur = c.execute("INSERT OR IGNORE INTO ingress (event_id,payload,admitted_at) VALUES (?,?,?)",
                            (event.event_id, json.dumps(event.to_dict()), time.time()))
            return cur.rowcount == 1

    def unprocessed_ingress(self, older_than_s=0.0, limit=500):
        """Admitted readings that were never committed (lost from the queue or by a crash)."""
        cutoff = time.time() - older_than_s
        return self._read("SELECT event_id,payload FROM ingress WHERE processed_at IS NULL "
                          "AND admitted_at <= ? ORDER BY admitted_at LIMIT ?", (cutoff, limit))

    def commit_event(self, event, decisions=()):
        """ONE transaction: event, outbox row, command intents (PENDING, with a sequence number)
        and the ingress row marked processed. Safe to call again for the same event.
        Returns True if the event was new."""
        with self.tx() as c:
            cur = c.execute(
                "INSERT OR IGNORE INTO sensor_events "
                "(event_id,node_id,sensor,value,unit,observed_at,received_at) VALUES (?,?,?,?,?,?,?)",
                (event.event_id, event.node_id, event.sensor, event.value,
                 event.unit, event.observed_at, event.received_at))
            new = cur.rowcount == 1
            if new:
                c.execute("INSERT OR IGNORE INTO outbox (event_id,payload,enqueued_at) VALUES (?,?,?)",
                          (event.event_id, json.dumps(event.to_dict()), time.time()))
            seq = c.execute("SELECT COALESCE(MAX(seq),0) FROM edge_decisions").fetchone()[0]
            for d in decisions:
                cur = c.execute(
                    "INSERT OR IGNORE INTO edge_decisions (decision_id,event_id,rule,actuator,command_value,"
                    "issued_mono,expires_mono,status,created_at,rule_version,boot_epoch,seq) "
                    "VALUES (?,?,?,?,?,?,?,'PENDING',?,?,?,?)",
                    (d.decision_id, d.event_id, d.rule, d.actuator, d.command_value, d.issued_mono,
                     d.expires_mono, d.created_at, d.rule_version, d.boot_epoch, seq + 1))
                if cur.rowcount == 1:
                    seq += 1
                    d.seq = seq
            c.execute("UPDATE ingress SET processed_at=? WHERE event_id=? AND processed_at IS NULL",
                      (time.time(), event.event_id))
            return new

    def pending_intents(self, limit=500):
        """Committed commands not yet applied or rejected, in actuator sequence order."""
        return self._read("SELECT * FROM edge_decisions WHERE status='PENDING' AND seq IS NOT NULL "
                          "ORDER BY seq LIMIT ?", (limit,))

    def apply_intent(self, row, epoch, now_mono=None):
        """Apply one PENDING command to the simulated actuator, or reject it, in ONE transaction.

        Rejected as EXPIRED if it was issued in another boot epoch or its TTL has passed (monotonic
        clock), as SUPERSEDED if the actuator already applied a newer sequence number. Otherwise the
        actuator state, the receipt and status DISPATCHED are committed together.
        Returns (status, reason). Calling it again for a finished command changes nothing."""
        now = time.monotonic() if now_mono is None else now_mono
        with self.tx() as c:
            cur = c.execute("SELECT status, reason FROM edge_decisions WHERE decision_id=?",
                            (row["decision_id"],)).fetchone()
            if cur is None or cur["status"] != "PENDING":
                return (cur["status"], cur["reason"]) if cur else ("MISSING", None)
            last = c.execute("SELECT last_seq FROM actuator_state WHERE actuator=?",
                             (row["actuator"],)).fetchone()
            if row["boot_epoch"] != epoch:
                status, reason = "EXPIRED", "issued in another boot epoch"
            elif now >= row["expires_mono"]:
                status, reason = "EXPIRED", "ttl passed"
            elif last is not None and last["last_seq"] >= row["seq"]:
                status, reason = "SUPERSEDED", f"actuator already at seq {last['last_seq']}"
            else:
                status, reason = "DISPATCHED", None
                ts = time.time()
                c.execute("INSERT INTO actuator_state (actuator,value,last_seq,decision_id,boot_epoch,applied_at) "
                          "VALUES (?,?,?,?,?,?) ON CONFLICT(actuator) DO UPDATE SET value=excluded.value,"
                          "last_seq=excluded.last_seq,decision_id=excluded.decision_id,"
                          "boot_epoch=excluded.boot_epoch,applied_at=excluded.applied_at",
                          (row["actuator"], row["command_value"], row["seq"], row["decision_id"], epoch, ts))
                c.execute("INSERT INTO actuator_receipts (decision_id,actuator,value,seq,applied_at) "
                          "VALUES (?,?,?,?,?)",
                          (row["decision_id"], row["actuator"], row["command_value"], row["seq"], ts))
            c.execute("UPDATE edge_decisions SET status=?, reason=?, applied_at=? WHERE decision_id=?",
                      (status, reason, time.time(), row["decision_id"]))
            return status, reason

    def actuator_states(self):
        return self._read("SELECT * FROM actuator_state ORDER BY actuator")

    def recovery_report(self):
        """Exit rule of the assessment: every admitted event and every intent has exactly one
        durable disposition, and every applied command has exactly one receipt."""
        q = lambda sql: self._read(sql)[0]["n"]
        admitted = q("SELECT COUNT(*) AS n FROM ingress")
        unprocessed = q("SELECT COUNT(*) AS n FROM ingress WHERE processed_at IS NULL")
        lost = q("SELECT COUNT(*) AS n FROM ingress i WHERE processed_at IS NOT NULL AND NOT EXISTS "
                 "(SELECT 1 FROM sensor_events e WHERE e.event_id=i.event_id)")
        by_status = {r["status"]: r["n"] for r in self._read(
            "SELECT status, COUNT(*) AS n FROM edge_decisions WHERE seq IS NOT NULL GROUP BY status")}
        dispatched = by_status.get("DISPATCHED", 0)
        receipts = q("SELECT COUNT(*) AS n FROM actuator_receipts")
        orphan_receipts = q("SELECT COUNT(*) AS n FROM actuator_receipts r WHERE NOT EXISTS (SELECT 1 FROM "
                            "edge_decisions d WHERE d.decision_id=r.decision_id AND d.status='DISPATCHED')")
        dup_decisions = q("SELECT COUNT(*) AS n FROM (SELECT event_id, rule, rule_version FROM edge_decisions "
                          "WHERE seq IS NOT NULL GROUP BY 1,2,3 HAVING COUNT(*) > 1)")
        return {"admitted": admitted, "unprocessed": unprocessed, "admitted_but_lost": lost,
                "intents": by_status, "dispatched": dispatched, "receipts": receipts,
                "orphan_receipts": orphan_receipts, "duplicate_intents": dup_decisions,
                "ok": lost == 0 and receipts == dispatched and orphan_receipts == 0 and dup_decisions == 0}

    # ---- decisions (used directly by the scenario programs) ----
    def insert_decision(self, d):
        with self.tx() as c:
            c.execute(
                "INSERT INTO edge_decisions (decision_id,event_id,rule,actuator,command_value,"
                "issued_mono,expires_mono,status,created_at,rule_version,boot_epoch) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (d.decision_id, d.event_id, d.rule, d.actuator, d.command_value,
                 d.issued_mono, d.expires_mono, d.status, d.created_at,
                 getattr(d, "rule_version", None), getattr(d, "boot_epoch", None)))

    def set_decision_status(self, decision_id, status):
        with self.tx() as c:
            c.execute("UPDATE edge_decisions SET status=? WHERE decision_id=?", (status, decision_id))

    # ---- process health ----
    def upsert_health(self, h):
        with self.tx() as c:
            c.execute(
                "INSERT INTO process_health (name,pid,state,rss_kb,cpu_pct,ctx_voluntary,"
                "ctx_involuntary,restarts,updated_at) VALUES (?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(name) DO UPDATE SET pid=excluded.pid,state=excluded.state,"
                "rss_kb=excluded.rss_kb,cpu_pct=excluded.cpu_pct,ctx_voluntary=excluded.ctx_voluntary,"
                "ctx_involuntary=excluded.ctx_involuntary,restarts=excluded.restarts,"
                "updated_at=excluded.updated_at",
                (h.name, h.pid, h.state, h.rss_kb, h.cpu_pct, h.ctx_voluntary,
                 h.ctx_involuntary, h.restarts, h.updated_at))

    def list_health(self):
        return self._read("SELECT * FROM process_health ORDER BY name")

    # ---- key/value (WAN state lives here so every process sees it) ----
    def kv_get(self, key, default=None):
        rows = self._read("SELECT value FROM kv WHERE key=?", (key,))
        return rows[0]["value"] if rows else default

    def kv_set(self, key, value):
        with self.tx() as c:
            c.execute("INSERT INTO kv (key,value) VALUES (?,?) "
                      "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

    def wan_online(self):
        return self.kv_get("wan_online", "1") == "1"

    def set_wan_online(self, online):
        self.kv_set("wan_online", "1" if online else "0")

    # ---- misc ----
    def counts(self):
        q = lambda t: self._read(f"SELECT COUNT(*) AS n FROM {t}")[0]["n"]
        return {"events": q("sensor_events"), "decisions": q("edge_decisions"),
                "sink": q("upstream_sink"), "admitted": q("ingress"),
                "receipts": q("actuator_receipts")}

    def backup(self, dest_path):
        """Safe online backup (SQLite backup API) + integrity check. Returns True if ok."""
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        src = self._connect()
        dst = sqlite3.connect(dest_path)
        try:
            src.backup(dst)
            return dst.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        finally:
            dst.close()
            src.close()
