"""Independent upstream receiver (mock Urban Data Platform) and the node-side HTTP sender.

The midterm node "delivered" by copying outbox rows into an upstream_sink table of its OWN
database, in the same transaction that marked them sent. That can never lose a response, so it
could not show what happens when the network fails between "the receiver stored it" and "the node
heard the ACK". Now the receiver is a separate process with a separate SQLite file, reached over
HTTP, and the node only trusts per-event ACKs.

Receiver contract
    POST /ingest   {"events": [{"event_id": "...", "payload": "<json>"}, ...]}
                   The whole batch is committed (INSERT OR IGNORE, event_id PRIMARY KEY) BEFORE the
                   reply is sent. Reply: {"acked": [event_id, ...], "accepted": n_new,
                   "duplicates": n_already_stored}
    GET  /stats    {"received": distinct stored, "duplicates": resends ignored}
    GET  /ids      {"ids": [every stored event_id]}            (ID reconciliation)
    POST /fault    {"drop_ack": n, "bogus_ack": n}             (fault injection, tests / demo)
                   drop_ack: commit the next n batches, then close the connection without replying
                             (the response is lost after the remote commit).
                   bogus_ack: for the next n batches, ACK one unrelated ID and leave one real ID out.

Node side
    drain_outbox_http() offers one batch and marks a row sent only if its own event_id is in the
    ACK list. Unknown ACK IDs are ignored and counted; offered rows without an ACK stay pending and
    are offered again (at-least-once delivery + idempotent receiver = each event stored once).
"""
import json
import logging
import os
import signal
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

log = logging.getLogger("edge.upstream")

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS received (
  event_id    TEXT PRIMARY KEY,
  payload     TEXT NOT NULL,
  received_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS stats (k TEXT PRIMARY KEY, v INTEGER NOT NULL);
INSERT OR IGNORE INTO stats (k, v) VALUES ('duplicates', 0);
"""


class Receiver:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.faults = {"drop_ack": 0, "bogus_ack": 0}
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        c = self._conn()
        try:
            c.executescript(SCHEMA)
        finally:
            c.close()

    def _conn(self):
        c = sqlite3.connect(self.db_path, timeout=10)
        c.execute("PRAGMA busy_timeout=10000")
        return c

    def ingest(self, events):
        """Persist the batch in one transaction, then return the per-ID ACK (and the fault plan)."""
        with self.lock:
            c = self._conn()
            try:
                with c:                                        # commit happens here, before any reply
                    new = 0
                    for e in events:
                        cur = c.execute("INSERT OR IGNORE INTO received (event_id,payload,received_at) "
                                        "VALUES (?,?,?)", (e["event_id"], e["payload"], time.time()))
                        new += cur.rowcount
                    dup = len(events) - new
                    c.execute("UPDATE stats SET v = v + ? WHERE k = 'duplicates'", (dup,))
            finally:
                c.close()
            acked = [e["event_id"] for e in events]
            drop = False
            if self.faults["drop_ack"] > 0:
                self.faults["drop_ack"] -= 1
                drop = True
            elif self.faults["bogus_ack"] > 0 and acked:
                self.faults["bogus_ack"] -= 1
                acked = acked[:-1] + ["unrelated-" + acked[-1]]
            return {"acked": acked, "accepted": new, "duplicates": dup}, drop

    def stats(self):
        c = self._conn()
        try:
            received = c.execute("SELECT COUNT(*) FROM received").fetchone()[0]
            dup = c.execute("SELECT v FROM stats WHERE k = 'duplicates'").fetchone()[0]
            return {"received": received, "duplicates": dup}
        finally:
            c.close()

    def ids(self):
        c = self._conn()
        try:
            return [r[0] for r in c.execute("SELECT event_id FROM received")]
        finally:
            c.close()


def make_server(host, port, db_path):
    receiver = Receiver(db_path)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _send(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body(self):
            n = int(self.headers.get("Content-Length", "0"))
            return json.loads(self.rfile.read(n) or b"{}")

        def do_GET(self):
            if self.path == "/stats":
                self._send(200, receiver.stats())
            elif self.path == "/ids":
                self._send(200, {"ids": receiver.ids()})
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self):
            try:
                body = self._body()
            except ValueError:
                return self._send(400, {"error": "bad json"})
            if self.path == "/fault":
                for k in ("drop_ack", "bogus_ack"):
                    receiver.faults[k] = max(0, int(body.get(k, receiver.faults[k])))
                return self._send(200, dict(receiver.faults))
            if self.path != "/ingest":
                return self._send(404, {"error": "not found"})
            events = body.get("events")
            if not isinstance(events, list) or not all(
                    isinstance(e, dict) and "event_id" in e and "payload" in e for e in events):
                return self._send(400, {"error": "bad request"})
            reply, drop = receiver.ingest(events)
            if drop:                                   # stored, but the ACK never reaches the node
                self.close_connection = True
                try:
                    self.connection.shutdown(2)
                except OSError:
                    pass
                return
            self._send(200, reply)

    srv = ThreadingHTTPServer((host, port), Handler)
    srv.receiver = receiver
    return srv


def serve(host="127.0.0.1", port=9100, db_path="data/upstream_mock.db"):
    srv = make_server(host, port, db_path)
    try:
        srv.serve_forever()
    finally:
        srv.server_close()


def upstream_settings(cfg):
    """(host, port, db_path, url) of the node's receiver, or None if it has no `upstream:` section."""
    up = cfg.get("upstream")
    if not up or not up.get("enabled", True):
        return None
    db = cfg["paths"].get("upstream_db") or os.path.splitext(cfg["paths"]["db"])[0] + "_upstream.db"
    host, port = up.get("host", "127.0.0.1"), int(up["port"])
    return host, port, db, f"http://{host}:{port}"


def upstream_main(cfg, stop):
    """Supervisor worker: the receiver as its own process with its own database."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [Upstream] %(message)s")
    host, port, db, _ = upstream_settings(cfg)
    srv = make_server(host, port, db)
    t = threading.Thread(target=srv.serve_forever, kwargs={"poll_interval": 0.3}, daemon=True)
    t.start()
    log.info("receiver on http://%s:%s, store %s", host, port, db)
    stop.wait()
    srv.shutdown()
    srv.server_close()


# ---------------- node side ----------------

def _http(url, path, obj=None, timeout=2.0):
    data = None if obj is None else json.dumps(obj).encode()
    req = urllib.request.Request(url.rstrip("/") + path, data=data, method="GET" if obj is None else "POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def fetch_stats(url, timeout=2.0):
    return _http(url, "/stats", timeout=timeout)


def fetch_ids(url, timeout=10.0):
    return set(_http(url, "/ids", timeout=timeout)["ids"])


def set_fault(url, timeout=2.0, **faults):
    return _http(url, "/fault", faults, timeout=timeout)


def drain_outbox_http(storage, url, batch_size=50, timeout=2.0):
    """Offer one batch of pending outbox rows; mark sent ONLY the rows whose event_id was ACKed.

    Returns {"offered", "acked", "unrelated", "missing", "error"}. On any network error nothing is
    marked, so nothing can be lost; a lost ACK only causes a resend that the receiver deduplicates."""
    rows = storage._read("SELECT id, event_id, payload FROM outbox WHERE sent_at IS NULL "
                         "ORDER BY id LIMIT ?", (batch_size,))
    res = {"offered": len(rows), "acked": 0, "unrelated": 0, "missing": 0, "error": None}
    if not rows:
        return res
    try:
        reply = _http(url, "/ingest", {"events": [{"event_id": r["event_id"], "payload": r["payload"]}
                                                  for r in rows]}, timeout=timeout)
        acked = set(reply.get("acked", []))
    except (urllib.error.URLError, ConnectionError, TimeoutError, OSError, ValueError) as e:
        res["error"] = type(e).__name__
        _bump(storage, {"offered": len(rows), "failed_posts": 1})
        return res
    offered = {r["event_id"]: r["id"] for r in rows}
    ok = [offered[eid] for eid in offered if eid in acked]
    res["acked"] = len(ok)
    res["unrelated"] = len(acked - offered.keys())          # ACK for something we never offered
    res["missing"] = len(offered) - len(ok)                 # offered, not ACKed: stays pending
    if ok:
        now = time.time()
        with storage.tx() as c:
            c.executemany("UPDATE outbox SET sent_at=? WHERE id=? AND sent_at IS NULL", [(now, i) for i in ok])
    _bump(storage, {"offered": len(rows), "acked": res["acked"], "unrelated_acks": res["unrelated"],
                    "missing_acks": res["missing"], "receiver_duplicates": int(reply.get("duplicates", 0))})
    return res


def _bump(storage, deltas):
    """Running sync counters in kv (shown by the dashboard)."""
    with storage.tx() as c:
        row = c.execute("SELECT value FROM kv WHERE key='sync_stats'").fetchone()
        cur = json.loads(row["value"]) if row else {}
        for k, v in deltas.items():
            cur[k] = cur.get(k, 0) + v
        c.execute("INSERT INTO kv (key,value) VALUES ('sync_stats',?) "
                  "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (json.dumps(cur),))


def reconcile(storage, url=None, timeout=10.0):
    """Generated / committed / acknowledged ID sets (G, C, A) and their differences.

    G = admitted to ingress, C = committed sensor_events, A = marked sent on the node,
    R = stored at the receiver. After a full drain the no-drop profile requires
    G = C = A = R and zero pending. Queue drops are reported separately: a reading dropped from
    the full in-memory queue is still in ingress and is committed by the Processor's sweep."""
    ids = lambda sql: {r["event_id"] for r in storage._read(sql)}
    G = ids("SELECT event_id FROM ingress")
    C = ids("SELECT event_id FROM sensor_events")
    A = ids("SELECT event_id FROM outbox WHERE sent_at IS NOT NULL")
    P = ids("SELECT event_id FROM outbox WHERE sent_at IS NULL")
    rep = {"generated": len(G), "committed": len(C), "acknowledged": len(A), "pending": len(P),
           "admitted_not_committed": len(G - C), "committed_not_admitted": len(C - G)}
    if url:
        try:
            R = fetch_ids(url, timeout)
            stats = fetch_stats(url, timeout)
        except (urllib.error.URLError, OSError, ValueError) as e:
            rep.update(receiver_error=type(e).__name__)
            R, stats = None, {}
        if R is not None:
            rep.update(receiver=len(R), receiver_duplicates_ignored=stats.get("duplicates", 0),
                       acked_but_not_at_receiver=len(A - R),       # false "sent": must be 0
                       at_receiver_unknown_to_node=len(R - C),     # must be 0
                       committed_not_delivered=len(C - R),         # = still pending / in flight
                       pending_but_already_at_receiver=len(P & R))  # lost ACKs, resend will dedupe
            rep["conserved"] = (len(A - R) == 0 and len(R - C) == 0 and (C - R) <= P)
            rep["fully_drained"] = rep["conserved"] and G == C == A == R and not P
    else:
        rep["conserved"] = A | P == C
        rep["fully_drained"] = rep["conserved"] and G == C == A and not P
    try:
        rep["sync_stats"] = json.loads(storage.kv_get("sync_stats", "{}"))
        rep["queue"] = json.loads(storage.kv_get("queue_stats", "{}"))
    except ValueError:
        pass
    return rep
