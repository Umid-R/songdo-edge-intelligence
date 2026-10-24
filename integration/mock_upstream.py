"""Mock upstream consumer (course README: integration/ = "Mock upstream consumers").

Stands in for the Urban Data Platform the edge node uploads to. It is a separate process
with its own SQLite store, reached over HTTP, so a WAN cut can be simulated for real by
stopping it (connection refused) and restoring it by starting it again.

    python3 integration/mock_upstream.py --port 9100 --db data/upstream_mock.db   # run it

Receiver contract:
    POST /ingest   body {"events": [{"event_id": "...", "payload": "<json>"}, ...]}
                   -> {"accepted": n_new, "duplicates": n_already_seen}
    GET  /stats    -> {"received": distinct events stored, "duplicates": resends ignored}

Idempotent: event_id is the PRIMARY KEY, so a batch that is sent twice (for example the
node crashed after the POST but before marking the rows sent) is stored only once.

drain_outbox_http(storage, url) is the node-side half: it sends pending outbox rows and marks
them sent only after the receiver answered 200 (at-least-once delivery + receiver dedup =
exactly-once storage). The built-in SyncWorker still drains to the local upstream_sink table;
this module demonstrates the same contract across a real process and network boundary.
"""
import argparse
import json
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SCHEMA = """
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
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        with self._conn() as c:
            c.executescript(SCHEMA)

    def _conn(self):
        c = sqlite3.connect(self.db_path, timeout=10)
        c.execute("PRAGMA journal_mode=WAL")
        return c

    def ingest(self, events):
        with self.lock, self._conn() as c:
            new = 0
            for e in events:
                cur = c.execute("INSERT OR IGNORE INTO received (event_id, payload, received_at) VALUES (?,?,?)",
                                (e["event_id"], e["payload"], time.time()))
                new += cur.rowcount
            dup = len(events) - new
            c.execute("UPDATE stats SET v = v + ? WHERE k = 'duplicates'", (dup,))
            return {"accepted": new, "duplicates": dup}

    def stats(self):
        with self._conn() as c:
            received = c.execute("SELECT COUNT(*) FROM received").fetchone()[0]
            dup = c.execute("SELECT v FROM stats WHERE k = 'duplicates'").fetchone()[0]
            return {"received": received, "duplicates": dup}


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

        def do_GET(self):
            if self.path == "/stats":
                self._send(200, receiver.stats())
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self):
            if self.path != "/ingest":
                self._send(404, {"error": "not found"})
                return
            try:
                n = int(self.headers.get("Content-Length", "0"))
                events = json.loads(self.rfile.read(n))["events"]
            except (ValueError, KeyError):
                self._send(400, {"error": "bad request"})
                return
            self._send(200, receiver.ingest(events))

    return ThreadingHTTPServer((host, port), Handler)


def serve(host="127.0.0.1", port=9100, db_path="data/upstream_mock.db"):
    srv = make_server(host, port, db_path)
    try:
        srv.serve_forever()
    finally:
        srv.server_close()


def drain_outbox_http(storage, url, batch_size=50, timeout=2.0):
    """Send one batch of pending outbox rows to the upstream; mark them sent only on HTTP 200.

    Returns rows delivered (0 when nothing is pending or the upstream is unreachable)."""
    conn = storage._connect()
    try:
        rows = conn.execute("SELECT id, event_id, payload FROM outbox WHERE sent_at IS NULL "
                            "ORDER BY id LIMIT ?", (batch_size,)).fetchall()
    finally:
        conn.close()
    if not rows:
        return 0
    body = json.dumps({"events": [{"event_id": r["event_id"], "payload": r["payload"]} for r in rows]}).encode()
    req = urllib.request.Request(url.rstrip("/") + "/ingest", data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return 0
    except (urllib.error.URLError, ConnectionError, TimeoutError, OSError):
        return 0                                   # WAN down: rows stay pending, nothing is lost
    now = time.time()
    with storage.tx() as c:                        # acknowledged: mark the whole batch sent
        c.executemany("UPDATE outbox SET sent_at = ? WHERE id = ?", [(now, r["id"]) for r in rows])
    return len(rows)


def fetch_stats(url, timeout=2.0):
    with urllib.request.urlopen(url.rstrip("/") + "/stats", timeout=timeout) as resp:
        return json.loads(resp.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9100)
    ap.add_argument("--db", default="data/upstream_mock.db")
    args = ap.parse_args()
    print(f"mock upstream on http://{args.host}:{args.port}  (POST /ingest, GET /stats), store {args.db}")
    serve(args.host, args.port, args.db)


if __name__ == "__main__":
    main()
