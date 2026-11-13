"""Zero-dependency local HTTP API + web console (http.server only).

Node:       GET  /api/status, GET /api/reconcile, POST /api/wan/cut, POST /api/wan/restore
Scenarios:  GET  /api/scenarios                       registry (25 official tasks + profiles)
            POST /api/runs        {"scenario_id", "profile"}           start one run
            POST /api/batches     {"theme" | "scenario_ids", "profile"} run several, one by one
            GET  /api/runs, /api/runs/<id>, /api/runs/<id>/log?start=N, /api/runs/<id>/evidence
            POST /api/runs/<id>/cancel, GET /api/batches/<id>, POST /api/batches/<id>/cancel
A request only ever names a scenario ID and a profile ID from the registry: no module path or
command text from the browser is used.
"""
import json
import logging
import os
import re
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from edge import registry as reg
from edge.runner import RunManager

from edge.storage import Storage
from edge.upstream import fetch_stats, reconcile, upstream_settings


def node_status(cfg, storage):
    """Live node status. Delivery numbers come from the independent receiver when there is one."""
    up = upstream_settings(cfg)
    counts = storage.counts()
    pending = storage.outbox_pending()
    receiver = {"url": up[3] if up else None, "reachable": None}
    if up:
        try:
            st = fetch_stats(up[3], timeout=0.5)
            receiver.update(reachable=True, **st)
            counts["sink"] = st["received"]
        except OSError:
            receiver["reachable"] = False
        delivered = counts["sink"] if receiver["reachable"] else None
        # count form of the ID check in /api/reconcile: the receiver holds no more than the node
        # committed, and everything committed is either at the receiver or still pending
        conserved = (delivered is not None and delivered <= counts["events"]
                     and counts["events"] - pending <= delivered)
        conservation = {"events": counts["events"], "sink": delivered, "pending": pending,
                        "conserved": conserved}
    else:
        conservation = storage.conservation_report()
    try:
        queue = json.loads(storage.kv_get("queue_stats", "{}"))
        sync = json.loads(storage.kv_get("sync_stats", "{}"))
    except ValueError:
        queue, sync = {}, {}
    return {"node": cfg["node"]["id"], "ts": time.time(), "wan_online": storage.wan_online(),
            "outbox_pending": pending, "counts": counts, "conservation": conservation,
            "receiver": receiver, "queue": queue, "sync": sync,
            "actuators": storage.actuator_states(), "recovery": storage.recovery_report(),
            "processes": storage.list_health()}


_RUN = re.compile(r"^/api/runs/([A-Za-z0-9-]{1,64})(/log|/evidence|/cancel)?$")
_BATCH = re.compile(r"^/api/batches/([A-Za-z0-9-]{1,64})(/cancel)?$")
MAX_BODY = 16 * 1024


def make_handler(cfg, storage, runs=None):
    index_path = os.path.join(cfg["paths"]["static"], "index.html")
    up = upstream_settings(cfg)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):   # keep the console quiet
            pass

        def _send(self, code, body, ctype="application/json", headers=None):
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)

        def _err(self, code, msg):
            self._send(code, {"error": msg})

        def _json_body(self):
            n = int(self.headers.get("Content-Length", "0") or 0)
            if n > MAX_BODY:
                raise ValueError("request body too large")
            raw = self.rfile.read(n) if n else b"{}"
            body = json.loads(raw or b"{}")
            if not isinstance(body, dict):
                raise ValueError("JSON object expected")
            return body

        def do_GET(self):
            url = urlparse(self.path)
            path, qs = url.path, parse_qs(url.query)
            if path == "/api/status":
                return self._send(200, node_status(cfg, storage))
            if path == "/api/reconcile":
                return self._send(200, reconcile(storage, up[3] if up else None))
            if path in ("/", "/index.html"):
                with open(index_path, "rb") as f:
                    return self._send(200, f.read(), "text/html; charset=utf-8")
            if runs is None:
                return self._err(404, "not found")
            if path == "/api/scenarios":
                return self._send(200, {"themes": {k: {"title": v[0], "site": v[2], "members": v[3]}
                                                   for k, v in reg.THEMES.items()},
                                        "scenarios": [s.to_dict() for s in runs.registry.values()]})
            if path == "/api/runs":
                return self._send(200, {"runs": [r.summary() for r in reversed(runs.runs.values())],
                                        "batches": list(reversed(runs.batches.values()))})
            m = _RUN.match(path)
            if m:
                rid, sub = m.groups()
                try:
                    run = runs.get(rid)
                except KeyError:
                    return self._err(404, "Unknown run ID")
                if sub == "/log":
                    try:
                        start = int(qs.get("start", ["0"])[0])
                        limit = int(qs.get("limit", ["1000"])[0])
                    except ValueError:
                        return self._err(400, "start/limit must be integers")
                    return self._send(200, runs.log(rid, start, limit))
                if sub == "/evidence":
                    return self._send(200, json.dumps(runs.evidence(run), indent=2).encode(), headers={
                        "Content-Disposition": f'attachment; filename="evidence_{run.scenario.id}_{rid}.json"'})
                if sub is None:
                    d = run.summary()
                    d["telemetry_series"] = list(run.telemetry)[-300:]
                    d["checks"] = runs.evidence(run)["checks"] if run.state in ("passed", "failed", "timeout",
                                                                                "cancelled", "error") else None
                    return self._send(200, d)
            m = _BATCH.match(path)
            if m and m.group(2) is None:
                b = runs.batches.get(m.group(1))
                if b is None:
                    return self._err(404, "Unknown batch ID")
                d = dict(b)
                d["runs"] = [runs.runs[r].summary() for r in b["run_ids"] if r in runs.runs]
                return self._send(200, d)
            return self._err(404, "not found")

        def do_POST(self):
            path = urlparse(self.path).path
            if path == "/api/wan/cut":
                storage.set_wan_online(False)
                return self._send(200, {"wan_online": storage.wan_online()})
            if path == "/api/wan/restore":
                storage.set_wan_online(True)
                return self._send(200, {"wan_online": storage.wan_online()})
            if runs is None:
                return self._err(404, "not found")
            try:
                body = self._json_body()
            except ValueError as e:
                return self._err(400, str(e))
            try:
                if path == "/api/runs":
                    run = runs.start(body.get("scenario_id"), body.get("profile", "standard"))
                    return self._send(201, run.summary())
                if path == "/api/batches":
                    if "scenario_ids" in body:
                        ids = body["scenario_ids"]
                        if not isinstance(ids, list) or not ids or len(ids) > 50:
                            return self._err(400, "scenario_ids must be a non-empty list")
                    else:
                        theme = body.get("theme")
                        if theme is not None and theme not in reg.THEMES:
                            return self._err(400, "Unknown theme")
                        ids = [s.id for s in reg.select(theme)]
                    b = runs.start_batch(ids, body.get("profile", "standard"))
                    return self._send(201, b)
            except reg.RegistryError as e:
                return self._err(400, str(e))
            except RuntimeError as e:
                return self._err(409, str(e))
            m = _RUN.match(path)
            if m and m.group(2) == "/cancel":
                try:
                    return self._send(200, runs.cancel(m.group(1)).summary())
                except KeyError:
                    return self._err(404, "Unknown run ID")
            m = _BATCH.match(path)
            if m and m.group(2) == "/cancel":
                b = runs.batches.get(m.group(1))
                if b is None:
                    return self._err(404, "Unknown batch ID")
                b["cancelled"] = True
                for rid in b["run_ids"]:
                    runs.cancel(rid)
                return self._send(200, {"batch_id": b["batch_id"], "cancelled": True})
            return self._err(404, "not found")

    return Handler


def api_main(cfg, stop):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [API] %(message)s")
    storage = Storage(cfg["paths"]["db"], cfg["paths"]["schema"], init=False)
    root = cfg.get("_root") or os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    results = cfg["paths"].get("results") or os.path.join(root, "results", "runs")
    runs = RunManager(root, results_dir=results)
    server = ThreadingHTTPServer((cfg["network"]["api_host"], cfg["network"]["api_port"]),
                                 make_handler(cfg, storage, runs))
    t = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.3}, daemon=True)
    t.start()
    logging.info("dashboard on http://%s:%s", cfg["network"]["api_host"], cfg["network"]["api_port"])
    stop.wait()
    runs.cancel_all()                  # no scenario process outlives the console
    server.shutdown()
    server.server_close()
