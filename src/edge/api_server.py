"""Zero-dependency local HTTP API + dashboard (http.server only)."""
import json
import logging
import os
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from edge.storage import Storage


def make_handler(cfg, storage):
    index_path = os.path.join(cfg["paths"]["static"], "index.html")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):   # keep the console quiet
            pass

        def _send(self, code, body, ctype="application/json"):
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            if self.path == "/api/status":
                self._send(200, {
                    "node": cfg["node"]["id"], "ts": time.time(),
                    "wan_online": storage.wan_online(),
                    "outbox_pending": storage.outbox_pending(),
                    "counts": storage.counts(),
                    "conservation": storage.conservation_report(),
                    "processes": storage.list_health()})
            elif self.path in ("/", "/index.html"):
                with open(index_path, "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self):
            if self.path == "/api/wan/cut":
                storage.set_wan_online(False)
            elif self.path == "/api/wan/restore":
                storage.set_wan_online(True)
            else:
                return self._send(404, {"error": "not found"})
            self._send(200, {"wan_online": storage.wan_online()})

    return Handler


def api_main(cfg, stop):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [API] %(message)s")
    storage = Storage(cfg["paths"]["db"], cfg["paths"]["schema"], init=False)
    server = ThreadingHTTPServer((cfg["network"]["api_host"], cfg["network"]["api_port"]),
                                 make_handler(cfg, storage))
    t = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.3}, daemon=True)
    t.start()
    logging.info("dashboard on http://%s:%s", cfg["network"]["api_host"], cfg["network"]["api_port"])
    stop.wait()
    server.shutdown()
    server.server_close()
