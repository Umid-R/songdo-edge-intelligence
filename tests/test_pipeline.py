"""End-to-end smoke test: run the whole node, cut the WAN, restore, check conservation."""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from edge.storage import Storage   # noqa: E402


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def call(port, path, method="GET"):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method)
    with urllib.request.urlopen(req, timeout=3) as r:
        return json.loads(r.read())


class TestOfflineDemo(unittest.TestCase):
    def test_wan_cut_and_restore(self):
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(ROOT, "config", "node.yaml"), encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            port = free_port()
            cfg["network"]["api_port"] = port
            cfg["sensor"]["interval_s"] = 0.1
            cfg["paths"] = {
                "db": os.path.join(tmp, "t.db"), "backups": os.path.join(tmp, "bk"),
                "pidfile": os.path.join(tmp, "pids.json"),
                "schema": os.path.join(ROOT, "sql", "schema.sql"),
                "static": os.path.join(ROOT, "src", "edge", "ui"),
                "socket": os.path.join(tmp, "s.sock")}
            cfg_path = os.path.join(tmp, "config", "node.yaml")
            os.makedirs(os.path.dirname(cfg_path))
            with open(cfg_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(cfg, f)

            env = dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src"))
            proc = subprocess.Popen([sys.executable, "-m", "edge.supervisor",
                                     "--config", cfg_path, "--duration", "14"],
                                    env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                for _ in range(40):                       # wait for the API
                    try:
                        call(port, "/api/status")
                        break
                    except Exception:
                        time.sleep(0.25)
                time.sleep(2)
                call(port, "/api/wan/cut", "POST")
                time.sleep(4)
                offline = call(port, "/api/status")
                self.assertFalse(offline["wan_online"])
                self.assertGreater(offline["outbox_pending"], 5)      # backlog grows
                self.assertGreater(offline["counts"]["events"], 0)    # node keeps working
                call(port, "/api/wan/restore", "POST")
                time.sleep(3)
                back = call(port, "/api/status")
                self.assertTrue(back["wan_online"])
                self.assertLess(back["outbox_pending"], offline["outbox_pending"])
            finally:
                proc.wait(timeout=30)
            self.assertEqual(proc.returncode, 0)                      # clean exit
            report = Storage(cfg["paths"]["db"], cfg["paths"]["schema"], init=False).conservation_report()
            self.assertTrue(report["conserved"], report)              # zero lost events


if __name__ == "__main__":
    unittest.main()
