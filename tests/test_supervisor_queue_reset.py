"""SIGKILL the Processor while it waits on the queue: generation must keep going.

A multiprocessing.Queue reader holds the queue's internal read lock while it waits for data.
A reader killed in that wait never releases it, so the next reader cannot take items and a
producer that drops the oldest item when the queue is full spins forever. Found in the 600 s
bundle run (events stopped ~150 s after the kill, once the queue had filled up).

The supervisor now treats the queue as poisoned when either end dies abnormally: it builds a
new queue and restarts both ends on it (the old queue's contents are in the durable ingress
table, so nothing is lost). BoundedQueue.put() also gives up after a bounded time instead of
spinning.
"""
import json
import os
import signal
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
sys.path.insert(0, os.path.join(ROOT, "tests"))
from platform_skip import linux_only     # noqa: E402
from edge.storage import Storage         # noqa: E402


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def status(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status", timeout=5) as r:
        return json.loads(r.read())


@linux_only("SIGKILL of a supervised worker")
class TestQueueResetAfterKill(unittest.TestCase):
    def test_generation_continues_after_processor_sigkill(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            with open(os.path.join(ROOT, "config", "os2.yaml"), encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            port = free_port()
            cfg["network"]["api_port"] = port
            cfg["upstream"]["port"] = free_port()
            cfg["ipc"]["queue_maxsize"] = 20             # fills in ~3 s, so a hang shows quickly
            cfg["sensor"]["interval_s"] = 0.1
            cfg["supervisor"]["restart_delay_s"] = 0.2
            cfg["paths"].update({"db": os.path.join(tmp, "n.db"), "pidfile": os.path.join(tmp, "pids.json"),
                                 "backups": os.path.join(tmp, "bk"), "socket": os.path.join(tmp, "s.sock"),
                                 "results": os.path.join(tmp, "runs"),
                                 "schema": os.path.join(ROOT, "sql", "schema.sql"),
                                 "static": os.path.join(ROOT, "src", "ui", "static")})
            cfg_path = os.path.join(tmp, "node.yaml")
            with open(cfg_path, "w", encoding="utf-8") as f:
                yaml.safe_dump(cfg, f)
            proc = subprocess.Popen([sys.executable, "-m", "edge.supervisor", "--config", cfg_path],
                                    env=dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src")),
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                for _ in range(80):
                    try:
                        status(port)
                        break
                    except OSError:
                        time.sleep(0.25)
                time.sleep(2)
                for kill in range(3):                    # kill it while it waits on the queue
                    with open(cfg["paths"]["pidfile"], encoding="utf-8") as f:
                        pid = json.load(f)["Processor"]["pid"]
                    os.kill(pid, signal.SIGKILL)
                    time.sleep(2.5)
                before = status(port)["counts"]["events"]
                time.sleep(6)                            # queue of 20 at 20/s would be full 5x over
                after = status(port)
                self.assertGreater(after["counts"]["events"], before + 40,
                                   "generation stopped after the Processor was SIGKILLed")
                self.assertGreaterEqual(after["recovery"]["admitted"], after["counts"]["events"])
                with open(cfg["paths"]["pidfile"], encoding="utf-8") as f:
                    pids = json.load(f)
                self.assertGreaterEqual(pids["Processor"]["restarts"], 3)
                self.assertGreaterEqual(pids["Supervisor"]["queue_resets"], 3)
                health = {p["name"]: p for p in after["processes"]}
                self.assertGreaterEqual(health["Processor"]["restarts"], 3)   # HealthMon still alive
            finally:
                proc.send_signal(signal.SIGTERM)
                proc.wait(30)
            st = Storage(cfg["paths"]["db"], cfg["paths"]["schema"], init=False)
            rep = st.recovery_report()                   # clean stop: everything admitted is committed
            st.close()
            self.assertEqual(rep["unprocessed"], 0, rep)
            self.assertTrue(rep["ok"], rep)


if __name__ == "__main__":
    unittest.main()
