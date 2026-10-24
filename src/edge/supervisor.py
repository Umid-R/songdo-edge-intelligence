"""Multi-process supervisor: starts workers, reaps and restarts crashed ones,
shuts everything down cleanly on SIGTERM/SIGINT (no zombies).

Run:  PYTHONPATH=src python -m edge.supervisor [--config config/node.yaml] [--duration SECONDS]
"""
import argparse
import json
import logging
import multiprocessing as mp
import os
import signal
import time
from collections import deque

from edge.api_server import api_main
from edge.config import load_config
from edge.ipc import STOP, BoundedQueue, StopFlag
from edge.storage import Storage
from edge.sync_worker import sync_main
from edge.workers import health_main, processor_main, sensor_gen_main

log = logging.getLogger("supervisor")


class Supervisor:
    def __init__(self, cfg):
        self.cfg = cfg
        self.ctx = mp.get_context("spawn")
        self.stop_event = StopFlag(self.ctx)   # lock-free: survives SIGKILLed waiters
        self.queue = BoundedQueue(cfg["ipc"]["queue_maxsize"], self.ctx)
        self.procs = {}
        self.restarts = {}
        self._restart_times = {}
        self._stopping = False

    def _specs(self):
        c, q, s = self.cfg, self.queue, self.stop_event
        return {
            "SensorGen": (sensor_gen_main, (c, q, s)),
            "Processor": (processor_main, (c, q, s)),
            "HealthMon": (health_main, (c, s)),
            "SyncWorker": (sync_main, (c, s)),
            "API": (api_main, (c, s)),
        }

    def _spawn(self, name):
        fn, args = self._specs()[name]
        p = self.ctx.Process(target=fn, args=args, name=name)
        p.start()
        self.procs[name] = p
        self.restarts.setdefault(name, 0)
        self._write_pidfile()
        log.info("started %s pid=%s", name, p.pid)

    def _write_pidfile(self):
        data = {"Supervisor": {"pid": os.getpid(), "restarts": 0}}
        for name, p in self.procs.items():
            data[name] = {"pid": p.pid, "restarts": self.restarts.get(name, 0)}
        path = self.cfg["paths"]["pidfile"]
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, path)          # atomic

    def start(self):
        Storage(self.cfg["paths"]["db"], self.cfg["paths"]["schema"])   # create schema once
        for name in self._specs():
            self._spawn(name)

    def _reap_and_restart(self):
        sup = self.cfg["supervisor"]
        for name, p in list(self.procs.items()):
            if p.is_alive():
                continue
            p.join(0)                  # reap: no zombie
            now = time.monotonic()
            times = self._restart_times.setdefault(name, deque())
            while times and now - times[0] > sup["window_s"]:
                times.popleft()
            if len(times) >= sup["max_restarts"]:
                log.error("%s exceeded restart limit, giving up", name)
                del self.procs[name]
                continue
            log.warning("%s died (exit code %s), restarting", name, p.exitcode)
            time.sleep(sup["restart_delay_s"])
            times.append(now)
            self.restarts[name] += 1
            self._spawn(name)

    def run(self, duration=0):
        signal.signal(signal.SIGTERM, self._on_signal)
        signal.signal(signal.SIGINT, self._on_signal)
        self.start()
        t0 = time.monotonic()
        try:
            while not self._stopping:
                self._reap_and_restart()
                if duration and time.monotonic() - t0 >= duration:
                    break
                time.sleep(0.2)
        finally:
            self.stop()

    def _on_signal(self, signum, frame):
        log.info("signal %s received, shutting down", signum)
        self._stopping = True

    def stop(self):
        self._stopping = True
        self.stop_event.set()
        if "SensorGen" in self.procs:                 # stop the producer first
            self.procs["SensorGen"].join(5)
        self.queue.put(STOP)
        for name, p in self.procs.items():
            p.join(5)
            if p.is_alive():
                log.warning("%s did not exit, terminating", name)
                p.terminate()
                p.join(2)
        log.info("all workers stopped")
        try:
            os.remove(self.cfg["paths"]["pidfile"])
        except FileNotFoundError:
            pass


def main():
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=os.path.join(root, "config", "node.yaml"))
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until signal)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    Supervisor(load_config(args.config)).run(args.duration)


if __name__ == "__main__":
    main()
