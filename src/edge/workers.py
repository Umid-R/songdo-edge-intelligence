"""Worker process entry points: SensorGen, Processor, HealthMon."""
import json
import logging
import random
import signal
import time
from queue import Empty

from edge.ipc import STOP
from edge.models import ProcessHealth, SensorEvent
from edge.procfs import ProcSampler
from edge.storage import Storage
from edge.decision import RuleEngine, dispatch


def _init(name):
    signal.signal(signal.SIGINT, signal.SIG_IGN)   # supervisor handles Ctrl+C
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [{name}] %(message)s")
    return logging.getLogger(name)


def sensor_gen_main(cfg, q, stop):
    _init("SensorGen")
    rng = random.Random()
    node = cfg["node"]["id"]
    while not stop.is_set():
        for s in cfg["sensor"]["streams"]:
            value = s["base"] + rng.gauss(0, s.get("noise", 0))
            if rng.random() < s.get("spike_prob", 0):
                value = s["spike_value"]
            ev = SensorEvent(node_id=node, sensor=s["name"], value=round(value, 2), unit=s.get("unit", ""))
            q.put(ev.to_dict())
        stop.wait(cfg["sensor"]["interval_s"])


def processor_main(cfg, q, stop):
    log = _init("Processor")
    storage = Storage(cfg["paths"]["db"], cfg["paths"]["schema"], init=False)
    engine = RuleEngine(cfg["rules"])
    while True:
        try:
            item = q.get(timeout=0.5)
        except Empty:
            if stop.is_set():
                break
            continue
        if item == STOP:
            break
        ev = SensorEvent.from_dict(item)
        ev.received_at = time.time()
        storage.insert_event_with_outbox(ev)
        for d in engine.evaluate(ev):
            storage.insert_decision(d)
            storage.set_decision_status(d.decision_id, dispatch(d))
    log.info("exiting cleanly")


def health_main(cfg, stop):
    _init("HealthMon")
    storage = Storage(cfg["paths"]["db"], cfg["paths"]["schema"], init=False)
    sampler = ProcSampler()
    while not stop.is_set():
        try:
            with open(cfg["paths"]["pidfile"], encoding="utf-8") as f:
                pids = json.load(f)
        except (FileNotFoundError, ValueError):
            pids = {}
        for name, info in pids.items():
            m = sampler.sample(info["pid"])
            if m is None:
                continue
            storage.upsert_health(ProcessHealth(
                name=name, pid=info["pid"], state=m["state"], rss_kb=m["rss_kb"],
                cpu_pct=m["cpu_pct"], ctx_voluntary=m["ctx_voluntary"],
                ctx_involuntary=m["ctx_involuntary"], restarts=info.get("restarts", 0)))
        stop.wait(cfg["health"]["interval_s"])
