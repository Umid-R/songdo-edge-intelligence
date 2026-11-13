"""Worker process entry points: SensorGen, Processor, HealthMon."""
import json
import logging
import os
import random
import signal
import time
from queue import Empty

from edge.ipc import STOP
from edge.models import ProcessHealth, SensorEvent
from edge.procfs import ProcSampler
from edge.storage import Storage
from edge.decision import RuleEngine, boot_epoch


def _init(name):
    signal.signal(signal.SIGINT, signal.SIG_IGN)   # supervisor handles Ctrl+C
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [{name}] %(message)s")
    return logging.getLogger(name)


def crash_point(name):
    """Fault injection for the crash-recovery tests (tests/test_crash_recovery.py).

    If EDGE_CRASH_AT names this point, the process dies here exactly like a SIGKILL (no cleanup,
    no finally blocks), once: EDGE_CRASH_MARK is a file created just before dying, so the restarted
    worker runs normally. Without the variables this is a dictionary lookup and nothing else."""
    if os.environ.get("EDGE_CRASH_AT") != name:
        return
    mark = os.environ.get("EDGE_CRASH_MARK")
    if mark:
        if os.path.exists(mark):
            return
        with open(mark, "w", encoding="utf-8") as f:
            f.write(name)
    os._exit(137)


def sensor_gen_main(cfg, q, stop):
    _init("SensorGen")
    rng = random.Random(cfg["sensor"].get("seed"))
    node = cfg["node"]["id"]
    storage = Storage(cfg["paths"]["db"], cfg["paths"]["schema"], init=False)
    while not stop.is_set():
        for s in cfg["sensor"]["streams"]:
            value = s["base"] + rng.gauss(0, s.get("noise", 0))
            if rng.random() < s.get("spike_prob", 0):
                value = s["spike_value"]
            ev = SensorEvent(node_id=node, sensor=s["name"], value=round(value, 2), unit=s.get("unit", ""))
            storage.admit(ev)            # durable first: survives the queue and any Processor crash
            q.put(ev.to_dict())
        stop.wait(cfg["sensor"]["interval_s"])


class EventProcessor:
    """Commit + decide + actuate for one event, with every step recoverable after a crash."""

    def __init__(self, cfg, storage, log):
        self.storage = storage
        self.log = log
        self.engine = RuleEngine(cfg["rules"], deterministic_ids=True)
        self.epoch = boot_epoch()

    def process(self, ev):
        if ev.received_at is None:
            ev.received_at = time.time()
        age = max(0.0, time.time() - ev.observed_at)
        decisions = self.engine.evaluate(ev, age_s=age)
        crash_point("after_dequeue")             # event is only in this process's memory + ingress
        self.storage.commit_event(ev, decisions)
        crash_point("after_commit")              # event + intents committed, nothing applied yet
        self.apply_pending()

    def apply_pending(self):
        applied = 0
        for row in self.storage.pending_intents():
            status, reason = self.storage.apply_intent(row, self.epoch)
            if status == "DISPATCHED":
                self.log.info("ACTUATE %s=%s (rule %s, seq %s)", row["actuator"], row["command_value"],
                              row["rule"], row["seq"])
            else:
                self.log.warning("REJECT %s=%s (rule %s, seq %s): %s %s", row["actuator"],
                                 row["command_value"], row["rule"], row["seq"], status, reason)
            applied += 1
            crash_point("after_apply")           # applied + receipt committed
        return applied

    def recover(self, older_than_s=0.0):
        """Replay admitted-but-uncommitted readings, then every PENDING intent. Idempotent."""
        replayed = 0
        while True:
            rows = self.storage.unprocessed_ingress(older_than_s)
            if not rows:
                break
            for r in rows:
                ev = SensorEvent.from_dict(json.loads(r["payload"]))
                self.process(ev)
                replayed += 1
            if older_than_s:                     # periodic sweep: one batch per call is enough
                break
        self.apply_pending()
        return replayed


def processor_main(cfg, q, stop):
    log = _init("Processor")
    storage = Storage(cfg["paths"]["db"], cfg["paths"]["schema"], init=False)
    proc = EventProcessor(cfg, storage, log)
    pcfg = cfg.get("processor", {})
    sweep_s, sweep_age = pcfg.get("sweep_interval_s", 2.0), pcfg.get("sweep_age_s", 2.0)
    n = proc.recover()
    if n:
        log.warning("recovered %d admitted events that were never committed", n)
    next_sweep = time.monotonic() + sweep_s
    while True:
        try:
            item = q.get(timeout=0.5)
        except Empty:
            item = None
            if stop.is_set():
                break
        if item == STOP:
            break
        if item is not None:
            proc.process(SensorEvent.from_dict(item))
        if time.monotonic() >= next_sweep:       # readings dropped from the full queue, or
            n = proc.recover(sweep_age)          # admitted just before a SensorGen restart
            if n:
                log.info("sweep: committed %d admitted events that never arrived through the queue", n)
            try:
                storage.kv_set("queue_stats", json.dumps({"qsize": q.qsize(), "dropped": q.dropped,
                                                          "maxsize": getattr(q, "maxsize", None)}))
            except (NotImplementedError, OSError):
                pass
            next_sweep = time.monotonic() + sweep_s
    proc.recover()                               # nothing admitted is left behind on a clean stop
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
