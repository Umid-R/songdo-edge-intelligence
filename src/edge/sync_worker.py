"""Durable outbox synchronizer: drains the outbox only while the WAN is online."""
import logging
import signal

from edge.storage import Storage

log = logging.getLogger("edge.sync")


def sync_main(cfg, stop):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [SyncWorker] %(message)s")
    storage = Storage(cfg["paths"]["db"], cfg["paths"]["schema"], init=False)
    batch = cfg["sync"]["batch_size"]
    while not stop.is_set():
        if storage.wan_online():
            n = storage.drain_outbox_to_sink(batch)
            if n:
                log.info("drained %d events (pending=%d)", n, storage.outbox_pending())
        stop.wait(cfg["sync"]["interval_s"])
