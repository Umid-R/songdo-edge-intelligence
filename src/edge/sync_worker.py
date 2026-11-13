"""Durable outbox synchronizer: drains the outbox only while the WAN is online.

With an `upstream:` section in the config (all node configs) the outbox goes over HTTP to the
independent receiver process (edge.upstream) and a row is marked sent only when its own event_id
is ACKed. Without it, the old local upstream_sink table is used (kept for the scenario programs).
"""
import logging
import signal

from edge.storage import Storage
from edge.upstream import drain_outbox_http, upstream_settings

log = logging.getLogger("edge.sync")


def sync_main(cfg, stop):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [SyncWorker] %(message)s")
    storage = Storage(cfg["paths"]["db"], cfg["paths"]["schema"], init=False)
    batch = cfg["sync"]["batch_size"]
    up = upstream_settings(cfg)
    url = up[3] if up else None
    timeout = cfg["sync"].get("timeout_s", 2.0)
    while not stop.is_set():
        if storage.wan_online():
            if url:
                r = drain_outbox_http(storage, url, batch, timeout)
                if r["acked"] or r["error"] or r["missing"] or r["unrelated"]:
                    log.info("offered %d, acked %d, missing ACK %d, unrelated ACK %d, error %s (pending=%d)",
                             r["offered"], r["acked"], r["missing"], r["unrelated"], r["error"],
                             storage.outbox_pending())
                if r["acked"] == batch:
                    continue                    # backlog: send the next batch right away
            else:
                n = storage.drain_outbox_to_sink(batch)
                if n:
                    log.info("drained %d events (pending=%d)", n, storage.outbox_pending())
        stop.wait(cfg["sync"]["interval_s"])
