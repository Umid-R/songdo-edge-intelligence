"""Mock upstream consumer (course README: integration/ = "Mock upstream consumers").

Stands in for the Urban Data Platform the edge node uploads to. Since the final term the receiver
itself lives in src/edge/upstream.py, because the shared node now runs it as its own supervised
process (`Upstream`, separate SQLite file) and the SyncWorker drains to it over HTTP with per-event
ACKs. This file is the stand-alone entry point and keeps the old helper names.

    python3 integration/mock_upstream.py --port 9100 --db data/upstream_mock.db   # run it

Receiver contract: see edge.upstream (POST /ingest -> per-ID "acked" list, GET /stats, GET /ids,
POST /fault for lost-ACK / unrelated-ACK injection).
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from edge.upstream import Receiver, fetch_ids, fetch_stats, make_server, reconcile, serve, set_fault  # noqa: E402,F401
from edge.upstream import drain_outbox_http as _drain                                                 # noqa: E402


def drain_outbox_http(storage, url, batch_size=50, timeout=2.0):
    """Send one batch; rows are marked sent only for ACKed event_ids. Returns rows delivered."""
    return _drain(storage, url, batch_size, timeout)["acked"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9100)
    ap.add_argument("--db", default="data/upstream_mock.db")
    args = ap.parse_args()
    print(f"mock upstream on http://{args.host}:{args.port}  (POST /ingest, GET /stats, GET /ids), store {args.db}")
    serve(args.host, args.port, args.db)


if __name__ == "__main__":
    main()
