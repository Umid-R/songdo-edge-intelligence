"""Integration test: edge outbox -> HTTP -> mock upstream, across a real WAN cut.

    python3 integration/test_mock_upstream.py

1. 300 events are stored on the node; the upstream process is running: half are delivered.
2. A delivered batch is sent AGAIN (as if the node crashed before marking it sent): the
   receiver stores it once and counts the resend as duplicates.
3. WAN cut = the upstream process is killed: draining delivers nothing, every row stays pending.
4. WAN restored = the upstream starts again on the same store: the backlog drains to zero.
5. Conservation: events on the node == distinct events at the receiver, none lost, none doubled.
"""
import multiprocessing as mp
import os
import socket
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(ROOT, "src"))
sys.path.insert(0, HERE)

import mock_upstream as up                                            # noqa: E402
from edge.models import SensorEvent                                   # noqa: E402
from edge.storage import Storage                                      # noqa: E402

SCHEMA = os.path.join(ROOT, "sql", "schema.sql")


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start(port, db):
    p = mp.get_context("spawn").Process(target=up.serve, args=("127.0.0.1", port, db), daemon=True)
    p.start()
    end = time.monotonic() + 10
    while time.monotonic() < end:
        try:
            up.fetch_stats(f"http://127.0.0.1:{port}", timeout=0.5)
            return p
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("mock upstream did not start")


class TestMockUpstream(unittest.TestCase):
    def test_outbox_to_http_receiver_across_wan_cut(self):
        with tempfile.TemporaryDirectory() as tmp:
            st = Storage(os.path.join(tmp, "node.db"), SCHEMA)
            for i in range(300):
                st.insert_event_with_outbox(SensorEvent(node_id="EDGE_NODE_TEST", sensor="TEST", value=i))
            port, store = _free_port(), os.path.join(tmp, "upstream.db")
            url = f"http://127.0.0.1:{port}"

            server = _start(port, store)                              # 1. online
            for _ in range(3):
                self.assertEqual(up.drain_outbox_http(st, url, batch_size=50), 50)
            self.assertEqual(st.outbox_pending(), 150)

            conn = st._connect()                                       # 2. resend a delivered batch
            try:
                conn.execute("UPDATE outbox SET sent_at = NULL WHERE id IN "
                             "(SELECT id FROM outbox WHERE sent_at IS NOT NULL ORDER BY id LIMIT 50)")
                conn.commit()
            finally:
                conn.close()
            self.assertEqual(up.drain_outbox_http(st, url, batch_size=50), 50)
            self.assertEqual(up.fetch_stats(url), {"received": 150, "duplicates": 50})

            server.kill()                                              # 3. WAN cut
            server.join(5)
            self.assertEqual(up.drain_outbox_http(st, url, batch_size=50, timeout=0.5), 0)
            self.assertEqual(st.outbox_pending(), 150)

            server = _start(port, store)                               # 4. WAN restored
            try:
                while up.drain_outbox_http(st, url, batch_size=50):
                    pass
                stats = up.fetch_stats(url)
            finally:
                server.kill()
                server.join(5)

            self.assertEqual(st.outbox_pending(), 0)                   # 5. conservation
            self.assertEqual(stats["received"], st.counts()["events"])
            self.assertEqual(stats["received"], 300)
            self.assertEqual(stats["duplicates"], 50)


if __name__ == "__main__":
    unittest.main(verbosity=2)
