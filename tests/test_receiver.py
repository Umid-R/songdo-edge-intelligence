"""T2 (final-term assessment): the independent receiver and per-ID ACK handling.

The receiver is a separate process with its own SQLite file, reached over HTTP.
    - normal drain: generated = committed = acknowledged = stored at the receiver
    - ACK lost AFTER the receiver committed, then resend: zero duplicate stored IDs, no false "sent"
    - receiver returns an unrelated ACK ID and leaves one out: only offered+ACKed rows are marked sent
    - WAN cut (receiver killed) and restore: nothing marked, nothing lost, full drain afterwards
"""
import multiprocessing as mp
import os
import socket
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from edge import upstream as up                 # noqa: E402
from edge.models import SensorEvent             # noqa: E402
from edge.storage import Storage                # noqa: E402

N = 200


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_receiver(port, db):
    p = mp.get_context("spawn").Process(target=up.serve, args=("127.0.0.1", port, db), daemon=True)
    p.start()
    end = time.monotonic() + 10
    while time.monotonic() < end:
        try:
            up.fetch_stats(f"http://127.0.0.1:{port}", timeout=0.5)
            return p
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("receiver did not start")


class TestReceiver(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.st = Storage(os.path.join(self.tmp.name, "node.db"), os.path.join(ROOT, "sql", "schema.sql"))
        for i in range(N):                                   # admitted + committed, like the node does
            ev = SensorEvent(node_id="T", sensor="S", value=i, event_id=f"e{i:04d}")
            self.st.admit(ev)
            self.st.commit_event(ev)
        self.port = free_port()
        self.store = os.path.join(self.tmp.name, "receiver.db")    # a DIFFERENT database file
        self.url = f"http://127.0.0.1:{self.port}"
        self.srv = start_receiver(self.port, self.store)

    def tearDown(self):
        if self.srv.is_alive():
            self.srv.kill()
        self.srv.join(5)
        self.st.close()
        self.tmp.cleanup()

    def drain_all(self):
        for _ in range(100):
            r = up.drain_outbox_http(self.st, self.url, 50)
            if r["offered"] == 0:
                return
        self.fail("outbox did not drain")

    def assert_fully_reconciled(self):
        rep = up.reconcile(self.st, self.url)
        self.assertTrue(rep["fully_drained"], rep)
        self.assertEqual((rep["generated"], rep["committed"], rep["acknowledged"], rep["receiver"]), (N, N, N, N))
        self.assertEqual(rep["acked_but_not_at_receiver"], 0)
        self.assertEqual(rep["at_receiver_unknown_to_node"], 0)
        return rep

    def test_normal_drain_g_c_a_r_equal(self):
        self.assertNotEqual(os.path.abspath(self.store), os.path.abspath(self.st.db_path))
        self.drain_all()
        self.assert_fully_reconciled()

    def test_lost_ack_after_receiver_commit_then_resend(self):
        up.set_fault(self.url, drop_ack=1)
        r = up.drain_outbox_http(self.st, self.url, 50)
        self.assertIsNotNone(r["error"])                      # the node never heard the ACK
        self.assertEqual(r["acked"], 0)
        self.assertEqual(self.st.outbox_pending(), N)         # so nothing is marked sent
        self.assertEqual(up.fetch_stats(self.url)["received"], 50)   # but the receiver HAS them
        mid = up.reconcile(self.st, self.url)
        self.assertEqual(mid["pending_but_already_at_receiver"], 50)
        self.assertTrue(mid["conserved"], mid)
        self.drain_all()                                      # resend: receiver deduplicates
        self.assertEqual(up.fetch_stats(self.url), {"received": N, "duplicates": 50})
        self.assert_fully_reconciled()

    def test_unrelated_ack_is_ignored(self):
        up.set_fault(self.url, bogus_ack=1)
        r = up.drain_outbox_http(self.st, self.url, 50)
        self.assertEqual((r["acked"], r["unrelated"], r["missing"]), (49, 1, 1))
        self.assertEqual(self.st.outbox_pending(), N - 49)    # no false "sent" transition
        self.drain_all()
        self.assertEqual(up.fetch_stats(self.url)["duplicates"], 1)  # the un-ACKed one, resent once
        rep = self.assert_fully_reconciled()
        self.assertEqual(rep["sync_stats"]["unrelated_acks"], 1)

    def test_wan_cut_and_restore(self):
        up.drain_outbox_http(self.st, self.url, 50)
        self.srv.kill()                                       # WAN cut: connection refused
        self.srv.join(5)
        r = up.drain_outbox_http(self.st, self.url, 50, timeout=0.5)
        self.assertIsNotNone(r["error"])
        self.assertEqual(self.st.outbox_pending(), N - 50)
        self.srv = start_receiver(self.port, self.store)      # restore, same receiver store
        self.drain_all()
        self.assert_fully_reconciled()

    def test_bad_requests_rejected(self):
        import json
        import urllib.error
        import urllib.request
        for body in (b"not json", json.dumps({"events": [{"no_id": 1}]}).encode()):
            req = urllib.request.Request(self.url + "/ingest", data=body, method="POST")
            with self.assertRaises(urllib.error.HTTPError) as cm:
                urllib.request.urlopen(req, timeout=2)
            self.assertEqual(cm.exception.code, 400)


if __name__ == "__main__":
    unittest.main()
