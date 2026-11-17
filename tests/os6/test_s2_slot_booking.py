"""Tests for OS6-2 parking slot booking. Run: PYTHONPATH=src python -m unittest discover -s tests -v"""
import os
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge.teams.os6_parking import s2_slot_booking as m                  # noqa: E402
from edge.teams.os6_parking._common import FREE, OCCUPIED, SLOTS, init_lot   # noqa: E402


class TestS2SlotBooking(unittest.TestCase):
    def test_book_is_idempotent_and_reports_full(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            lot = {s: OCCUPIED for s in SLOTS}
            lot["B03"] = FREE
            conn = init_lot(os.path.join(tmp, "t.db"), lot)
            r1 = m.book(conn, "D01", "req-1", race_s=0)
            self.assertEqual(r1[:2], ("BOOKED", "B03"))
            self.assertEqual(m.book(conn, "D01", "req-1", race_s=0)[:3], ("DUPLICATE", "B03", r1[2]))
            self.assertEqual(m.book(conn, "D02", "req-2", race_s=0)[0], "FULL")

    def test_expired_hold_cannot_check_in(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            conn = init_lot(os.path.join(tmp, "t.db"), {s: FREE for s in SLOTS})
            _, _, bid, _ = m.book(conn, "D01", "req-1", hold_s=0.05, race_s=0)
            time.sleep(0.1)
            self.assertFalse(m.check_in(conn, bid))
            _, _, bid2, _ = m.book(conn, "D02", "req-2", hold_s=5, race_s=0)
            self.assertTrue(m.check_in(conn, bid2))

    def test_scenario_passes(self):
        ok, lines = m.run(seed=6)
        self.assertTrue(ok, "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
