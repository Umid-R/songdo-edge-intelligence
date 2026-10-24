"""Tests for OS3-5 offline sign dispatch and replay. Run: PYTHONPATH=src python -m unittest discover -s tests -v"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge.teams.os3_mobility import s5_speed_sign_replay as m   # noqa: E402


class TestS5SpeedSignReplay(unittest.TestCase):
    def test_scenario_passes(self):
        ok, lines = m.run(2.0)
        self.assertTrue(ok, "\n".join(lines))

    def test_sign_reverts_after_ttl(self):
        from edge.models import EdgeDecision
        import time
        sign = m.SpeedSign()
        now = time.monotonic()
        d = EdgeDecision("e", "r", "VARIABLE_SPEED_SIGN_KMH", 30, now, now + 0.05)
        self.assertEqual(sign.dispatch(d), "DISPATCHED")
        self.assertEqual(sign.effective_kmh(), 30)
        time.sleep(0.08)
        self.assertEqual(sign.effective_kmh(), 50)


if __name__ == "__main__":
    unittest.main()
