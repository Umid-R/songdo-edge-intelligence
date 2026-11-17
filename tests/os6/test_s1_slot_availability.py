"""Tests for OS6-1 parking slot availability. Run: PYTHONPATH=src python -m unittest discover -s tests -v"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge.teams.os6_parking import s1_slot_availability as m   # noqa: E402
from edge.teams.os6_parking._common import SLOTS               # noqa: E402


class TestS1SlotAvailability(unittest.TestCase):
    def test_trace_glitches_are_single_tick_and_truth_ends_stable(self):
        truth, raw, glitches = m.make_trace(6)
        self.assertGreater(sum(glitches.values()), 0)
        for s in SLOTS:
            self.assertEqual(len(set(truth[s][-5:])), 1, s)
            for t in range(1, m.TICKS):
                self.assertFalse(raw[s][t] != truth[s][t] and raw[s][t - 1] != truth[s][t - 1], (s, t))

    def test_scenario_passes(self):
        ok, lines = m.run(seed=6)
        self.assertTrue(ok, "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
