"""Tests for OS5-4 sluice gate. Run: PYTHONPATH=src python -m unittest discover -s tests -v"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge.models import EdgeDecision   # noqa: E402
from edge.teams.os5_reliable import _common as c   # noqa: E402
from edge.teams.os5_reliable import s4_sluice_gate as m   # noqa: E402


class TestS4SluiceGate(unittest.TestCase):
    def test_gate_logic_boundaries(self):
        f = c.gate_next_state
        self.assertEqual(f("CLOSED", 2.8), "CLOSED")       # strictly above 2.8 only
        self.assertEqual(f("CLOSED", 2.81), "OPEN")
        self.assertEqual(f("OPEN", 2.6), "OPEN")           # hysteresis band keeps the state
        self.assertEqual(f("CLOSED", 2.6), "CLOSED")
        self.assertEqual(f("OPEN", 2.5), "CLOSED")         # at or below 2.5 closes

    def test_stale_open_command_rejected(self):
        d = EdgeDecision(event_id="e", rule="flood_relief", actuator=m.ACTUATOR, command_value=1,
                         issued_mono=10.0, expires_mono=70.0)
        gate = m.SluiceGate()
        self.assertEqual(gate.submit(d, now_mono=70.0), "EXPIRED")
        self.assertEqual(gate.state, "CLOSED")
        self.assertEqual(gate.submit(d, now_mono=69.9), "DISPATCHED")
        self.assertEqual(gate.state, "OPEN")

    def test_scenario_passes(self):
        ok, lines = m.run()
        self.assertTrue(ok, "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
