"""Tests for OS2-3 e-stop & command expiry. Run: PYTHONPATH=src python -m unittest discover -s tests -v"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge.models import EdgeDecision   # noqa: E402
from edge.teams.os2_robot import s3_estop_ttl as m   # noqa: E402


def cmd(actuator, value, issued, ttl=0.5):
    return EdgeDecision(event_id="e", rule="r", actuator=actuator, command_value=value,
                        issued_mono=issued, expires_mono=issued + ttl)


class TestS3EstopTtl(unittest.TestCase):
    def test_threshold_is_strictly_below_0_8(self):
        latch = m.SafetyLatch()
        self.assertFalse(latch.on_proximity(0.80))
        self.assertTrue(latch.on_proximity(0.79))
        self.assertFalse(latch.on_proximity(0.50))         # already engaged: not a new engagement
        self.assertEqual(latch.engagements, 1)

    def test_expired_commands_rejected_on_monotonic_clock(self):
        latch = m.SafetyLatch()
        c = cmd("MOTOR_VEL", 0.5, 10.0)
        self.assertEqual(latch.submit(c, 10.4), m.ACCEPTED)
        self.assertEqual(latch.submit(c, 10.5), m.REJECTED_EXPIRED)
        self.assertEqual(latch.submit(c, 11.0), m.REJECTED_EXPIRED)

    def test_motion_blocked_while_latched_but_stop_allowed(self):
        latch = m.SafetyLatch()
        latch.on_proximity(0.3)
        self.assertEqual(latch.submit(cmd("MOTOR_VEL", 0.5, 1.0), 1.1), m.REJECTED_LATCHED)
        self.assertEqual(latch.submit(cmd("MOTOR_VEL", 0.0, 1.0), 1.1), m.ACCEPTED)

    def test_release_needs_hysteresis(self):
        latch = m.SafetyLatch()
        latch.on_proximity(0.3)
        self.assertFalse(latch.try_release(0.9))
        self.assertTrue(latch.try_release(1.0))

    def test_scenario_passes(self):
        ok, lines = m.run()
        self.assertTrue(ok, "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
