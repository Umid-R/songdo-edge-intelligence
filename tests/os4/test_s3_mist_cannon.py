"""Tests for OS4-3 mist cannon. Run: PYTHONPATH=src python -m unittest discover -s tests -v"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge.models import EdgeDecision   # noqa: E402
from edge.teams.os4_iot import s3_mist_cannon as m   # noqa: E402


def cmd(issued, ttl=30.0):
    return EdgeDecision(event_id="e", rule="dust_mist", actuator=m.ACTUATOR, command_value=1,
                        issued_mono=issued, expires_mono=issued + ttl)


class TestS3MistCannon(unittest.TestCase):
    def test_relay_is_a_lease(self):
        relay = m.MistRelay()
        self.assertFalse(relay.is_on(0.0))
        self.assertEqual(relay.submit(cmd(10.0), now_mono=10.0), "DISPATCHED")
        self.assertTrue(relay.is_on(39.9))
        self.assertFalse(relay.is_on(40.0))        # switches itself off when the TTL runs out

    def test_new_command_extends_the_lease(self):
        relay = m.MistRelay()
        relay.submit(cmd(10.0), now_mono=10.0)
        relay.submit(cmd(30.0), now_mono=30.0)
        self.assertTrue(relay.is_on(59.9))

    def test_stale_command_rejected(self):
        relay = m.MistRelay()
        self.assertEqual(relay.submit(cmd(10.0), now_mono=40.0), "EXPIRED")
        self.assertFalse(relay.is_on(40.0))

    def test_scenario_passes(self):
        ok, lines = m.run()
        self.assertTrue(ok, "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
