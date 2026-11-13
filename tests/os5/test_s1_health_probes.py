"""Tests for OS5-1 health probes. Run: PYTHONPATH=src python -m unittest discover -s tests -v"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge.teams.os5_reliable import s1_health_probes as m   # noqa: E402
sys.path.insert(0, os.path.join(ROOT, "tests"))
from platform_skip import linux_only   # noqa: E402


@linux_only("SIGKILL and /proc")
class TestS1HealthProbes(unittest.TestCase):
    def row(self, state="READY", updated_at=100.0):
        return {"pid": 1, "state": state, "updated_at": updated_at}

    def test_probe_verdicts(self):
        p = m.probe
        self.assertEqual(p(None, 100.0, False)["state"], "NO_HEARTBEAT")
        self.assertEqual(p(self.row(), 100.1, False)["state"], "DEAD")            # pid gone
        self.assertEqual(p(self.row(), 101.0, True)["state"], "HUNG")             # stale heartbeat
        ready = p(self.row("READY"), 100.1, True)
        self.assertTrue(ready["live"] and ready["ready"])
        not_ready = p(self.row("NOT_READY"), 100.1, True)
        self.assertTrue(not_ready["live"] and not not_ready["ready"])            # alive != ready
        starting = p(self.row("STARTING"), 100.1, True)
        self.assertTrue(starting["live"] and not starting["ready"])

    def test_liveness_timeout_boundary(self):
        self.assertTrue(m.probe(self.row(), 100.0 + m.LIVENESS_TIMEOUT_S, True)["live"])
        self.assertFalse(m.probe(self.row(), 100.0 + m.LIVENESS_TIMEOUT_S + 0.01, True)["live"])

    def test_scenario_passes(self):
        ok, lines = m.run()
        self.assertTrue(ok, "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
