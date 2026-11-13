"""Tests for OS2-4 crash & supervised restart. Run: PYTHONPATH=src python -m unittest discover -s tests -v"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge.teams.os2_robot import s4_crash_restart as m   # noqa: E402
sys.path.insert(0, os.path.join(ROOT, "tests"))
from platform_skip import linux_only   # noqa: E402


@linux_only("SIGKILL / SIGCHLD")
class TestS4CrashRestart(unittest.TestCase):
    def test_scenario_passes(self):
        ok, lines = m.run()
        self.assertTrue(ok, "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
