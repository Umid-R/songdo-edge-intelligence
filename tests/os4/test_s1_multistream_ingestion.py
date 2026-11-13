"""Tests for OS4-1 multi-stream ingestion. Run: PYTHONPATH=src python -m unittest discover -s tests -v"""
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge.teams.os4_iot import s1_multistream_ingestion as m   # noqa: E402
sys.path.insert(0, os.path.join(ROOT, "tests"))
from platform_skip import linux_only   # noqa: E402


@linux_only("selectors (epoll) on pipes")
class TestS1MultiStreamIngestion(unittest.TestCase):
    def test_frame_buffer_handles_partial_reads(self):
        fb = m.FrameBuffer()
        self.assertEqual(fb.feed(b'{"a":'), [])
        self.assertEqual(fb.feed(b'1}\n{"b"'), [b'{"a":1}'])
        self.assertEqual(fb.feed(b':2}\n{"c":3}\n'), [b'{"b":2}', b'{"c":3}'])

    def test_leq_is_an_energy_average_not_a_plain_mean(self):
        self.assertAlmostEqual(m.leq([60.0, 60.0]), 60.0, places=9)
        self.assertGreater(m.leq([50.0, 70.0]), 60.0)          # louder samples dominate
        self.assertAlmostEqual(m.leq([50.0, 70.0]), 67.0, delta=0.1)

    def test_scenario_passes(self):
        ok, lines = m.run()
        self.assertTrue(ok, "\n".join(lines))


if __name__ == "__main__":
    unittest.main()
