"""Sanity checks for config/node.yaml and the five theme configs."""
import glob
import os
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from edge.config import load_config   # noqa: E402
from edge.decision import OPS              # noqa: E402


class TestConfigs(unittest.TestCase):
    def setUp(self):
        self.paths = sorted(glob.glob(os.path.join(ROOT, "config", "*.yaml")))
        self.cfgs = [load_config(p) for p in self.paths]

    def test_all_five_themes_present(self):
        names = {os.path.basename(p) for p in self.paths}
        for i in range(1, 6):
            self.assertIn(f"os{i}.yaml", names)

    def test_required_sections(self):
        for cfg in self.cfgs:
            for key in ("node", "paths", "network", "ipc", "sensor", "rules", "supervisor", "sync", "health"):
                self.assertIn(key, cfg)

    def test_rules_reference_real_sensors_and_ops(self):
        for cfg in self.cfgs:
            sensors = {s["name"] for s in cfg["sensor"]["streams"]}
            for r in cfg["rules"]:
                self.assertIn(r["sensor"], sensors)
                self.assertIn(r["op"], OPS)

    def test_themes_do_not_collide(self):
        themes = [c for c in self.cfgs if os.path.basename(c["paths"]["db"]).startswith("os")]
        for field in (lambda c: c["network"]["api_port"], lambda c: c["paths"]["db"],
                      lambda c: c["paths"]["pidfile"], lambda c: c["node"]["id"]):
            values = [field(c) for c in themes]
            self.assertEqual(len(values), len(set(values)))


if __name__ == "__main__":
    unittest.main()
