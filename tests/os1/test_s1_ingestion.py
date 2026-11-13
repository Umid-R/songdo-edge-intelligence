"""Tests for OS1-1 sensor ingestion, cached GeoJSON and building mapping."""
import json
import os
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
import sys
sys.path.insert(0, str(ROOT / "src"))
from edge.teams.os1_building import s1_ingestion as m  # noqa: E402

SQUARE = [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]
HOLE = [[4, 4], [6, 4], [6, 6], [4, 6], [4, 4]]


class TestS1Ingestion(unittest.TestCase):
    def test_sensor_events_are_stored_with_outbox_and_mapped(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            ok, lines = m.run(ticks=7, db_path=os.path.join(tmp, "test.db"))
            self.assertTrue(ok, "\n".join(lines))
            report = "\n".join(lines)
            self.assertIn(f"events persisted: {len(m.ROOMS) * 2 * 7}", report)
            self.assertIn("R-101->B-CONVENSIA", report)
            self.assertIn("R-OUT->UNMAPPED", report)

    def test_report_says_when_the_inventory_is_only_a_sample(self):
        ok, lines = m.run(ticks=2)
        self.assertTrue(ok, "\n".join(lines))
        if not m.DEFAULT_INVENTORY.exists():
            self.assertIn("SAMPLE", lines[0])
            self.assertIn("NOT real VWorld data", lines[0])

    def test_missing_explicit_inventory_fails_loudly(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            ok, lines = m.run(ticks=3, inventory_path=os.path.join(tmp, "missing.geojson"))
            self.assertFalse(ok)
            self.assertIn("NO INVENTORY", lines[0])

    def test_point_in_polygon_with_hole_and_multipolygon(self):
        self.assertTrue(m.polygon_contains(2, 2, [SQUARE, HOLE]))
        self.assertFalse(m.polygon_contains(5, 5, [SQUARE, HOLE]))          # inside the courtyard hole
        self.assertFalse(m.polygon_contains(11, 5, [SQUARE]))
        feature = {"geometry": {"type": "MultiPolygon", "coordinates": [[SQUARE], [[[20, 20], [30, 20], [30, 30], [20, 30], [20, 20]]]]}}
        self.assertTrue(m.feature_contains(feature, 25, 25))
        self.assertFalse(m.feature_contains(feature, 15, 15))
        self.assertFalse(m.feature_contains({"geometry": {"type": "Point", "coordinates": [1, 1]}}, 1, 1))

    def test_locate_building_picks_the_right_footprint(self):
        feats = m.load_cached_inventory(m.SAMPLE_INVENTORY)
        self.assertEqual(m.building_id(m.locate_building(126.6534, 37.3888, feats)), "B-CONVENSIA")
        self.assertEqual(m.building_id(m.locate_building(126.6560, 37.3888, feats)), "B-OFFICE")
        self.assertIsNone(m.locate_building(126.6600, 37.3900, feats))

    def test_cached_geojson_is_loaded_and_room_can_be_matched(self):
        doc = {"type": "FeatureCollection", "features": [
            {"type": "Feature", "id": "B-12", "properties": {"room_id": "R-12"},
             "geometry": {"type": "Point", "coordinates": [126.6, 37.3]}}
        ]}
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = os.path.join(tmp, "cache.geojson")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(doc, f)
            features = m.load_cached_inventory(path)
            self.assertEqual(len(features), 1)
            self.assertEqual(m.match_building("R-12", features)["id"], "B-12")

    def test_invalid_geojson_is_rejected(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            path = os.path.join(tmp, "bad.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"type": "Feature", "features": []}, f)
            with self.assertRaises(ValueError):
                m.load_cached_inventory(path)


if __name__ == "__main__":
    unittest.main()
