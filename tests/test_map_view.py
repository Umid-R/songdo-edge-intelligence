"""Live 2D map (/map): the data behind it comes from the node's own database.

- the sign shows the commanded value only while the command is inside its TTL (monotonic clock),
  and the configured default after it expires
- readings come back oldest first, capped per stream
- the page and both endpoints are served by the console API
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from edge.api_server import make_handler                         # noqa: E402
from edge.config import load_config                              # noqa: E402
from edge.decision import RuleEngine, boot_epoch                  # noqa: E402
from edge.map_view import (READINGS_PER_STREAM, FaultError, effective_actuators, kill_processor,  # noqa: E402
                           map_context, map_live)
from edge.models import SensorEvent                               # noqa: E402
from edge.storage import Storage                                  # noqa: E402

SIGN = "VARIABLE_SPEED_SIGN_KMH"


def theme_cfg(tmp, theme="os3"):
    cfg = load_config(os.path.join(ROOT, "config", f"{theme}.yaml"))
    cfg["paths"]["db"] = os.path.join(tmp, f"{theme}.db")
    cfg["paths"]["pidfile"] = os.path.join(tmp, f"{theme}_pids.json")
    cfg.pop("upstream", None)                     # no receiver process in a unit test
    return cfg


def os3_cfg(tmp):
    return theme_cfg(tmp, "os3")


class TestMapView(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.cfg = os3_cfg(self.tmp.name)
        self.st = Storage(self.cfg["paths"]["db"], self.cfg["paths"]["schema"])

    def tearDown(self):
        self.st.close()
        self.tmp.cleanup()

    def _surge(self, count=15):
        ev = SensorEvent(node_id="T", sensor="PED_CROSSWALK_COUNT", value=count)
        decisions = RuleEngine(self.cfg["rules"], deterministic_ids=True).evaluate(ev)
        self.st.commit_event(ev, decisions)
        for row in self.st.pending_intents():
            self.assertEqual(self.st.apply_intent(row, boot_epoch())[0], "DISPATCHED")
        return decisions

    def test_context_has_scene_rules_and_real_bus_stops(self):
        ctx = map_context(self.cfg)
        self.assertEqual(ctx["scene"], "os3_intersection")
        self.assertEqual(ctx["defaults"][SIGN], 50)
        self.assertEqual(len(ctx["bus_stops"]["stops"]), 7)
        self.assertIn("320", [r["route_number"] for r in ctx["bus_stops"]["stops"][0]["routes"]])

    def test_sign_default_before_any_command(self):
        sign = {a["actuator"]: a for a in effective_actuators(self.cfg, self.st)}[SIGN]
        self.assertEqual((sign["value"], sign["active"]), (50, False))

    def test_sign_follows_ttl_on_the_monotonic_clock(self):
        d = self._surge()[0]
        sign = {a["actuator"]: a for a in effective_actuators(self.cfg, self.st, now_mono=d.issued_mono + 1)}[SIGN]
        self.assertEqual((sign["value"], sign["active"]), (30, True))
        self.assertAlmostEqual(sign["remaining_s"], 59, delta=0.1)
        self.assertAlmostEqual(sign["ttl_s"], 60, delta=0.01)
        sign = {a["actuator"]: a for a in effective_actuators(self.cfg, self.st, now_mono=d.expires_mono)}[SIGN]
        self.assertEqual((sign["value"], sign["active"], sign["commanded"]), (50, False, 30))

    def test_no_surge_no_command(self):
        self._surge(count=10)                      # strict ">": 10 is not a surge
        self.assertEqual(self.st.recent_decisions(), [])

    def test_live_readings_oldest_first_and_capped(self):
        t0 = time.time() - 100
        for i in range(READINGS_PER_STREAM + 15):
            self.st.commit_event(SensorEvent(node_id="T", sensor="RADAR_SPEED", value=float(i), observed_at=t0 + i))
        self._surge()
        live = map_live(self.cfg, self.st)
        radar = [r["v"] for r in live["readings"]["RADAR_SPEED"]]
        self.assertEqual(len(radar), READINGS_PER_STREAM)
        self.assertEqual(radar[-1], READINGS_PER_STREAM + 14)          # newest last
        self.assertEqual(radar, sorted(radar))
        self.assertEqual(live["events"], READINGS_PER_STREAM + 16)
        self.assertEqual(live["outbox_pending"], live["events"])
        self.assertEqual(live["decisions"][0]["status"], "DISPATCHED")
        self.assertTrue(live["wan_online"])


class TestThemeScenes(unittest.TestCase):
    """OS1-5, OS2-5 and OS5-5 scenes: config, reference geometry, decision counts."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)

    def tearDown(self):
        self.tmp.cleanup()

    def test_every_theme_but_os4_has_a_scene(self):
        scenes = {t: map_context(theme_cfg(self.tmp.name, t))["scene"] for t in ("os1", "os2", "os3", "os4", "os5")}
        self.assertEqual(scenes, {"os1": "os1_building", "os2": "os2_park", "os3": "os3_intersection",
                                  "os4": None, "os5": "os5_sluice"})

    def test_geometry_and_defaults(self):
        os1 = map_context(theme_cfg(self.tmp.name, "os1"))
        self.assertEqual([f["properties"]["building_id"] for f in os1["buildings"]["features"]], ["B-CONVENSIA", "B-OFFICE"])
        self.assertEqual(os1["defaults"]["HVAC_DAMPER_PCT"], 30)
        os2 = map_context(theme_cfg(self.tmp.name, "os2"))
        self.assertEqual(os2["path"]["features"][0]["geometry"]["type"], "LineString")
        self.assertGreater(len(os2["path"]["features"][0]["geometry"]["coordinates"]), 100)
        os5 = map_context(theme_cfg(self.tmp.name, "os5"))
        self.assertEqual(os5["faults"], ["kill_processor"])
        self.assertEqual(os5["defaults"]["BACKUP_SLUICE_GATE_OPEN"], 0)

    def test_flood_opens_gate_and_counts(self):
        cfg = theme_cfg(self.tmp.name, "os5")
        st = Storage(cfg["paths"]["db"], cfg["paths"]["schema"])
        try:
            engine = RuleEngine(cfg["rules"], deterministic_ids=True)
            for level in (2.8, 3.0):                       # strict ">": only 3.0 is a flood
                ev = SensorEvent(node_id="T", sensor="SLUICE_WATER_LEVEL_M", value=level)
                st.commit_event(ev, engine.evaluate(ev))
            for row in st.pending_intents():
                st.apply_intent(row, boot_epoch())
            live = map_live(cfg, st)
            self.assertEqual(live["decision_counts"], {"DISPATCHED": 1})
            gate = {a["actuator"]: a for a in live["actuators"]}["BACKUP_SLUICE_GATE_OPEN"]
            self.assertEqual((gate["value"], gate["active"]), (1, True))
        finally:
            st.close()


@unittest.skipUnless(os.path.isdir("/proc/self"), "uses /proc and SIGKILL")
class TestKillProcessor(unittest.TestCase):
    """POST /api/fault/kill-processor may only ever kill the node's own Processor."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.cfg = theme_cfg(self.tmp.name, "os5")
        self.victim = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])

    def tearDown(self):
        if self.victim.poll() is None:
            self.victim.kill()
        self.victim.wait()
        self.tmp.cleanup()

    def pidfile(self, sup, proc):
        with open(self.cfg["paths"]["pidfile"], "w", encoding="utf-8") as f:
            json.dump({"Supervisor": {"pid": sup}, "Processor": {"pid": proc}}, f)

    def code(self, cfg):
        with self.assertRaises(FaultError) as cm:
            kill_processor(cfg)
        return cm.exception.code

    def test_refused_where_not_enabled(self):
        self.assertEqual(self.code(os3_cfg(self.tmp.name)), 403)

    def test_refused_without_pidfile(self):
        self.assertEqual(self.code(self.cfg), 409)

    def test_refused_when_console_is_not_part_of_the_node(self):
        self.pidfile(os.getpid(), self.victim.pid)       # this test process is not a child of itself
        self.assertEqual(self.code(self.cfg), 409)
        self.assertIsNone(self.victim.poll())

    def test_kills_only_a_sibling_processor(self):
        """This test process plays the supervisor; a child plays the API and asks for the kill."""
        api = [sys.executable, "-c", "import json,sys; sys.path.insert(0, sys.argv[1]); "
               "from edge.map_view import kill_processor, FaultError; cfg = json.loads(sys.argv[2]);\n"
               "try: print(json.dumps(kill_processor(cfg)))\nexcept FaultError as e: print(json.dumps({'error': e.code}))",
               os.path.join(ROOT, "src")]
        cfg = {"map": self.cfg["map"], "paths": {"pidfile": self.cfg["paths"]["pidfile"]}}
        self.pidfile(os.getpid(), os.getpid())            # stale: the "Processor" PID is the supervisor itself
        out = json.loads(subprocess.run(api + [json.dumps(cfg)], capture_output=True, text=True, timeout=30).stdout)
        self.assertEqual(out, {"error": 409})
        self.pidfile(os.getpid(), self.victim.pid)
        out = json.loads(subprocess.run(api + [json.dumps(cfg)], capture_output=True, text=True, timeout=30).stdout)
        self.assertEqual((out["pid"], out["signal"]), (self.victim.pid, "SIGKILL"))
        self.assertEqual(self.victim.wait(timeout=10), -signal.SIGKILL)


class TestMapApi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        cls.cfg = os3_cfg(cls.tmp.name)
        cls.st = Storage(cls.cfg["paths"]["db"], cls.cfg["paths"]["schema"])
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(cls.cfg, cls.st))
        cls.base = f"http://127.0.0.1:{cls.srv.server_address[1]}"
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()
        cls.srv.server_close()
        cls.st.close()
        cls.tmp.cleanup()

    def get(self, path):
        with urllib.request.urlopen(self.base + path, timeout=5) as r:
            return r.status, r.headers.get("Content-Type"), r.read()

    def test_page_and_endpoints(self):
        code, ctype, body = self.get("/map")
        self.assertEqual(code, 200)
        self.assertIn("text/html", ctype)
        self.assertIn(b"Live map", body)
        self.assertNotIn(b"https://", body)       # nothing loaded from the Internet: works with WAN cut
        ctx = json.loads(self.get("/api/map/context")[2])
        self.assertEqual(ctx["scene"], "os3_intersection")
        live = json.loads(self.get("/api/map")[2])
        for key in ("wan_online", "readings", "actuators", "decisions", "outbox_pending", "delivered",
                    "decision_counts", "processes"):
            self.assertIn(key, live)

    def test_kill_refused_on_a_node_without_faults(self):
        req = urllib.request.Request(self.base + "/api/fault/kill-processor", data=b"{}", method="POST")
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(req, timeout=5)
        self.assertEqual(cm.exception.code, 403)

    def test_wan_cut_shows_on_the_map(self):
        req = urllib.request.Request(self.base + "/api/wan/cut", data=b"{}", method="POST")
        urllib.request.urlopen(req, timeout=5).close()
        try:
            self.assertFalse(json.loads(self.get("/api/map")[2])["wan_online"])
        finally:
            self.st.set_wan_online(True)


if __name__ == "__main__":
    unittest.main()
