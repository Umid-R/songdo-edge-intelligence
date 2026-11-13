"""T4 + T5 (final-term assessment): the scenario registry, the run manager and the console API.

T4: invalid ID / profile, duplicate launch, timeout, cancellation (whole process group, no orphan),
    bounded logs, a fresh run directory + TMPDIR + seed per run, evidence JSON.
T5: the CLI runner and the console API launch the same registry entries with the same argv and
    give the same verdicts (subset of fast official scenarios; the full 25/25 parity run is in
    scripts/make_run_bundle.py).
"""
import json
import os
import socket
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
from edge import registry as reg                       # noqa: E402
from edge.api_server import make_handler                # noqa: E402
from edge.registry import Profile, Scenario             # noqa: E402
from edge.runner import MAX_LINES, RunManager           # noqa: E402
from edge.storage import Storage                        # noqa: E402

FIX = os.path.join(ROOT, "tests", "fixtures")


def fake_registry():
    P = Profile
    return {"FAKE-1": Scenario("FAKE-1", "OS1", "fake_scenario", "Fake", "test", {
        "standard": P("standard", "pass", ("pass",), timeout_s=30),
        "fail": P("fail", "fail", ("fail",), timeout_s=30),
        "slow": P("slow", "sleep 30 s", ("sleep", "30"), timeout_s=60),
        "spawn": P("spawn", "spawns a child", ("spawn", "30"), timeout_s=60),
        "short": P("short", "times out", ("sleep", "30"), timeout_s=1.0),
        "flood": P("flood", "many lines", ("flood", str(MAX_LINES + 500)), timeout_s=60),
        "tmp": P("tmp", "tmpdir", ("tmp",), timeout_s=30),
        "bad": P("bad", "budget below outage", ("sleep", "1"), timeout_s=5, outage_s=10)})}


def wait_state(mgr, run, states, timeout=30):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if run.state in states:
            return run.state
        time.sleep(0.05)
    raise AssertionError(f"run stayed {run.state}")


def pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:                                                # a zombie is dead for our purposes
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(")")[-1].split()[0] != "Z"
    except OSError:
        return True


class TestRegistry(unittest.TestCase):
    def test_all_25_official_tasks_registered(self):
        ids = list(reg.SCENARIOS)
        self.assertEqual(ids, [f"OS{t}-{n}" for t in range(1, 6) for n in range(1, 6)])
        self.assertEqual(reg.check_modules_exist(ROOT), [])

    def test_long_profiles_have_budget_beyond_outage(self):
        for s in reg.SCENARIOS.values():
            for p in s.profiles.values():
                self.assertGreater(p.timeout_s, p.outage_s, (s.id, p.id))
        for sid in ("OS1-5", "OS2-5", "OS3-5", "OS5-5"):
            self.assertEqual(reg.get(sid).profiles["outage600"].outage_s, 600)

    def test_adapter_rejects_bad_input(self):
        for sid, prof in (("OS9-9", "standard"), ("../etc", "standard"), (None, "standard"),
                          ("OS1-1", "nope"), ("OS1-1", None), ("OS1-1", "outage600")):
            with self.assertRaises(reg.RegistryError):
                reg.scenario_command(sid, prof, "python", "/tmp")
        argv, timeout, _ = reg.scenario_command("OS2-5", "outage600", "py", "/r")
        self.assertEqual(argv, ["py", "-m", "edge.teams.os2_robot.s5_offline_navigation",
                                "--offline", "600", "--track", "/r/os2_track.csv"])
        self.assertEqual(timeout, 1200)


@unittest.skipUnless(os.name == "posix", "process-group checks are POSIX")
class TestRunManager(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.mgr = RunManager(ROOT, results_dir=os.path.join(self.tmp.name, "runs"),
                              registry=fake_registry(), extra_pythonpath=[FIX], python=sys.executable)

    def tearDown(self):
        self.mgr.cancel_all()
        self.tmp.cleanup()

    def evidence(self, run):
        with open(os.path.join(run.run_dir, "evidence.json"), encoding="utf-8") as f:
            return json.load(f)

    def test_pass_and_fail_verdicts_with_evidence(self):
        ok = self.mgr.run_sync("FAKE-1", "standard")
        bad = self.mgr.run_sync("FAKE-1", "fail")
        self.assertEqual((ok.state, ok.exit_code), ("passed", 0))
        self.assertEqual((bad.state, bad.exit_code), ("failed", 1))
        e = self.evidence(bad)
        self.assertEqual(e["verdict"], "FAILED")
        self.assertEqual(e["checks"]["verdict_line"], "[FAIL] FAKE-1 fake scenario")
        self.assertEqual(e["checks"]["failed_checks"], ["FAILED: something"])
        self.assertEqual(self.evidence(ok)["checks"]["counts"], {"passed": 3, "total": 3})
        for key in ("invocation", "source", "platform", "seed", "started_at", "duration_s"):
            self.assertIn(key, e)

    def test_fresh_run_dir_tmpdir_and_seed(self):
        a, b = self.mgr.run_sync("FAKE-1", "tmp"), self.mgr.run_sync("FAKE-1", "tmp")
        self.assertNotEqual(a.run_dir, b.run_dir)
        line = [ln for ln in a.lines if ln.startswith("tmp ")][0].split()
        self.assertEqual(line[1], os.path.join(a.run_dir, "tmp"))       # scenario tempfiles land here
        self.assertEqual((line[2], line[3]), (a.run_id, str(a.seed)))
        self.assertFalse(os.path.exists(os.path.join(a.run_dir, "tmp")))  # removed after the run

    def test_duplicate_launch_rejected(self):
        run = self.mgr.start("FAKE-1", "slow")
        wait_state(self.mgr, run, ("running",))
        with self.assertRaises(RuntimeError):
            self.mgr.start("FAKE-1", "standard")
        self.mgr.cancel(run.run_id)

    def test_invalid_input(self):
        for sid, prof in (("NOPE", "standard"), ("FAKE-1", "nope"), ("FAKE-1", "bad")):
            with self.assertRaises(reg.RegistryError):
                self.mgr.start(sid, prof)

    def test_timeout_kills_the_run(self):
        run = self.mgr.run_sync("FAKE-1", "short")
        self.assertEqual(run.state, "timeout")
        self.assertLess(run.elapsed_s, 10)
        self.assertFalse(self.mgr.group_alive(run.pgid))
        self.assertEqual(self.evidence(run)["verdict"], "TIMEOUT")

    def test_cancel_kills_the_whole_process_group(self):
        run = self.mgr.start("FAKE-1", "spawn")
        end = time.monotonic() + 15
        while not any(ln.startswith("child ") for ln in run.lines) and time.monotonic() < end:
            time.sleep(0.05)
        child = int([ln for ln in run.lines if ln.startswith("child ")][0].split()[1])
        self.assertTrue(pid_alive(child))
        self.mgr.cancel(run.run_id)
        wait_state(self.mgr, run, ("cancelled",))
        end = time.monotonic() + 5
        while pid_alive(child) and time.monotonic() < end:
            time.sleep(0.05)
        self.assertFalse(pid_alive(child), "grandchild survived the cancel")   # no orphan
        self.assertFalse(self.mgr.group_alive(run.pgid))
        self.assertEqual(self.mgr.cancel(run.run_id).state, "cancelled")       # idempotent

    def test_log_is_bounded(self):
        run = self.mgr.run_sync("FAKE-1", "flood")
        self.assertEqual(run.state, "passed")
        self.assertEqual(run.line_count, MAX_LINES + 501)
        self.assertEqual(len(run.lines), MAX_LINES)                 # memory bounded
        page = self.mgr.log(run.run_id, 0, 5000)
        self.assertEqual(page["dropped_before"], 501)
        self.assertLessEqual(len(page["lines"]), 2000)               # one response bounded
        self.assertEqual(self.mgr.log(run.run_id, page["next"], 10)["start"], page["next"])


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@unittest.skipUnless(os.name == "posix", "process-group checks are POSIX")
class TestConsoleApi(unittest.TestCase):
    """The HTTP surface the browser uses, on a real server."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cfg = {"node": {"id": "TEST"}, "paths": {"db": os.path.join(cls.tmp.name, "n.db"),
               "schema": os.path.join(ROOT, "sql", "schema.sql"), "static": os.path.join(ROOT, "src", "ui", "static")}}
        cls.storage = Storage(cfg["paths"]["db"], cfg["paths"]["schema"])
        cls.mgr = RunManager(ROOT, results_dir=os.path.join(cls.tmp.name, "runs"), python=sys.executable)
        cls.port = free_port()
        cls.srv = ThreadingHTTPServer(("127.0.0.1", cls.port), make_handler(cfg, cls.storage, cls.mgr))
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.mgr.cancel_all()
        cls.srv.shutdown()
        cls.srv.server_close()
        cls.storage.close()
        cls.tmp.cleanup()

    def call(self, path, body=None, method=None):
        data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                     method=method or ("POST" if data is not None else "GET"))
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read())

    def test_registry_and_console_page(self):
        code, body = self.call("/api/scenarios")
        self.assertEqual(code, 200)
        self.assertEqual(len(body["scenarios"]), 25)
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/", timeout=5) as r:
            self.assertIn(b"Songdo Edge Console", r.read())

    def test_safe_errors(self):
        self.assertEqual(self.call("/api/runs", {"scenario_id": "OS9-9"})[0], 400)
        self.assertEqual(self.call("/api/runs", {"scenario_id": "OS1-1", "profile": "x"})[0], 400)
        self.assertEqual(self.call("/api/runs", {"scenario_id": "edge.teams.os1_building.s1_ingestion"})[0], 400)
        self.assertEqual(self.call("/api/runs", b"not json")[0], 400)
        self.assertEqual(self.call("/api/runs", b"x" * 20000)[0], 400)
        self.assertEqual(self.call("/api/runs/nope")[0], 404)
        self.assertEqual(self.call("/api/runs/../../etc/passwd")[0], 404)
        self.assertEqual(self.call("/api/batches", {"theme": "OS9"})[0], 400)

    def test_run_log_evidence_and_duplicate(self):
        code, run = self.call("/api/runs", {"scenario_id": "OS3-2"})
        self.assertEqual(code, 201)
        self.assertEqual(self.call("/api/runs", {"scenario_id": "OS3-2"})[0], 409)   # duplicate launch
        rid = run["run_id"]
        end = time.monotonic() + 60
        while time.monotonic() < end:
            st = self.call(f"/api/runs/{rid}")[1]
            if st["state"] not in ("queued", "running"):
                break
            time.sleep(0.2)
        self.assertEqual(st["state"], "passed")
        self.assertTrue(st["checks"]["verdict_line"].startswith("[PASS] OS3-2"))
        log = self.call(f"/api/runs/{rid}/log?start=0")[1]
        self.assertGreater(log["total"], 0)
        code, ev = self.call(f"/api/runs/{rid}/evidence")
        self.assertEqual((code, ev["verdict"], ev["scenario"]["id"]), (200, "PASS", "OS3-2"))


class TestUiCliParity(unittest.TestCase):
    """T5 on a fast subset: the CLI script and the console API give identical verdicts."""
    SUBSET = ["OS1-2", "OS2-3", "OS3-2", "OS4-2", "OS5-4"]

    def test_cli_and_api_agree(self):
        with tempfile.TemporaryDirectory() as tmp:
            cli = subprocess.run([sys.executable, os.path.join(ROOT, "scenarios", "run_all_scenarios.py"),
                                  "--results", os.path.join(tmp, "cli", "runs")] +
                                 sum((["--only", s] for s in self.SUBSET[:1]), []),
                                 capture_output=True, text=True, timeout=120)
            self.assertEqual(cli.returncode, 0, cli.stdout + cli.stderr)
            mgr = RunManager(ROOT, results_dir=os.path.join(tmp, "a", "runs"), python=sys.executable)
            mgr2 = RunManager(ROOT, results_dir=os.path.join(tmp, "b", "runs"), python=sys.executable)
            for sid in self.SUBSET:
                a = mgr.run_sync(sid)                       # CLI path (run_sync)
                b = mgr2.start(sid)                         # console path (start, background thread)
                wait_state(mgr2, b, ("passed", "failed", "timeout", "error"), timeout=120)
                self.assertEqual(a.argv[1:], b.argv[1:])
                self.assertEqual((a.state, a.exit_code), (b.state, b.exit_code), sid)
                self.assertEqual(a.state, "passed", sid)
            man = [f for f in os.listdir(os.path.join(tmp, "cli", "manifests"))]
            self.assertEqual(len(man), 1)


if __name__ == "__main__":
    unittest.main()
