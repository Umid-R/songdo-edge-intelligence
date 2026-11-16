#!/usr/bin/env python3
"""Build the Linux run bundle the assessment asks for: one folder (and zip) that proves the code
runs, with the exact versions, source hash, commands, exit codes, logs and evidence.

    python3 scripts/make_run_bundle.py                       # ~8 min: checks + 25 scenarios twice
    python3 scripts/make_run_bundle.py --node-outage 600     # + the integrated 10-minute node trace
    python3 scripts/make_run_bundle.py --long                # + the four 600 s scenario profiles (~45 min)
    python3 scripts/make_run_bundle.py --node-outage 600 --long   # everything (~1 h), idle machine

Steps (each one's command, exit code, duration and output are recorded):
    1. scripts/preflight.sh                      environment audit
    2. tests/test_os_fundamentals.py             the five OS checks
    3. unittest discover -s tests                every unit / scenario / T1-T5 test
    4. integration/test_mock_upstream.py         outbox -> HTTP receiver across a WAN cut
    5. all 25 scenarios via the CLI path         (RunManager.run_sync)       -> manifest
    6. all 25 scenarios via the console path     (RunManager.start, threads) -> manifest + parity
    7. --node-outage 600: the console task NODE-OUTAGE (src/edge/node_outage.py) through the same runner
       as a UI run: supervised node + receiver process, WAN cut, Processor SIGKILLed halfway, WAN
       restored, generation frozen at a recorded boundary, backlog drained, then G = C = A = R and zero
       pending on both sides of the receiver boundary; monotonic trace every 5 s, API polled throughout.
    8. --long: the four outage600 scenario profiles, one at a time.
Output: results/bundle_<timestamp>/ (manifest.json, logs/, runs/<run_id>/..., manifests/, test_evidence/) + .zip
"""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge import registry as reg                                  # noqa: E402
from edge.runner import RunManager, git_revision, platform_info   # noqa: E402


def python_exe():
    venv = os.path.join(ROOT, ".venv", "bin", "python")
    return venv if os.path.exists(venv) else sys.executable


def source_tree_hash():
    """sha256 over every tracked source file (path + content), so the bundle names exact code."""
    files = []
    for top in ("src", "tests", "scripts", "scenarios", "integration", "sql", "config", "systemd", "container"):
        for dirpath, dirs, names in os.walk(os.path.join(ROOT, top)):
            dirs[:] = [d for d in dirs if d != "__pycache__"]
            files += [os.path.join(dirpath, n) for n in names if not n.endswith(".pyc")]
    h = hashlib.sha256()
    for p in sorted(files):
        h.update(os.path.relpath(p, ROOT).encode())
        with open(p, "rb") as f:
            h.update(hashlib.sha256(f.read()).digest())
    return h.hexdigest(), len(files)


class Bundle:
    def __init__(self, out):
        self.out = out
        self.steps = []
        os.makedirs(os.path.join(out, "logs"), exist_ok=True)

    def step(self, name, argv, timeout=1800, env=None):
        print(f"\n== {name}: {' '.join(argv)}", flush=True)
        t0 = time.monotonic()
        try:
            p = subprocess.run(argv, cwd=ROOT, capture_output=True, text=True, timeout=timeout,
                               env=dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src"), **(env or {})))
            code, out = p.returncode, p.stdout + ("\n--- stderr ---\n" + p.stderr if p.stderr else "")
        except subprocess.TimeoutExpired as e:
            code, out = "timeout", (e.stdout or b"").decode(errors="replace") if isinstance(e.stdout, bytes) else (e.stdout or "")
        dt = round(time.monotonic() - t0, 1)
        log = os.path.join("logs", f"{len(self.steps) + 1:02d}_{name}.txt")
        with open(os.path.join(self.out, log), "w", encoding="utf-8") as f:
            f.write(out)
        tail = [ln for ln in out.strip().splitlines() if ln.strip()][-3:]
        print(f"   exit {code} in {dt} s :: " + " | ".join(tail)[-300:], flush=True)
        self.steps.append({"name": name, "argv": argv, "exit_code": code, "duration_s": dt, "log": log,
                           "ok": code == 0})
        return code == 0


def run_all(mgr, source):
    """All 25 standard runs; source 'cli' = run_sync, 'ui' = start() in background threads."""
    ids = list(reg.SCENARIOS)
    batch = mgr.new_cli_batch(ids, "standard")
    for sid in ids:
        if source == "cli":
            run = mgr.run_sync(sid, "standard", batch_id=batch)
        else:
            run = mgr.start(sid, "standard")
            run.batch_id = batch
            while run.state in ("queued", "running"):
                time.sleep(0.2)
        mgr.batches[batch]["run_ids"].append(run.run_id)
        print(f"   {source} {sid:6} {run.state:8} {run.elapsed_s:6.1f} s", flush=True)
    mgr.batches[batch]["state"] = "finished"
    manifest, _ = mgr.write_manifest(batch, source)
    return manifest


def node_outage(out, outage_s):
    """Step 7 runs through the runner like a console run (registry entry NODE-OUTAGE), so the bundle's
    fault/restore trace IS a UI-path run with the usual evidence.json next to its artifacts."""
    mgr = RunManager(ROOT, results_dir=os.path.join(out, "runs"), python=python_exe())
    run = mgr.run_sync("NODE-OUTAGE", f"outage{int(outage_s)}",
                       echo=lambda line: print("   |", line, flush=True))
    path = os.path.join(run.run_dir, "node_outage", "node_outage.json")
    try:
        with open(path, encoding="utf-8") as f:
            result = json.load(f)
    except (OSError, ValueError):
        result = {"passed": False, "error": "no node_outage.json"}
    result["run_id"], result["state"] = run.run_id, run.state
    result["file"] = os.path.relpath(path, out)
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--node-outage", type=int, default=0, choices=[0, 60, 600],
                    help="integrated node WAN cut in seconds (600 = handbook), run as the NODE-OUTAGE console task")
    ap.add_argument("--long", action="store_true", help="also run the four outage600 scenario profiles")
    ap.add_argument("--no-parity", action="store_true", help="skip the second (console path) 25-run pass")
    ap.add_argument("--out", help="output folder (default results/bundle_<timestamp>)")
    args = ap.parse_args()

    stamp = time.strftime("%Y%m%d-%H%M%S")
    out = args.out or os.path.join(ROOT, "results", f"bundle_{stamp}")
    b = Bundle(out)
    tree, nfiles = source_tree_hash()
    py = python_exe()
    started = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    print(f"bundle -> {out}\nsource tree sha256 {tree} ({nfiles} files), git {git_revision(ROOT)}")

    if os.name == "posix" and shutil.which("bash"):
        b.step("preflight", ["bash", "scripts/preflight.sh"], timeout=120)
    b.step("os_fundamentals", [py, "tests/test_os_fundamentals.py"], timeout=600)
    b.step("unit_and_scenario_tests", [py, "-m", "unittest", "discover", "-s", "tests", "-v"], timeout=1800,
           env={"SONGDO_EVIDENCE_DIR": os.path.join(out, "test_evidence")})     # T1/T2/T3/T4 traces
    b.step("integration_receiver", [py, "integration/test_mock_upstream.py"], timeout=300)

    print("\n== 25 scenarios, CLI path")
    cli = run_all(RunManager(ROOT, results_dir=os.path.join(out, "runs"), python=py), "cli")
    parity = None
    if not args.no_parity:
        print("\n== 25 scenarios, console path")
        ui = run_all(RunManager(ROOT, results_dir=os.path.join(out, "runs"), python=py), "ui")
        a = {r["scenario_id"]: (r["state"], r["exit_code"]) for r in cli["runs"]}
        c = {r["scenario_id"]: (r["state"], r["exit_code"]) for r in ui["runs"]}
        parity = {"scenarios": len(a), "identical_verdicts": sum(a[k] == c.get(k) for k in a),
                  "mismatches": {k: {"cli": a[k], "ui": c.get(k)} for k in a if a[k] != c.get(k)}}
        print(f"   parity: {parity['identical_verdicts']}/{parity['scenarios']} identical verdicts")

    node = None
    if args.node_outage:
        print(f"\n== integrated node outage: {args.node_outage} s (console task NODE-OUTAGE)")
        node = node_outage(out, args.node_outage)

    long_runs = []
    if args.long:
        print("\n== 600 s scenario profiles (one at a time)")
        mgr = RunManager(ROOT, results_dir=os.path.join(out, "runs"), python=py)
        for sid in ("OS1-5", "OS2-5", "OS3-5", "OS5-5"):
            run = mgr.run_sync(sid, "outage600")
            long_runs.append({"scenario_id": sid, "run_id": run.run_id, "state": run.state,
                              "exit_code": run.exit_code, "duration_s": run.elapsed_s})
            print(f"   {sid} outage600 {run.state} {run.elapsed_s:.0f} s", flush=True)

    manifest = {
        "schema": "songdo-run-bundle/1", "started_at": started, "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "git_revision": git_revision(ROOT), "source_tree_sha256": tree, "source_files": nfiles,
        "platform": platform_info(), "python_executable": py,
        "dependencies": subprocess.run([py, "-m", "pip", "freeze"], capture_output=True, text=True).stdout.split(),
        "handbook_platform": "Ubuntu 24.04 LTS, Python 3.12",
        "steps": b.steps,
        "scenarios_cli": {"passed": cli["passed"], "total": cli["total"], "skipped": cli.get("skipped", 0)},
        "parity": parity, "node_outage": None if node is None else
        {"passed": node.get("passed"), "checks": node.get("checks"), "run_id": node.get("run_id"),
         "file": node.get("file")},
        "long_runs": long_runs,
        "fixtures": "SAMPLE / SIMULATED data are labelled in sources/source_registry.csv",
    }
    with open(os.path.join(out, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    shutil.make_archive(out, "zip", os.path.dirname(out), os.path.basename(out))
    ok = all(s["ok"] for s in b.steps) and cli["passed"] == cli["total"] and \
        (parity is None or not parity["mismatches"]) and (node is None or node.get("passed")) and \
        all(r["state"] == "passed" for r in long_runs)
    print(f"\n{'BUNDLE OK' if ok else 'BUNDLE HAS FAILURES'}: {out}.zip")
    print(f"  steps {sum(s['ok'] for s in b.steps)}/{len(b.steps)} ok · scenarios {cli['passed']}/{cli['total']}"
          + (f" · parity {parity['identical_verdicts']}/{parity['scenarios']}" if parity else "")
          + (f" · node outage {'PASS' if node.get('passed') else 'FAIL'}" if node else "")
          + (f" · long {sum(r['state'] == 'passed' for r in long_runs)}/{len(long_runs)}" if long_runs else ""))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
