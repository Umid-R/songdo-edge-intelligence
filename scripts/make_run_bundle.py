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
    7. --node-outage: the supervised node (config/os2.yaml by default) with its receiver process:
       WAN cut for N s, the Processor SIGKILLed in the middle of the cut, WAN restored, generation
       frozen at a recorded boundary, backlog drained, then G = C = A = R and zero pending checked
       on both sides of the receiver boundary. Trace on the monotonic clock every 5 s.
    8. --long: the four outage600 scenario profiles, one at a time.
Output: results/bundle_<timestamp>/ (manifest.json, logs/, runs/, manifests/, node_outage/) + .zip
"""
import argparse
import hashlib
import json
import os
import platform
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.request

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


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def http(port, path, method="GET"):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def node_outage(out, outage_s, config):
    """Step 7: the integrated outage on the real supervised node, both sides of the receiver."""
    import yaml
    d = os.path.join(out, "node_outage")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(ROOT, "config", config), encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    port = free_port()
    cfg["network"]["api_port"] = port
    cfg["upstream"]["port"] = free_port()
    cfg["paths"].update({"db": os.path.join(d, "node.db"), "upstream_db": os.path.join(d, "receiver.db"),
                         "pidfile": os.path.join(d, "pids.json"), "backups": os.path.join(d, "backups"),
                         "socket": os.path.join(d, "node.sock"),
                         "schema": os.path.join(ROOT, "sql", "schema.sql"),
                         "static": os.path.join(ROOT, "src", "ui", "static"),
                         "results": os.path.join(d, "runs")})
    os.makedirs(os.path.join(d, "config"), exist_ok=True)
    cfg_path = os.path.join(d, "config", "node.yaml")
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f)
    trace, events = [], []

    def mark(what, **kw):
        e = {"event": what, "mono": round(time.monotonic(), 3), "wall": time.strftime("%H:%M:%S"), **kw}
        events.append(e)
        print(f"   [{e['wall']}] {what} {kw if kw else ''}", flush=True)

    def sample(phase):
        s = http(port, "/api/status")
        trace.append({"mono": round(time.monotonic(), 3), "phase": phase, "wan_online": s["wan_online"],
                      "events": s["counts"]["events"], "decisions": s["counts"]["decisions"],
                      "receipts": s["counts"]["receipts"], "pending": s["outbox_pending"],
                      "receiver": s["conservation"]["sink"], "conserved": s["conservation"]["conserved"],
                      "restarts": {p["name"]: p["restarts"] for p in s["processes"]}})
        return s

    log = open(os.path.join(d, "supervisor.log"), "w", encoding="utf-8")
    proc = subprocess.Popen([python_exe(), "-m", "edge.supervisor", "--config", cfg_path], cwd=ROOT,
                            env=dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src")),
                            stdout=log, stderr=subprocess.STDOUT)
    result = {"config": config, "outage_s": outage_s}
    try:
        for _ in range(80):
            try:
                http(port, "/api/status")
                break
            except OSError:
                time.sleep(0.25)
        mark("node started", pid=proc.pid)
        time.sleep(10)
        sample("online")
        http(port, "/api/wan/cut", "POST")
        mark("WAN cut")
        t_cut, killed = time.monotonic(), False
        while time.monotonic() - t_cut < outage_s:
            time.sleep(5)
            s = sample("offline")
            if not killed and time.monotonic() - t_cut >= outage_s / 2:
                with open(cfg["paths"]["pidfile"], encoding="utf-8") as f:
                    pid = json.load(f)["Processor"]["pid"]
                os.kill(pid, signal.SIGKILL)
                killed = True
                mark("Processor SIGKILLed during the cut", pid=pid)
        result["during_cut"] = {"receiver_at_cut": trace[0]["receiver"] if trace else None,
                                "pending_before_restore": s["outbox_pending"]}
        http(port, "/api/wan/restore", "POST")
        mark("WAN restored")
        for _ in range(12):
            time.sleep(5)
            sample("draining")
        http(port, "/api/generation/pause", "POST")
        mark("generation frozen (recorded boundary)")
        time.sleep(3)
        for _ in range(120):
            s = sample("frozen")
            if s["outbox_pending"] == 0:
                break
            time.sleep(1)
        mark("backlog drained", pending=s["outbox_pending"])
        rec = http(port, "/api/reconcile")
        result.update(reconcile=rec, recovery=s["recovery"], final_status={
            k: s[k] for k in ("counts", "outbox_pending", "conservation", "receiver", "queue", "sync")})
        restarts = trace[-1]["restarts"].get("Processor", 0)
        result["checks"] = {
            "node kept committing events during the cut":
                any(t["phase"] == "offline" for t in trace) and
                max(t["events"] for t in trace if t["phase"] == "offline") >
                min(t["events"] for t in trace if t["phase"] == "offline"),
            "receiver got nothing new while the WAN was cut":
                len({t["receiver"] for t in trace if t["phase"] == "offline"}) <= 1,
            "Processor was restarted by the supervisor": restarts >= 1,
            "recovery exit rule holds (no admitted event lost, receipts = dispatched)": bool(s["recovery"]["ok"]),
            "G = C = A = R and zero pending after drain": bool(rec.get("fully_drained")),
            "no false 'sent' and no unknown IDs at the receiver":
                rec.get("acked_but_not_at_receiver") == 0 and rec.get("at_receiver_unknown_to_node") == 0,
        }
        result["passed"] = all(result["checks"].values())
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(30)
        except subprocess.TimeoutExpired:
            proc.kill()
        log.close()
        result["supervisor_exit_code"] = proc.returncode
        result["events"], result["trace"] = events, trace
        with open(os.path.join(d, "node_outage.json"), "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
        with open(os.path.join(d, "trace.csv"), "w", encoding="utf-8") as f:
            f.write("mono,phase,wan_online,events,decisions,receipts,pending,receiver,conserved\n")
            for t in trace:
                f.write(",".join(str(t[k]) for k in ("mono", "phase", "wan_online", "events", "decisions",
                                                     "receipts", "pending", "receiver", "conserved")) + "\n")
        for name in ("node.db", "node.db-wal", "node.db-shm", "receiver.db", "receiver.db-wal", "receiver.db-shm"):
            p = os.path.join(d, name)
            if os.path.exists(p):
                os.remove(p)                       # keep the bundle small: the JSON holds the evidence
    for k, v in result.get("checks", {}).items():
        print(f"   {'ok    ' if v else 'FAILED'} {k}")
    return result


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--node-outage", type=float, default=0, help="integrated node WAN cut in seconds (600 = handbook)")
    ap.add_argument("--node-config", default="os2.yaml", help="theme config for the node outage")
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
    b.step("unit_and_scenario_tests", [py, "-m", "unittest", "discover", "-s", "tests", "-v"], timeout=1800)
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
        print(f"\n== integrated node outage: {args.node_outage:.0f} s on {args.node_config}")
        node = node_outage(out, args.node_outage, args.node_config)

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
        {"passed": node.get("passed"), "checks": node.get("checks"), "file": "node_outage/node_outage.json"},
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
