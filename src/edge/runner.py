"""Scenario run manager: one implementation behind both the CLI runner and the web console.

Every run gets
    - a run ID and its own directory results/runs/<run_id>/ (log, evidence.json, artifacts)
    - its own TMPDIR inside that directory: every scenario creates its SQLite databases and Unix
      sockets with tempfile, so each run has a fresh database and socket namespace
    - a recorded seed (PYTHONHASHSEED and SONGDO_SEED)
    - its own process group, so cancel / timeout stop the scenario AND every child it started
    - a bounded in-memory log (last MAX_LINES lines) and a log file capped at MAX_LOG_BYTES
and ends with one machine-readable evidence.json: argv, exit status, verdict, check lines,
timing, CPU / RSS telemetry of the scenario process, source hash and platform versions.
"""
import collections
import datetime as _dt
import hashlib
import json
import os
import platform
import random
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

from edge import registry as reg

MAX_LINES = 4000              # in memory, per run
MAX_LOG_BYTES = 5_000_000     # on disk, per run
MAX_PARALLEL = 2              # standard runs at the same time (exclusive profiles run alone)
TERMINAL = ("passed", "failed", "timeout", "cancelled", "error")

_VERDICT = re.compile(r"^\s*\[(PASS|FAIL)\]\s*(.*)")
_COUNT = re.compile(r"(\d+)\s*/\s*(\d+)\s+checks passed")


def _now_iso():
    return _dt.datetime.now(_dt.timezone.utc).astimezone().isoformat(timespec="seconds")


def git_revision(root):
    """Current commit, read from .git directly (the git binary is not required)."""
    try:
        head = open(os.path.join(root, ".git", "HEAD"), encoding="utf-8").read().strip()
        if head.startswith("ref: "):
            ref = head[5:]
            path = os.path.join(root, ".git", *ref.split("/"))
            if os.path.exists(path):
                return open(path, encoding="utf-8").read().strip()
            packed = os.path.join(root, ".git", "packed-refs")
            for line in open(packed, encoding="utf-8"):
                if line.strip().endswith(ref):
                    return line.split()[0]
        return head
    except OSError:
        return None


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def platform_info():
    info = {"python": platform.python_version(), "implementation": platform.python_implementation(),
            "platform": platform.platform(), "machine": platform.machine(), "kernel": platform.release(),
            "sqlite": sqlite3.sqlite_version, "cpu_count": os.cpu_count()}
    try:
        with open("/etc/os-release", encoding="utf-8") as f:
            for line in f:
                if line.startswith("PRETTY_NAME="):
                    info["os"] = line.split("=", 1)[1].strip().strip('"')
    except OSError:
        pass
    return info


def parse_checks(lines):
    """Pull the verdict and the check results out of a scenario's output."""
    verdict_line, summary, failed, counts = None, [], [], None
    for ln in lines:
        m = _VERDICT.match(ln)
        if m and verdict_line is None:
            verdict_line = ln.strip()
            continue
        if verdict_line is not None and ln.startswith((" ", "\t")) and len(summary) < 40:
            summary.append(ln.strip())
        if "FAILED" in ln:
            failed.append(ln.strip())
        c = _COUNT.search(ln)
        if c:
            counts = {"passed": int(c.group(1)), "total": int(c.group(2))}
    return {"verdict_line": verdict_line, "summary": summary, "failed_checks": failed[:50], "counts": counts}


class Run:
    def __init__(self, run_id, scenario, profile, argv, timeout_s, run_dir, seed, batch_id=None):
        self.run_id, self.scenario, self.profile = run_id, scenario, profile
        self.argv, self.timeout_s, self.run_dir, self.seed = argv, timeout_s, run_dir, seed
        self.batch_id = batch_id
        self.state = "queued"
        self.created_at = _now_iso()
        self.started_at = self.ended_at = None
        self._t0 = self._t1 = None
        self.exit_code = None
        self.lines = collections.deque(maxlen=MAX_LINES)
        self.line_count = 0                      # total lines ever printed (log cursor)
        self.log_bytes = 0
        self.log_truncated = False
        self.telemetry = collections.deque(maxlen=1200)
        self.error = None
        self.proc = None
        self.pgid = None
        self.cancel_requested = False
        self.lock = threading.Lock()

    @property
    def elapsed_s(self):
        if self._t0 is None:
            return 0.0
        return round((self._t1 or time.monotonic()) - self._t0, 2)

    def summary(self):
        tele = list(self.telemetry)
        return {"run_id": self.run_id, "batch_id": self.batch_id, "scenario_id": self.scenario.id,
                "title": self.scenario.title, "theme": self.scenario.theme, "profile": self.profile.id,
                "profile_label": self.profile.label, "state": self.state, "exit_code": self.exit_code,
                "created_at": self.created_at, "started_at": self.started_at, "ended_at": self.ended_at,
                "elapsed_s": self.elapsed_s, "timeout_s": self.timeout_s, "outage_s": self.profile.outage_s,
                "pid": self.proc.pid if self.proc else None, "line_count": self.line_count,
                "telemetry": tele[-1] if tele else None,
                "peak_rss_kb": max((t["rss_kb"] for t in tele), default=None),
                "verdict_line": parse_checks(self.lines)["verdict_line"], "error": self.error}


class RunManager:
    def __init__(self, root, results_dir=None, python=None, registry=None, extra_pythonpath=(),
                 max_parallel=MAX_PARALLEL):
        self.root = root
        self.results_dir = results_dir or os.path.join(root, "results", "runs")
        self.python = python or self._default_python(root)
        self.registry = registry or reg.SCENARIOS
        self.extra_pythonpath = list(extra_pythonpath)
        self.max_parallel = max_parallel
        self.runs = collections.OrderedDict()
        self.batches = collections.OrderedDict()
        self.lock = threading.RLock()
        self._sem = threading.Semaphore(max_parallel)
        self._threads = []
        os.makedirs(self.results_dir, exist_ok=True)

    @staticmethod
    def _default_python(root):
        for cand in (os.path.join(root, ".venv", "bin", "python"), os.path.join(root, ".venv", "Scripts", "python.exe")):
            if os.path.exists(cand):
                return cand
        return sys.executable

    # ---- validation (shared adapter) ----
    def _command(self, scenario_id, profile_id, run_dir):
        if self.registry is reg.SCENARIOS:
            return reg.scenario_command(scenario_id, profile_id, self.python, run_dir)
        entry = self.registry.get(scenario_id) if isinstance(scenario_id, str) else None
        if entry is None:
            raise reg.RegistryError("Unknown scenario ID")
        prof = entry.profiles.get(profile_id) if isinstance(profile_id, str) else None
        if prof is None:
            raise reg.RegistryError("Unsupported run profile")
        if prof.timeout_s <= prof.outage_s:
            raise reg.RegistryError("Run budget must exceed outage duration")
        args = [a.replace("{run_dir}", run_dir) for a in prof.fixed_args]
        return [self.python, "-m", entry.module, *args], prof.timeout_s, prof

    def _active(self):
        return [r for r in self.runs.values() if r.state in ("queued", "running")]

    def create(self, scenario_id, profile_id="standard", batch_id=None):
        """Validate and register a run (state 'queued'). Raises RegistryError / RuntimeError."""
        with self.lock:
            run_id = time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]
            run_dir = os.path.join(self.results_dir, run_id)
            argv, timeout_s, prof = self._command(scenario_id, profile_id, run_dir)
            active = self._active()
            if batch_id is None:
                if any(r.scenario.id == scenario_id for r in active):
                    raise RuntimeError(f"{scenario_id} is already running")
                if prof.exclusive and active:
                    raise RuntimeError("the 600 s profile runs alone: wait for the active runs to finish")
                if any(r.profile.exclusive for r in active):
                    raise RuntimeError("a 600 s run is in progress: it runs alone")
            os.makedirs(os.path.join(run_dir, "tmp"), exist_ok=True)
            run = Run(run_id, self.registry[scenario_id], prof, argv, timeout_s, run_dir,
                      seed=random.SystemRandom().randrange(1, 2**31), batch_id=batch_id)
            self.runs[run_id] = run
            while len(self.runs) > 500:                       # bounded history in memory
                old = next(iter(self.runs))
                if self.runs[old].state in TERMINAL:
                    self.runs.pop(old)
                else:
                    break
            return run

    def start(self, scenario_id, profile_id="standard"):
        """Launch in the background; returns the Run immediately."""
        run = self.create(scenario_id, profile_id)
        self._spawn_thread(self._execute, (run,), f"run-{run.run_id}")
        return run

    def _spawn_thread(self, target, args, name):
        t = threading.Thread(target=target, args=args, daemon=True, name=name)
        self._threads = [x for x in self._threads if x.is_alive()] + [t]
        t.start()

    def wait_idle(self, timeout=30):
        """Join every background run / batch thread (used on shutdown and by the tests)."""
        end = time.monotonic() + timeout
        for t in list(self._threads):
            t.join(max(0.0, end - time.monotonic()))

    def run_sync(self, scenario_id, profile_id="standard", batch_id=None, echo=None):
        run = self.create(scenario_id, profile_id, batch_id=batch_id)
        self._execute(run, echo=echo)
        return run

    def start_batch(self, scenario_ids, profile_id="standard"):
        """Run several scenarios one after another (the 'Run all 25' button)."""
        with self.lock:
            active = self._active()
            if active:
                raise RuntimeError("finish or cancel the active runs before starting a batch")
            for sid in scenario_ids:                          # validate everything first
                self._command(sid, profile_id, self.results_dir)
            batch_id = "batch-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]
            batch = {"batch_id": batch_id, "profile": profile_id, "scenario_ids": list(scenario_ids),
                     "run_ids": [], "state": "running", "created_at": _now_iso(), "cancelled": False}
            self.batches[batch_id] = batch

        def go():
            for sid in scenario_ids:
                if batch["cancelled"]:
                    break
                run = self.create(sid, profile_id, batch_id=batch_id)
                batch["run_ids"].append(run.run_id)
                self._execute(run)
            batch["state"] = "cancelled" if batch["cancelled"] else "finished"
            batch["ended_at"] = _now_iso()
            self.write_manifest(batch_id, "ui")

        self._spawn_thread(go, (), batch_id)
        return batch

    # ---- execution ----
    def _env(self, run):
        tmp = os.path.join(run.run_dir, "tmp")
        pp = os.pathsep.join([os.path.join(self.root, "src"), *self.extra_pythonpath])
        env = dict(os.environ, PYTHONPATH=pp, PYTHONUNBUFFERED="1", PYTHONHASHSEED=str(run.seed),
                   SONGDO_RUN_ID=run.run_id, SONGDO_SEED=str(run.seed), SONGDO_RUN_DIR=run.run_dir,
                   TMPDIR=tmp, TEMP=tmp, TMP=tmp)
        env.pop("EDGE_CRASH_AT", None)
        return env

    def _execute(self, run, echo=None):
        if run.profile.exclusive:
            got = [self._sem.acquire() for _ in range(self.max_parallel)]   # alone on the machine
        else:
            self._sem.acquire()
            got = [True]
        try:
            if run.cancel_requested:
                run.state = "cancelled"
                return
            self._spawn_and_wait(run, echo)
        except Exception as e:                                # never leave a run "running"
            run.state, run.error = "error", f"{type(e).__name__}: {e}"
        finally:
            for _ in got:
                self._sem.release()
            run._t1 = run._t1 or time.monotonic()
            run.ended_at = run.ended_at or _now_iso()
            self._finish(run)

    def _spawn_and_wait(self, run, echo):
        kwargs = {}
        if os.name == "posix":
            kwargs["start_new_session"] = True                # own session + process group
        else:
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        log_path = os.path.join(run.run_dir, "output.log")
        run.started_at, run._t0 = _now_iso(), time.monotonic()
        run.state = "running"
        run.proc = subprocess.Popen(run.argv, cwd=self.root, env=self._env(run), stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, **kwargs)
        if os.name == "posix":
            run.pgid = os.getpgid(run.proc.pid)
        tele = threading.Thread(target=self._sample, args=(run,), daemon=True)
        tele.start()
        reader = threading.Thread(target=self._read_output, args=(run, log_path, echo), daemon=True)
        reader.start()
        try:
            run.proc.wait(timeout=run.timeout_s)
        except subprocess.TimeoutExpired:
            run.state = "timeout"
            self._kill_group(run)
        run._t1 = time.monotonic()
        run.ended_at = _now_iso()
        self._kill_group(run, grace=0.5)                      # stragglers the scenario left behind
        reader.join(5)
        run.exit_code = run.proc.returncode
        if run.state == "running":
            if run.cancel_requested:
                run.state = "cancelled"
            else:
                run.state = "passed" if run.exit_code == 0 else "failed"

    def _read_output(self, run, log_path, echo):
        with open(log_path, "w", encoding="utf-8", errors="replace") as log:
            for raw in iter(run.proc.stdout.readline, b""):
                line = raw.decode("utf-8", errors="replace").rstrip("\n")
                with run.lock:
                    run.lines.append(line)
                    run.line_count += 1
                if echo:
                    echo(line)
                if run.log_bytes < MAX_LOG_BYTES:
                    log.write(line + "\n")
                    run.log_bytes += len(raw)
                elif not run.log_truncated:
                    run.log_truncated = True
                    log.write(f"[log truncated at {MAX_LOG_BYTES} bytes; the last {MAX_LINES} lines "
                              "are in evidence.json]\n")
            run.proc.stdout.close()

    def _sample(self, run):
        try:
            from edge.procfs import ProcSampler
            sampler = ProcSampler()
        except Exception:                                     # no /proc and no psutil
            return
        while run.proc and run.proc.poll() is None:
            m = sampler.sample(run.proc.pid)
            if m:
                run.telemetry.append({"t": run.elapsed_s, "rss_kb": m["rss_kb"], "cpu_pct": m["cpu_pct"],
                                      "ctx_voluntary": m["ctx_voluntary"],
                                      "ctx_involuntary": m["ctx_involuntary"]})
            time.sleep(1.0)

    def _kill_group(self, run, grace=3.0):
        """SIGTERM the run's whole process group, then SIGKILL whatever is left."""
        p = run.proc
        if p is None:
            return
        if os.name == "posix" and run.pgid:
            for sig, wait in ((signal.SIGTERM, grace), (signal.SIGKILL, 2.0)):
                try:
                    os.killpg(run.pgid, sig)
                except (ProcessLookupError, PermissionError):
                    break                                     # group already gone
                end = time.monotonic() + wait
                while time.monotonic() < end:
                    p.poll()                                  # reap the leader so it is not a zombie
                    if not self.group_alive(run.pgid):
                        break
                    time.sleep(0.05)
                if not self.group_alive(run.pgid):
                    break
        elif p.poll() is None:
            p.terminate()
            try:
                p.wait(grace)
            except subprocess.TimeoutExpired:
                p.kill()
        try:
            p.wait(5)
        except subprocess.TimeoutExpired:
            pass

    @staticmethod
    def group_alive(pgid):
        try:
            os.killpg(pgid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    def cancel(self, run_id):
        run = self.get(run_id)
        if run.state in TERMINAL:
            return run
        run.cancel_requested = True
        if run.batch_id and run.batch_id in self.batches:
            self.batches[run.batch_id]["cancelled"] = True
        if run.state == "running":
            run.state = "cancelled"
            self._kill_group(run)
        return run

    def cancel_all(self, wait=10):
        for b in self.batches.values():
            b["cancelled"] = True
        for r in list(self.runs.values()):
            if r.state in ("queued", "running"):
                self.cancel(r.run_id)
        self.wait_idle(wait)

    def get(self, run_id):
        run = self.runs.get(run_id) if isinstance(run_id, str) else None
        if run is None:
            raise KeyError("Unknown run ID")
        return run

    def log(self, run_id, start=0, limit=1000):
        run = self.get(run_id)
        with run.lock:
            lines = list(run.lines)
            total = run.line_count
        first = total - len(lines)                            # oldest line still in memory
        start = max(int(start), first)
        chunk = lines[start - first:start - first + max(1, min(int(limit), 2000))]
        return {"run_id": run_id, "start": start, "next": start + len(chunk), "total": total,
                "dropped_before": first, "lines": chunk, "state": run.state}

    # ---- evidence ----
    def evidence(self, run):
        src = os.path.join(self.root, "src", *run.scenario.module.split(".")) + ".py"
        lines = list(run.lines)
        artifacts = []
        for dirpath, _, files in os.walk(run.run_dir):
            if os.path.basename(dirpath) == "tmp" or os.sep + "tmp" + os.sep in dirpath + os.sep:
                continue
            for f in files:
                if f != "evidence.json":
                    p = os.path.join(dirpath, f)
                    artifacts.append({"file": os.path.relpath(p, run.run_dir), "bytes": os.path.getsize(p)})
        tele = list(run.telemetry)
        return {
            "schema": "songdo-run-evidence/1",
            "run_id": run.run_id, "batch_id": run.batch_id,
            "scenario": run.scenario.to_dict(),
            "profile": {"id": run.profile.id, "label": run.profile.label, "args": list(run.profile.fixed_args),
                        "timeout_s": run.timeout_s, "outage_s": run.profile.outage_s},
            "invocation": {"argv": [os.path.basename(run.argv[0])] + run.argv[1:],
                           "python_executable": run.argv[0], "cwd": self.root,
                           "env": {"PYTHONPATH": "src", "TMPDIR": "<run_dir>/tmp",
                                   "PYTHONHASHSEED": str(run.seed), "SONGDO_RUN_ID": run.run_id}},
            "seed": run.seed,
            "state": run.state, "exit_code": run.exit_code,
            "verdict": "PASS" if run.state == "passed" else run.state.upper(),
            "started_at": run.started_at, "ended_at": run.ended_at, "duration_s": run.elapsed_s,
            "checks": parse_checks(lines),
            "telemetry": {"samples": len(tele), "peak_rss_kb": max((t["rss_kb"] for t in tele), default=None),
                          "max_cpu_pct": max((t["cpu_pct"] for t in tele), default=None),
                          "series": tele[-120:]},
            "output": {"lines_total": run.line_count, "lines_kept": len(lines), "log_file": "output.log",
                       "log_truncated": run.log_truncated, "tail": lines[-200:]},
            "source": {"module_file": os.path.relpath(src, self.root),
                       "module_sha256": sha256_file(src) if os.path.exists(src) else None,
                       "git_revision": git_revision(self.root)},
            "platform": platform_info(),
            "artifacts": artifacts,
            "error": run.error,
            "data_labels": "see sources/source_registry.csv (REAL / SAMPLE / SIMULATED)",
        }

    def _finish(self, run):
        try:
            shutil.rmtree(os.path.join(run.run_dir, "tmp"), ignore_errors=True)   # fresh DBs are per run
            with open(os.path.join(run.run_dir, "evidence.json"), "w", encoding="utf-8") as f:
                json.dump(self.evidence(run), f, indent=2)
        except OSError as e:
            run.error = run.error or f"evidence not written: {e}"

    def write_manifest(self, batch_id, source):
        """One manifest per batch: every run's verdict, for UI-vs-CLI parity checks (T5)."""
        batch = self.batches[batch_id]
        runs = [self.runs[r] for r in batch["run_ids"] if r in self.runs]
        manifest = {"schema": "songdo-run-manifest/1", "source": source, "batch_id": batch_id,
                    "profile": batch["profile"], "created_at": batch["created_at"],
                    "ended_at": batch.get("ended_at"), "git_revision": git_revision(self.root),
                    "platform": platform_info(),
                    "passed": sum(r.state == "passed" for r in runs), "total": len(runs),
                    "runs": [{"scenario_id": r.scenario.id, "run_id": r.run_id, "state": r.state,
                              "exit_code": r.exit_code, "duration_s": r.elapsed_s} for r in runs]}
        path = os.path.join(os.path.dirname(self.results_dir), "manifests", f"{batch_id}_{source}.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
        batch["manifest"] = os.path.relpath(path, self.root)
        return manifest, path

    def new_cli_batch(self, scenario_ids, profile_id):
        batch_id = "cli-" + time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:4]
        self.batches[batch_id] = {"batch_id": batch_id, "profile": profile_id, "scenario_ids": list(scenario_ids),
                                  "run_ids": [], "state": "running", "created_at": _now_iso(), "cancelled": False}
        return batch_id
