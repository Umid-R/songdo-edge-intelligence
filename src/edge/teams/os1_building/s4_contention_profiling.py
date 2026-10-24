
"""OS1-4: Contention Profiling & Resource Sampling."""

import os
import platform
import sys
import tempfile
import threading
import time
from pathlib import Path

from edge.models import ProcessHealth
from edge.storage import Storage

ROOT = Path(__file__).resolve().parents[4]
SCHEMA = ROOT / "sql" / "schema.sql"


class PlatformProcSampler:
    """Use psutil on Windows and /proc on Linux."""

    def __init__(self):
        self.is_windows = platform.system() == "Windows"

        if self.is_windows:
            try:
                import psutil
            except ImportError as exc:
                raise RuntimeError(
                    "Install psutil with: py -m pip install psutil"
                ) from exc

            self.psutil = psutil
        else:
            from edge.procfs import ProcSampler
            self.linux_sampler = ProcSampler()
            

    def sample(self, pid):
        if not self.is_windows:
            return self.linux_sampler.sample(pid)

        try:
            process = self.psutil.Process(pid)
            memory = process.memory_info()
            switches = process.num_ctx_switches()

            return {
                "state": process.status(),
                "ticks": 0,
                "rss_kb": memory.rss // 1024,
                "cpu_pct": process.cpu_percent(interval=None),
                "ctx_voluntary": switches.voluntary,
                "ctx_involuntary": switches.involuntary,
            }

        except (
            self.psutil.NoSuchProcess,
            self.psutil.AccessDenied,
            self.psutil.ZombieProcess,
        ):
            return None


def sample_once(db, sampler, pid=None, name="os1-s4-contention"):
    """Sample one PID and persist its metrics."""

    pid = os.getpid() if pid is None else int(pid)
    metrics = sampler.sample(pid)

    if metrics is None:
        return None

    health = ProcessHealth(
        name=name,
        pid=pid,
        state=metrics["state"],
        rss_kb=metrics["rss_kb"],
        cpu_pct=metrics["cpu_pct"],
        ctx_voluntary=metrics["ctx_voluntary"],
        ctx_involuntary=metrics["ctx_involuntary"],
        restarts=0,
    )

    db.upsert_health(health)
    return metrics


def run(samples=3, interval_s=2.0, db_path=None, pid=None):
    """Sample resource metrics under synthetic CPU load."""

    if samples < 1 or interval_s < 0:
        raise ValueError("samples must be >= 1 and interval_s must be >= 0")

    temp_dir = tempfile.TemporaryDirectory() if db_path is None else None
    stop = threading.Event()

    try:
        path = db_path or os.path.join(temp_dir.name, "os1_s4.db")
        db = Storage(path, str(SCHEMA))

        # Create the sampler here, inside run().
        sampler = PlatformProcSampler()

        def synthetic_load():
            value = 1
            while not stop.is_set():
                value = (value * 1103515245 + 12345) & 0x7FFFFFFF

        worker = threading.Thread(
            target=synthetic_load,
            name="os1-synthetic-load",
            daemon=True,
        )
        worker.start()

        collected = []

        try:
            for index in range(samples):
                metrics = sample_once(
                    db,
                    sampler,
                    pid=pid,
                    name="os1-s4-contention",
                )

                if metrics is not None:
                    collected.append(metrics)

                if index + 1 < samples and interval_s:
                    time.sleep(interval_s)

        finally:
            stop.set()
            worker.join(timeout=1.0)

        rows = db.list_health()
        latest = rows[0] if rows else {}

        ok = (
            len(collected) == samples
            and latest.get("rss_kb", 0) > 0
            and "ctx_voluntary" in latest
            and "ctx_involuntary" in latest
        )

        source = "psutil" if platform.system() == "Windows" else "/proc"

        return ok, [
            f"samples collected: {len(collected)} (requested {samples})",
            f"RSS: {latest.get('rss_kb', 0)} KiB; "
            f"CPU: {latest.get('cpu_pct', 0)}%",
            f"context switches: voluntary={latest.get('ctx_voluntary', 0)}, "
            f"involuntary={latest.get('ctx_involuntary', 0)}",
            f"metrics collected using {source} and persisted in process_health",
        ]

    finally:
        if temp_dir is not None:
            temp_dir.cleanup()


def main():
    ok, lines = run()

    print(
        f"[{'PASS' if ok else 'FAIL'}] "
        "OS1-4 Contention profiling & resource sampling"
    )

    for line in lines:
        print("      ", line)

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
