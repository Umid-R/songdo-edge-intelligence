"""Read per-process metrics straight from /proc (no psutil needed on Linux).

Linux is the target platform. On a machine without /proc (Windows, macOS) the module still
imports, and read_proc() falls back to psutil when it is installed, else returns None, so the
dashboard shows no metrics instead of the whole program crashing at import time.
"""
import os
import time

try:
    CLK_TCK = os.sysconf("SC_CLK_TCK")      # clock ticks per second for utime/stime
except (AttributeError, ValueError, OSError):
    CLK_TCK = 100                            # Linux default; only used with /proc

HAVE_PROCFS = os.path.isdir("/proc/self")

psutil = None
if not HAVE_PROCFS:                          # optional fallback outside Linux, imported only there
    try:
        import psutil
    except ImportError:                      # pragma: no cover
        psutil = None


def _read_proc_linux(pid):
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as f:
            status = f.read()
        with open(f"/proc/{pid}/stat", encoding="utf-8") as f:
            stat = f.read()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    fields = {}
    for line in status.splitlines():
        key, _, value = line.partition(":")
        fields[key] = value.strip()
    # comm (field 2) may contain spaces/parentheses: split after the last ')'
    rest = stat[stat.rindex(")") + 2:].split()
    return {
        "state": rest[0],
        "ticks": int(rest[11]) + int(rest[12]),          # utime + stime
        "rss_kb": int(fields.get("VmRSS", "0 kB").split()[0]),
        "ctx_voluntary": int(fields.get("voluntary_ctxt_switches", 0)),
        "ctx_involuntary": int(fields.get("nonvoluntary_ctxt_switches", 0)),
    }


def _read_proc_psutil(pid):
    if psutil is None:
        return None
    try:
        p = psutil.Process(pid)
        with p.oneshot():
            t = p.cpu_times()
            ctx = p.num_ctx_switches()
            return {"state": p.status()[:1].upper(), "ticks": int((t.user + t.system) * CLK_TCK),
                    "rss_kb": p.memory_info().rss // 1024,
                    "ctx_voluntary": ctx.voluntary, "ctx_involuntary": ctx.involuntary}
    except (psutil.Error, OSError):
        return None


def read_proc(pid):
    """Return a metrics dict for pid, or None if the process is gone (or cannot be read)."""
    return _read_proc_linux(pid) if HAVE_PROCFS else _read_proc_psutil(pid)


class ProcSampler:
    """Keeps the previous CPU tick count per pid to compute CPU%."""

    def __init__(self):
        self._prev = {}

    def sample(self, pid):
        m = read_proc(pid)
        if m is None:
            self._prev.pop(pid, None)
            return None
        now = time.monotonic()
        cpu = 0.0
        if pid in self._prev:
            t0, ticks0 = self._prev[pid]
            if now > t0:
                cpu = 100.0 * (m["ticks"] - ticks0) / CLK_TCK / (now - t0)
        self._prev[pid] = (now, m["ticks"])
        m["cpu_pct"] = round(max(cpu, 0.0), 2)
        return m
