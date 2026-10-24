"""Read per-process metrics straight from /proc (no psutil needed)."""
import os
import time

CLK_TCK = os.sysconf("SC_CLK_TCK")


def read_proc(pid):
    """Return a metrics dict for pid, or None if the process is gone."""
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
