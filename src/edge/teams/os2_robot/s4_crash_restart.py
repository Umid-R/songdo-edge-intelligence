"""OS2-4: Worker Crash & Supervised Automatic Restart

Mission : Kill the motion worker with SIGKILL and prove the supervisor brings it back within
          2.0 s, with its state restored.
Concept : The supervisor TRAPS SIGCHLD (the kernel's "a child changed state" signal). The
          handler only bumps a counter; the signal also writes a byte into a wake-up pipe
          that wakes the supervisor's select() loop, which then reaps the dead child with
          wait() (no zombie) and spawns a replacement. State survives because the worker
          writes its progress to SQLite (WAL) on every tick, and a fresh worker reads it back.

Owner   : Umid, Umrbek
Test    : tests/os2/test_s4_crash_restart.py
Run     : PYTHONPATH=src python -m edge.teams.os2_robot.s4_crash_restart

Lesson learned while building this (worth explaining in the defense): the stop flag is a
lock-free shared integer (RawValue), NOT a multiprocessing.Event. If a process is SIGKILLed
while blocked inside Event.wait(), the Event's internal condition keeps a phantom sleeper
and the supervisor's later Event.set() blocks forever. A raw shared int has no lock to poison.

Measured: recovery time = (monotonic time of the new worker's first heartbeat) - (monotonic
time just before the kill). The monotonic clock is system-wide on Linux, so values from two
different processes are comparable. The worker's ticks are simulated motion progress.
"""
import json
import multiprocessing as mp
import os
import select
import signal
import sys
import tempfile
import time

from edge.storage import Storage

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
STATE_KEY = "os2_motion_state"
TICK_S = 0.05
RECOVERY_LIMIT_S = 2.0


def motion_worker(db_path, schema_path, stop):
    """The disposable worker: restores its state, then advances and saves it every tick."""
    st = Storage(db_path, schema_path, init=False)
    prev = st.kv_get(STATE_KEY)
    restored_from = json.loads(prev)["ticks"] if prev else 0
    ticks = restored_from
    while not stop.value:
        ticks += 1
        st.kv_set(STATE_KEY, json.dumps({"pid": os.getpid(), "ticks": ticks,
                                         "restored_from": restored_from,
                                         "t_mono": time.monotonic()}))
        time.sleep(TICK_S)


class MotionSupervisor:
    def __init__(self, db_path, schema_path, restart_delay_s=0.1):
        self.ctx = mp.get_context("spawn")
        self.db_path, self.schema_path = db_path, schema_path
        self.restart_delay_s = restart_delay_s
        self.stop = self.ctx.RawValue("i", 0)     # lock-free stop flag (see docstring)
        self.proc = None
        self.restarts = 0
        self.sigchld_count = 0
        self.last_exitcode = None
        self._r, self._w = os.pipe()
        os.set_blocking(self._r, False)
        os.set_blocking(self._w, False)
        self._old_handler = None
        self._old_wakeup = -1

    def _on_sigchld(self, signum, frame):
        self.sigchld_count += 1                   # keep handlers tiny: just record it

    def start(self):
        self._old_handler = signal.signal(signal.SIGCHLD, self._on_sigchld)
        self._old_wakeup = signal.set_wakeup_fd(self._w, warn_on_full_buffer=False)
        self._spawn()

    def _spawn(self):
        self.proc = self.ctx.Process(target=motion_worker, name="MotionWorker",
                                     args=(self.db_path, self.schema_path, self.stop))
        self.proc.start()

    def wait(self, timeout):
        """Sleep until a signal arrives on the wake-up pipe (or the timeout passes)."""
        select.select([self._r], [], [], timeout)
        try:
            while os.read(self._r, 64):
                pass
        except BlockingIOError:
            pass

    def poll(self):
        """If the worker died: reap it (wait), then restart it. True when restarted."""
        if self.proc is not None and not self.proc.is_alive():
            self.proc.join(0)
            self.last_exitcode = self.proc.exitcode
            time.sleep(self.restart_delay_s)
            self.restarts += 1
            self._spawn()
            return True
        return False

    def shutdown(self):
        self.stop.value = 1
        if self.proc is not None:
            self.proc.join(5)
            if self.proc.is_alive():
                self.proc.terminate()
                self.proc.join(2)
        signal.set_wakeup_fd(self._old_wakeup)
        if self._old_handler is not None:
            signal.signal(signal.SIGCHLD, self._old_handler)
        os.close(self._r)
        os.close(self._w)


def zombie_children():
    """Defunct (state Z) direct children of this process, read from /proc."""
    me, out = str(os.getpid()), []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat", encoding="utf-8") as f:
                stat = f.read()
        except OSError:
            continue
        rest = stat[stat.rindex(")") + 2:].split()
        if rest[1] == me and rest[0] == "Z":
            out.append(int(name))
    return out


def _state(st):
    raw = st.kv_get(STATE_KEY)
    return json.loads(raw) if raw else None


def _wait_state(st, sup, cond, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        sup.wait(0.02)
        sup.poll()
        s = _state(st)
        if s and cond(s):
            return s
    raise TimeoutError("condition not reached")


def run(kill_after_ticks=10, tail_ticks=10):
    """Returns (ok, report_lines)."""
    schema = os.path.join(ROOT, "sql", "schema.sql")
    with tempfile.TemporaryDirectory() as tmp:
        db = os.path.join(tmp, "os2_motion.db")
        st = Storage(db, schema)
        sup = MotionSupervisor(db, schema)
        try:
            sup.start()
            before = _wait_state(st, sup, lambda s: s["ticks"] >= kill_after_ticks)
            victim = before["pid"]
            kill_mono = time.monotonic()
            os.kill(victim, signal.SIGKILL)                       # inject the crash
            after = _wait_state(st, sup, lambda s: s["pid"] != victim)
            recovery_s = after["t_mono"] - kill_mono
            final = _wait_state(st, sup, lambda s: s["ticks"] >= after["ticks"] + tail_ticks)
            zombies_running = zombie_children()
        except TimeoutError as e:
            return False, [f"timeout: {e}"]
        finally:
            sup.shutdown()
        zombies_after = zombie_children()

    checks = {
        f"recovered in {recovery_s:.3f} s (limit {RECOVERY_LIMIT_S} s)": recovery_s < RECOVERY_LIMIT_S,
        f"killed worker exit code {sup.last_exitcode} (-9 = SIGKILL)": sup.last_exitcode == -9,
        f"SIGCHLD trapped {sup.sigchld_count}x by the handler": sup.sigchld_count >= 1,
        "supervisor restarted it exactly once": sup.restarts == 1,
        "new worker has a different pid": after["pid"] != victim,
        f"state restored: ticks before kill {before['ticks']}, new worker resumed from "
        f"{after['restored_from']}": after["restored_from"] >= before["ticks"],
        f"progress continues after restart (ticks now {final['ticks']})": final["ticks"] > after["ticks"],
        "no zombie processes (while running and after shutdown)": not zombies_running and not zombies_after,
    }
    return all(checks.values()), [f"{'ok ' if v else 'FAILED '}{k}" for k, v in checks.items()]


def main():
    ok, lines = run()
    print(f"[{'PASS' if ok else 'FAIL'}] OS2-4 Worker crash & supervised restart")
    for line in lines:
        print("      ", line)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
