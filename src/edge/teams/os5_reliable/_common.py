"""Shared building blocks for the OS5 scenarios (this file is not a scenario itself).

Crash-safe pipeline used by OS5-2 and OS5-5
-------------------------------------------
  producer --> os5_inbox (SQLite, durable) --> Processor worker --> sensor_events + outbox
                                                    ^ restarted by RestartingSupervisor

The Processor is STATELESS: for each inbox row it does everything in ONE SQLite transaction
(store the event, enqueue it in the outbox, evaluate the flood rule, record the decision, update
the gate state, mark the row done). A SIGKILL at any instant therefore leaves either all of that
row's effects or none of them, and the restarted worker simply continues with the rows that are
not done yet. Result: zero loss and zero duplicates (event_id is also unique).

Honest note: the shared node pipeline (edge/workers.py) hands events over through an in-memory
queue, so a SIGKILL can lose the single event that was in flight. The inbox design below is what
gives the "without data loss" guarantee that OS5-2 asks for.
"""
import json
import multiprocessing as mp
import os
import select
import signal
import threading
import time
from collections import deque

from edge.models import SensorEvent
from edge.storage import Storage
from edge.decision import RuleEngine, dispatch

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
OPEN_ABOVE_M = 2.8            # handbook: open the backup gate above the critical level
CLOSE_AT_OR_BELOW_M = 2.5     # ASSUMED hysteresis (not in the handbook): close again at or below
GATE_KEY = "os5_gate"
BEAT_KEY = "os5_processor_beat"
RECOVERY_LIMIT_S = 2.0


def gate_next_state(current, level, open_above=OPEN_ABOVE_M, close_at=CLOSE_AT_OR_BELOW_M):
    """Pure gate logic: strictly above 2.8 m opens, at/below 2.5 m closes, in between keeps state."""
    if level > open_above:
        return "OPEN"
    if level <= close_at:
        return "CLOSED"
    return current


def flood_level(i):
    """SIMULATED canal water level (m) for reading i: calm, flood from reading 100, then recedes."""
    if i < 100:
        return 1.8
    if i < 110:
        return 3.0
    if i < 130:
        return 3.2
    if i < 140:
        return 2.9
    if i < 150:
        return 2.6
    return 2.0


def make_event(i, node_id, level, observed_at):
    return SensorEvent(node_id=node_id, sensor="SLUICE_WATER_LEVEL_M", value=level, unit="m",
                       observed_at=observed_at, event_id=f"os5-{i:05d}")


# ---------------------------------------------------------------- durable inbox + processor
def init_inbox(st):
    with st.tx() as c:
        c.execute("CREATE TABLE IF NOT EXISTS os5_inbox "
                  "(seq INTEGER PRIMARY KEY, payload TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)")
        c.execute("CREATE TABLE IF NOT EXISTS os5_gate_log "
                  "(id INTEGER PRIMARY KEY AUTOINCREMENT, seq INTEGER, state TEXT, at_wall REAL)")


def inbox_put(st, seq, event):
    with st.tx() as c:
        c.execute("INSERT OR IGNORE INTO os5_inbox (seq,payload) VALUES (?,?)",
                  (seq, json.dumps(event.to_dict())))


def process_row(st, engine, seq, payload):
    """Everything for one inbox row in a single transaction (all-or-nothing)."""
    ev = SensorEvent.from_dict(json.loads(payload))
    ev.received_at = time.time()
    with st.tx() as c:
        cur = c.execute(
            "INSERT OR IGNORE INTO sensor_events (event_id,node_id,sensor,value,unit,observed_at,received_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (ev.event_id, ev.node_id, ev.sensor, ev.value, ev.unit, ev.observed_at, ev.received_at))
        if cur.rowcount and ev.sensor == "SLUICE_WATER_LEVEL_M":
            c.execute("INSERT INTO outbox (event_id,payload,enqueued_at) VALUES (?,?,?)",
                      (ev.event_id, json.dumps(ev.to_dict()), time.time()))
            row = c.execute("SELECT value FROM kv WHERE key=?", (GATE_KEY,)).fetchone()
            state = row[0] if row else "CLOSED"
            for d in engine.evaluate(ev):
                status = dispatch(d)
                c.execute("INSERT INTO edge_decisions (decision_id,event_id,rule,actuator,command_value,"
                          "issued_mono,expires_mono,status,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                          (d.decision_id, d.event_id, d.rule, d.actuator, d.command_value,
                           d.issued_mono, d.expires_mono, status, time.time()))
            new_state = gate_next_state(state, ev.value)
            if new_state != state or row is None:
                c.execute("INSERT INTO kv (key,value) VALUES (?,?) "
                          "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (GATE_KEY, new_state))
                if new_state != state:
                    c.execute("INSERT INTO os5_gate_log (seq,state,at_wall) VALUES (?,?,?)",
                              (seq, new_state, time.time()))
        c.execute("UPDATE os5_inbox SET done=1 WHERE seq=?", (seq,))


def processor_main(db_path, schema_path, stop, rules):
    """The disposable worker. Safe to SIGKILL at any moment."""
    st = Storage(db_path, schema_path, init=False)
    engine = RuleEngine(rules)
    last_beat = 0.0
    while not stop.is_set():
        now = time.monotonic()
        if now - last_beat >= 0.05:
            st.kv_set(BEAT_KEY, json.dumps({"pid": os.getpid(), "t_mono": now}))
            last_beat = now
        rows = st._read("SELECT seq,payload FROM os5_inbox WHERE done=0 ORDER BY seq LIMIT 20")
        if not rows:
            time.sleep(0.01)
            continue
        for r in rows:
            process_row(st, engine, r["seq"], r["payload"])


class Producer(threading.Thread):
    """Simulated sensor front-end: writes one reading per interval into the durable inbox."""

    def __init__(self, st, node_id, n_events, interval_s, level_fn=flood_level):
        super().__init__(daemon=True)
        self.st, self.node_id, self.n, self.interval = st, node_id, n_events, interval_s
        self.level_fn = level_fn
        self.written = 0
        self.flood_mono = None            # monotonic time the first flood reading was written

    def run(self):
        deadline, base = time.monotonic(), time.time()
        for i in range(self.n):
            delay = deadline - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            deadline += self.interval
            ev = make_event(i, self.node_id, self.level_fn(i), base + i * self.interval)
            inbox_put(self.st, i, ev)
            if self.flood_mono is None and ev.value > OPEN_ABOVE_M:
                self.flood_mono = time.monotonic()
            self.written = i + 1


def sync_loop(st, stop_evt):
    """Upstream sync worker (thread): drains the outbox only while the WAN is online."""
    while not stop_evt.is_set():
        if st.wan_online() and st.drain_outbox_to_sink(200):
            continue
        time.sleep(0.05)


def report(st):
    q = lambda sql: st._read(sql)[0]["n"]
    return {
        "events": q("SELECT COUNT(*) AS n FROM sensor_events"),
        "distinct": q("SELECT COUNT(DISTINCT event_id) AS n FROM sensor_events"),
        "outbox": q("SELECT COUNT(*) AS n FROM outbox"),
        "pending": q("SELECT COUNT(*) AS n FROM outbox WHERE sent_at IS NULL"),
        "sink": q("SELECT COUNT(*) AS n FROM upstream_sink"),
        "inbox": q("SELECT COUNT(*) AS n FROM os5_inbox"),
        "inbox_done": q("SELECT COUNT(*) AS n FROM os5_inbox WHERE done=1"),
        "decisions": q("SELECT COUNT(*) AS n FROM edge_decisions"),
    }


# ---------------------------------------------------------------- supervision
class RestartingSupervisor:
    """Restarts a crashed worker. Traps SIGCHLD (handler + wake-up pipe + select), reaps with
    wait() so there are no zombies, and enforces a restart burst limit (like systemd
    StartLimitBurst): too many crashes inside window_s and it gives up instead of looping."""

    def __init__(self, target, args, stop, restart_delay_s=0.1, max_restarts=10, window_s=60.0, ctx=None):
        self.ctx = ctx or mp.get_context("spawn")
        self.target, self.args, self.stop = target, args, stop
        self.restart_delay_s, self.max_restarts, self.window_s = restart_delay_s, max_restarts, window_s
        self.proc = None
        self.restarts = 0
        self.sigchld_count = 0
        self.exit_codes = []
        self.gave_up = False
        self._times = deque()
        self._r, self._w = os.pipe()
        os.set_blocking(self._r, False)
        os.set_blocking(self._w, False)
        self._old_handler, self._old_wakeup = None, -1

    def _on_sigchld(self, signum, frame):
        self.sigchld_count += 1

    def start(self):
        self._old_handler = signal.signal(signal.SIGCHLD, self._on_sigchld)
        self._old_wakeup = signal.set_wakeup_fd(self._w, warn_on_full_buffer=False)
        self._spawn()

    def _spawn(self):
        self.proc = self.ctx.Process(target=self.target, args=self.args, name="os5-worker")
        self.proc.start()

    def wait(self, timeout):
        select.select([self._r], [], [], timeout)
        try:
            while os.read(self._r, 64):
                pass
        except BlockingIOError:
            pass

    def poll(self):
        if self.proc is None or self.gave_up or self.proc.is_alive():
            return False
        self.proc.join(0)
        self.exit_codes.append(self.proc.exitcode)
        now = time.monotonic()
        while self._times and now - self._times[0] > self.window_s:
            self._times.popleft()
        if len(self._times) >= self.max_restarts:
            self.gave_up = True
            return False
        time.sleep(self.restart_delay_s)
        self._times.append(now)
        self.restarts += 1
        self._spawn()
        return True

    def shutdown(self):
        self.stop.set()
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


def wait_until(cond, timeout, sup=None, step=0.02):
    end = time.monotonic() + timeout
    while True:
        value = cond()
        if value:
            return value
        if time.monotonic() >= end:
            raise TimeoutError("condition not reached")
        if sup is not None:
            sup.wait(step)
            sup.poll()
        else:
            time.sleep(step)


def current_beat(st):
    raw = st.kv_get(BEAT_KEY)
    return json.loads(raw) if raw else None


def kill_and_recover(sup, st, timeout=10.0):
    """SIGKILL the current worker; return how long until a NEW worker is alive and working."""
    victim = sup.proc.pid
    kill_mono = time.monotonic()
    os.kill(victim, signal.SIGKILL)
    beat = wait_until(lambda: (lambda b: b if b and b["pid"] != victim else None)(current_beat(st)),
                      timeout, sup)
    return {"victim": victim, "new_pid": beat["pid"], "recovery_s": beat["t_mono"] - kill_mono,
            "kill_mono": kill_mono}


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
