"""The one scenario registry, shared by the CLI runner and the web console.

Every official handbook task (OS1-1 ... OS5-5) is listed here explicitly: ID -> module, title,
team and the run profiles it supports. Nothing is discovered by scanning files and nothing the
browser sends is ever used as a module path or as command text: a run request names a scenario ID
and a profile ID, and scenario_command() turns that into a fixed argv (the assessment's adapter).
"""
import os
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

THEMES = {
    "OS1": ("OS1 Smart Building Edge Gateway", "os1_building", "Songdo Convensia", "Aidar, Arkar"),
    "OS2": ("OS2 Robot Edge Controller", "os2_robot", "Songdo Central Park", "Umid, Umrbek"),
    "OS3": ("OS3 Autonomous Mobility Edge Node", "os3_mobility", "Techno-Park Intersection", "Arlen, Onill"),
    "OS4": ("OS4 IoT Edge Gateway", "os4_iot", "Techno-Strip Tower", "Nazarbek"),
    "OS5": ("OS5 Fault-Tolerant Smart-City Node", "os5_reliable", "Waterfront Lake Sluice", "Umidillo"),
    "NODE": ("Integrated node (shared by all themes)", "", "Supervised node + receiver process", "Team A"),
}


@dataclass(frozen=True)
class Profile:
    id: str
    label: str
    fixed_args: Tuple[str, ...] = ()
    timeout_s: float = 240.0
    outage_s: float = 0.0          # length of the WAN cut this profile asks for (0 = none)
    exclusive: bool = False        # long-run timing profile: run it alone on an idle machine


@dataclass(frozen=True)
class Scenario:
    id: str
    theme: str
    module: str
    title: str
    concept: str
    profiles: Dict[str, Profile] = field(default_factory=dict)
    requires_linux: str = ""       # non-empty: the Linux kernel feature it demonstrates on purpose
    fixtures: str = ""             # what data it runs on (REAL / SAMPLE / SIMULATED), for the evidence
    fault: str = ""                # which fault or stress it injects, for the evidence

    @property
    def members(self):
        return THEMES[self.theme][3]

    def to_dict(self):
        return {"id": self.id, "theme": self.theme, "theme_title": THEMES[self.theme][0],
                "site": THEMES[self.theme][2], "members": self.members, "module": self.module,
                "title": self.title, "concept": self.concept, "requires_linux": self.requires_linux,
                "fixtures": self.fixtures, "fault": self.fault,
                "profiles": [{"id": p.id, "label": p.label, "args": list(p.fixed_args),
                              "timeout_s": p.timeout_s, "outage_s": p.outage_s, "exclusive": p.exclusive}
                             for p in self.profiles.values()]}


STANDARD = Profile("standard", "Standard check")


def _outage_profiles(flag, extra=()):
    """60 s demo cut and the handbook's 10-minute cut. The timeout covers the cut plus drain."""
    return {
        "outage60": Profile("outage60", "WAN cut 60 s", (flag, "60", *extra), timeout_s=240, outage_s=60),
        "outage600": Profile("outage600", "WAN cut 600 s (handbook)", (flag, "600", *extra),
                             timeout_s=1200, outage_s=600, exclusive=True),
    }


# Scenarios that use a Linux kernel feature on purpose. The handbook platform is Ubuntu 24.04;
# on another OS the runner reports these as SKIPPED with this reason instead of failing.
LINUX_ONLY = {
    "OS1-4": "/proc/<pid>/stat and /proc/<pid>/status sampling",
    "OS2-1": "AF_UNIX domain sockets",
    "OS2-4": "SIGKILL, SIGCHLD and wait()",
    "OS4-1": "selectors (epoll) on non-blocking pipes",
    "OS4-4": "flock(1) and the systemd timer",
    "OS5-1": "SIGKILL and /proc liveness checks",
    "OS5-2": "SIGKILL, SIGCHLD and wait()",
    "OS5-3": "SIGKILL of a deadlocked process",
    "OS5-5": "SIGKILL, SIGCHLD and wait()",
}


# Data each task runs on (labels as in sources/source_registry.csv) and the fault it injects.
# Both go into every run's evidence.json ("mark fixture data and the fault mechanism in each manifest").
FIXTURES = {
    "OS1-1": "SAMPLE VWorld footprints (data/vworld_buildings_SAMPLE.geojson); SIMULATED CO2 / temperature",
    "OS1-2": "synthetic occupancy increments (no external data)",
    "OS1-3": "SIMULATED CO2 readings",
    "OS1-4": "live /proc samples of the scenario's own processes (no fixture)",
    "OS1-5": "SIMULATED room sensor events",
    "OS2-1": "SIMULATED LiDAR readings",
    "OS2-2": "none: timing of the scenario's own control loop",
    "OS2-3": "SIMULATED LiDAR proximity values around 0.8 m",
    "OS2-4": "SIMULATED worker ticks",
    "OS2-5": "SAMPLE Central Park pathway geometry; SIMULATED LiDAR trace (data/os2_sensor_trace_SIMULATED.csv)",
    "OS3-1": "REAL Incheon BIS stops/routes (data/incheon_bis_route_stops_20260630.csv); SIMULATED vehicle + pedestrian stream",
    "OS3-2": "synthetic queue items (alarms + logs)",
    "OS3-3": "synthetic 5,000-item burst",
    "OS3-4": "synthetic shared state",
    "OS3-5": "SIMULATED vehicle surge",
    "OS4-1": "SIMULATED PM2.5 / noise / light streams",
    "OS4-2": "SIMULATED readings with injected network delay",
    "OS4-3": "SIMULATED PM2.5 readings",
    "OS4-4": "SIMULATED writes into a live SQLite database",
    "OS4-5": "SIMULATED PM2.5; AirKorea station 131233 baseline SIMULATED unless data/airkorea_131233_pm25.csv exists",
    "OS5-1": "none: health of the scenario's own workers",
    "OS5-2": "SIMULATED water levels",
    "OS5-3": "none: lock experiment",
    "OS5-4": "SIMULATED flood hydrograph (MEIS not used)",
    "OS5-5": "SIMULATED water levels",
}
FAULTS = {
    "OS1-1": "none (ingestion and point-in-polygon mapping)",
    "OS1-2": "unsynchronised concurrent writers (lost-update race)",
    "OS1-3": "command TTL expiry on the monotonic clock",
    "OS1-4": "CPU contention between sampled processes",
    "OS1-5": "WAN cut (mock link down), then drain",
    "OS2-1": "injected wrong MOVE commands vetoed by the SafetyMonitor process",
    "OS2-2": "two CPU-burner processes competing with the control loop",
    "OS2-3": "obstacle below 0.8 m and a command older than the 500 ms TTL",
    "OS2-4": "SIGKILL of the worker process",
    "OS2-5": "WAN cut during navigation",
    "OS3-1": "none (context correlation)",
    "OS3-2": "log flood while an alarm arrives",
    "OS3-3": "overload burst: 5,000 items into 500 slots; slow consumer",
    "OS3-4": "concurrent writers with stale versions",
    "OS3-5": "WAN cut, replay and deliberate duplicate resend",
    "OS4-1": "slow and fast producers, frames split across reads",
    "OS4-2": "out-of-order and late delivery",
    "OS4-3": "threshold crossing and relay TTL expiry",
    "OS4-4": "overlapping backup runs (flock) during concurrent writes",
    "OS4-5": "WAN cut and deliberate duplicate resend",
    "OS5-1": "dependency down, SIGSTOP (hang) and SIGKILL",
    "OS5-2": "three SIGKILLs of the processor",
    "OS5-3": "inverted lock order (deadlock)",
    "OS5-4": "water level rising through 2.8 m and falling back",
    "OS5-5": "simultaneous WAN cut and worker SIGKILL",
}


def _s(sid, file, title, concept, extra=None):
    theme = sid.split("-")[0]
    profiles = {"standard": STANDARD}
    profiles.update(extra or {})
    return Scenario(sid, theme, f"edge.teams.{THEMES[theme][1]}.{file}", title, concept, profiles,
                    LINUX_ONLY.get(sid, ""), FIXTURES[sid], FAULTS[sid])


SCENARIOS: Dict[str, Scenario] = {s.id: s for s in [
    _s("OS1-1", "s1_ingestion", "Local Room Sensor Ingestion & Cached Inventory",
       "file descriptors, cached VWorld geometry, atomic event + outbox write"),
    _s("OS1-2", "s2_race_condition", "Competing Occupancy Writers & Race Condition Demonstration",
       "LOAD/ADD/STORE race, critical section, Lock"),
    _s("OS1-3", "s3_ventilation_rule", "Local Ventilation Rule & Damper Actuator Dispatch",
       "monotonic clock, command TTL 15 s"),
    _s("OS1-4", "s4_contention_profiling", "Contention Profiling & Resource Sampling",
       "/proc/<pid>/stat, context switches, 2 s sampling"),
    _s("OS1-5", "s5_wan_outage", "Signature WAN Outage & Offline Outbox Queueing",
       "durable outbox, idempotent drain", _outage_profiles("--outage")),
    _s("OS2-1", "s1_robot_ipc_topology", "Multi-Process Robot Architecture & IPC Topology",
       "process isolation, bounded queue + Unix domain socket"),
    _s("OS2-2", "s2_jitter", "Periodic Monotonic Deadlines & Jitter Measurement",
       "scheduler, absolute monotonic deadlines, J[n] = |dt[n] - T|",
       {"long": Profile("long", "600 ticks (30 s per loop)", ("--ticks", "600"), timeout_s=300)}),
    _s("OS2-3", "s3_estop_ttl", "Obstacle Proximity E-Stop & Actuator Command Expiry",
       "fail-safe latch, 500 ms monotonic TTL"),
    _s("OS2-4", "s4_crash_restart", "Worker Crash & Supervised Automatic Restart",
       "SIGKILL, SIGCHLD, wait(), durable state"),
    _s("OS2-5", "s5_offline_navigation", "Autonomous Offline Pathway Navigation",
       "local control without the network, durable outbox",
       _outage_profiles("--offline", ("--track", "{run_dir}/os2_track.csv"))),
    _s("OS3-1", "s1_bus_stop_context", "Incheon Bus Stop Context & Vehicle Influx Stream",
       "cached reference data vs live stream"),
    _s("OS3-2", "s2_priority_queue", "Priority-Aware Bounded Queue Processing",
       "bounded priority queue, latency"),
    _s("OS3-3", "s3_overload_drop", "Overload Experiment & Drop Policy Verification",
       "backpressure, drop_oldest, bounded RSS"),
    _s("OS3-4", "s4_versioned_transactions", "Transactional State Update & Version Guarding",
       "ACID transactions, version compare-and-swap"),
    _s("OS3-5", "s5_speed_sign_replay", "Offline Variable Speed Sign Dispatch & Event Replay",
       "local decisions, TTL, replay + dedup", _outage_profiles("--duration")),
    _s("OS4-1", "s1_multistream_ingestion", "Heterogeneous Multi-Stream Sensor Ingestion",
       "non-blocking I/O, selectors (epoll)"),
    _s("OS4-2", "s2_event_vs_receipt_time", "Event-Time vs. Receipt-Time Handling",
       "observed_at vs received_at, watermark"),
    _s("OS4-3", "s3_mist_cannon", "Dust Suppression Mist Cannon Relay Actuation",
       "strict threshold, 30 s TTL lease"),
    _s("OS4-4", "s4_scheduled_backup", "Scheduled Database Backup via systemd Timer / Cron",
       "SQLite online backup API, flock"),
    _s("OS4-5", "s5_backlog_drain", "WAN Outage Backlog Drain & AirKorea Baseline Benchmark",
       "at-least-once delivery + idempotent receiver"),
    _s("OS5-1", "s1_health_probes", "Health Check Architecture & Readiness State Machine",
       "liveness vs readiness, heartbeats"),
    _s("OS5-2", "s2_worker_kill_recovery", "Disposable Worker Kill & Preserved State Recovery",
       "disposable workers, durable inbox"),
    _s("OS5-3", "s3_deadlock_timeout", "Deadlock Detection & Mutex Timeout Recovery",
       "circular wait, acquire(timeout=0.2), lock ordering"),
    _s("OS5-4", "s4_sluice_gate", "Disaster Flood Relief Sluice Gate Actuation",
       "strict 2.8 m threshold, TTL, hysteresis"),
    _s("OS5-5", "s5_compound_failure", "Compound Failure Survivability (WAN Cut + Worker Crash)",
       "combined fault tolerance", _outage_profiles("--cut")),
]}


# Not a handbook task: the whole supervised node + receiver through a WAN cut and a Processor SIGKILL,
# so the fault/restore trace and both sides of the receiver boundary come from ONE console run.
SYSTEM_RUNS: Dict[str, Scenario] = {"NODE-OUTAGE": Scenario(
    "NODE-OUTAGE", "NODE", "edge.node_outage", "Integrated node outage: WAN cut + Processor SIGKILL",
    "supervised node, durable ingress, receiver ACKs, G = C = A = R",
    {"outage60": Profile("outage60", "WAN cut 60 s", ("--outage", "60"), timeout_s=300, outage_s=60),
     "outage600": Profile("outage600", "WAN cut 600 s (handbook)", ("--outage", "600"), timeout_s=1200,
                          outage_s=600, exclusive=True)},
    requires_linux="SIGKILL of a supervised worker",
    fixtures="SIMULATED sensor streams of config/os2.yaml (SRC-08)",
    fault="WAN cut + SIGKILL of the Processor halfway through the cut; generation frozen after the drain")}

ALL: Dict[str, Scenario] = {**SCENARIOS, **SYSTEM_RUNS}     # what the console and runner can launch


class RegistryError(ValueError):
    pass


def get(scenario_id) -> Scenario:
    s = ALL.get(scenario_id) if isinstance(scenario_id, str) else None
    if s is None:
        raise RegistryError("Unknown scenario ID")
    return s


def scenario_command(scenario_id, profile_id, python, run_dir) -> Tuple[List[str], float, Profile]:
    """The only way a run turns into a command line (CLI and web console both use it)."""
    entry = get(scenario_id)
    settings = entry.profiles.get(profile_id) if isinstance(profile_id, str) else None
    if settings is None:
        raise RegistryError("Unsupported run profile")
    if settings.timeout_s <= settings.outage_s:
        raise RegistryError("Run budget must exceed outage duration")
    args = [a.replace("{run_dir}", run_dir) for a in settings.fixed_args]
    return [python, "-m", entry.module, *args], settings.timeout_s, settings


def select(theme=None, only=None):
    """Registry entries in handbook order, optionally one theme / scenario number (e.g. 's5')."""
    out = []
    for s in ALL.values():
        if s.id in SYSTEM_RUNS and not ((theme and theme.upper() == s.theme) or (only and only.upper() == s.id)):
            continue                          # system runs only when asked for by theme or ID
        if theme and s.theme != theme.upper():
            continue
        if only and not (s.module.rsplit(".", 1)[1].startswith(only) or s.id == only.upper()):
            continue
        out.append(s)
    return out


def check_modules_exist(root):
    """Every registered module has its file (used by the tests)."""
    missing = []
    for s in SCENARIOS.values():
        path = os.path.join(root, "src", *s.module.split(".")) + ".py"
        if not os.path.exists(path):
            missing.append(s.module)
    return missing
