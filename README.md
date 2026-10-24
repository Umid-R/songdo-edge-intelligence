# Songdo Edge Intelligence (송도 엣지 인텔리전스)
## Inha University — Operating System (운영체제), Prof. Mehdi Pirahandeh

**Platform**: Ubuntu 24.04 LTS | Python 3.12 | SQLite 3 (WAL mode)
**Mission**: *"Make Songdo compute locally and reliably."*
**Signature demonstration**: **"Disconnect Internet → system continues operating."**

A multi-process Linux edge node that keeps sensing, deciding and actuating when the WAN is cut, then
drains its durable outbox with zero lost events. Five themes (OS1–OS5) × 5 scenarios = **25 scenarios**,
all on one shared core. Python standard library plus `pyyaml` (and `psutil`, used only as a fallback
outside Linux).

---

## Quickstart (the course README's five steps)

Run from the repository root (`cd songdo-edge-intelligence`). On Ubuntu, `python` is `python3`.

```bash
# Step 1: preflight audit (kernel, shell, Python, SQLite, ps/pkill/awk/sed/flock/curl)
bash scripts/preflight.sh

# Step 2: virtual environment + dependencies (psutil, pyyaml) + database data/songdo_edge.db in WAL mode
bash scripts/setup.sh

# Step 3: automated OS concurrency suite (WAL + atomic outbox, bounded queue, race + mutex,
#         deadlock timeout recovery, online backup integrity)
python3 tests/test_os_fundamentals.py

# Step 4: all 25 scenarios (a few minutes); --theme os2 for one theme, --only s5 for one scenario number
python3 scenarios/run_all_scenarios.py

# Step 5: signature offline demo + local web console
bash scripts/start_demo.sh OS1         # dashboard: http://127.0.0.1:8001 (the port is printed on start)
```

In the dashboard: live PIDs, CPU %, RSS memory and ingested events. **Simulate WAN Internet Cut** →
the badge shows OFFLINE, events and local decisions continue, *Outbox queued* grows.
**Restore WAN Connection** → the backlog drains, *Conserved* stays `yes`.

Ports: `start_demo.sh` with no theme (config/node.yaml) and the Docker image use **8000**; the
theme nodes use **8001–8005** (OS1–OS5) so all five can run at the same time without colliding.

More tests: `PYTHONPATH=src python3 -m unittest discover -s tests -v` (all unit + scenario tests),
`python3 integration/test_mock_upstream.py` (outbox → HTTP → mock upstream across a WAN cut).

---

## File layout (as in the course README)

```
songdo-edge-intelligence/
├── config/
│   ├── node.yaml                  # master configuration (port 8000, data/songdo_edge.db)
│   └── os1.yaml ... os5.yaml      # one per theme (node id, sensors, rules, port, db)
├── container/
│   ├── Dockerfile                 # non-root Ubuntu 24.04 image, HEALTHCHECK
│   └── compose.yaml               # CPU/RAM cgroup limits + healthcheck
├── data/
│   ├── backups/                   # safe online SQLite backups
│   ├── songdo_edge.db             # created by scripts/setup.sh (not committed)
│   └── ...                        # cached open data + SAMPLE/SIMULATED files (see sources/)
├── docs/handbook/                 # where the handbook lives (see its README)
├── integration/                   # mock upstream consumer + integration test
├── scenarios/
│   └── run_all_scenarios.py       # executable runner for all 25 scenarios
├── scripts/
│   ├── preflight.sh               # system environment audit
│   ├── setup.sh                   # environment initializer
│   ├── start_demo.sh              # multi-process supervisor and web launcher
│   ├── backup.sh                  # flocked online backup
│   └── generate_os2_sim_data.py   # OS2 SAMPLE path + SIMULATED LiDAR trace (seed 2026)
├── sources/
│   └── source_registry.csv        # Korean open data registry and what is REAL / SAMPLE / SIMULATED
├── sql/
│   └── schema.sql                 # SQLite DDL (WAL, events, decisions, outbox, sink, health)
├── src/
│   ├── edge/
│   │   ├── api_server.py          # zero-dependency local HTTP API server
│   │   ├── decision.py            # local rule engine, monotonic-clock command TTL
│   │   ├── ipc.py                 # BoundedQueue (500, drop_oldest), Unix socket, StopFlag
│   │   ├── models.py              # SensorEvent, EdgeDecision, ProcessHealth
│   │   ├── storage.py             # SQLite manager, atomic outbox, online backup
│   │   ├── supervisor.py          # multi-process supervisor (restart, clean shutdown)
│   │   ├── sync_worker.py         # durable offline outbox synchronizer
│   │   ├── workers.py             # SensorGen, Processor, HealthMon processes
│   │   ├── config.py, procfs.py   # config loader, /proc sampling
│   │   ├── core/                  # backward-compatibility alias for the old edge.core.* imports
│   │   └── teams/os1_building ... os5_reliable/   # one file per scenario (25)
│   └── ui/static/
│       └── index.html             # local edge console (no frameworks)
├── systemd/
│   ├── songdo-edge@.service       # user service, one instance per theme
│   └── songdo-backup@.service/.timer   # scheduled flocked backup
├── tests/
│   ├── test_os_fundamentals.py    # the five-check OS suite (Step 3)
│   ├── test_core.py, test_pipeline.py, test_configs.py
│   └── os1/ ... os5/              # scenario tests
├── requirements.txt
├── Songdo_Edge_Intelligence_Handbook.pdf   # instructor's PDF: copy it here locally (not in the public repo)
└── README.md
```

---

## How it works
```
SensorGen --(BoundedQueue 500, drop_oldest)--> Processor --> SQLite WAL (events + outbox in ONE transaction)
HealthMon (/proc -> process_health)                 |        decisions with monotonic TTL -> actuator
API (http.server dashboard, /api/status, /api/wan/*)    SyncWorker (drains the outbox only while WAN is online)
Supervisor starts, monitors (SIGCHLD / wait), restarts and cleanly stops every process.
```

## Operations

**systemd (user service).** `%i` is the theme (`OS1` … `OS5`, or `node`):
```bash
mkdir -p ~/.config/systemd/user && cp systemd/songdo-edge@.service ~/.config/systemd/user/
systemctl --user daemon-reload && systemctl --user start songdo-edge@OS1.service
systemctl --user status songdo-edge@OS1.service
journalctl --user-unit songdo-edge@OS1.service -f -o short-iso
```
Burst limit 5 starts / 60 s, `Restart=on-failure`, `MemoryMax=512M`, `CPUQuota=50%`, `TasksMax=64`.

**Flocked backup:** `flock -n /tmp/songdo_backup.lock -c "./scripts/backup.sh"` (timer: `systemd/songdo-backup@.timer`).

**Container:**
```bash
docker build -f container/Dockerfile -t songdo-edge . && docker run --rm -p 8000:8000 songdo-edge
docker compose -f container/compose.yaml up --build      # 1 CPU, 512 MB, healthcheck
```

**Mock upstream:** `python3 integration/mock_upstream.py --port 9100` (POST /ingest, GET /stats; dedup by event_id).

---

## Teams and their 25 scenarios

| Team | Theme | Folder | Config | Port | Members | Scenarios (one file each) |
|---|---|---|---|---|---|---|
| Team 1 | OS1 Smart Building Edge Gateway | `src/edge/teams/os1_building/` | `config/os1.yaml` | 8001 | | OS1-1 Local Room Sensor Ingestion & Cached Inventory<br>OS1-2 Competing Occupancy Writers & Race Condition Demonstration<br>OS1-3 Local Ventilation Rule & Damper Actuator Dispatch<br>OS1-4 Contention Profiling & Resource Sampling<br>OS1-5 Signature WAN Outage & Offline Outbox Queueing |
| Team 2 | OS2 Robot Edge Controller | `src/edge/teams/os2_robot/` | `config/os2.yaml` | 8002 | | OS2-1 Multi-Process Robot Architecture & IPC Topology<br>OS2-2 Periodic Monotonic Deadlines & Jitter Measurement<br>OS2-3 Obstacle Proximity E-Stop & Actuator Command Expiry<br>OS2-4 Worker Crash & Supervised Automatic Restart<br>OS2-5 Autonomous Offline Pathway Navigation |
| Team 3 | OS3 Autonomous Mobility Edge Node | `src/edge/teams/os3_mobility/` | `config/os3.yaml` | 8003 | | OS3-1 Incheon Bus Stop Context & Vehicle Influx Stream<br>OS3-2 Priority-Aware Bounded Queue Processing<br>OS3-3 Overload Experiment & Drop Policy Verification<br>OS3-4 Transactional State Update & Version Guarding<br>OS3-5 Offline Variable Speed Sign Dispatch & Event Replay |
| Team 4 | OS4 IoT Edge Gateway | `src/edge/teams/os4_iot/` | `config/os4.yaml` | 8004 | | OS4-1 Heterogeneous Multi-Stream Sensor Ingestion<br>OS4-2 Event-Time vs. Receipt-Time Handling<br>OS4-3 Dust Suppression Mist Cannon Relay Actuation<br>OS4-4 Scheduled Database Backup via systemd Timer / Cron<br>OS4-5 WAN Outage Backlog Drain & AirKorea Baseline Benchmark |
| Team 5 | OS5 Fault-Tolerant Smart-City Node | `src/edge/teams/os5_reliable/` | `config/os5.yaml` | 8005 | | OS5-1 Health Check Architecture & Readiness State Machine<br>OS5-2 Disposable Worker Kill & Preserved State Recovery<br>OS5-3 Deadlock Detection & Mutex Timeout Recovery<br>OS5-4 Disaster Flood Relief Sluice Gate Actuation<br>OS5-5 Compound Failure Survivability (WAN Cut + Worker Crash) |

Each scenario also runs alone: `PYTHONPATH=src python3 -m edge.teams.<theme_folder>.<scenario>`.

## Requirement map (handbook rubric → where it is)

| Rubric item | Where |
|---|---|
| Multi-process roles (Sensor, Processor, Health, UI), bounded queues, sockets, zombie-free shutdown | `src/edge/supervisor.py`, `workers.py`, `ipc.py`; OS2-1, OS2-4 |
| Race condition vs. Mutex, deadlock prevention, WAL durability, backup | `tests/test_os_fundamentals.py`; OS1-2, OS5-3, OS4-4 |
| Bash ladder, rule engine, monotonic command TTL | `scripts/*.sh`, `src/edge/decision.py`; OS1-3, OS2-3, OS4-3, OS5-4 |
| systemd unit with burst limits + resource limits, flocked backups, container healthcheck | `systemd/`, `scripts/backup.sh`, `container/` |
| 10-minute WAN disconnect, event conservation, idempotent receiver dedup | OS1-5 (`--outage 600`), OS2-5 (`--offline 600`), OS3-5 (`--duration 600`), OS5-5 (`--cut 600`), OS4-5; `integration/` |
| Zero-dependency dashboard with PIDs, CPU/RAM, outbox | `src/ui/static/index.html`, `src/edge/api_server.py` |
| Grounded data with honest labels | `sources/source_registry.csv` |

## Known gaps (be honest about them in the defense)
- Data: OS1-1 uses SAMPLE VWorld footprints, OS2-5 an APPROXIMATE Central Park path and a SIMULATED LiDAR trace, OS4-5 a SIMULATED AirKorea baseline, OS5-4 SIMULATED water levels. Details in `sources/source_registry.csv`.
- OS2-5 10-minute run: every obstacle stopped and every event delivered in all runs; the "longest gap between ticks < 300 ms" check passed on two machines (200 and 255 ms) and failed on a busy laptop (327 and 404 ms).
- Container: the Dockerfile and compose file have not been built on a team machine yet (only the node's container settings and the healthcheck command were tested).
- systemd: the unit's start command was tested; the unit itself has not yet been run under a real `systemctl --user`.
- The scenarios are separate programs; `start_demo.sh <theme>` runs the shared node, not the scenarios.
- OS3: the scripts `build_bus_stops.py` and `os3_live_demo.py` mentioned by the OS3 team are not in the repo.

## Git workflow
1. `main` always runs: never push broken code to it.
2. One branch per change, merged via pull request reviewed by someone else.
3. `git pull` from `main` often. Run the tests before every PR.
4. Everyone must be able to explain their own code: 20 points of the grade is an individual oral defense.
