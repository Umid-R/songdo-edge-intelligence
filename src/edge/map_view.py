"""Data for the live 2D map page (/map). One page, one scene per theme (config `map.scene`):

  os1_building      OS1-5  Songdo Convensia: CO2 rule drives the HVAC damper, WAN outage
  os2_park          OS2-5  Central Park path: robot, LiDAR e-stop, offline navigation
  os3_intersection  OS3-5  Techno-Park intersection: pedestrian surge -> variable speed sign
  os5_sluice        OS5-5  Waterfront sluice: flood -> gate, WAN cut + Processor SIGKILL

GET  /api/map/context            static: scene, node, streams, rules, defaults, reference data
GET  /api/map                    live:   readings, actuator state (with TTL), decisions, WAN,
                                         outbox / delivered, processes (PID, restarts)
POST /api/fault/kill-processor   OS5-5 demo only (config `map.faults`): SIGKILL the node's Processor

The map shows what the node itself stores. Movement on the page (cars, robot, people, water)
is drawn from these values; the page says which parts are drawn.
"""
import json
import os
import signal
import time

from edge.decision import boot_epoch
from edge.upstream import fetch_stats, upstream_settings

READINGS_PER_STREAM = 60      # sparkline length (about 30 s at the default 0.5 s interval)
DECISIONS_SHOWN = 8


def _load_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _unique_routes(routes):
    """One entry per route number and direction (the BIS CSV lists e.g. route 320 under two route IDs)."""
    seen, out = set(), []
    for r in routes:
        key = (r["route_number"], r["direction"])
        if key not in seen:
            seen.add(key)
            out.append({"route_number": r["route_number"], "direction": r["direction"]})
    return out


def _bus_stops(path):
    data = _load_json(path) if path else None
    if not data:
        return None
    return {"source": data.get("source"), "reference_date": data.get("reference_date"), "note": data.get("note"),
            "stops": [{"stop_id": s["stop_id"], "stop_name": s["stop_name"],
                       "routes": _unique_routes(s.get("routes", []))} for s in data.get("stops", [])]}


def _geojson(path):
    """A small GeoJSON file as-is (features + the file's own note), or None."""
    data = _load_json(path) if path else None
    if not data or data.get("type") != "FeatureCollection":
        return None
    return {"name": data.get("name"), "note": data.get("note"), "features": data.get("features", [])}


def map_context(cfg):
    m = cfg.get("map") or {}
    paths = cfg.get("paths", {})
    return {"scene": m.get("scene"), "node": cfg["node"], "faults": list(m.get("faults") or []),
            "sensor_interval_s": cfg.get("sensor", {}).get("interval_s"),
            "streams": [{k: s.get(k) for k in ("name", "unit", "base")} for s in cfg.get("sensor", {}).get("streams", [])],
            "rules": cfg.get("rules", []), "defaults": m.get("defaults", {}),
            "bus_stops": _bus_stops(paths.get("bus_stops")),
            "path": _geojson(paths.get("path_geojson")),
            "buildings": _geojson(paths.get("buildings_geojson"))}


def effective_actuators(cfg, storage, now_mono=None):
    """Per actuator: the commanded value while its command is inside its TTL (same boot epoch),
    otherwise the configured default. Actuators named in a rule but never commanded are included."""
    now = time.monotonic() if now_mono is None else now_mono
    epoch = boot_epoch()
    defaults = (cfg.get("map") or {}).get("defaults", {})
    names = [r["actuator"] for r in cfg.get("rules", [])]
    out = {n: {"actuator": n, "value": defaults.get(n), "active": False, "commanded": None,
               "remaining_s": 0.0, "ttl_s": None, "rule": None, "decision_id": None} for n in names}
    for row in storage.actuator_commands():
        a = out.setdefault(row["actuator"], {"actuator": row["actuator"], "value": defaults.get(row["actuator"])})
        exp = row["expires_mono"]
        active = exp is not None and row["boot_epoch"] == epoch and now < exp
        a.update(commanded=row["value"], decision_id=row["decision_id"], rule=row["rule"],
                 ttl_s=(exp - row["issued_mono"]) if exp is not None and row["issued_mono"] is not None else None,
                 active=active, remaining_s=round(exp - now, 2) if active else 0.0)
        if active:
            a["value"] = row["value"]
    return list(out.values())


def map_live(cfg, storage, receiver_timeout=0.4):
    readings = {}
    for s in cfg.get("sensor", {}).get("streams", []):
        rows = storage.latest_readings(s["name"], READINGS_PER_STREAM)
        readings[s["name"]] = [{"v": r["value"], "t": r["observed_at"]} for r in reversed(rows)]
    counts = storage.counts()
    delivered = None
    up = upstream_settings(cfg)
    if up:
        try:
            delivered = fetch_stats(up[3], timeout=receiver_timeout)["received"]
        except (OSError, KeyError, ValueError):
            delivered = None
    else:
        delivered = counts.get("sink")
    return {"ts": time.time(), "wan_online": storage.wan_online(),
            "generation_paused": storage.kv_get("generation_paused", "0") == "1",
            "events": counts.get("events"), "outbox_pending": storage.outbox_pending(),
            "delivered": delivered, "receiver_reachable": delivered is not None,
            "readings": readings, "actuators": effective_actuators(cfg, storage),
            "decisions": storage.recent_decisions(DECISIONS_SHOWN),
            "decision_counts": storage.decision_counts(),
            "processes": [{k: p.get(k) for k in ("name", "pid", "state", "restarts", "updated_at")}
                          for p in storage.list_health()]}


def _ppid(pid):
    """Parent PID from /proc/<pid>/stat (field 4; the command name in field 2 may contain spaces)."""
    with open(f"/proc/{pid}/stat", encoding="utf-8") as f:
        stat = f.read()
    return int(stat.rsplit(")", 1)[1].split()[1])


class FaultError(Exception):
    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code


def kill_processor(cfg):
    """SIGKILL this node's Processor, for the OS5-5 compound-failure demo.

    Only if the theme enables it (`map.faults: [kill_processor]`), and only a PID that the
    supervisor's pidfile names as "Processor" AND whose parent is that supervisor AND that is a
    sibling of this API process: a stale pidfile can never make the console kill an unrelated
    process. The supervisor then reaps it (SIGCHLD + wait) and restarts it."""
    if "kill_processor" not in ((cfg.get("map") or {}).get("faults") or []):
        raise FaultError(403, "fault injection is not enabled for this node")
    if not os.path.isdir("/proc/self"):
        raise FaultError(501, "needs Linux /proc")
    data = _load_json(cfg["paths"]["pidfile"]) or {}
    try:
        sup, pid = int(data["Supervisor"]["pid"]), int(data["Processor"]["pid"])
    except (KeyError, TypeError, ValueError):
        raise FaultError(409, "no running Processor in the pidfile")
    if os.getppid() != sup:
        raise FaultError(409, "this console is not part of the running node")
    try:
        if _ppid(pid) != sup:
            raise FaultError(409, "pidfile is stale: that PID is not the node's Processor")
        os.kill(pid, signal.SIGKILL)
    except (FileNotFoundError, ProcessLookupError):
        raise FaultError(409, "Processor is not running (already restarting?)")
    return {"killed": "Processor", "pid": pid, "signal": "SIGKILL", "ts": time.time()}


def map_page_path(cfg):
    return os.path.join(cfg["paths"]["static"], "map.html")
