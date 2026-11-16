"""Integrated node outage: the supervised node with its receiver process, run as ONE scenario run.

The assessment asks to "show both sides of the receiver boundary and a fault/restore trace in the
same UI run". This module is registered as NODE-OUTAGE in edge.registry, so the console starts it like
any task (60 s or 600 s profile), streams its trace live, and exports it as run evidence.

    python3 -m edge.node_outage --outage 600            # what the 600 s profile runs
    python3 -m edge.node_outage --outage 60 --config os5.yaml

What happens (every step logged with the monotonic clock):
    1. start a fresh supervised node (own databases, own ports) + its receiver process
    2. WAN cut for --outage seconds; the Processor is SIGKILLed halfway through the cut
    3. WAN restored, backlog drains; generation is frozen at a recorded boundary
    4. both sides are compared: generated / committed / acknowledged on the node, stored at the receiver
The console API is polled the whole time, which also shows it stays reachable through the outage.
Artifacts in the run directory: node_outage.json (checks, events, full trace), trace.csv, supervisor.log.
"""
import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def grew_until_end(samples, window_s):
    """Events still increasing over the last window_s seconds of these samples."""
    if len(samples) < 2:
        return False
    end = samples[-1]
    start = next((t for t in samples if end["mono"] - t["mono"] <= window_s), samples[0])
    return end["events"] > start["events"]


class NodeOutage:
    def __init__(self, out, outage_s, config="os2.yaml", python=None, echo=print):
        self.out, self.outage_s, self.config = out, float(outage_s), config
        self.python = python or sys.executable
        self.echo = echo
        self.trace, self.events, self.api_latency_ms, self.api_failures = [], [], [], 0
        self.t0 = None

    # ---- helpers ----
    def http(self, path, method="GET"):
        t = time.monotonic()
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", method=method)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                body = json.loads(r.read())
        except OSError:
            self.api_failures += 1
            raise
        self.api_latency_ms.append(round((time.monotonic() - t) * 1000, 1))
        return body

    def mark(self, what, **kw):
        e = {"event": what, "mono": round(time.monotonic(), 3), "t": self.rel(), "wall": time.strftime("%H:%M:%S"), **kw}
        self.events.append(e)
        self.echo(f"[{e['wall']}] +{e['t']:6.1f} s  {what}" + (f"  {kw}" if kw else ""))

    def rel(self):
        return round(time.monotonic() - self.t0, 1) if self.t0 else 0.0

    def sample(self, phase):
        s = self.http("/api/status")
        row = {"mono": round(time.monotonic(), 3), "t": self.rel(), "phase": phase, "wan_online": s["wan_online"],
               "events": s["counts"]["events"], "decisions": s["counts"]["decisions"],
               "receipts": s["counts"]["receipts"], "pending": s["outbox_pending"],
               "receiver": s["conservation"]["sink"], "conserved": s["conservation"]["conserved"],
               "queue": (s.get("queue") or {}).get("qsize"),
               "restarts": {p["name"]: p["restarts"] for p in s["processes"]}}
        self.trace.append(row)
        self.echo(f"    +{row['t']:6.1f} s {phase:8}  node: committed {row['events']:6} decisions {row['decisions']:4} "
                  f"outbox {row['pending']:6} | receiver: stored {row['receiver']}")
        return s

    # ---- the run ----
    def run(self):
        import yaml
        os.makedirs(self.out, exist_ok=True)
        cfg_file = self.config if os.path.isabs(self.config) else os.path.join(ROOT, "config", self.config)
        with open(cfg_file, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        self.port = free_port()
        sock_dir = tempfile.mkdtemp(prefix="songdo-node-")
        cfg["network"]["api_port"] = self.port
        cfg["upstream"]["port"] = free_port()
        cfg["paths"].update({"db": os.path.join(self.out, "node.db"),
                             "upstream_db": os.path.join(self.out, "receiver.db"),
                             "pidfile": os.path.join(self.out, "pids.json"),
                             "backups": os.path.join(self.out, "backups"),
                             "socket": os.path.join(sock_dir, "node.sock"),
                             "schema": os.path.join(ROOT, "sql", "schema.sql"),
                             "static": os.path.join(ROOT, "src", "ui", "static"),
                             "results": os.path.join(self.out, "inner_runs")})
        cfg_path = os.path.join(self.out, "node_config.yaml")
        with open(cfg_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(cfg, f)
        step = max(1.0, min(5.0, self.outage_s / 30))           # 5 s for 600 s, 2 s for 60 s
        result = {"config": self.config, "outage_s": self.outage_s, "sample_interval_s": step,
                  "fault": f"WAN cut {self.outage_s:.0f} s (SyncWorker link down) + SIGKILL of the Processor "
                           "halfway through the cut; generation frozen after the drain",
                  "fixtures": "SIMULATED sensor streams of the node config (sources/source_registry.csv SRC-08)"}
        log = open(os.path.join(self.out, "supervisor.log"), "w", encoding="utf-8")
        env = dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src"))
        env.pop("EDGE_CRASH_AT", None)
        proc = subprocess.Popen([self.python, "-m", "edge.supervisor", "--config", cfg_path], cwd=ROOT, env=env,
                                stdout=log, stderr=subprocess.STDOUT, start_new_session=(os.name == "posix"))
        self.t0 = time.monotonic()
        s = None
        try:
            for _ in range(120):
                try:
                    self.http("/api/status")
                    break
                except OSError:
                    time.sleep(0.25)
            self.api_failures = 0                                 # startup polls do not count
            self.mark("node started", supervisor_pid=proc.pid, api_port=self.port)
            time.sleep(min(10.0, step * 3))
            self.sample("online")
            self.http("/api/wan/cut", "POST")
            self.mark("WAN cut")
            t_cut, kill_done = time.monotonic(), False
            while time.monotonic() - t_cut < self.outage_s:
                time.sleep(min(step, max(0.05, self.outage_s - (time.monotonic() - t_cut))))
                s = self.sample("offline")
                if not kill_done and time.monotonic() - t_cut >= self.outage_s / 2:
                    with open(cfg["paths"]["pidfile"], encoding="utf-8") as f:
                        pid = json.load(f)["Processor"]["pid"]
                    os.kill(pid, signal.SIGKILL)
                    kill_done = True
                    self.mark("Processor SIGKILLed during the cut", pid=pid)
            self.http("/api/wan/restore", "POST")
            self.mark("WAN restored")
            t_restore = time.monotonic()
            while time.monotonic() - t_restore < max(20.0, min(60.0, self.outage_s / 10)):
                time.sleep(step)
                s = self.sample("draining")
            self.http("/api/generation/pause", "POST")
            self.mark("generation frozen (recorded boundary)")
            time.sleep(3)
            for _ in range(120):
                s = self.sample("frozen")
                if s["outbox_pending"] == 0:
                    break
                time.sleep(1)
            self.mark("backlog drained", pending=s["outbox_pending"])
            rec = self.http("/api/reconcile")
            result.update(reconcile=rec, recovery=s["recovery"], final_status={
                k: s[k] for k in ("counts", "outbox_pending", "conservation", "receiver", "queue", "sync")})
            off = [t for t in self.trace if t["phase"] == "offline"]
            restarts = self.trace[-1]["restarts"].get("Processor", 0)
            result["checks"] = {
                "node kept committing events during the cut":
                    bool(off) and max(t["events"] for t in off) > min(t["events"] for t in off),
                "generation kept going after the Processor was killed (end of the cut still growing)":
                    kill_done and grew_until_end(off, max(10.0, min(30.0, self.outage_s / 4))),
                "receiver got nothing new while the WAN was cut": len({t["receiver"] for t in off}) <= 1,
                "Processor was restarted by the supervisor": restarts >= 1,
                "recovery exit rule holds (no admitted event lost, receipts = dispatched)": bool(s["recovery"]["ok"]),
                "G = C = A = R and zero pending after drain": bool(rec.get("fully_drained")),
                "no false 'sent' and no unknown IDs at the receiver":
                    rec.get("acked_but_not_at_receiver") == 0 and rec.get("at_receiver_unknown_to_node") == 0,
                "console API answered every poll during the outage": self.api_failures == 0,
            }
            result["passed"] = all(result["checks"].values())
        except Exception as e:                                   # report, never leave the node running
            result["passed"], result["error"] = False, f"{type(e).__name__}: {e}"
        finally:
            try:
                proc.send_signal(signal.SIGTERM)
                proc.wait(30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(5)
            log.close()
            result["supervisor_exit_code"] = proc.returncode
            result["api"] = {"polls": len(self.api_latency_ms), "failures": self.api_failures,
                             "max_latency_ms": max(self.api_latency_ms, default=None)}
            result["events"], result["trace"] = self.events, self.trace
            with open(os.path.join(self.out, "node_outage.json"), "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)
            with open(os.path.join(self.out, "trace.csv"), "w", encoding="utf-8") as f:
                cols = ("mono", "t", "phase", "wan_online", "events", "decisions", "receipts", "pending",
                        "receiver", "conserved", "queue")
                f.write(",".join(cols) + "\n")
                for t in self.trace:
                    f.write(",".join(str(t[k]) for k in cols) + "\n")
            for name in os.listdir(self.out):                    # databases are bulky; the JSON holds the evidence
                if name.split(".db")[0] in ("node", "receiver") and ".db" in name:
                    os.remove(os.path.join(self.out, name))
            try:
                os.rmdir(sock_dir)
            except OSError:
                pass
        return result


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--outage", type=float, default=60, help="WAN cut in seconds (600 = handbook)")
    ap.add_argument("--config", default="os2.yaml", help="theme config in config/ (default os2.yaml)")
    ap.add_argument("--out", help="artifact folder (default: the run directory, else results/node_outage_<ts>)")
    args = ap.parse_args()
    out = args.out or (os.path.join(os.environ["SONGDO_RUN_DIR"], "node_outage") if os.environ.get("SONGDO_RUN_DIR")
                       else os.path.join(ROOT, "results", time.strftime("node_outage_%Y%m%d-%H%M%S")))
    print(f"Integrated node outage: {args.outage:.0f} s WAN cut on {args.config}, Processor SIGKILL halfway; "
          f"artifacts in {out}", flush=True)
    r = NodeOutage(out, args.outage, args.config, echo=lambda m: print(m, flush=True)).run()
    rec = r.get("reconcile", {})
    print(f"[{'PASS' if r.get('passed') else 'FAIL'}] NODE-OUTAGE integrated node outage ({args.outage:.0f} s)")
    if rec:
        print(f"       G {rec.get('generated')} = C {rec.get('committed')} = A {rec.get('acknowledged')} = "
              f"R {rec.get('receiver')}, pending {rec.get('pending')}, receiver duplicates ignored "
              f"{rec.get('receiver_duplicates_ignored')}")
    print(f"       console API: {r['api']['polls']} polls, {r['api']['failures']} failures, "
          f"max latency {r['api']['max_latency_ms']} ms")
    if r.get("error"):
        print(f"       FAILED with {r['error']}")
    for k, v in r.get("checks", {}).items():
        print(f"       {'ok' if v else 'FAILED'} {k}")
    return 0 if r.get("passed") else 1


if __name__ == "__main__":
    sys.exit(main())
