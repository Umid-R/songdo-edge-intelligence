#!/usr/bin/env python3
"""Run the official scenarios (OS1-1 ... OS5-5) and print a summary table.

    python3 scenarios/run_all_scenarios.py                    # all 25, standard profile (~2 min)
    python3 scenarios/run_all_scenarios.py --theme os4        # one theme
    python3 scenarios/run_all_scenarios.py --only s5          # scenario number s5 of every theme
    python3 scenarios/run_all_scenarios.py --only OS2-5 --profile outage600   # the 10-minute run
    python3 scenarios/run_all_scenarios.py --list             # registry: IDs, modules, profiles

The scenarios come from the shared registry (src/edge/registry.py) and run through the same
RunManager as the web console (src/edge/runner.py): same argv, same isolation (own run directory,
TMPDIR, seed and process group), same evidence. Each run writes results/runs/<run_id>/evidence.json
and the whole batch writes results/manifests/<batch>_cli.json, so CLI and console verdicts can be
compared run for run. Exit code: 0 if everything passed, 1 otherwise.
"""
import argparse
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from edge import registry as reg           # noqa: E402
from edge.runner import RunManager         # noqa: E402

LABEL = {"passed": "PASS", "failed": "FAIL", "timeout": "TIMEOUT", "cancelled": "CANCEL", "error": "ERROR"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--theme", type=str.upper, choices=list(reg.THEMES), help="only this theme")
    ap.add_argument("--only", help="scenario number (e.g. s5) or ID (e.g. OS2-5)")
    ap.add_argument("--profile", default="standard", help="run profile (standard, outage60, outage600, long)")
    ap.add_argument("--results", default=os.path.join(ROOT, "results", "runs"), help="run directory root")
    ap.add_argument("--verbose", "-v", action="store_true", help="stream every scenario's output")
    ap.add_argument("--list", action="store_true", help="print the registry and exit")
    args = ap.parse_args()

    selected = reg.select(args.theme, args.only)
    if args.list:
        for s in selected:
            print(f"{s.id:6} {s.module:48} {', '.join(s.profiles):28} {s.title}")
        return 0
    if args.profile != "standard":
        selected = [s for s in selected if args.profile in s.profiles]
    if not selected:
        print("no scenarios match (check --theme / --only / --profile)")
        return 1

    mgr = RunManager(ROOT, results_dir=args.results)
    batch_id = mgr.new_cli_batch([s.id for s in selected], args.profile)
    echo = (lambda line: print("        |", line, flush=True)) if args.verbose else None
    runs = []
    for s in selected:
        run = mgr.run_sync(s.id, args.profile, batch_id=batch_id, echo=echo)
        mgr.batches[batch_id]["run_ids"].append(run.run_id)
        runs.append(run)
        status = LABEL.get(run.state, run.state)
        print(f"{status:7} {s.theme} {s.module.rsplit('.', 1)[1]:34} {run.elapsed_s:6.1f} s   {s.id}", flush=True)
        if run.state != "passed" and not args.verbose:
            for line in list(run.lines)[-12:]:
                print("        ", line)
    mgr.batches[batch_id]["state"] = "finished"
    manifest, path = mgr.write_manifest(batch_id, "cli")
    print(f"\n{manifest['passed']}/{manifest['total']} scenarios passed")
    print(f"evidence: {os.path.relpath(args.results, ROOT)}/<run_id>/evidence.json   "
          f"manifest: {os.path.relpath(path, ROOT)}")
    return 0 if manifest["passed"] == manifest["total"] else 1


if __name__ == "__main__":
    sys.exit(main())
