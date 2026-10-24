#!/usr/bin/env python3
"""Run every scenario (OS1..OS5 x 5 = 25) as its own process and print a summary table.

    python3 scenarios/run_all_scenarios.py              # all 25 (takes a few minutes)
    python3 scenarios/run_all_scenarios.py --theme os4  # one theme
    python3 scenarios/run_all_scenarios.py --only s5    # scenarios whose file name starts with s5

Each scenario is run as `python -m edge.teams.<theme_folder>.<scenario>` from the repo root and counts
as PASSED when its exit code is 0. Exit code of this script: 0 if everything passed, 1 otherwise.
"""
import argparse
import glob
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
THEMES = {"os1": "os1_building", "os2": "os2_robot", "os3": "os3_mobility", "os4": "os4_iot", "os5": "os5_reliable"}


def discover(theme=None, only=None):
    found = []
    for key, folder in THEMES.items():
        if theme and key != theme.lower():
            continue
        for path in sorted(glob.glob(os.path.join(ROOT, "src", "edge", "teams", folder, "s[0-9]_*.py"))):
            name = os.path.splitext(os.path.basename(path))[0]
            if only and not name.startswith(only):
                continue
            found.append((key.upper(), name, f"edge.teams.{folder}.{name}"))
    return found


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--theme", choices=[*THEMES, *[k.upper() for k in THEMES]], help="only this theme")
    ap.add_argument("--only", help="only scenarios whose file name starts with this (e.g. s5)")
    ap.add_argument("--timeout", type=int, default=240, help="seconds per scenario")
    args = ap.parse_args()
    env = dict(os.environ, PYTHONPATH=os.path.join(ROOT, "src"))
    venv_py = os.path.join(ROOT, ".venv", "bin", "python")
    python = venv_py if os.path.exists(venv_py) else sys.executable   # .venv has pyyaml (setup.sh)
    scenarios = discover(args.theme, args.only)
    if not scenarios:
        print("no scenarios found")
        return 1
    results = []
    for theme, name, module in scenarios:
        t0 = time.monotonic()
        try:
            p = subprocess.run([python, "-m", module], cwd=ROOT, env=env, capture_output=True,
                               text=True, timeout=args.timeout)
            status = "PASS" if p.returncode == 0 else "FAIL"
            tail = (p.stdout + p.stderr).strip().splitlines()
        except subprocess.TimeoutExpired:
            status, tail = "TIMEOUT", []
        dt = time.monotonic() - t0
        results.append((theme, name, status, dt))
        print(f"{status:7} {theme} {name:34} {dt:6.1f} s", flush=True)
        if status != "PASS":
            for line in tail[-12:]:
                print("        ", line)
    passed = sum(1 for r in results if r[2] == "PASS")
    print(f"\n{passed}/{len(results)} scenarios passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
