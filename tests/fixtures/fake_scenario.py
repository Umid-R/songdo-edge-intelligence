"""Stand-in scenario for the runner tests (never part of the official registry).

    fake_scenario pass | fail | sleep <s> | spawn <s> | flood <lines> | tmp
"""
import os
import subprocess
import sys
import tempfile
import time

mode = sys.argv[1] if len(sys.argv) > 1 else "pass"
if mode == "pass":
    print("[PASS] FAKE-1 fake scenario")
    print("       3/3 checks passed")
elif mode == "fail":
    print("[FAIL] FAKE-1 fake scenario")
    print("       FAILED: something")
    sys.exit(1)
elif mode == "sleep":
    print("sleeping", flush=True)
    time.sleep(float(sys.argv[2]))
elif mode == "spawn":                      # a child that would outlive us without group kill
    child = subprocess.Popen([sys.executable, "-c", f"import time; time.sleep({sys.argv[2]})"])
    print(f"child {child.pid}", flush=True)
    time.sleep(float(sys.argv[2]))
elif mode == "flood":
    for i in range(int(sys.argv[2])):
        print(f"line {i}")
    print("[PASS] FAKE-1 flood")
elif mode == "tmp":
    print("tmp", tempfile.gettempdir(), os.environ.get("SONGDO_RUN_ID"), os.environ.get("SONGDO_SEED"))
    print("[PASS] FAKE-1 tmp")
