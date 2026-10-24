#!/usr/bin/env python3
"""Moved to scenarios/run_all_scenarios.py (the path in the course README). Kept so old commands still work."""
import os
import runpy
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.argv[0] = os.path.join(ROOT, "scenarios", "run_all_scenarios.py")
runpy.run_path(sys.argv[0], run_name="__main__")
