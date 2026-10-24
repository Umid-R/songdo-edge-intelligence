#!/usr/bin/env bash
# Create .venv, install dependencies, create the database (WAL mode).
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r requirements.txt
mkdir -p data/backups data/run
PYTHONPATH=src .venv/bin/python - <<'PY'
from edge.config import load_config
from edge.storage import Storage
cfg = load_config("config/node.yaml")
s = Storage(cfg["paths"]["db"], cfg["paths"]["schema"])
print("database ready, journal_mode =", s.journal_mode())
PY
echo "Setup complete."
