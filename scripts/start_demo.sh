#!/usr/bin/env bash
# Start an edge node (supervisor + all workers). Ctrl+C stops it cleanly.
#   bash scripts/start_demo.sh OS1     -> config/os1.yaml (dashboard on port 8001)
#   bash scripts/start_demo.sh         -> config/node.yaml (generic, port 8000)
set -euo pipefail
cd "$(dirname "$0")/.."
CONFIG=config/node.yaml
if [[ "${1:-}" =~ ^[Oo][Ss][1-5]$ ]]; then
  CONFIG="config/$(echo "$1" | tr 'A-Z' 'a-z').yaml"
  shift
elif [[ "${1:-}" == "node" ]]; then
  shift                                   # songdo-edge@node.service -> config/node.yaml
fi
PORT=$(awk '/api_port:/ {print $2}' "$CONFIG")
PY=python3
[ -x .venv/bin/python ] && PY=.venv/bin/python
echo "Config: $CONFIG   Dashboard: http://127.0.0.1:${PORT}   (Ctrl+C to stop)"
PYTHONPATH=src exec "$PY" -m edge.supervisor --config "$CONFIG" "$@"
