"""Backward-compatibility alias only.

The shared modules now live at the paths the course README lists (src/edge/ipc.py,
models.py, storage.py, plus config.py and procfs.py). This alias keeps any old
`from edge.core.storage import Storage` style import working. New code: import from `edge.*`.
"""
import sys

from edge import config, ipc, models, procfs, storage

for _name, _mod in (("config", config), ("ipc", ipc), ("models", models),
                    ("procfs", procfs), ("storage", storage)):
    sys.modules[f"{__name__}.{_name}"] = _mod
