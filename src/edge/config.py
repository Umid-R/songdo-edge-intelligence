"""Config loader. Relative paths in node.yaml are resolved from the repo root."""
import os

import yaml


def load_config(path):
    path = os.path.abspath(path)
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    root = os.path.dirname(os.path.dirname(path))
    cfg["_root"] = root
    for key, value in cfg.get("paths", {}).items():
        if not os.path.isabs(value):
            cfg["paths"][key] = os.path.join(root, value)
    return cfg
