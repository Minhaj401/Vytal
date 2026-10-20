from __future__ import annotations
import os
from pathlib import Path
import yaml

DEFAULTS = {
    "kafka": {"bootstrap_servers": "localhost:9092", "topic": "vitals.raw", "partitions": 3, "acks": "all"},
    "streaming": {"watermark": "2 minutes", "window": "5 minutes", "slide": "1 minute",
                  "trigger": "30 seconds", "starting_offsets": "latest"},
    "hdfs": {"uri": "", "base": "/vytals"},
    "postgres": {"host": "localhost", "port": 5432, "db": "vytals", "user": "vytals", "password": "vytals"},
    "api": {"host": "0.0.0.0", "port": 8000},
    "model": {"path": "models/xgb_risk.json", "version": "v1"},
}


def _merge(base: dict, override: dict) -> dict:
    out = dict(base)
    for k, v in override.items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def load_config(path: str | None = None) -> dict:
    p = path or os.environ.get("VYTALS_CONFIG") or "config/default.yaml"
    ROOT = Path(__file__).resolve().parents[2]  # .../vytal
    # try absolute/cwd first, then repo-root relative
    candidates = [Path(p), Path.cwd() / p, ROOT / p]
    for c in candidates:
        if c.exists():
            with open(c) as f:
                data = yaml.safe_load(f) or {}
            return _merge(DEFAULTS, data)
    return dict(DEFAULTS)
