from __future__ import annotations
import os
from pathlib import Path
import yaml

DEFAULTS = {
    "kafka": {"bootstrap_servers": "localhost:9092", "topic": "vitals.raw", "partitions": 3, "acks": "all"},
    "streaming": {"watermark": "2 minutes", "window": "5 minutes", "slide": "1 minute"},
    "postgres": {"host": "localhost", "port": 5432, "db": "vytals", "user": "vytals", "password": "vytals"},
    "api": {"host": "0.0.0.0", "port": 8000},
    "model": {"path": "models/xgb_risk.json", "version": "v1"},
}

def load_config(path: str | None = None) -> dict:
    p = path or os.environ.get("VYTALS_CONFIG") or "config/default.yaml"
    ROOT = Path(__file__).resolve().parents[2]  # .../vytal
    # try absolute/cwd first, then repo-root relative
    candidates = [Path(p), Path.cwd() / p, ROOT / p]
    for c in candidates:
        if c.exists():
            with open(c) as f:
                data = yaml.safe_load(f) or {}
            merged = {**DEFAULTS, **data}
            return merged
    return dict(DEFAULTS)
