"""In-memory replay engine: powers Next.js live UI without requiring Kafka.
Replays canonical CSV, steps through event_time, computes features + risk on the fly.
"""
from __future__ import annotations
import threading, time
from collections import defaultdict, deque
import pandas as pd
from vytals.data.loader import load_any
from vytals.features.windowed import build_features
from vytals.ml.risk_model import RiskModel

class Replay:
    def __init__(self, csv_path: str, model_path: str = "models/xgb_risk.json", window_rows: int = 30):
        self.df = load_any(csv_path)
        self.df["event_time"] = pd.to_datetime(self.df["event_time"], utc=True)
        self.df = self.df.sort_values("event_time").reset_index(drop=True)
        self.model = RiskModel.load(model_path)
        self.window_rows = window_rows
        self.idx = 0
        self.buffers: dict[str, deque] = defaultdict(lambda: deque(maxlen=window_rows))
        self.latest: dict = {}   # patient_id -> last record + risk
        self.history: dict[str, list] = defaultdict(list)
        self.lock = threading.RLock()
        self.running = False

    @staticmethod
    def _json_safe(v):
        # numpy / pandas scalars -> plain python; NaN -> None
        try:
            import math
            if v is None:
                return None
            if isinstance(v, float) and math.isnan(v):
                return None
            if hasattr(v, "item"):
                try:
                    v = v.item()
                except Exception:
                    pass
            if isinstance(v, float) and math.isnan(v):
                return None
            return v
        except Exception:
            return None

    def step(self, n: int = 5) -> list[dict]:
        out = []
        with self.lock:
            for _ in range(n):
                if self.idx >= len(self.df):
                    self.idx = 0  # loop; clear stale windows so new loop starts clean
                    self.buffers.clear()
                r = self.df.iloc[self.idx]
                self.idx += 1
                pid = str(r["patient_id"])
                rec = {}
                for c in self.df.columns:
                    if c in ("patient_id", "event_time", "seq"):
                        continue
                    rec[c] = self._json_safe(r[c])
                rec["patient_id"] = pid
                rec["event_time"] = r["event_time"].isoformat()
                rec["seq"] = int(r["seq"]) if pd.notna(r.get("seq")) else None
                self.buffers[pid].append(rec)
                buf = pd.DataFrame(list(self.buffers[pid]))
                feats = build_features(buf)
                scored = self.model.predict(feats)
                s = scored.iloc[0].to_dict()
                live = {**rec, "p_risk": float(s["p_risk"]), "risk_score": float(s["risk_score"]), "risk_level": s["risk_level"]}
                self.latest[pid] = live
                self.history[pid].append(live)
                if len(self.history[pid]) > 400:
                    self.history[pid] = self.history[pid][-400:]
                out.append(live)
        return out

    def patients(self):
        with self.lock:
            return sorted(self.latest.keys()) or sorted(self.df.patient_id.astype(str).unique().tolist())

    def loop(self, batch: int = 4, interval: float = 1.0):
        self.running = True
        while self.running:
            try:
                self.step(batch)
            except Exception as e:
                print(f"[replay] step failed: {e}")
            time.sleep(interval)
