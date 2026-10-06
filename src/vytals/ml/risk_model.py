"""XGBoost risk model. Output: Risk Score 0-100 + level. Internal P(risk)."""
from __future__ import annotations
import json
from pathlib import Path
import numpy as np
import pandas as pd

try:
    import xgboost as xgb
except Exception:
    xgb = None

from vytals.features.windowed import FEATURE_COLS

LEVELS = [(25, "LOW"), (50, "MODERATE"), (75, "HIGH"), (101, "CRITICAL")]

def risk_level(score: float) -> str:
    for thresh, name in LEVELS:
        if score < thresh:
            return name
    return "CRITICAL"

def p_to_score(p: float) -> float:
    return round(float(np.clip(p * 100.0, 0, 100)), 1)

class RiskModel:
    def __init__(self, booster=None, fallback_thresholds: bool = True):
        self.booster = booster
        self.fallback = fallback_thresholds

    @staticmethod
    def rule_score(feats: pd.DataFrame) -> np.ndarray:
        """Transparent physiology rules -> 0..100. Used as fallback AND label generator."""
        def col(n, d=0):
            return feats[n].fillna(d).astype(float) if n in feats else pd.Series([d] * len(feats))
        s = np.zeros(len(feats))
        hr, spo2, rr, tmp, sbp, dbp = (col(f"{v}_current", {"hr":75,"spo2":97,"rr":16,"temp_c":37,"sbp":120,"dbp":78}[v]) for v in ["hr","spo2","rr","temp_c","sbp","dbp"])
        hr_m = col("hr_mean", 75); spo2_m = col("spo2_mean", 97)
        # tachy/bradycardia
        s += np.where(hr > 120, 22, np.where(hr > 100, 12, np.where(hr < 40, 25, np.where(hr < 50, 12, 0))))
        s += np.where(spo2 < 85, 30, np.where(spo2 < 90, 20, np.where(spo2 < 94, 10, 0)))
        s += np.where((rr > 30) | (rr < 8), 18, np.where((rr > 24) | (rr < 10), 9, 0))
        s += np.where((tmp >= 39) | (tmp < 35), 16, np.where((tmp >= 38) | (tmp < 36), 8, 0))
        s += np.where((sbp > 180) | (sbp < 80), 18, np.where((sbp > 160) | (sbp < 90), 9, 0))
        s += np.where((dbp > 120) | (dbp < 40), 12, np.where((dbp > 100) | (dbp < 50), 6, 0))
        # variability / trend penalties
        s += np.clip(col("hr_std", 0) / 15 * 8, 0, 10)
        s += np.clip(col("spo2_std", 0) / 3 * 8, 0, 10)
        s += np.clip(np.abs(col("hr_slope", 0)) * 12, 0, 8)
        s += np.clip(np.abs(col("sbp_slope", 0)) * 6, 0, 8)
        s += np.where(col("low_quality", 0) > 0, 5, 0)
        # sustained abnormality (mean)
        s += np.where((hr_m > 110) | (hr_m < 50), 6, 0)
        s += np.where(spo2_m < 92, 8, 0)
        return np.clip(s, 0, 100)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        if self.booster is not None and xgb is not None:
            dmat = xgb.DMatrix(X[FEATURE_COLS].astype(float))
            raw = np.asarray(self.booster.predict(dmat), dtype=float)
            # train.py is reg:squarederror with 0..100 labels -> P(risk) = score/100.
            # Heuristic kept for back-compat with any 0..1 classifier checkpoint:
            # per-row decision (not batch mean) so mixed batches can't poison each other.
            p = np.where(raw > 1.5, raw / 100.0, raw)
            return np.clip(p, 0, 1)
        # fallback: rule score / 100 as pseudo-probability
        return np.clip(self.rule_score(X) / 100.0, 0, 1)

    def predict(self, feats: pd.DataFrame) -> pd.DataFrame:
        p = self.predict_proba(feats)
        scores = [p_to_score(v) for v in p]
        out = feats[["patient_id"]].copy() if "patient_id" in feats else pd.DataFrame({"patient_id": ["?"] * len(feats)})
        out["p_risk"] = np.round(p, 4)
        out["risk_score"] = scores
        out["risk_level"] = [risk_level(s) for s in scores]
        if "window_end" in feats:
            out["window_end"] = feats["window_end"].values
        return out

    def save(self, path: str | Path):
        path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
        if self.booster is not None:
            self.booster.save_model(str(path))
        else:
            path.write_text(json.dumps({"fallback": True, "version": "v1"}))

    @classmethod
    def load(cls, path: str | Path) -> "RiskModel":
        path = Path(path)
        if not path.exists():
            return cls(None)
        # XGBoost .json checkpoints are JSON too — only treat as fallback marker
        # when the file actually parses to {"fallback": true}. Never crash on binary.
        try:
            if path.suffix == ".json" and path.stat().st_size < 1_000_000:
                txt = path.read_text(encoding="utf-8")
                if txt.lstrip().startswith("{"):
                    try:
                        if json.loads(txt).get("fallback"):
                            return cls(None)
                    except Exception:
                        pass  # valid XGBoost JSON model, fall through to booster load
        except (UnicodeDecodeError, OSError):
            pass
        if xgb is None:
            return cls(None)
        try:
            bst = xgb.Booster()
            bst.load_model(str(path))
        except Exception:
            # corrupt / wrong-format checkpoint -> safe rules fallback, never crash API
            return cls(None)
        return cls(bst)
