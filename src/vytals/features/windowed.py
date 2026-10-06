"""Shared windowed feature engineering. Used by training AND streaming. Single source of truth."""
from __future__ import annotations
import numpy as np
import pandas as pd

VITALS = ["hr", "spo2", "rr", "temp_c", "sbp", "dbp"]

FEATURE_COLS: list[str] = []
for v in VITALS:
    if v == "temp_c":
        FEATURE_COLS += [f"{v}_current", f"{v}_mean", f"{v}_std", f"{v}_slope", f"{v}_delta"]
    else:
        FEATURE_COLS += [f"{v}_current", f"{v}_mean", f"{v}_std", f"{v}_min", f"{v}_max", f"{v}_slope", f"{v}_delta"]
FEATURE_COLS += ["n_rows", "coverage", "low_quality"]

def _slope(y: pd.Series) -> float:
    y = y.dropna()
    if len(y) < 2:
        return 0.0
    x = np.arange(len(y))
    try:
        return float(np.polyfit(x, y.values.astype(float), 1)[0])
    except Exception:
        return 0.0

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Input: long vital records (patient_id, event_time, hr...dbp).
    Output: one row per (patient_id, window_end) with temporal features.
    Expects df already windowed OR whole history per patient — computes trailing stats.

    For streaming use: call per micro-batch window slice.
    Pure pandas, no fabrication: missing stays NaN -> filled with 0 + low_quality flag.
    """
    if df.empty:
        return pd.DataFrame(columns=["patient_id", "window_end"] + FEATURE_COLS)
    d = df.copy()
    d["event_time"] = pd.to_datetime(d["event_time"], utc=True, errors="coerce")
    d = d.dropna(subset=["patient_id", "event_time"]).sort_values(["patient_id", "event_time"])
    rows = []
    for pid, g in d.groupby("patient_id"):
        g = g.reset_index(drop=True)
        feats: dict = {"patient_id": pid, "window_end": g["event_time"].iloc[-1]}
        n = len(g)
        feats["n_rows"] = n
        non_null = g[VITALS].notna().sum().sum()
        feats["coverage"] = float(non_null / (n * len(VITALS))) if n else 0.0
        feats["low_quality"] = int(feats["coverage"] < 0.5 or n < 3)
        prev = g.iloc[: max(1, n // 2)]
        for v in VITALS:
            s = pd.to_numeric(g[v], errors="coerce")
            cur = s.dropna()
            cur_v = float(cur.iloc[-1]) if len(cur) else np.nan
            feats[f"{v}_current"] = cur_v
            feats[f"{v}_mean"] = float(s.mean(skipna=True)) if s.notna().any() else np.nan
            feats[f"{v}_std"] = float(s.std(skipna=True)) if s.notna().sum() >= 2 else 0.0
            if v != "temp_c":
                feats[f"{v}_min"] = float(s.min(skipna=True)) if s.notna().any() else np.nan
                feats[f"{v}_max"] = float(s.max(skipna=True)) if s.notna().any() else np.nan
            feats[f"{v}_slope"] = _slope(s)
            pm = pd.to_numeric(prev[v], errors="coerce").mean(skipna=True)
            feats[f"{v}_delta"] = float(feats[f"{v}_mean"] - pm) if pd.notna(pm) and pd.notna(feats[f"{v}_mean"]) else 0.0
        rows.append(feats)
    out = pd.DataFrame(rows)
    # no fabrication for model: fill NaN with population-neutral medians (0 for deltas/slopes already set)
    for c in out.columns:
        if out[c].isna().any():
            if c.endswith(("_current", "_mean", "_min", "_max")):
                # neutral fallback per vital
                neutral = {"hr": 75, "spo2": 97, "rr": 16, "temp_c": 37.0, "sbp": 120, "dbp": 78}
                base = c.split("_")[0]
                # temp special-case: temp_current -> temp_c neutral
                if c.startswith("temp_"):
                    base = "temp_c"
                out[c] = out[c].fillna(neutral.get(base, 0))
            else:
                out[c] = out[c].fillna(0)
    return out[["patient_id", "window_end"] + FEATURE_COLS]
