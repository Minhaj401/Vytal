"""Batch inference: records -> features -> risk 0..100."""
from __future__ import annotations
import argparse
import pandas as pd
from vytals.data.loader import load_any
from vytals.features.windowed import build_features
from vytals.ml.risk_model import RiskModel

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--model", default="models/xgb_risk.json")
    ap.add_argument("--limit", type=int, default=20)
    a = ap.parse_args()
    df = load_any(a.input)
    # per-patient latest window: last 30 rows per patient (~15 min at 30s cadence)
    outs = []
    m = RiskModel.load(a.model)
    d = df.copy()
    d["event_time"] = pd.to_datetime(d["event_time"], utc=True)
    for pid, g in d.groupby("patient_id"):
        g = g.sort_values("event_time").tail(30)
        f = build_features(g)
        outs.append(m.predict(f))
    res = pd.concat(outs, ignore_index=True).sort_values("risk_score", ascending=False)
    print(res.head(a.limit).to_string(index=False))

if __name__ == "__main__":
    main()
