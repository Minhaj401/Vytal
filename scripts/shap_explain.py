#!/usr/bin/env python
"""SHAP explainability for the risk model (abstract obj 14).

- global: mean |SHAP| per feature across windows -> docs/screenshots/shap_global.png
- local : SHAP contributions for the single riskiest window -> docs/screenshots/shap_patient.png

Usage: PYTHONPATH=src .venv/bin/python scripts/shap_explain.py
"""
from __future__ import annotations
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "data/cache/windows_real.parquet"
SHOTS = ROOT / "docs/screenshots"

def main() -> int:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shap
    from vytals.features.windowed import FEATURE_COLS
    from vytals.ml.risk_model import RiskModel

    if CACHE.exists():
        w = pd.read_parquet(CACHE)
    else:
        from vytals.data.loader import load_any
        from vytals.ml.train import windowize
        w = windowize(load_any(str(ROOT / "data/vitals_real.csv")))

    model = RiskModel.load(str(ROOT / "models/xgb_risk_real.json"))
    assert model.booster is not None, "no booster — train first (make train-real)"

    X = w[FEATURE_COLS].astype(float).fillna(0.0)
    sample = X.sample(min(4000, len(X)), random_state=42)

    expl = shap.TreeExplainer(model.booster)
    sv = expl.shap_values(sample)                    # (n, p)
    mean_abs = np.abs(sv).mean(axis=0)
    order = np.argsort(mean_abs)

    # global bar
    SHOTS.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(9, 7))
    top = order[-15:]
    plt.barh([FEATURE_COLS[i] for i in top], mean_abs[top], color="#3b82f6")
    plt.xlabel("mean |SHAP| (contribution to risk score)")
    plt.title("Vytals XGBoost — global feature importance (SHAP)")
    plt.tight_layout()
    plt.savefig(SHOTS / "shap_global.png", dpi=140)
    plt.close()

    print("top 10 features by mean |SHAP|:")
    for i in order[-10:][::-1]:
        print(f"  {FEATURE_COLS[i]:<24} {mean_abs[i]:.4f}")

    # local: riskiest window
    preds = model.predict(w.assign(**{c: X[c] for c in FEATURE_COLS}))
    i = int(preds["risk_score"].idxmax())
    row = X.iloc[[i]]
    sv_row = expl.shap_values(row)[0]
    k = 10
    top_k = np.argsort(np.abs(sv_row))[-k:]
    plt.figure(figsize=(9, 6))
    colors = ["#ef4444" if v > 0 else "#22c55e" for v in sv_row[top_k]]
    plt.barh([FEATURE_COLS[j] for j in top_k], sv_row[top_k], color=colors)
    plt.xlabel("SHAP value (push toward higher risk)")
    plt.title(f"Riskiest window — {preds.loc[i, 'patient_id']} @ {preds.loc[i, 'window_end']} "
              f"(risk {preds.loc[i, 'risk_score']:.1f}, {preds.loc[i, 'risk_level']})")
    plt.tight_layout()
    plt.savefig(SHOTS / "shap_patient.png", dpi=140)
    plt.close()
    print(f"\nlocal explanation for riskiest window saved -> docs/screenshots/shap_patient.png")
    print("global plot -> docs/screenshots/shap_global.png")
    return 0

if __name__ == "__main__":
    sys.exit(main())
