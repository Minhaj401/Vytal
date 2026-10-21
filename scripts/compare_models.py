#!/usr/bin/env python
"""Compare ML models for the risk task under patient-wise CV (abstract obj 13).

Models: LogisticRegression, RandomForest, XGBoost (supervised on rule-derived
risk labels) + IsolationForest (unsupervised anomaly detection, no labels).

Metrics (abstract §5): Precision, Recall, F1, ROC-AUC for "high risk"
(rule score >= 50), patient-wise GroupShuffleSplit CV so one patient's windows
never land in both sides. IsolationForest reports AUROC only (it has no
predictions to threshold — decision scores vs the high-risk label).

Usage: PYTHONPATH=src .venv/bin/python scripts/compare_models.py [--folds 5]
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / "data/cache/windows_real.parquet"

def load_xy():
    from vytals.data.loader import load_any
    from vytals.ml.train import windowize
    from vytals.features.windowed import FEATURE_COLS
    from vytals.ml.risk_model import RiskModel
    if CACHE.exists():
        w = pd.read_parquet(CACHE)
    else:
        print("(no window cache — building, takes a few minutes)")
        w = windowize(load_any(str(ROOT / "data/vitals_real.csv")))
    y = RiskModel.rule_score(w)          # transparent physiology rules (same as train.py)
    X = w[FEATURE_COLS].astype(float)
    return X, y, w["patient_id"].to_numpy()

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--folds", type=int, default=5)
    a = ap.parse_args()

    X, y, groups = load_xy()
    yb = (y >= 50).astype(int)           # HIGH-or-worse = actionable
    print(f"data: {len(X)} windows, {len(np.unique(groups))} patients, "
          f"high-risk rate {yb.mean():.1%}\n")

    from sklearn.model_selection import GroupShuffleSplit
    from sklearn.preprocessing import StandardScaler
    from sklearn.pipeline import make_pipeline
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import RandomForestClassifier, IsolationForest
    from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score
    import xgboost as xgb

    gss = GroupShuffleSplit(n_splits=a.folds, test_size=0.2, random_state=42)
    splits = list(gss.split(X, yb, groups))

    def supervised(name, make):
        rows = []
        for tr, te in splits:
            clf = make()
            clf.fit(X.iloc[tr], yb[tr])
            prob = (clf.predict_proba(X.iloc[te])[:, 1] if hasattr(clf, "predict_proba")
                    else clf.decision_function(X.iloc[te]))
            pred = (prob >= 0.5).astype(int)
            rows.append([precision_score(yb[te], pred, zero_division=0),
                         recall_score(yb[te], pred, zero_division=0),
                         f1_score(yb[te], pred, zero_division=0),
                         roc_auc_score(yb[te], prob)])
        return name, np.mean(rows, axis=0)

    results = [
        supervised("LogisticRegression", lambda: make_pipeline(
            StandardScaler(), LogisticRegression(max_iter=1000))),
        supervised("RandomForest", lambda: RandomForestClassifier(
            n_estimators=200, min_samples_leaf=3, random_state=42, n_jobs=-1)),
        supervised("XGBoost", lambda: xgb.XGBClassifier(
            n_estimators=300, max_depth=5, learning_rate=0.1,
            eval_metric="logloss", random_state=42)),
    ]

    # unsupervised anomaly detection — no labels seen
    if_rows = []
    for tr, te in splits:
        iso = IsolationForest(contamination=0.15, random_state=42, n_jobs=-1)
        iso.fit(X.iloc[tr])
        score = -iso.score_samples(X.iloc[te])          # higher = more anomalous
        if_rows.append(roc_auc_score(yb[te], score))
    results.append(("IsolationForest*", np.array([np.nan, np.nan, np.nan, np.mean(if_rows)])))

    print(f"{'model':<22}{'Precision':>10}{'Recall':>9}{'F1':>8}{'AUROC':>8}   (mean over {a.folds} patient-wise folds)")
    for name, (p, r, f, auc) in results:
        fmt = lambda v: f"{v:.3f}" if v == v else "  —  "
        print(f"{name:<22}{fmt(p):>10}{fmt(r):>9}{fmt(f):>8}{fmt(auc):>8}")
    print("\n* IsolationForest is unsupervised (no labels in training) — AUROC only.")
    print("target = rule-derived risk >= 50 (HIGH+); split is patient-wise (no leakage).")
    return 0

if __name__ == "__main__":
    sys.exit(main())
