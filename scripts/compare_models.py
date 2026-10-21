#!/usr/bin/env python
"""Compare ML models for the risk task under patient-wise CV (abstract obj 13).

Models: LogisticRegression, RandomForest, XGBoost (supervised on rule-derived
risk labels) + IsolationForest (unsupervised anomaly detection, no labels).

Metrics (abstract §5): Precision, Recall, F1, ROC-AUC for "high risk"
(rule score >= 50), patient-wise GroupShuffleSplit CV so one patient's windows
never land in both sides. IsolationForest reports AUROC only (it has no
predictions to threshold — decision scores vs the high-risk label).
The shipped model (XGBoost) also gets an out-of-fold confusion matrix at
threshold 0.5 — printed and saved to docs/screenshots/confusion_matrix.png.

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
    oof: dict[str, tuple[np.ndarray, np.ndarray]] = {}

    def supervised(name, make, collect=False):
        rows = []
        ys, ps = [], []
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
            if collect:
                ys.append(yb[te]); ps.append(pred)
        if collect:
            oof[name] = (np.concatenate(ys), np.concatenate(ps))
        return name, np.mean(rows, axis=0)

    results = [
        supervised("LogisticRegression", lambda: make_pipeline(
            StandardScaler(), LogisticRegression(max_iter=1000))),
        supervised("RandomForest", lambda: RandomForestClassifier(
            n_estimators=200, min_samples_leaf=3, random_state=42, n_jobs=-1)),
        supervised("XGBoost", lambda: xgb.XGBClassifier(
            n_estimators=300, max_depth=5, learning_rate=0.1,
            eval_metric="logloss", random_state=42), collect=True),
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

    # out-of-fold confusion matrix for the shipped model (threshold 0.5)
    from sklearn.metrics import confusion_matrix
    yt, pr = oof["XGBoost"]
    tn, fp, fn, tp = confusion_matrix(yt, pr, labels=[0, 1]).ravel()
    print(f"\nXGBoost confusion matrix — out-of-fold, threshold 0.5 "
          f"({a.folds} patient-wise folds, {len(yt):,} windows):")
    print(f"                 pred LOW   pred HIGH")
    print(f"  actual LOW     {tn:>8}   {fp:>8}")
    print(f"  actual HIGH    {fn:>8}   {tp:>8}")
    print(f"  (TN={tn:,} FP={fp:,} FN={fn:,} TP={tp:,})")
    save_confusion_plot(tn, fp, fn, tp, a.folds, len(yt))
    return 0

def save_confusion_plot(tn: int, fp: int, fn: int, tp: int, n_folds: int, n_rows: int) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    m = np.array([[tn, fp], [fn, tp]])
    fig, ax = plt.subplots(figsize=(4.4, 3.8))
    ax.imshow(m, cmap="Blues")
    labels = ["LOW (<50)", "HIGH (≥50)"]
    ax.set_xticks([0, 1], labels)
    ax.set_yticks([0, 1], labels)
    ax.set_xlabel("Predicted")
    ax.set_ylabel("Actual")
    thresh = m.max() / 2
    for i in range(2):
        for j in range(2):
            ax.text(j, i, f"{m[i, j]:,}", ha="center", va="center", fontsize=14,
                    fontweight="bold", color="white" if m[i, j] > thresh else "#12395c")
    ax.set_title(f"XGBoost — out-of-fold confusion (thr 0.5)\n"
                 f"{n_folds} patient-wise folds · {n_rows:,} windows")
    fig.tight_layout()
    out = ROOT / "docs/screenshots/confusion_matrix.png"
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  plot -> {out.relative_to(ROOT)}")

if __name__ == "__main__":
    sys.exit(main())
