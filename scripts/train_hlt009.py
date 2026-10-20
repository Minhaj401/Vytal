"""Train + evaluate XGBoost on the HLT-009 synthetic ICU sample.

Tasks (both episode-wise split, no leakage):
  1. regression     : predict news2_score (MAE / R2)
  2. classification : NEWS2 >= 5 (urgent-response threshold) -> accuracy / P / R / F1 / AUC + confusion matrix
  3. classification : qSOFA >= 2 (sepsis screen; HLT-009 supplies the score)

Features come from the shared windowed builder (src/vytals/features/windowed.py)
so training matches the streaming/API feature definitions.

Run:  .venv/bin/python scripts/train_hlt009.py
"""
from __future__ import annotations
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from vytals.features.windowed import FEATURE_COLS, build_features  # noqa: E402

WINDOW = "60min"  # 5-min cadence -> 12 rows per window


def windowize_with_labels(df: pd.DataFrame, window: str = WINDOW) -> pd.DataFrame:
    """Non-overlapping windows per episode; label = score at window end."""
    d = df.copy()
    d["event_time"] = pd.to_datetime(d["event_time"], utc=True)
    out = []
    for pid, g in d.groupby("patient_id"):
        g = g.sort_values("event_time")
        for _, w in g.groupby(pd.Grouper(key="event_time", freq=window)):
            if w.empty:
                continue
            f = build_features(w.assign(patient_id=pid))
            if f.empty:
                continue
            f = f.copy()
            f["news2_label"] = float(w["news2_score"].iloc[-1])
            f["qsofa_label"] = float(w["qsofa_score"].iloc[-1])
            out.append(f)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def confusion(y_true, y_pred) -> dict:
    tp = int(((y_true == 1) & (y_pred == 1)).sum())
    fp = int(((y_true == 0) & (y_pred == 1)).sum())
    fn = int(((y_true == 1) & (y_pred == 0)).sum())
    tn = int(((y_true == 0) & (y_pred == 0)).sum())
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn}


def fmt_cm(cm: dict) -> str:
    return (
        "                 pred=0   pred=1\n"
        f"  actual=0       {cm['tn']:6d}   {cm['fp']:6d}\n"
        f"  actual=1       {cm['fn']:6d}   {cm['tp']:6d}"
    )


def main():
    csv = ROOT / "data/hlt009.csv"
    if not csv.exists():
        raise SystemExit("data/hlt009.csv missing — run: python scripts/import_hlt009.py")
    df = pd.read_csv(csv)
    print(f"loaded {len(df)} rows, {df.patient_id.nunique()} episodes")
    F = windowize_with_labels(df)
    if F.empty:
        raise SystemExit("no windows built")
    n_all = len(F)
    F = F.dropna(subset=["news2_label", "qsofa_label"]).reset_index(drop=True)
    print(f"windows: {len(F)} (dropped {n_all - len(F)} without labels)  features: {len(FEATURE_COLS)}")

    X = F[FEATURE_COLS].astype(float)

    import xgboost as xgb
    from sklearn.model_selection import GroupShuffleSplit
    from sklearn.metrics import (
        mean_absolute_error, r2_score, accuracy_score, precision_score,
        recall_score, f1_score, roc_auc_score, confusion_matrix,
    )

    groups = F["patient_id"].values
    gss = GroupShuffleSplit(n_splits=1, test_size=0.25, random_state=42)
    tr, te = next(gss.split(X, groups=groups))
    print(f"train episodes={len(set(groups[tr]))} test episodes={len(set(groups[te]))}")

    metrics: dict = {"dataset": "HLT-009 sample (synthetic, CC-BY-NC-4.0)",
                     "git": "xpertsystems/hlt009-sample", "window": WINDOW,
                     "rows": int(len(df)), "episodes": int(df.patient_id.nunique()),
                     "windows": int(len(F)),
                     "train_episodes": len(set(groups[tr])), "test_episodes": len(set(groups[te]))}

    # ---- 1. regression: news2_score ----
    y = F["news2_label"].values.astype(float)
    dtr, dte = xgb.DMatrix(X.iloc[tr], label=y[tr]), xgb.DMatrix(X.iloc[te], label=y[te])
    params = {"objective": "reg:squarederror", "max_depth": 5, "eta": 0.08,
              "subsample": 0.9, "colsample_bytree": 0.9, "eval_metric": "mae", "seed": 42}
    bst = xgb.train(params, dtr, num_boost_round=300, evals=[(dte, "test")],
                    early_stopping_rounds=30, verbose_eval=False)
    pred = bst.predict(dte)
    reg = {"mae": round(float(mean_absolute_error(y[te], pred)), 3),
           "r2": round(float(r2_score(y[te], pred)), 4)}
    print(f"\n[regression] NEWS2: MAE={reg['mae']}  R2={reg['r2']}")
    Path(ROOT / "models").mkdir(exist_ok=True)
    bst.save_model(str(ROOT / "models/xgb_hlt009_news2.json"))
    metrics["regression_news2"] = reg

    # ---- 2. classification: NEWS2 >= 5 (urgent-response threshold) ----
    for name, label_col, thr, model_file in [
        ("news2_ge5", "news2_label", 5.0, "models/xgb_hlt009_news2_ge5.json"),
        # qSOFA never exceeds 1 in this sample (mental status is not a stream),
        # so the usable cut is "any abnormal component".
        ("qsofa_ge1", "qsofa_label", 1.0, "models/xgb_hlt009_qsofa_ge1.json"),
    ]:
        yb = (F[label_col].values.astype(float) >= thr).astype(int)
        clf = xgb.XGBClassifier(
            n_estimators=300, max_depth=5, learning_rate=0.08, subsample=0.9,
            colsample_bytree=0.9, eval_metric="logloss", random_state=42,
            early_stopping_rounds=30, n_jobs=4,
        )
        clf.fit(X.iloc[tr], yb[tr], eval_set=[(X.iloc[te], yb[te])], verbose=False)
        yp = clf.predict(X.iloc[te])
        yprob = clf.predict_proba(X.iloc[te])[:, 1]
        cm = confusion(yb[te], yp)
        res = {
            "threshold": thr,
            "positives_test": int(yb[te].sum()), "n_test": int(len(te)),
            "accuracy": round(float(accuracy_score(yb[te], yp)), 4),
            "precision": round(float(precision_score(yb[te], yp, zero_division=0)), 4),
            "recall": round(float(recall_score(yb[te], yp, zero_division=0)), 4),
            "f1": round(float(f1_score(yb[te], yp, zero_division=0)), 4),
            "auc": round(float(roc_auc_score(yb[te], yprob)), 4) if 0 < yb[te].sum() < len(te) else None,
            "confusion_matrix": cm,
        }
        print(f"\n[classification] {name}: acc={res['accuracy']} P={res['precision']} "
              f"R={res['recall']} F1={res['f1']} AUC={res['auc']}")
        print(fmt_cm(cm))
        print("sklearn cm [[tn,fp],[fn,tp]]:", confusion_matrix(yb[te], yp).tolist())
        clf.get_booster().save_model(str(ROOT / model_file))
        metrics[name] = res

    out = ROOT / "data/hlt009_metrics.json"
    out.write_text(json.dumps(metrics, indent=2))
    print(f"\nmetrics -> {out}")


if __name__ == "__main__":
    main()
