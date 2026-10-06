"""Train XGBoost risk regressor. Labels from physiology rules + noise -> 0..100 supervised target."""
from __future__ import annotations
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

from vytals.data.loader import load_any
from vytals.features.windowed import build_features, FEATURE_COLS, VITALS
from vytals.ml.risk_model import RiskModel

def windowize(df: pd.DataFrame, window: str = "5min") -> pd.DataFrame:
    """Slice long history into non-overlapping time windows per patient for training rows."""
    d = df.copy()
    d["event_time"] = pd.to_datetime(d["event_time"], utc=True)
    feats = []
    for pid, g in d.groupby("patient_id"):
        g = g.sort_values("event_time")
        for _, w in g.groupby(pd.Grouper(key="event_time", freq=window)):
            if w.empty:
                continue
            f = build_features(w.assign(patient_id=pid))
            feats.append(f)
    return pd.concat(feats, ignore_index=True) if feats else pd.DataFrame()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", default="models/xgb_risk.json")
    ap.add_argument("--window", default="5min")
    a = ap.parse_args()

    df = load_any(a.input)
    print(f"loaded {len(df)} rows, {df.patient_id.nunique()} patients")
    F = windowize(df, a.window)
    if F.empty:
        raise SystemExit("no training windows — check input")
    print(f"windows: {len(F)} features: {len(FEATURE_COLS)}")
    y = RiskModel.rule_score(F).astype(float)
    # add slight noise so model learns smooth mapping, keep in range
    rng = np.random.default_rng(7)
    y = np.clip(y + rng.normal(0, 2, len(y)), 0, 100)
    print(f"label mean={y.mean():.1f} std={y.std():.1f} min={y.min():.1f} max={y.max():.1f}")

    import xgboost as xgb
    from sklearn.model_selection import GroupShuffleSplit
    from sklearn.metrics import mean_absolute_error, r2_score
    X = F[FEATURE_COLS].astype(float)
    groups = F["patient_id"].values
    # patient-wise split: no patient in both train and test (prevents leakage)
    gss = GroupShuffleSplit(n_splits=1, test_size=0.2, random_state=42)
    tr_idx, te_idx = next(gss.split(X, y, groups))
    X_train, X_test, y_train, y_test = X.iloc[tr_idx], X.iloc[te_idx], y[tr_idx], y[te_idx]
    print(f"train patients={len(set(groups[tr_idx]))} test patients={len(set(groups[te_idx]))}")
    dtrain, dtest = xgb.DMatrix(X_train, label=y_train), xgb.DMatrix(X_test, label=y_test)
    params = {"objective": "reg:squarederror", "max_depth": 5, "eta": 0.08,
              "subsample": 0.9, "colsample_bytree": 0.9, "eval_metric": "mae", "seed": 42}
    bst = xgb.train(params, dtrain, num_boost_round=300,
                    evals=[(dtest, "test")], early_stopping_rounds=30, verbose_eval=50)
    pred = bst.predict(dtest)
    print(f"MAE={mean_absolute_error(y_test, pred):.2f} R2={r2_score(y_test, pred):.3f}")
    imp = sorted(bst.get_score(importance_type="gain").items(), key=lambda kv: -kv[1])[:10]
    print("top features:", imp)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    bst.save_model(a.out)
    print(f"saved -> {a.out}")

if __name__ == "__main__":
    main()
