#!/usr/bin/env python
"""Validate streamed risk scores against real ICU outcomes (Challenge 2012).

Joins PG `risk_scores` (or a scores CSV) to Outcomes-a.txt by RecordID and asks
the only question that matters: does a high model score track real badness?

  - AUROC for in-hospital death using per-patient max risk as the discriminator
  - mean/median max risk: survivors vs non-survivors
  - Spearman correlation: max risk vs SAPS-I, SOFA, length of stay
  - risk-level mix among dead vs alive

Usage:
  PYTHONPATH=src .venv/bin/python scripts/validate_outcomes.py
  ... --agg mean            # per-patient mean instead of max risk
  ... --scores-csv p.csv    # skip PG, read (patient_id,risk_score) csv
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def load_scores(args) -> pd.DataFrame:
    if args.scores_csv:
        return pd.read_csv(args.scores_csv)
    import psycopg2
    from vytals.config import load_config
    pg = load_config()["postgres"]
    conn = psycopg2.connect(host=pg["host"], port=pg["port"], dbname=pg["db"],
                            user=pg["user"], password=pg["password"], connect_timeout=5)
    try:
        return pd.read_sql(
            "SELECT patient_id, risk_score, risk_level, window_end FROM risk_scores",
            conn)
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outcomes", default=str(ROOT / "data/physionet2012/Outcomes-a.txt"))
    ap.add_argument("--scores-csv", default=None, help="optional CSV with patient_id,risk_score (skips PG)")
    ap.add_argument("--agg", choices=["max", "mean"], default="mean")
    a = ap.parse_args()

    scores = load_scores(a)
    if scores.empty:
        print("no risk_scores found — run the stream first (make producer + make stream)")
        return 1
    scores["patient_id"] = scores["patient_id"].astype(str)
    per_pt = (scores.groupby("patient_id")["risk_score"]
              .agg(a.agg).rename(f"risk_{a.agg}").reset_index())

    oc = pd.read_csv(a.outcomes)
    oc["RecordID"] = oc["RecordID"].astype(str)
    df = per_pt.merge(oc, left_on="patient_id", right_on="RecordID", how="inner")
    r = f"risk_{a.agg}"

    print(f"matched {len(df)} patients (scores from {scores.patient_id.nunique()} in PG, "
          f"{len(oc)} outcomes)\n")

    # 1) survivors vs non-survivors
    dead = df[df["In-hospital_death"] == 1]
    alive = df[df["In-hospital_death"] == 0]
    print(f"{'group':<14}{'n':>5}{'mean risk':>11}{'median risk':>13}")
    for name, g in (("survivors", alive), ("non-survivors", dead)):
        if len(g):
            print(f"{name:<14}{len(g):>5}{g[r].mean():>11.1f}{g[r].median():>13.1f}")

    # 2) AUROC for death
    try:
        from sklearn.metrics import roc_auc_score
        if len(dead) and len(alive):
            auc = roc_auc_score(df["In-hospital_death"], df[r])
            print(f"\nAUROC (in-hospital death, {r}): {auc:.3f}")
    except Exception as e:
        print(f"\nAUROC unavailable: {e}")

    # 3) correlations with severity scores
    print(f"\nSpearman corr vs {r}:")
    for col in ("SAPS-I", "SOFA", "Length_of_stay"):
        sub = df[[r, col]].dropna()
        sub = sub[sub[col] >= 0]  # -1 = censored LOS
        if len(sub) > 5:
            rho = sub[r].corr(sub[col], method="spearman")
            print(f"  {col:<16} rho={rho:+.3f}  (n={len(sub)})")

    # 4) level mix
    print("\nrisk_level mix by mortality (row %):")
    lvl = (scores.assign(dead=scores["patient_id"].map(
               oc.set_index("RecordID")["In-hospital_death"]))
           .groupby(["dead", "risk_level"]).size().unstack(fill_value=0))
    lvl_pct = lvl.div(lvl.sum(axis=1), axis=0).round(3) * 100
    lvl_pct.index = ["alive" if i == 0 else "dead" for i in lvl_pct.index]
    print(lvl_pct.to_string())

    print("\ninterpretation: the model was trained on rule-based derangement labels,")
    print("not mortality — correlation here is external validity, not train/eval leakage.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
