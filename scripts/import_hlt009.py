"""Convert the HLT-009 sample (vitals_timeseries.csv) into the vytals canonical schema.

    episode_id -> patient_id          hr_bpm -> hr          spo2_pct -> spo2
    rr_bpm -> rr                      temp_c -> temp_c
    nbp_sys_mmhg|ibp_sys_mmhg -> sbp   (non-invasive preferred)
    nbp_dia_mmhg|ibp_dia_mmhg -> dbp
    news2_score, qsofa_score -> carried through as extra columns (the dataset's labels)

Run:  python scripts/import_hlt009.py [--input data/hlt009/vitals_timeseries.csv] [--out data/hlt009.csv]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="data/hlt009/vitals_timeseries.csv")
    ap.add_argument("--out", default="data/hlt009.csv")
    a = ap.parse_args()

    v = pd.read_csv(a.input)
    ts = pd.to_datetime(v["timestamp"], errors="coerce")
    keep = v["episode_id"].notna() & ts.notna()
    v, ts = v[keep], ts[keep]

    out = pd.DataFrame({
        "patient_id": v["episode_id"],
        "event_time": ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seq": v.groupby("episode_id").cumcount() + 1,
        "hr": v["hr_bpm"],
        "spo2": v["spo2_pct"],
        "rr": v["rr_bpm"],
        "temp_c": v["temp_c"],
        "sbp": v["nbp_sys_mmhg"].combine_first(v["ibp_sys_mmhg"]),
        "dbp": v["nbp_dia_mmhg"].combine_first(v["ibp_dia_mmhg"]),
        "news2_score": v["news2_score"],
        "qsofa_score": v["qsofa_score"],
    })
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(a.out, index=False)
    print(f"wrote {a.out}: {len(out)} rows, {out.patient_id.nunique()} episodes")


if __name__ == "__main__":
    main()
