"""Canonical loader: CSV / JSON / JSONL -> canonical vital records.

Canonical schema:
  patient_id:str, event_time:ISO8601 UTC, seq:int|None,
  hr, spo2, rr, temp_c, sbp, dbp (float|None)

Missing values allowed, never fabricated (stays None/NaN).
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import pandas as pd

CANONICAL_COLS = ["patient_id", "event_time", "seq", "hr", "spo2", "rr", "temp_c", "sbp", "dbp"]

# tolerant alias map for messy datasets
ALIASES = {
    "patient_id": ["patient_id", "subject_id", "patient", "id", "hadm_id"],
    "event_time": ["event_time", "timestamp", "time", "datetime", "charttime"],
    "seq": ["seq", "seqno", "row", "index"],
    "hr": ["hr", "heart_rate", "heartrate", "HR"],
    "spo2": ["spo2", "SpO2", "spo_2", "o2sat", "oxygen"],
    "rr": ["rr", "resp_rate", "respiratory_rate", "RR", "resp"],
    "temp_c": ["temp_c", "temp", "temperature", "Temp", "temp_f"],
    "sbp": ["sbp", "sys", "systolic", "SBP", "nibp_s", "abp_s"],
    "dbp": ["dbp", "dias", "diastolic", "DBP", "nibp_d", "abp_d"],
}

RANGES = {
    "hr": (20, 250), "spo2": (50, 100), "rr": (4, 60),
    "temp_c": (30, 43), "sbp": (50, 280), "dbp": (20, 180),
}

def _pick(df: pd.DataFrame, canon: str):
    for a in ALIASES[canon]:
        if a in df.columns:
            return a
    # case-insensitive fallback
    low = {c.lower(): c for c in df.columns}
    for a in ALIASES[canon]:
        if a.lower() in low:
            return low[a.lower()]
    return None

def to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    if _pick(df, "patient_id") is None:
        raise ValueError("input needs a patient id column (patient_id/subject_id/id)")
    if _pick(df, "event_time") is None:
        raise ValueError("input needs a timestamp column (event_time/timestamp/time)")
    out = pd.DataFrame()
    for col in CANONICAL_COLS:
        src = _pick(df, col)
        out[col] = df[src] if src is not None else None
    # temp_f -> temp_c conversion
    if "temp_f" in df.columns and out["temp_c"].isna().all():
        try:
            out["temp_c"] = (df["temp_f"].astype(float) - 32) * 5.0 / 9.0
        except Exception:
            pass
    # types
    out["patient_id"] = out["patient_id"].astype(str)
    out["event_time"] = pd.to_datetime(out["event_time"], utc=True, errors="coerce")
    for c in ["hr", "spo2", "rr", "temp_c", "sbp", "dbp"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
        lo, hi = RANGES[c]
        out.loc[(out[c] < lo) | (out[c] > hi), c] = pd.NA  # out-of-range -> missing, never fabricate
    out = out.dropna(subset=["patient_id", "event_time"]).sort_values(["patient_id", "event_time"]).reset_index(drop=True)
    # seq
    if out["seq"].isna().all():
        out["seq"] = out.groupby("patient_id").cumcount() + 1
    out["seq"] = pd.to_numeric(out["seq"], errors="coerce").astype("Int64")
    out["event_time"] = out["event_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return out[CANONICAL_COLS]

def load_any(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
    elif path.suffix.lower() in (".json", ".jsonl", ".ndjson"):
        try:
            df = pd.read_json(path, lines=True)
        except ValueError:
            with open(path) as f:
                df = pd.DataFrame([json.loads(l) for l in f if l.strip()])
    else:
        raise ValueError(f"unsupported format: {path.suffix} (use csv/json/jsonl)")
    return to_canonical(df)

def main():
    ap = argparse.ArgumentParser(description="Vytals canonical loader")
    ap.add_argument("--input", required=True)
    ap.add_argument("--out", default=None, help="optional canonical csv out")
    ap.add_argument("--limit", type=int, default=5)
    a = ap.parse_args()
    df = load_any(a.input)
    print(f"rows={len(df)} patients={df.patient_id.nunique()} cols={list(df.columns)}")
    print(df.head(a.limit).to_string(index=False))
    print(f"missing%: {(df[['hr','spo2','rr','temp_c','sbp','dbp']].isna().mean()*100).round(1).to_dict()}")
    if a.out:
        df.to_csv(a.out, index=False)
        print(f"wrote {a.out}")

if __name__ == "__main__":
    main()
