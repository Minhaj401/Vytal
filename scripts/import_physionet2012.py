"""Import PhysioNet/CinC Challenge 2012 (set-a) -> canonical vitals CSV.

Source: https://physionet.org/content/challenge-2012/1.0.0/ (open access, no credentialing).
Long format per record: Time,Parameter,Value — real ICU monitor/charted data, 48h span.

Param mapping (Challenge 2012 -> canonical):
  HR         -> hr
  SaO2       -> spo2   (dataset has no SpO2; SaO2 = arterial O2 saturation, same clinical meaning for hypoxemia)
  RespRate   -> rr
  Temp       -> temp_c (already degC)
  NISysABP   -> sbp    (fallback: SysABP, ArtSysABP — invasive when non-invasive absent)
  NIDiasABP  -> dbp    (fallback: DiasABP, ArtDiasABP)

Metadata (Age/Gender/Height/ICUType/Weight/RecordID) and labs are not vitals — dropped.
Missing stays missing; loader range-clamps, never fabricates.

Default: keep patients with >=1 hr AND >=1 bp AND >=1 o2sat so every demo patient
has all key signal types at least once. --limit 0 imports all qualifying patients.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from vytals.data.loader import to_canonical

VITAL_PARAMS = {
    "HR": "hr",
    "SaO2": "spo2",
    "RespRate": "rr",
    "Temp": "temp_c",
}
SBP_SOURCES = ["NISysABP", "SysABP", "ArtSysABP"]
DBP_SOURCES = ["NIDiasABP", "DiasABP", "ArtDiasABP"]
BASE = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def parse_record(path: Path) -> tuple[str, list[dict]] | None:
    """One record file -> (patient_id, [{event_time, hr, spo2, ...}]) or None if unusable."""
    by_time: dict[str, dict] = {}
    pid = path.stem
    for i, line in enumerate(path.read_text().splitlines()):
        if i == 0 or not line.strip():
            continue
        parts = line.split(",", 2)
        if len(parts) != 3:
            continue
        t, param, val = parts
        if param == "RecordID":
            pid = val.strip()
            continue
        if param not in VITAL_PARAMS and param not in SBP_SOURCES and param not in DBP_SOURCES:
            continue
        try:
            v = float(val)
        except ValueError:
            continue
        slot = by_time.setdefault(t, {})
        if param in VITAL_PARAMS:
            slot[VITAL_PARAMS[param]] = v
        elif param in SBP_SOURCES and "sbp" not in slot:
            slot["sbp"] = v
        elif param in DBP_SOURCES and "dbp" not in slot:
            slot["dbp"] = v
    rows = []
    for t in sorted(by_time):
        h, m = (int(x) for x in t.split(":")[:2])
        rows.append({"event_time": BASE + timedelta(hours=h, minutes=m), **by_time[t]})
    if not rows:
        return None
    return pid, rows


def main():
    ap = argparse.ArgumentParser(description="PhysioNet Challenge 2012 -> canonical vitals CSV")
    ap.add_argument("--src", default="data/physionet2012/set-a", help="directory of record .txt files")
    ap.add_argument("--out", default="data/vitals_real.csv")
    ap.add_argument("--limit", type=int, default=300, help="max patients to import (0 = all qualifying)")
    ap.add_argument("--select", choices=["richest", "first"], default="richest",
                    help="richest = most-observed records first (better demo density); first = file order")
    ap.add_argument("--no-require-o2sat", action="store_true",
                    help="keep patients without any SaO2 (default requires hr+bp+o2sat)")
    a = ap.parse_args()

    src = Path(a.src)
    files = sorted(src.glob("*.txt"))
    if not files:
        raise SystemExit(f"no record files in {src} — download set-a.zip first (see README)")

    kept: list[tuple[int, str, list[dict]]] = []
    n_scan = n_drop = 0
    for f in files:
        n_scan += 1
        rec = parse_record(f)
        if rec is None:
            n_drop += 1
            continue
        pid, rows = rec
        df = pd.DataFrame(rows)
        has_hr = "hr" in df
        has_bp = "sbp" in df or "dbp" in df
        has_o2 = "spo2" in df
        if not (has_hr and has_bp):
            n_drop += 1
            continue
        if not a.no_require_o2sat and not has_o2:
            n_drop += 1
            continue
        richness = int(df.reindex(columns=["hr", "spo2", "rr", "temp_c", "sbp", "dbp"]).notna().sum().sum())
        kept.append((richness, pid, rows))

    if not kept:
        raise SystemExit("no qualifying records — try --no-require-o2sat")

    if a.select == "richest":
        kept.sort(key=lambda x: -x[0])
    else:
        kept.sort(key=lambda x: x[1])
    if a.limit:
        kept = kept[: a.limit]

    long_rows = []
    for _, pid, rows in kept:
        for r in rows:
            long_rows.append({"patient_id": pid, **r})
    raw = pd.DataFrame(long_rows)
    canon = to_canonical(raw)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    canon.to_csv(a.out, index=False)
    miss = (canon[["hr", "spo2", "rr", "temp_c", "sbp", "dbp"]].isna().mean() * 100).round(1).to_dict()
    print(f"scanned={n_scan} kept={len(kept)} dropped={n_drop}")
    print(f"wrote {a.out}: {len(canon)} rows, {canon.patient_id.nunique()} patients")
    print(f"missing%: {miss}")


if __name__ == "__main__":
    main()
