"""Convert PhysioNet/CinC Challenge 2012 set-a records into the vytals canonical schema.

Records are "Time,Parameter,Value" text files (RecordID arrives as a 00:00 parameter row).

Vital mapping (documented, not fabricated):
    HR -> hr                      SaO2 -> spo2   (arterial blood gas; the challenge has no pulse-ox)
    RespRate -> rr                Temp -> temp_c
    NISysABP or SysABP -> sbp     (non-invasive preferred)
    NIDiasABP or DiasABP -> dbp

Value -1 = missing (challenge spec) -> empty field.
Time "HH:MM" = elapsed since ICU admission -> base 2026-01-01T00:00:00Z + offset.
Recomputed news2_score/qsofa_score are appended (src/vytals/data/clinicalscore.py).

Run:  python scripts/import_physionet2012.py [--dir data/physionet2012/set-a] [--out data/physionet2012.csv]
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from vytals.data.clinicalscore import news2, qsofa  # noqa: E402

BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
PARAM_MAP = {"HR": "hr", "SaO2": "spo2", "RespRate": "rr", "Temp": "temp_c"}


def parse_offset(s: str):
    """'HH:MM' elapsed-since-admission -> minutes, or None."""
    try:
        h, m = s.strip().split(":")
        return int(h) * 60 + int(m)
    except ValueError:
        return None


def num(s: str):
    try:
        v = float(s)
    except ValueError:
        return None
    return None if v < 0 else v  # -1 = missing marker in this dataset


def convert_record(path: str):
    """One record file -> (record_id, readings sorted by time)."""
    rec_id = Path(path).stem
    readings: dict[int, dict] = {}
    with open(path, newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        col = {h.strip(): i for i, h in enumerate(header)}
        ct, cp, cv = col["Time"], col["Parameter"], col["Value"]
        for row in reader:
            if len(row) <= max(ct, cp, cv):
                continue
            param = row[cp].strip()
            if param == "RecordID":
                if row[cv].strip():
                    rec_id = row[cv].strip()
                continue
            off = parse_offset(row[ct])
            if off is None:
                continue
            val = num(row[cv])
            if val is None:
                continue
            r = readings.setdefault(off, {})
            if param in PARAM_MAP:
                r[PARAM_MAP[param]] = val
            elif param == "NISysABP":
                r["sbp"] = val
            elif param == "SysABP" and "sbp" not in r:
                r["sbp"] = val
            elif param == "NIDiasABP":
                r["dbp"] = val
            elif param == "DiasABP" and "dbp" not in r:
                r["dbp"] = val
    out = [(BASE + timedelta(minutes=off), readings[off]) for off in sorted(readings) if readings[off]]
    return rec_id, out


def fmt(v) -> str:
    return "" if v is None else f"{v:.3f}"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dir", default="data/physionet2012/set-a")
    ap.add_argument("--out", default="data/physionet2012.csv")
    a = ap.parse_args()

    files = sorted(glob.glob(os.path.join(a.dir, "*.txt")))
    if not files:
        raise SystemExit(f"no records in {a.dir} (download set-a first: make data-real)")
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)

    rows = patients = 0
    with open(a.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["patient_id", "event_time", "seq", "hr", "spo2", "rr", "temp_c",
                    "sbp", "dbp", "news2_score", "qsofa_score"])
        for path in files:
            try:
                rec_id, recs = convert_record(path)
            except (OSError, KeyError, csv.Error) as e:
                print(f"skip {path}: {e}", file=sys.stderr)
                continue
            if not recs:
                continue
            patients += 1
            for i, (ts, r) in enumerate(recs, 1):
                w.writerow([rec_id, ts.strftime("%Y-%m-%dT%H:%M:%SZ"), i,
                            fmt(r.get("hr")), fmt(r.get("spo2")), fmt(r.get("rr")),
                            fmt(r.get("temp_c")), fmt(r.get("sbp")), fmt(r.get("dbp")),
                            news2(r.get("hr"), r.get("spo2"), r.get("rr"), r.get("temp_c"), r.get("sbp")),
                            qsofa(r.get("rr"), r.get("sbp"))])
                rows += 1
    print(f"wrote {a.out}: {rows} rows, {patients} patients")


if __name__ == "__main__":
    main()
