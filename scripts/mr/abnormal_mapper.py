#!/usr/bin/env python3
"""Hadoop Streaming mapper — abnormal-event frequency over Challenge 2012 long-format records.

Input line: Time,Parameter,Value  (e.g. "00:07,HR,145")
Emits (param, total) and (param, abnormal) per valid reading.
Thresholds mirror the RiskModel rule spirit (risk_model.py) + standard clinical ranges.
"""
import sys

# param -> (low_bad, high_bad); None = one-sided
THRESHOLDS = {
    "HR": (50, 120),
    "SaO2": (90, None),
    "RespRate": (8, 30),
    "Temp": (35.0, 38.5),
    "NISysABP": (90, 180),
    "NIDiasABP": (50, 110),
    "SysABP": (90, 180),
    "DiasABP": (50, 110),
}

def main():
    for line in sys.stdin:
        parts = line.rstrip("\n").split(",")
        if len(parts) != 3:
            continue
        _, param, raw = parts
        param = param.strip()
        if param not in THRESHOLDS:
            continue  # GCS/labs/RecordID etc. — not vitals
        try:
            v = float(raw)
        except ValueError:
            continue  # missing = -1 or junk stays missing, never counted
        lo, hi = THRESHOLDS[param]
        bad = (lo is not None and v < lo) or (hi is not None and v > hi)
        print(f"{param}\ttotal")
        if bad:
            print(f"{param}\tabnormal")

if __name__ == "__main__":
    main()
