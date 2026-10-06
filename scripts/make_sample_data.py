"""Generate realistic synthetic vitals CSV for demo/replay/training."""
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta

def gen(patients=12, minutes=180, freq_s=30, seed=42):
    rng = np.random.default_rng(seed)
    base = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
    rows = []
    profiles = []
    for i in range(patients):
        pid = f"P{i+1:03d}"
        # assign archetype: 70% stable, 20% deteriorating, 10% critical
        r = rng.random()
        arch = "stable" if r < 0.7 else ("deteriorate" if r < 0.9 else "critical")
        profiles.append((pid, arch))
    seq = 0
    n_steps = int(minutes * 60 / freq_s)
    for t in range(n_steps):
        et = base + timedelta(seconds=t * freq_s)
        for pid, arch in profiles:
            seq += 1
            drift = t / n_steps
            if arch == "stable":
                hr = rng.normal(76, 6); spo2 = rng.normal(97.2, 0.9); rr = rng.normal(16, 2)
                tmp = rng.normal(36.9, 0.3); sbp = rng.normal(122, 9); dbp = rng.normal(78, 6)
            elif arch == "deteriorate":
                hr = rng.normal(76 + 35 * drift, 7); spo2 = rng.normal(97.2 - 6 * drift, 1.2); rr = rng.normal(16 + 10 * drift, 2.5)
                tmp = rng.normal(36.9 + 1.2 * drift, 0.35); sbp = rng.normal(122 + 30 * drift, 11); dbp = rng.normal(78 + 12 * drift, 7)
            else:
                hr = rng.normal(118, 12); spo2 = rng.normal(88.5, 2.2); rr = rng.normal(27, 3.5)
                tmp = rng.normal(38.6, 0.5); sbp = rng.normal(165, 14); dbp = rng.normal(102, 9)
            # 3% missing at random (never fabricated downstream)
            vals = [hr, spo2, rr, tmp, sbp, dbp]
            for j in range(len(vals)):
                if rng.random() < 0.03:
                    vals[j] = np.nan
            hr, spo2, rr, tmp, sbp, dbp = vals
            rows.append([pid, et.strftime("%Y-%m-%dT%H:%M:%SZ"), seq, hr, spo2, rr, tmp, sbp, dbp])
    df = pd.DataFrame(rows, columns=["patient_id", "event_time", "seq", "hr", "spo2", "rr", "temp_c", "sbp", "dbp"])
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--patients", type=int, default=12)
    ap.add_argument("--minutes", type=int, default=180)
    ap.add_argument("--out", default="data/vitals.csv")
    a = ap.parse_args()
    df = gen(a.patients, a.minutes)
    df.to_csv(a.out, index=False)
    print(f"wrote {a.out}: {len(df)} rows, {df.patient_id.nunique()} patients")

if __name__ == "__main__":
    main()
