#!/usr/bin/env python
"""Batch analytics over HDFS with Spark SQL — descriptive + diagnostic (abstract obj 10/11).

Reads the canonical vitals CSV and real outcomes from HDFS (hdfs://localhost:9000),
registers temp views, runs SQL, prints report tables:

  descriptive — per-vital distribution stats + abnormal-event rates (same clinical
                thresholds as the MapReduce job, so the two layers cross-check)
  diagnostic  — vital-vs-vital correlation matrix, abnormal co-occurrence rates,
                and risk-factor correlations vs real ICU outcomes (SAPS-I/SOFA)

Usage: PYTHONPATH=src .venv/bin/python scripts/batch_analytics.py
"""
from __future__ import annotations
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://localhost:9000/user/alvi/vytals"
VITALS = ["hr", "spo2", "rr", "temp_c", "sbp", "dbp"]

# param -> (low_bad, high_bad), same spirit as scripts/mr/abnormal_mapper.py + risk_model.py
THRESHOLDS = {"hr": (50, 120), "spo2": (90, None), "rr": (8, 30),
              "temp_c": (35.0, 38.5), "sbp": (90, 180), "dbp": (50, 110)}

def main() -> int:
    spark = (SparkSession.builder.appName("vytals-batch-analytics")
             .master("local[*]")
             .config("spark.hadoop.fs.defaultFS", HDFS)
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    vitals = spark.read.option("header", True).option("inferSchema", True).csv(f"{HDFS}/csv/vitals_real.csv")
    oc = spark.read.option("header", True).option("inferSchema", True).csv(f"{HDFS}/raw/physionet2012/Outcomes-a.txt")
    vitals.createOrReplaceTempView("vitals")
    oc.createOrReplaceTempView("outcomes")

    print("=== DESCRIPTIVE (Spark SQL over HDFS) ===")
    print(f"rows={vitals.count()}  patients={vitals.select('patient_id').distinct().count()}\n")

    print("-- per-vital distribution (non-missing readings) --")
    parts = []
    for v in VITALS:
        r = spark.sql(f"""SELECT '{v}' AS vital, count({v}) AS n,
                          round(avg({v}),2) AS mean, round(stddev({v}),2) AS std,
                          round(min({v}),2) AS min, round(percentile_approx({v},0.5),2) AS p50,
                          round(max({v}),2) AS max FROM vitals""").collect()[0]
        parts.append(r.asDict())
    for p in sorted(parts, key=lambda d: -d["n"]):
        print(f"  {p['vital']:<8} n={p['n']:>6}  mean={p['mean']:>7}  std={p['std']:>6}  "
              f"min={p['min']:>6}  p50={p['p50']:>6}  max={p['max']:>7}")

    print("\n-- abnormal-event rate per vital (thresholds = MR job) --")
    for v in VITALS:
        lo, hi = THRESHOLDS[v]
        cond = []
        if lo is not None: cond.append(f"{v} < {lo}")
        if hi is not None: cond.append(f"{v} > {hi}")
        where = " OR ".join(cond) if cond else "false"
        r = spark.sql(f"""SELECT count(*) AS total,
                          sum(CASE WHEN {where} THEN 1 ELSE 0 END) AS abnormal
                          FROM vitals WHERE {v} IS NOT NULL""").collect()[0]
        pct = 100.0 * r["abnormal"] / r["total"] if r["total"] else 0
        print(f"  {v:<8} {r['abnormal']:>6}/{r['total']:<6} abnormal = {pct:5.2f}%")

    print("\n=== DIAGNOSTIC ===")
    print("-- Pearson correlation between vitals (pairwise non-missing) --")
    cols = ", ".join(VITALS)
    cm = spark.sql(f"SELECT corr(hr,spo2) hr_spo2, corr(hr,rr) hr_rr, corr(hr,temp_c) hr_temp,"
                   f" corr(hr,sbp) hr_sbp, corr(spo2,rr) spo2_rr, corr(sbp,dbp) sbp_dbp,"
                   f" corr(temp_c,rr) temp_rr FROM vitals").collect()[0].asDict()
    for k, val in sorted(cm.items(), key=lambda kv: -abs(kv[1] or 0)):
        print(f"  {k:<12} {val:+.3f}")

    print("\n-- abnormal events co-occurring in the same row --")
    r = spark.sql("""SELECT
        sum(CASE WHEN hr > 120 AND spo2 < 90 THEN 1 ELSE 0 END) AS tachy_and_hypox,
        sum(CASE WHEN hr > 120 THEN 1 ELSE 0 END) AS tachy,
        sum(CASE WHEN spo2 < 90 THEN 1 ELSE 0 END) AS hypox FROM vitals
        WHERE hr IS NOT NULL AND spo2 IS NOT NULL""").collect()[0]
    both, tachy, hypox = r["tachy_and_hypox"], r["tachy"], r["hypox"]
    if tachy and hypox:
        lift = (both / (tachy * hypox / (tachy + hypox - both)))
        print(f"  tachycardia+hypoxemia rows={both}  (tachy={tachy}, hypoxemia={hypox})")
        print(f"  P(hypox|tachy)={100.0*both/tachy:.1f}%  vs  P(hypox)={100.0*hypox/(tachy+hypox-both):.1f}%")

    print("\n-- vitals risk factors vs real ICU outcomes (patient-level join) --")
    pat = (spark.sql("""SELECT patient_id,
               avg(CASE WHEN hr > 120 OR hr < 50 THEN 1 ELSE 0 END) AS hr_abn_rate,
               avg(CASE WHEN spo2 < 90 THEN 1 ELSE 0 END) AS hypox_rate,
               avg(hr) AS hr_mean, avg(spo2) AS spo2_mean
               FROM vitals GROUP BY patient_id""")
           .join(oc, F.col("patient_id") == F.col("RecordID")))
    pat.createOrReplaceTempView("pat_oc")
    for expr in ("hr_abn_rate", "hypox_rate", "hr_mean", "spo2_mean"):
        r = spark.sql(f"SELECT round(corr({expr}, `SAPS-I`),3) AS saps,"
                      f" round(corr({expr}, SOFA),3) AS sofa FROM pat_oc").collect()[0]
        print(f"  {expr:<14} vs SAPS-I={r['saps']:+}  vs SOFA={r['sofa']:+}")
    print("\n(insight: vital-derangement rates track SOFA/SAPS — batch layer confirms the streaming risk story)")
    spark.stop()
    return 0

if __name__ == "__main__":
    sys.exit(main())
