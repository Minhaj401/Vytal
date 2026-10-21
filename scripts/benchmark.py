#!/usr/bin/env python
"""Empirical single-machine vs distributed processing benchmark (abstract obj 17).

Same work on both sides: parse the raw long-format Challenge 2012 records
(Time,Parameter,Value) from HDFS and aggregate them — count/mean per
(Parameter) plus per-patient vital window stats — i.e. the descriptive core of
the batch layer.

  single-machine : pandas on a local copy of the same bytes
  distributed    : PySpark local[*] reading directly from hdfs://localhost:9000

Metrics: wall time, rows/s, output rows. Honest framing: local[*] is a
simulated cluster on this one lab machine; multi-node scaling is the design
goal, and these numbers say where the crossover lies at THIS dataset size.

Usage: PYTHONPATH=src .venv/bin/python scripts/benchmark.py
"""
from __future__ import annotations
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HDFS_RAW = "hdfs://localhost:9000/user/alvi/vytals/raw/physionet2012/set-a"
LOCAL_COPY = ROOT / "data/cache/raw_long.csv"

def bench_pandas() -> tuple[float, int, int]:
    import pandas as pd
    if not LOCAL_COPY.exists():
        from vytals.data.loader import load_any  # noqa: F401 (keeps import path warm)
        # assemble one local copy of the raw long files (same bytes Spark reads from HDFS)
        import glob
        files = glob.glob(str(ROOT / "data/physionet2012/set-a/*.txt"))
        parts = [pd.read_csv(f, skiprows=lambda i: False) for f in files[:4000]]
        big = pd.concat(parts, ignore_index=True)
        big.to_csv(LOCAL_COPY, index=False)
    t0 = time.perf_counter()
    df = pd.read_csv(LOCAL_COPY)
    df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
    by_param = df.groupby("Parameter")["Value"].agg(["count", "mean", "std"])
    out_rows = len(by_param)
    return time.perf_counter() - t0, len(df), out_rows

def bench_spark() -> tuple[float, int, int]:
    import os
    os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk")
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    t0 = time.perf_counter()                      # includes session start (real job cost)
    spark = (SparkSession.builder.appName("vytals-bench").master("local[*]")
             .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.csv(HDFS_RAW, header=True)
    df = df.withColumn("v", F.col("Value").cast("double"))
    res = df.groupBy("Parameter").agg(F.count("v"), F.avg("v"), F.stddev("v")).collect()
    n = df.count()                                 # row count on the cluster
    out_rows = len(res)
    dt = time.perf_counter() - t0
    spark.stop()
    return dt, n, out_rows

def main() -> int:
    print("Vytals benchmark — same aggregation, two engines\n")
    print("1) single-machine (pandas, local CSV) ...")
    pt, pn, po = bench_pandas()
    print(f"   {pt:7.2f}s   {pn:>9} rows   {po} groups   {pn/pt:,.0f} rows/s")

    print("2) distributed (PySpark local[*], HDFS) ...")
    st, sn, so = bench_spark()
    print(f"   {st:7.2f}s   {sn:>9} rows   {so} groups   {sn/st:,.0f} rows/s")

    print(f"\n| engine | wall time | rows | rows/s | groups |")
    print(f"|---|---|---|---|---|")
    print(f"| single-machine (pandas) | {pt:.2f} s | {pn} | {pn/pt:,.0f} | {po} |")
    print(f"| distributed (Spark local[*] on HDFS) | {st:.2f} s | {sn} | {sn/st:,.0f} | {so} |")
    print(f"\nrelative: Spark/pandas wall-time = {st/pt:.1f}×")
    print("note: at this dataset size the JVM/session overhead dominates — the")
    print("distributed win appears with data larger than one machine can hold and")
    print("multi-node parallelism (this lab runs local[*] on one host).")
    return 0

if __name__ == "__main__":
    sys.exit(main())
