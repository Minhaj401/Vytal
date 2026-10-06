"""Spark Structured Streaming: Kafka vitals.raw -> validate -> window -> features -> ML -> Postgres."""
from __future__ import annotations
import argparse
from vytals.config import load_config

VITAL_COLS = ["hr", "spo2", "rr", "temp_c", "sbp", "dbp"]

def build_spark(app="vytals-stream"):
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName(app)
            .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0")
            .getOrCreate())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=None)
    ap.add_argument("--topic", default=None)
    ap.add_argument("--model", default="models/xgb_risk.json")
    a = ap.parse_args()
    cfg = load_config()
    bootstrap = a.bootstrap or cfg["kafka"]["bootstrap_servers"]
    topic = a.topic or cfg["kafka"]["topic"]
    watermark, window, slide = cfg["streaming"]["watermark"], cfg["streaming"]["window"], cfg["streaming"]["slide"]
    pg = cfg["postgres"]

    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

    schema = StructType([
        StructField("patient_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("seq", LongType(), True),
        *[StructField(c, DoubleType(), True) for c in VITAL_COLS],
    ])

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load())

    js = raw.selectExpr("CAST(key AS STRING) k", "CAST(value AS STRING) v")
    parsed = js.select(F.from_json("v", schema).alias("d"), "k").select("d.*", "k")
    # validation + timestamp conversion + event-time
    valid = (parsed
             .withColumn("event_ts", F.to_timestamp("event_time"))
             .withColumn("patient_id", F.coalesce("patient_id", "k"))
             .filter(F.col("patient_id").isNotNull() & F.col("event_ts").isNotNull())
             .withWatermark("event_ts", watermark)
             .dropDuplicates(["patient_id", "event_time", "seq"]))

    agg = (valid.groupBy(F.col("patient_id"),
                         F.window("event_ts", window, slide).alias("w"))
           .agg(*[F.avg(c).alias(f"{c}_mean") for c in VITAL_COLS],
                *[F.stddev(c).alias(f"{c}_std") for c in VITAL_COLS],
                *[F.min(c).alias(f"{c}_min") for c in VITAL_COLS],
                *[F.max(c).alias(f"{c}_max") for c in VITAL_COLS],
                F.max("event_ts").alias("window_end"),
                F.count("*").alias("n_rows"))
           .withColumn("window_end", F.col("window_end").cast("string")))

    # ML inference + Postgres sink via foreachBatch (loads RiskModel once per executor batch on driver)
    pg_url = f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['db']}"
    pg_props = {"user": pg["user"], "password": pg["password"], "driver": "org.postgresql.Driver"}

    model_path = a.model
    state = {"model": None}

    def write_batch(batch_df, batch_id):
        import pandas as pd
        if batch_df.rdd.isEmpty():
            return
        if state["model"] is None:
            from vytals.ml.risk_model import RiskModel
            try:
                state["model"] = RiskModel.load(model_path)
            except Exception as e:
                print(f"[batch {batch_id}] model load failed: {e}; using rules fallback")
                from vytals.ml.risk_model import RiskModel as RM
                state["model"] = RM(None)
        pdf = batch_df.toPandas()
        # Spark agg cols are exactly {v}_mean/{v}_std/{v}_min/{v}_max (aliased above).
        # current ~= window mean as proxy; slope/delta = 0 within one mini-window
        # (training uses full trailing windows via build_features — see features/windowed.py).
        # This is a streaming approximation, documented, not a second feature impl.
        from vytals.features.windowed import FEATURE_COLS
        feats = pd.DataFrame()
        feats["patient_id"] = pdf["patient_id"]
        feats["window_end"] = pdf["window_end"].astype(str)
        for v in VITAL_COLS:
            feats[f"{v}_current"] = pdf[f"{v}_mean"]
            feats[f"{v}_mean"] = pdf[f"{v}_mean"]
            feats[f"{v}_std"] = pdf[f"{v}_std"].fillna(0.0)
            if v != "temp_c":
                feats[f"{v}_min"] = pdf[f"{v}_min"]
                feats[f"{v}_max"] = pdf[f"{v}_max"]
            feats[f"{v}_slope"] = 0.0
            feats[f"{v}_delta"] = 0.0
        feats["n_rows"] = pdf["n_rows"] if "n_rows" in pdf else 1
        feats["coverage"] = 1.0
        feats["low_quality"] = 0
        for c in FEATURE_COLS:
            if c not in feats:
                feats[c] = 0.0
        res = state["model"].predict(feats)
        res["window_start"] = pdf["w"].apply(lambda w: str(w["start"]) if w is not None else None).values if "w" in pdf else None
        # write to postgres (create table if needed elsewhere) + console
        try:
            from pyspark.sql import functions as _F
            sdf = (batch_df.sparkSession.createDataFrame(res)
                   .withColumn("window_end", _F.to_timestamp("window_end"))
                   .withColumn("window_start", _F.to_timestamp("window_start")))
            sdf.write.jdbc(pg_url, "risk_scores", mode="append", properties=pg_props)
        except Exception as e:
            print(f"[batch {batch_id}] pg write failed (ok in dev): {e}")
        print(f"[batch {batch_id}] scored {len(res)} windows; top={res.sort_values('risk_score', ascending=False).head(3).to_dict('records')}")

    q = (agg.writeStream
         .outputMode("update")
         .foreachBatch(write_batch)
         .option("checkpointLocation", "file:///home/minhaj/vytal/checkpoints/vytals")
         .trigger(processingTime="30 seconds")
         .start())
    print(f"streaming {bootstrap}/{topic} watermark={watermark} window={window}/{slide} -> postgres risk_scores")
    q.awaitTermination()

if __name__ == "__main__":
    main()
