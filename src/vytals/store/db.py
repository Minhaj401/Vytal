"""Postgres helpers."""
from __future__ import annotations
from vytals.config import load_config

DDL = """
CREATE TABLE IF NOT EXISTS risk_scores (
  patient_id TEXT NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  window_start TIMESTAMPTZ,
  p_risk DOUBLE PRECISION,
  risk_score DOUBLE PRECISION,
  risk_level TEXT,
  PRIMARY KEY (patient_id, window_end)
);
CREATE TABLE IF NOT EXISTS vitals_raw (
  patient_id TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  seq BIGINT,
  hr DOUBLE PRECISION, spo2 DOUBLE PRECISION, rr DOUBLE PRECISION,
  temp_c DOUBLE PRECISION, sbp DOUBLE PRECISION, dbp DOUBLE PRECISION
);
"""

def dsn(cfg=None) -> str:
    cfg = (cfg or load_config())["postgres"]
    return f"postgresql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['db']}"

def ensure():
    import psycopg2
    cfg = load_config()["postgres"]
    conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["db"],
                            user=cfg["user"], password=cfg["password"], connect_timeout=5)
    conn.autocommit = True
    try:
        with conn.cursor() as c:
            # psycopg2 can't always run multi-statement strings — split on ';'
            for stmt in [s.strip() for s in DDL.split(";") if s.strip()]:
                c.execute(stmt)
    finally:
        conn.close()
