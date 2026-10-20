"""Postgres read path for the API — reads the serving table written by Spark.

Kept free of FastAPI imports so the row mapping is unit-testable without a database.
"""
from __future__ import annotations
from datetime import datetime

from vytals.config import load_config

VITALS = ("hr", "spo2", "rr", "temp_c", "sbp", "dbp")


def connect(cfg: dict | None = None):
    import psycopg2
    pg = (cfg or load_config())["postgres"]
    return psycopg2.connect(host=pg["host"], port=pg["port"], dbname=pg["db"],
                            user=pg["user"], password=pg["password"], connect_timeout=3)


def iso(v):
    return v.isoformat() if isinstance(v, datetime) else v


def point(row: dict) -> dict:
    """risk_scores row -> the LivePoint shape the web app already consumes."""
    out = {
        "patient_id": row["patient_id"],
        "event_time": iso(row["window_end"]),
        "seq": None,  # window rows have no raw sequence number
        "p_risk": row.get("p_risk"),
        "risk_score": row.get("risk_score"),
        "risk_level": row.get("risk_level"),
    }
    for v in VITALS:
        out[v] = row.get(v)
    return out


def patient(row: dict) -> dict:
    return {
        "patient_id": row["patient_id"],
        "risk_score": row["risk_score"],
        "risk_level": row["risk_level"],
        "hr": row.get("hr"),
        "spo2": row.get("spo2"),
        "event_time": iso(row["window_end"]),
    }


def _fetch(conn, sql: str, params: tuple = ()) -> list[dict]:
    from psycopg2.extras import RealDictCursor
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def latest_all(conn) -> list[dict]:
    rows = _fetch(conn, "SELECT DISTINCT ON (patient_id) * FROM risk_scores "
                        "ORDER BY patient_id, window_end DESC")
    return sorted((patient(r) for r in rows), key=lambda x: -(x["risk_score"] or 0))


def latest(conn, pid: str) -> dict | None:
    rows = _fetch(conn, "SELECT * FROM risk_scores WHERE patient_id = %s "
                        "ORDER BY window_end DESC LIMIT 1", (pid,))
    return point(rows[0]) if rows else None


def history(conn, pid: str, limit: int = 120) -> list[dict]:
    rows = _fetch(conn, "SELECT * FROM risk_scores WHERE patient_id = %s "
                        "ORDER BY window_end DESC LIMIT %s", (pid, limit))
    return [point(r) for r in reversed(rows)]
