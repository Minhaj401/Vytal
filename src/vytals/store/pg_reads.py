"""Postgres-backed reads for the dashboard's live mode (VYTALS_SOURCE=pg).

`risk_scores` is the streamed truth (Kafka -> Spark -> XGBoost -> PG). The stream
writes scores only, so vitals come from the canonical CSV that was replayed into
Kafka, as-of-joined to the score timestamps (risk is carried between windows,
exactly what the monitor would display).
"""
from __future__ import annotations

import time
from pathlib import Path

import pandas as pd

from vytals.data.loader import load_any

VITALS = ["hr", "spo2", "rr", "temp_c", "sbp", "dbp"]
_SCORE_TTL = 3.0


class PgLive:
    def __init__(self, csv_path: str | Path):
        self.vitals = self._load_vitals(csv_path)
        self._scores: pd.DataFrame | None = None
        self._merged: pd.DataFrame | None = None
        self._t = 0.0
        self.refresh(force=True)  # raises if PG is unreachable / empty

    @staticmethod
    def _load_vitals(csv_path: str | Path) -> pd.DataFrame:
        df = load_any(str(csv_path))
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
        # merge_asof (by=patient) needs global time order, not patient-grouped
        df = df.dropna(subset=["event_time"]).sort_values("event_time")
        return df.reset_index(drop=True)

    def _fetch_scores(self) -> pd.DataFrame:
        import psycopg2
        from vytals.store.db import dsn

        conn = psycopg2.connect(dsn(), connect_timeout=5)
        try:
            with conn.cursor() as c:
                c.execute(
                    "SELECT patient_id, window_end, p_risk, risk_score, risk_level"
                    " FROM risk_scores ORDER BY patient_id, window_end"
                )
                rows = c.fetchall()
        finally:
            conn.close()
        if not rows:
            raise RuntimeError("risk_scores is empty — run the stream first (make producer + make stream)")
        df = pd.DataFrame(rows, columns=["patient_id", "window_end", "p_risk", "risk_score", "risk_level"])
        df["patient_id"] = df["patient_id"].astype(str)
        df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
        return df.sort_values("window_end").reset_index(drop=True)

    def refresh(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and self._scores is not None and now - self._t < _SCORE_TTL:
            return
        self._scores = self._fetch_scores()
        # vitals (left) carry the chart values; risk as-of <= event_time (backward)
        self._merged = pd.merge_asof(
            self.vitals, self._scores,
            left_on="event_time", right_on="window_end",
            by="patient_id", direction="backward",
        )
        self._t = now

    # --- row -> API dict -------------------------------------------------
    @staticmethod
    def _num(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return float(v)

    def _point(self, r) -> dict:
        t = r["event_time"]
        return {
            "patient_id": str(r["patient_id"]),
            "event_time": t.strftime("%Y-%m-%dT%H:%M:%SZ") if pd.notna(t) else None,
            "seq": int(r["seq"]) if pd.notna(r.get("seq")) else None,
            "hr": self._num(r.get("hr")),
            "spo2": self._num(r.get("spo2")),
            "rr": self._num(r.get("rr")),
            "temp_c": self._num(r.get("temp_c")),
            "sbp": self._num(r.get("sbp")),
            "dbp": self._num(r.get("dbp")),
            "p_risk": self._num(r.get("p_risk")),
            "risk_score": self._num(r.get("risk_score")),
            "risk_level": None if pd.isna(r.get("risk_level")) else r["risk_level"],
        }

    def _latest_row(self) -> pd.DataFrame:
        """Latest charted row per patient that has a streamed score (risk display source of truth)."""
        scored = self._merged.dropna(subset=["risk_score"])
        return scored.sort_values("event_time").groupby("patient_id", as_index=False).tail(1)

    # --- API payloads ----------------------------------------------------
    def patients(self) -> list[dict]:
        self.refresh()
        out = [self._point(r) for _, r in self._latest_row().iterrows()]
        out.sort(key=lambda x: -(x["risk_score"] or 0))
        return out

    def live(self, pid: str) -> dict | None:
        self.refresh()
        rows = self._merged[self._merged["patient_id"] == str(pid)]
        if rows.empty:
            return None
        return self._point(rows.iloc[-1])

    def history(self, pid: str, limit: int) -> list[dict]:
        self.refresh()
        rows = self._merged[self._merged["patient_id"] == str(pid)].tail(limit)
        return [self._point(r) for _, r in rows.iterrows()]

    def analysis(self) -> dict:
        self.refresh()
        latest = self._latest_row()
        if latest.empty:
            return {"counts": {}, "avg_risk": 0, "max_risk": 0, "n_patients": 0, "critical": [], "dist": {}}
        counts = latest["risk_level"].value_counts().to_dict()
        crit = [self._point(r) for _, r in latest.nlargest(5, "risk_score").iterrows()]
        return {
            "counts": counts,
            "avg_risk": round(float(latest["risk_score"].mean()), 1),
            "max_risk": round(float(latest["risk_score"].max()), 1),
            "n_patients": int(latest["patient_id"].nunique()),
            "critical": crit,
            "dist": counts,
        }

    def stats(self) -> dict:
        self.refresh()
        return {
            "n_scores": int(len(self._scores)),
            "n_patients": int(self._scores["patient_id"].nunique()),
            "last_window": self._scores["window_end"].max().strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
