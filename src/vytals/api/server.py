"""FastAPI backend for the Next.js dashboard.

Two data sources, selected with VYTALS_SOURCE:
  - postgres (full stack): reads the risk_scores serving table written by Spark
  - replay   (default, dev/CI): in-memory CSV replay, no Kafka/Spark/HDFS needed
"""
from __future__ import annotations
import os, threading
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from vytals.config import load_config
from vytals.sim.replay import Replay
from vytals.store import queries

ROOT = Path(__file__).resolve().parents[3]  # .../vytal
SOURCE = os.environ.get("VYTALS_SOURCE", "replay").strip().lower()

@asynccontextmanager
async def lifespan(_app: FastAPI):
    if SOURCE == "postgres":
        try:
            from vytals.store.db import ensure
            ensure()
        except Exception as e:
            print(f"postgres startup check failed: {e}")
    else:
        try:
            get_replay()
        except Exception as e:
            print(f"replay startup failed: {e}")
    yield

app = FastAPI(title="Vytals API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

REPLAY: Replay | None = None
REPLAY_ERROR: str | None = None

def resolve_repo_file(cand: str) -> Path:
    p = Path(cand)
    if p.exists():
        return p
    if (Path.cwd() / cand).exists():
        return Path.cwd() / cand
    return ROOT / cand

def get_replay() -> Replay:
    global REPLAY, REPLAY_ERROR
    if REPLAY is None:
        load_config()
        cand = os.environ.get("VYTALS_DATA") or "data/vitals.csv"
        p = resolve_repo_file(cand)
        if not p.exists():
            cands = list((ROOT / "data").glob("*.csv"))
            p = cands[0] if cands else p
        if not p.exists():
            REPLAY_ERROR = f"no vitals CSV found (tried {cand}). Generate one: python scripts/make_sample_data.py"
            raise FileNotFoundError(REPLAY_ERROR)
        model = os.environ.get("VYTALS_MODEL") or "models/xgb_risk.json"
        mp = resolve_repo_file(model)
        try:
            REPLAY = Replay(str(p), str(mp))
        except Exception as e:
            REPLAY_ERROR = str(e)
            raise
        # warm up so patients list is non-empty
        REPLAY.step(60)
        th = threading.Thread(target=REPLAY.loop, kwargs={"batch": 3, "interval": 1.0}, daemon=True)
        th.start()
    return REPLAY

def need_replay() -> Replay:
    try:
        return get_replay()
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))

def db():
    try:
        return queries.connect()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"postgres unavailable: {e}")

@app.get("/api/health")
def health():
    if SOURCE == "postgres":
        try:
            db().close()
            return {"ok": True, "error": None, "source": SOURCE}
        except HTTPException as e:
            return {"ok": False, "error": e.detail, "source": SOURCE}
    ok = REPLAY is not None or REPLAY_ERROR is None
    return {"ok": ok, "error": REPLAY_ERROR, "source": "replay"}

@app.get("/api/patients")
def patients():
    if SOURCE == "postgres":
        conn = db()
        try:
            return {"patients": queries.latest_all(conn)}
        finally:
            conn.close()
    r = need_replay()
    out = []
    for pid in r.patients():
        live = r.latest.get(pid)
        if live:
            out.append({"patient_id": pid, "risk_score": live["risk_score"], "risk_level": live["risk_level"],
                        "hr": live.get("hr"), "spo2": live.get("spo2"), "event_time": live.get("event_time")})
        else:
            out.append({"patient_id": pid, "risk_score": 0, "risk_level": "LOW"})
    out.sort(key=lambda x: -x["risk_score"])
    return {"patients": out}

@app.get("/api/patients/{pid}/live")
def live(pid: str):
    if SOURCE == "postgres":
        conn = db()
        try:
            d = queries.latest(conn, pid)
        finally:
            conn.close()
        if not d:
            return JSONResponse({"error": "unknown patient"}, status_code=404)
        return d
    r = need_replay()
    d = r.latest.get(pid)
    if not d:
        return JSONResponse({"error": "unknown patient"}, status_code=404)
    return d

@app.get("/api/patients/{pid}/history")
def history(pid: str, limit: int = Query(default=120, ge=1, le=400)):
    if SOURCE == "postgres":
        conn = db()
        try:
            return {"patient_id": pid, "points": queries.history(conn, pid, limit)}
        finally:
            conn.close()
    r = need_replay()
    h = r.history.get(pid, [])[-limit:]
    return {"patient_id": pid, "points": h}

def _analysis(lives: list[dict]) -> dict:
    if not lives:
        return {"counts": {}, "avg_risk": 0, "critical": []}
    from collections import Counter
    # plain-python aggregation: a DataFrame round-trip turns the None (missing
    # vital) values back into NaN, which Starlette refuses to serialize -> 500
    scores = [v["risk_score"] for v in lives]
    counts = dict(sorted(Counter(v["risk_level"] for v in lives).items(), key=lambda kv: -kv[1]))
    top = sorted(lives, key=lambda v: v["risk_score"], reverse=True)[:5]
    return {
        "counts": counts,
        "avg_risk": round(sum(scores) / len(scores), 1),
        "max_risk": round(max(scores), 1),
        "n_patients": len(lives),
        "critical": [{"patient_id": v["patient_id"], "risk_score": v["risk_score"],
                      "risk_level": v["risk_level"], "hr": v.get("hr"), "spo2": v.get("spo2")} for v in top],
        "dist": counts,
    }

@app.get("/api/analysis")
def analysis():
    if SOURCE == "postgres":
        conn = db()
        try:
            lives = queries.latest_all(conn)
        finally:
            conn.close()
    else:
        r = need_replay()
        lives = list(r.latest.values())
    return _analysis(lives)

@app.post("/api/advance")
def advance(steps: int = Query(default=10, ge=1, le=200)):
    if SOURCE == "postgres":
        raise HTTPException(status_code=409, detail="advance is replay-only; the full pipeline advances via Kafka")
    r = need_replay()
    out = r.step(steps)
    return {"advanced": len(out), "latest": out[-1] if out else None}
