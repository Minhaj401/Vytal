"""FastAPI backend for Next.js dashboard. Serves live replay + history + analysis.

Two data sources (VYTALS_SOURCE):
  replay (default) — in-memory replay engine, works with zero infra
  pg               — streamed scores from PostgreSQL (risk_scores) + vitals from
                     the canonical CSV the producer replayed; use after a full
                     `make producer` + `make stream` run
"""
from __future__ import annotations
import math, os, threading
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from vytals.config import load_config
from vytals.sim.replay import Replay

ROOT = Path(__file__).resolve().parents[3]  # .../vytal

def json_safe(obj):
    """NaN/Inf/numpy -> None so starlette's json.dumps(allow_nan=False) never 500s."""
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_safe(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if hasattr(obj, "item"):
        try:
            return json_safe(obj.item())
        except Exception:
            return None
    return obj

@asynccontextmanager
async def lifespan(_app: FastAPI):
    if SOURCE == "pg":
        try:
            get_pg()
        except Exception as e:
            print(f"pg source unavailable ({e}) — falling back to replay")
    if PG is None:
        try:
            get_replay()
        except Exception as e:
            print(f"replay startup failed: {e}")
    yield

app = FastAPI(title="Vytals API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SOURCE = (os.environ.get("VYTALS_SOURCE") or "replay").strip().lower()
REPLAY: Replay | None = None
REPLAY_ERROR: str | None = None
PG = None
PG_ERROR: str | None = None

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

def pg_mode() -> bool:
    return SOURCE == "pg" and PG is not None

def get_pg():
    global PG, PG_ERROR
    if PG is None:
        from vytals.store.pg_reads import PgLive
        load_config()
        cand = os.environ.get("VYTALS_DATA") or "data/vitals_real.csv"
        p = resolve_repo_file(cand)
        if not p.exists():
            alts = [ROOT / "data/vitals_real.csv", ROOT / "data/vitals.csv"]
            p = next((x for x in alts if x.exists()), p)
        if not p.exists():
            raise FileNotFoundError(f"no vitals CSV for pg mode (tried {cand})")
        try:
            PG = PgLive(p)
        except Exception as e:
            PG_ERROR = str(e)
            raise
    return PG

@app.get("/api/health")
def health():
    if pg_mode():
        try:
            return {"ok": True, "source": "pg", **PG.stats()}
        except Exception as e:
            return {"ok": False, "source": "pg", "error": str(e)}
    ok = REPLAY is not None or REPLAY_ERROR is None
    return {"ok": ok, "source": "replay", "error": REPLAY_ERROR}

@app.get("/api/patients")
def patients():
    if pg_mode():
        return {"patients": PG.patients()}
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
    if pg_mode():
        d = PG.live(pid)
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
    if pg_mode():
        return {"patient_id": pid, "points": PG.history(pid, limit)}
    r = need_replay()
    h = r.history.get(pid, [])[-limit:]
    return {"patient_id": pid, "points": h}

@app.get("/api/analysis")
def analysis():
    if pg_mode():
        return json_safe(PG.analysis())
    r = need_replay()
    lives = [v for v in r.latest.values()]
    if not lives:
        return {"counts": {}, "avg_risk": 0, "critical": []}
    import pandas as pd
    df = pd.DataFrame(lives)
    counts = df.risk_level.value_counts().to_dict()
    # to_dict("records") re-introduces NaN for missing vitals — sanitize before JSON
    crit = df.sort_values("risk_score", ascending=False).head(5)[
        ["patient_id", "risk_score", "risk_level", "hr", "spo2"]].to_dict("records")
    return json_safe({
        "counts": counts,
        "avg_risk": round(float(df.risk_score.mean()), 1),
        "max_risk": round(float(df.risk_score.max()), 1),
        "n_patients": len(df),
        "critical": crit,
        "dist": counts,
    })

@app.post("/api/advance")
def advance(steps: int = Query(default=10, ge=1, le=200)):
    if pg_mode():
        return JSONResponse(
            {"detail": "pg mode serves streamed scores — advancing is replay-only"},
            status_code=409)
    r = need_replay()
    out = r.step(steps)
    return {"advanced": len(out), "latest": out[-1] if out else None}
