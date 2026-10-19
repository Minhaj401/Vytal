# Vytals — Technical Documentation

> Engineering reference: architecture, data flow, models, API, design decisions, and known limitations.
> Non-technical overview: [FEATURES.md](FEATURES.md) · Setup: [INSTALLATION.md](INSTALLATION.md) · Status: [PROGRESS.md](PROGRESS.md)

## 1. Architecture

```
Patient vitals (CSV / JSONL, synthetic or PhysioNet Challenge 2012)
    │
    ├─ demo path ──────────────────────────────────────────────┐
    │  Replay engine (in-process, threads)                     │
    │                                                          ↓
    │                              FastAPI :8000  ←→  Next.js :3000
    │
    └─ streaming path
       Producer (kafka-python) → Kafka topic vitals.raw (3 partitions, key=patient_id, acks=all)
            ↓
       Spark Structured Streaming
            ├─ JSON parse + schema validate, drop bad timestamps
            ├─ watermark 2 min (event-time), dedupe (patient_id, event_time, seq)
            ├─ rolling window 5 min / slide 1 min per patient
            ├─ feature vector (same FEATURE_COLS as training)
            ├─ XGBoost risk model → 0–100 + level
            └─ foreachBatch → PostgreSQL risk_scores
```

### Component map

| Layer | Path | Notes |
|---|---|---|
| Config | `src/vytals/config.py` + `config/default.yaml` | YAML merged over code defaults; `VYTALS_CONFIG` overrides path |
| Loader | `src/vytals/data/loader.py` | CSV/JSON/JSONL → canonical schema; alias resolution; range clamp |
| Features | `src/vytals/features/windowed.py` | `build_features()` — single source of truth for train + stream + replay |
| ML | `src/vytals/ml/{risk_model,train,predict}.py` | XGBoost regressor; rule-based labels; patient-wise split |
| Replay | `src/vytals/sim/replay.py` | in-memory engine, background thread, `RLock` |
| API | `src/vytals/api/server.py` | FastAPI + CORS + lifespan warm-up |
| Producer | `src/vytals/ingest/producer.py` | CSV → Kafka, inter-arrival pacing via `--speed` |
| Stream job | `src/vytals/streaming/job.py` | Spark SS: kafka → windows → model → PG |
| Store | `src/vytals/store/db.py` | `ensure()` DDL for `risk_scores`, `vitals_raw` |
| Data gen | `scripts/make_sample_data.py` | synthetic archetypes (stable/deteriorating/critical) |
| Real import | `scripts/import_physionet2012.py` | Challenge 2012 long-format → canonical CSV |
| UI | `web/` | Next.js 14, React 18, Recharts, Tailwind, Apple-HIG light theme |

## 2. Canonical data schema

```
patient_id:str  event_time:ISO8601 UTC  seq:int
hr, spo2, rr, temp_c, sbp, dbp : float | None
```

- **Aliases** accepted on input (`loader.ALIASES`): e.g. `subject_id→patient_id`, `charttime→event_time`, `heart_rate→hr`, `RespRate→rr`, `NISysABP→sbp`, `temp_f→temp_c` (converted).
- **Range clamp** (`loader.RANGES`): hr 20–250, spo2 50–100, rr 4–60, temp_c 30–43, sbp 50–280, dbp 20–180. Out-of-range → `pd.NA` (**never fabricated**).
- Missing values allowed everywhere except `patient_id`/`event_time` (rows dropped if those are null).
- `seq` auto-generated per patient when absent.

## 3. Feature engineering (`features/windowed.py`)

`build_features(df)` → one row per `(patient_id, window_end)`:

- Per vital: `_current`, `_mean`, `_std`, `_min`, `_max`, `_slope` (least-squares over the window), `_delta` (mean vs. first-half mean) — `temp_c` omits min/max.
- Global: `n_rows`, `coverage` (non-null fraction over 6 vitals), `low_quality` (coverage < 0.5 or < 3 rows).
- NaN policy for model input: `_current/_mean/_min/_max` fill with population-neutral constants (hr 75, spo2 97, rr 16, temp 37, sbp 120, dbp 78), everything else 0. Raw records themselves are never modified.
- **Used by all three paths** (train, streaming sink, replay) — no duplicated feature logic.

`FEATURE_COLS` length: 38 (6 vitals × 6–7 + 3 global).

## 4. Risk model (`ml/`)

- **Algorithm:** XGBoost `reg:squarederror`, `max_depth 5`, `eta 0.08`, `subsample/colsample 0.9`, 300 rounds, early stopping 30.
- **Labels:** `RiskModel.rule_score()` — transparent physiology thresholds (tachycardia, hypoxemia, RR/temp/BP derangement, variability/slope penalties, low-quality penalty), clipped 0–100, + Gaussian noise σ=2 so the model learns a smooth mapping.
- **Split:** `GroupShuffleSplit` by `patient_id` (20% test) — no patient in both splits (prevents leakage).
- **Inference:** `predict_proba` maps raw score → 0–1 (`raw/100` when raw > 1.5, back-compat with 0–1 checkpoints); `predict` adds `risk_score` (0–100, 1 dp) + `risk_level`.
- **Levels:** LOW < 25 ≤ MODERATE < 50 ≤ HIGH < 75 ≤ CRITICAL.
- **Robustness:** `RiskModel.load()` never raises — missing/corrupt/incompatible file → rule-based fallback (`booster=None`). Rules are also the label generator, so fallback output stays coherent.

### Measured performance (patient-wise hold-out)

| Dataset | MAE | R² | Top features |
|---|---|---|---|
| Synthetic (demo) | 2.78 | 0.899 | rr_mean, hr_max, spo2_mean |
| Real Challenge 2012 | 1.62 | 0.963 | hr_max, sbp_min, hr_mean, dbp_min |

Artifacts: `models/xgb_risk.json` (synthetic), `models/xgb_risk_real.json` (real).

## 5. Replay engine (`sim/replay.py`)

Powers the dashboard with **zero infrastructure** (no Kafka/Spark/Java):

- Loads canonical CSV, sorts globally by `event_time`, steps row-by-row (`step(n)`).
- Per-patient rolling buffer (`deque(maxlen=30)` rows) → `build_features` → model → `latest[pid]` + `history[pid]` (capped 400).
- Background daemon thread: `loop(batch=3, interval=1.0)`; exceptions logged, thread never dies.
- Loops on exhaustion, clearing buffers for a clean restart.
- `threading.RLock` shared with request handlers.
- `_json_safe` converts numpy scalars / NaN → `None` at write time.

## 6. HTTP API (`api/server.py`)

| Method | Route | Purpose |
|---|---|---|
| GET | `/api/health` | `{ok, error}` |
| GET | `/api/patients` | ranked list (risk desc) with latest vitals |
| GET | `/api/patients/{id}/live` | latest record + risk (404 if not yet stepped) |
| GET | `/api/patients/{id}/history?limit≤400` | trend points |
| GET | `/api/analysis` | counts by level, avg/max risk, top-5 critical |
| POST | `/api/advance?steps≤200` | manual replay advance (demo/testing) |

- Startup: `lifespan` warms replay (`step(60)` + starts loop); failure is caught and reported via `/api/health` instead of crashing.
- Missing CSV → `FileNotFoundError` → HTTP 503 with actionable message.
- **JSON safety:** starlette serializes with `allow_nan=False`; any pandas `NaN` escaping to a response = 500. `json_safe()` (`server.py:16`) recursively maps NaN/Inf/numpy → `None`; the `/api/analysis` response (pandas `to_dict("records")` re-introduces NaN for missing vitals) passes through it. *This fixed a real 500 observed in production logs.*
- CORS open (dev convenience; frontend is same-origin in practice).

## 7. Streaming path

### Producer (`ingest/producer.py`)
- `kafka-python`, `acks=all`, `linger_ms=20`, `retries=5`, key = `patient_id` (partition affinity per patient).
- Inter-arrival sleep from `event_time` deltas / `--speed` (1 = realtime, 60 = 60×, 0 = flat out); capped 5 s/gap.
- Fail-fast: `max_block_ms=8000` + explicit `SystemExit` with a start-infra hint when no broker.

### Spark job (`streaming/job.py`)
- Session: `spark.jars.packages` pulls `spark-sql-kafka-0-10_2.12` + `postgresql` JDBC at startup (needs network/Ivy cache on first run).
- Pipeline: raw Kafka → `from_json` on declared schema → `to_timestamp` + `coalesce(patient_id, k)` → filter nulls → `withWatermark("event_ts","2 minutes")` → `dropDuplicates(["patient_id","event_time","seq"])` → `groupBy(patient_id, window(event_ts, "5 minutes","1 minute"))` with avg/stddev/min/max + `window_end`, `n_rows`.
- Sink: `foreachBatch` → builds `FEATURE_COLS` frame (current≈window mean, slope/delta=0 — documented approximation) → model → JDBC append into `risk_scores`; per-batch console summary; PG failure logged, batch continues (dev-friendly).
- Trigger: 30 s micro-batches; `outputMode("update")`.
- ⚠️ Checkpoint location is currently **hardcoded** (`job.py:126`) — must point at a writable `file://` dir on the target machine before first run (see Progress → known issues).

### Postgres (`store/db.py`, config port **5434**)
```sql
risk_scores(patient_id, window_end, window_start, p_risk, risk_score, risk_level)
  PK (patient_id, window_end)
vitals_raw(patient_id, event_time, seq, hr, spo2, rr, temp_c, sbp, dbp)
```

## 8. Real dataset import (`scripts/import_physionet2012.py`)

- **Source:** PhysioNet/CinC Challenge 2012 set A — 4000 ICU records × 48 h, long format `Time,Parameter,Value`. Open access; downloaded as `set-a.zip` (AWS Open Data mirror `physionet-open` is fast; physionet.org throttles).
- **Mapping:** `HR→hr`, `SaO2→spo2` *(dataset has no SpO₂ channel; SaO₂ = arterial O₂ saturation)*, `RespRate→rr`, `Temp→temp_c`, `NISysABP→sbp` / `NIDiasABP→dbp` with invasive `SysABP/DiasABP/Art*ABP` fallback per timestamp.
- **Selection:** patients must have ≥1 hr, ≥1 bp, ≥1 o2sat (`--no-require-o2sat` to relax); sorted by observation **richness**, `--limit` (default 300, `0` = all qualifying) — richest-first gives the best demo density; `--select first` for file order.
- Timestamps anchored at a fixed epoch (2026-01-01) — offsets preserved, no clinical values touched.
- Output passes through `to_canonical()` (clamp + seq + sort) → `data/vitals_real.csv`.
- Realism note: charting sparsity is inherent to the dataset (HR/BP ~91% present, temp ~60%, SaO₂/RR intermittent) — features' `coverage`/`low_quality` handle it.
- Bonus: `data/physionet2012/Outcomes-a.txt` — SAPS-I, SOFA, LOS, in-hospital death for outcome validation.

## 9. Frontend (`web/`)

- Next.js 14 (app router) + React 18 + Recharts + Tailwind; `web/lib/api.ts` typed client; `NEXT_PUBLIC_API` for cross-origin API.
- Routes: `/` (rankings, distribution, critical, stats) · `/patients/[id]` (gauges, HR trace, SpO₂+risk area chart, raw table).
- Null-safe formatting: `fmt()` renders missing vitals as `—`.
- Live refresh via `no-store` fetch polling (no WebSocket layer).

## 10. Configuration

`config/default.yaml` (merged over `config.DEFAULTS`; env `VYTALS_CONFIG`):

| Key | Value |
|---|---|
| `kafka.bootstrap_servers` / `topic` / `partitions` | `localhost:9092` / `vitals.raw` / 3 |
| `streaming.watermark/window/slide` | `2 minutes` / `5 minutes` / `1 minute` |
| `postgres.*` | `localhost:5434`, db/user/pass `vytals` |
| `model.path` | `models/xgb_risk.json` |

Env overrides: `VYTALS_DATA` (replay CSV), `VYTALS_MODEL` (model file), `VYTALS_CONFIG`, `NEXT_PUBLIC_API`.

Host port map: **5432** system PG · **5433** other project · **5434** vytals PG · **9092** kafka · **8000** API · **3000** web.

## 11. Design decisions & trade-offs

1. **Rule-based labels** — no labelled clinical risk target exists in the dataset; transparent rules + noise give an explainable, self-contained supervised task. Honest limitation: not clinically validated ground truth.
2. **One `build_features()`** for training, replay, and stream — prevents train/serve skew; cost: streaming uses a coarser per-micro-batch approximation (slope/delta ≈ 0).
3. **Demo path independent of streaming path** — dashboard works with zero infra (important for evaluation), at the cost of two ingestion modes to maintain.
4. **Never fabricate vitals** — missing → `NaN` → neutral model fills + visible `—` in UI; low-quality windows flagged.
5. **Model load never crashes the API** — rule fallback keeps the service up; logged.
6. **NaN→None JSON guard** — one recursive helper instead of sprinkling conversions (starlette `allow_nan=False` hard-fails otherwise).
7. **`windowize` via `dt.floor` + groupby** — `pd.Grouper(freq)` materializes every empty 5-min slice across 48 h sparse records (~576/patient) and made real-data training appear to hang; floor+groupby visits only non-empty bins.
8. **Richest-first import selection** — same real dataset, but better demo density than file-order (`--select first` still available).

## 12. CI (`.github/workflows/ci.yml`)

- **backend (ubuntu, py3.12):** deps → `py_compile` (core modules + importer) → sample data → loader → small train → predict → API smoke (`/api/health`, `/api/patients`, `/api/analysis`).
- **frontend:** `npm ci` → `tsc --noEmit` → `next build`.

## 13. Known limitations / tech debt

| Item | Impact | Status |
|---|---|---|
| ~~Stream checkpoint path hardcoded (`job.py`)~~ | ~~streaming job fails on any non-`/home/minhaj` machine~~ | resolved 2026-10-19 — repo-relative `<repo>/checkpoints/vytals` (created at runtime), `--checkpoint` flag + `VYTALS_CHECKPOINT` env |
| Legacy `docker-compose` v1 broken vs Docker 29 | use `docker run` (documented) | workaround in place |
| PG-backed API read path exists but UI reads replay | live PG scores not yet surfaced in dashboard | open (post-MVP) |
| Spark jar resolution needs network on first run | cold start slower | documented |
| `windowize` ~8 min on real set (28k windows) | one-time training cost | accepted |
| SaO₂/RR sparse in Challenge 2012 | some UI gauges show `—` | dataset property, documented |
| Java 21-only host | Spark 3.5 needs Java 8/11/17 → install JDK 17 for streaming | resolved 2026-10-19 (jdk17-openjdk; `JAVA_HOME` in `~/.bashrc` + `~/hadoop/conf/hadoop-env.sh`) |

## 14. Distributed storage (HDFS)

Single-node HDFS (Hadoop **3.4.3**, JDK **17**, replication **1**) is the raw-data layer: the PhysioNet Challenge 2012 ICU records live in HDFS as the "big data" source of truth, and Spark reads them straight off `hdfs://localhost:9000`.

- **Config:** `~/hadoop/conf/` — `core-site.xml` (`fs.defaultFS=hdfs://localhost:9000`), `hdfs-site.xml` (NN/DN dirs under `~/hadoop/tmp/dfs`, replication 1, heap-capped daemons).
- **Lifecycle:** `make hdfs-up` (format-once + start NameNode/DataNode) · `make hdfs-down` · `make hdfs-status` · `make hdfs-upload` → all via `scripts/hdfs.sh` (also picks `JAVA_HOME` itself).
- **Layout:**

  ```
  /user/alvi/vytals/
    raw/physionet2012/set-a/        4000 ICU record files (long format, 35.9 MB)
    raw/physionet2012/Outcomes-a.txt  real outcomes (SAPS-I, SOFA, LOS, in-hospital death)
    raw/physionet2012/set-a.zip       original archive (provenance)
    csv/vitals.csv                    synthetic pipeline CSV (demo)
    csv/vitals_real.csv               imported real data — 28 285 rows / 300 patients
  ```

- **Verified (2026-10-19):** `hdfs dfsadmin -report` → 1 live datanode; `hdfs dfs -cat` on a raw record returns chart lines; PySpark `local[2]` session reading `hdfs://localhost:9000/user/alvi/vytals/csv/vitals_real.csv` → 28 285 rows / 300 patients (matches local counts), and reads a raw record file (273 rows).
- **Why single-node:** the course objective is the HDFS storage layer + how the pipeline talks to it; one NameNode/DataNode on the lab machine demonstrates the API (write-once blocks, replication, `hdfs dfs`) without a cluster to babysit. Scaling out is config + more DataNodes, not code.
- **Small-file note:** 4000 × ~9 KB files is the realistic ICU shape (one file per patient) but HDFS prefers fewer/larger blocks — `put -t 8` parallelizes the upload; if this ever becomes a bottleneck, concatenate per-patient files into sharded partitions (Hive/Spark can read either).
