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

### Live mode — `VYTALS_SOURCE=pg` (dashboard reads the stream's scores)

Two sources behind the same routes, so the frontend needs no changes:

- **`replay` (default)** — in-memory replay engine; works with zero infra.
- **`pg`** — `store/pg_reads.py:PgLive` serves streamed truth: risk scores from PG `risk_scores` (TTL 3 s cache, one `merge_asof` refresh), vitals from the canonical CSV the producer replayed (**as-of join**: each charted reading carries the most recent score ≤ its timestamp — exactly what a bedside monitor displays between updates).
  - `/api/patients` + `/api/analysis` rank by latest scored window per patient (risk from PG, HR/SpO₂ from the joined chart row); `/history` returns chart-cadence points with streamed `risk_score/risk_level/p_risk`; `/advance` → 409 (replay-only).
  - If PG is unreachable/empty at startup, it logs the reason and **falls back to replay** — the dashboard still renders. `/api/health` reports the active `source`.
  - Verified 2026-10-21 against the full replay's 28 270 streamed rows: 300 patients, 0.4 s init, no NaN leaks.
- Run: `VYTALS_SOURCE=pg make api` (needs PG up + a completed `make producer`/`make stream` run).

## 7. Streaming path

### Producer (`ingest/producer.py`)
- `kafka-python`, `acks=all`, `linger_ms=20`, `retries=5`, key = `patient_id` (partition affinity per patient).
- Inter-arrival sleep from `event_time` deltas / `--speed` (1 = realtime, 60 = 60×, 0 = flat out); capped 5 s/gap.
- Fail-fast: `max_block_ms=8000` + explicit `SystemExit` with a start-infra hint when no broker.

### Spark job (`streaming/job.py`)
- Session: `spark.jars.packages` pulls `spark-sql-kafka-0-10_2.12` (**version matched to the installed pyspark at runtime**) + `postgresql` JDBC at startup (needs network/Ivy cache on first run).
- Pipeline: raw Kafka → `from_json` on declared schema → `to_timestamp` + `coalesce(patient_id, k)` → filter nulls → `withWatermark("event_ts","2 minutes")` → `dropDuplicates(["patient_id","event_time","seq"])` → `groupBy(patient_id, window(event_ts, "5 minutes","1 minute"))` with avg/stddev/min/max + `window_end`, `n_rows`.
- Sink: `foreachBatch` → builds `FEATURE_COLS` frame (current≈window mean, slope/delta=0 — documented approximation) → model → **pandas → psycopg2** `INSERT … ON CONFLICT (patient_id, window_end) DO NOTHING` into `risk_scores`; per-batch console summary; PG failure logged, batch continues (dev-friendly).
  - Not Spark JDBC: `spark.createDataFrame(pandas_df)` and `batch_df.rdd.*` take the Python-RDD pickle path, which blows up with `RecursionError: Stack overflow` on pyspark 3.5 + Python 3.14. JVM-side `limit(1).count()` for emptiness + direct psycopg2 write sidesteps it entirely.
- Trigger: 30 s micro-batches; `outputMode("update")`.
- Checkpoint: `<repo>/checkpoints/vytals` (auto-created; `--checkpoint DIR` or `VYTALS_CHECKPOINT` env overrides). **Dedupe caveat:** `dropDuplicates((patient_id, event_time, seq))` state persists in the checkpoint — re-producing the same CSV after a previous run drops every message as a duplicate; use a fresh checkpoint dir for a re-run.
- **Verified e2e 2026-10-21:** Kafka (`vitals.raw`, 3 partitions) → Spark → PG on real data — batches score 1.6–2k windows, PG fills LOW→CRITICAL (e.g. 1903 rows / 240 patients / max 86.5 after ~2 min of replay).

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
| pyspark 3.5 + Python 3.14: `toPandas()` → `ModuleNotFoundError: distutils` | stream dies on first batch | resolved 2026-10-21 — `setuptools` in requirements shims distutils |
| pyspark 3.5 + Python 3.14: `createDataFrame(pandas)` / `.rdd` → `RecursionError: Stack overflow` | PG sink unusable via Spark | resolved 2026-10-21 — psycopg2 sink + JVM-side emptiness check |
| Legacy `docker-compose` v1 broken vs Docker 29 | use `docker run` (documented) | workaround in place |
| ~~PG-backed API read path exists but UI reads replay~~ | ~~live PG scores not surfaced in dashboard~~ | resolved 2026-10-21 — `VYTALS_SOURCE=pg` mode (`store/pg_reads.py`), auto-retry until scores exist; replay remains default |
| Spark jar resolution needs network on first run | cold start slower | documented |
| Stream dedupe state vs re-replayed data | re-producing the same CSV after a prior run silently drops everything as duplicate | documented — use a fresh `VYTALS_CHECKPOINT` for a re-run (found while staging the demo video) |
| `windowize` ~8 min on real set (28k windows) | one-time training cost | accepted |
| SaO₂/RR sparse in Challenge 2012 | some UI gauges show `—` | dataset property, documented |
| Java 21-only host | Spark 3.5 needs Java 8/11/17 → install JDK 17 for streaming | resolved 2026-10-19 (jdk17-openjdk; `JAVA_HOME` in `~/.bashrc` + `~/hadoop/conf/hadoop-env.sh`) |

## 14. Distributed storage (HDFS)

Single-node HDFS (Hadoop **3.4.3**, JDK **17**, replication **1**) is the raw-data layer: the PhysioNet Challenge 2012 ICU records live in HDFS as the "big data" source of truth, and Spark reads them straight off `hdfs://localhost:9000`.

- **Config:** `~/hadoop/conf/` — `core-site.xml` (`fs.defaultFS=hdfs://localhost:9000`), `hdfs-site.xml` (NN/DN dirs under `~/hadoop/tmp/dfs`, replication 1, heap-capped daemons).
- **Lifecycle:** `make hdfs-up` (format-once + start NameNode/DataNode **and YARN** ResourceManager/NodeManager — MapReduce needs them) · `make hdfs-down` · `make hdfs-status` · `make hdfs-upload` → all via `scripts/hdfs.sh` (also picks `JAVA_HOME` itself).
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

## 15. Outcome validation (`scripts/validate_outcomes.py`, `make validate`)

The risk model was trained on **rule-based derangement labels**, never on mortality — so the honest question is external validity: do the streamed scores track real ICU outcomes? The script joins PG `risk_scores` (per-patient aggregate) to `Outcomes-a.txt` (real SAPS-I, SOFA, LOS, in-hospital death) by RecordID and reports AUROC, survivor/non-survivor separation, Spearman correlations, and the risk-level mix by mortality.

Measured on the live e2e replay (300 patients, mid-replay, 2026-10-21):

| Metric | Value |
|---|---|
| AUROC — in-hospital death (per-patient **mean** risk) | **0.733** |
| AUROC (per-patient max risk) | 0.547 — ceiling effect, max saturates ~74 for everyone |
| mean risk — survivors vs non-survivors | 34.8 vs 41.8 |
| Spearman vs SOFA / SAPS-I / LOS | +0.351 / +0.301 / +0.096 |
| level mix | dead: 27.8% HIGH, 9.7% LOW — alive: 13.8% HIGH, 29.3% LOW |

Read: the model was never shown a death label, yet separates decedents at AUROC ≈ 0.73 and tracks SOFA — exactly the sanity check a rule-derived target should pass. Max-risk is the wrong aggregate (everyone's worst 5-min window is abnormal); mean over the stay discriminates. **Final numbers after the full 48 h replay** (all 300 patients scored, `make validate`): death AUROC **0.715**, SOFA ρ +0.337, SAPS-I +0.322 — stable vs the mid-replay snapshot above.

## 16. Batch processing layer — MapReduce on YARN (objective 4)

`scripts/mr/{abnormal_mapper,abnormal_reducer}.py` run through **hadoop-streaming 3.4.3 on YARN** — the classic MapReduce job over the raw long-format records in HDFS.

- **Job:** abnormal-event frequency per vital across all 4000 ICU records (1 757 980 readings). Mapper thresholds mirror the risk rules (HR <50/>120, SaO₂ <90, RespRate <8/>30, Temp <35/>38.5, Sys/Dias ABP <90/>180 & <50/>110, invasive and non-invasive); missing values (`-1`, junk) are never counted. Reducer sums `total` / `abnormal` per param.
- **Run:** `make mr` (`scripts/mr/run.sh`) — merges the 4000 tiny per-patient files into one 26 MB `set-a_all.txt` first, then submits. Result: `hdfs dfs -cat /user/alvi/vytals/mr/abnormal_freq/part-*`.
- **Result (15 s wall, 1 map + 1 reduce, 512 MB containers):**

  | param | readings | abnormal | % |
  |---|---|---|---|
  | NIDiasABP | 98 210 | 30 148 | 30.70 |
  | DiasABP | 145 567 | 30 970 | 21.28 |
  | SysABP | 145 650 | 13 580 | 9.32 |
  | NISysABP | 98 331 | 8 081 | 8.22 |
  | HR | 228 538 | 12 513 | 5.48 |
  | Temp | 86 405 | 4 369 | 5.06 |
  | RespRate | 55 051 | 2 351 | 4.27 |
  | SaO₂ | 8 185 | 188 | 2.30 |

- **Small-files lesson:** one file per patient is the realistic ICU shape but produced **4000 map tasks** (~1 JVM container each ≈ 75 min). Merging to a single split → one mapper, 15 s. (`CombineFileInputFormat` / `NLineInputFormat` were tried first — streaming's old-API `-inputformat` only accepts `mapred` classes and the `linespermap` key didn't take; the merge is simpler and deterministic.)
- **YARN sizing (lab box, 7.4 GB):** `yarn-site.xml` gives the NodeManager 4096 MB / 6 vcores with 512 MB map+reduce containers — the first attempt (1024 MB total) deadlocked because the 256 MB AM + a 1024 MB map could never co-schedule. `make hdfs-up` now starts ResourceManager + NodeManager alongside HDFS.

## 17. Descriptive & diagnostic analytics — Spark SQL (objectives 10/11)

`scripts/batch_analytics.py` (`make batch`) — one PySpark session over the HDFS copy of the canonical CSV (28 285 rows / 300 patients), plain SQL/DataFrame queries at cluster-scale:

- **Descriptive:** per-vital distribution (n, mean, std, min, p50, max) and abnormal-event rate using the same thresholds as the MapReduce job — e.g. HR 5.17 % abnormal, DBP 27.95 %, SBP 11.29 %.
- **Diagnostic:** pairwise Pearson correlations (sbp–dbp +0.524, hr–spo₂ −0.319, hr–rr +0.119 …); abnormal-event co-occurrence in the same reading; and a patient-level join against real `Outcomes-a.txt` — vital-derangement rates track **SOFA** (hr_abn ρ +0.123, hypox_rate +0.157, spo₂_mean −0.158) more than SAPS-I, confirming the streaming risk story in the batch layer.

## 18. Model comparison & SHAP (objectives 13/14)

- **Comparison — `scripts/compare_models.py` (`make compare`):** 27 886 windows / 300 patients, **patient-wise** 5-fold `GroupShuffleSplit` (no leakage), target = rule-derived risk ≥ 50 (HIGH+):

  | model | Precision | Recall | F1 | AUROC |
  |---|---|---|---|---|
  | LogisticRegression | 0.390 | 0.109 | 0.168 | 0.904 |
  | RandomForest | 0.988 | 0.645 | 0.775 | 0.997 |
  | **XGBoost** | **0.993** | **0.797** | **0.882** | **0.997** |
  | IsolationForest* | — | — | — | 0.930 |

  \* unsupervised — AUROC only. XGBoost wins on F1 and is the model shipped in the stream. Honest caveat: labels are rule-derived from the same physiology, so AUROC ≈ 1.0 measures separability of the rule, not clinical accuracy — the external check is §15.
- **SHAP — `scripts/shap_explain.py` (`make shap`):** TreeExplainer on the real booster. Global top features: `hr_max` 3.80, `temp_c_current` 1.99, `dbp_min` 1.50, `sbp_current` 1.33, `sbp_min` 1.15 — same physiology the trainer logs as gain. Plots: `docs/screenshots/shap_global.png` (mean |SHAP| per feature) and `shap_patient.png` (local explanation for the riskiest window).

## 19. Single-machine vs distributed benchmark (objective 17)

`scripts/benchmark.py` (`make bench`) runs **the identical aggregation** (per-patient × param group-by over all 1 757 980 raw readings, 42 groups) on both engines:

| engine | wall time | throughput | data path |
|---|---|---|---|
| single-machine — pandas | **0.57 s** | 3 073 987 rows/s | local CSV |
| distributed — PySpark `local[*]` | **34.81 s** | 50 496 rows/s | HDFS `hdfs://localhost:9000` |

Relative: Spark/pandas = **60.9×** at this scale — honest reading: on one 8-core lab host, JVM/session startup and HDFS read overhead dominate; the distributed win appears when the data exceeds one machine (the same Spark code paths to a multi-node cluster with no rewrite). The comparison is reported as measured, not spun.

## 20. Risk-score serving storage (objective 15)

The abstract allows "HBase **or another appropriate technology**" — this project uses **PostgreSQL** (already required for the streaming sink) instead of standing up an HBase cluster on a 7.4 GB lab box:

- `risk_scores(patient_id, window_end, …)` with **PK (patient_id, window_end)** — point retrieval of one patient's score series in milliseconds, exactly the dashboard's access pattern; the streaming sink upserts with `ON CONFLICT DO NOTHING` for replay idempotency.
- Written by the Spark stream, read by `make validate` (§15) and ready for the dashboard's live path — low-latency, indexed, durable. HBase's wide-column/region model would pay off at thousands of concurrent patient-window lookups per second; that's out of scope for a single-node demo and would add an entire extra cluster to operate.
