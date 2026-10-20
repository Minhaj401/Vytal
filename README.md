# Vytals — Real-Time Vital Risk Scoring


**Vytals** watches patient vitals (HR, SpO₂, RR, temp, BP) in real time, scores each patient's risk on a single 0–100 number, and streams it to a live dashboard. Built for the course project: *Scalable Big Data Analytics Framework for Real-Time Health-Risk Monitoring Using Multivariate Patient Vital-Sign Data.*

📄 **Project report:** [`docs/REPORT.md`](docs/REPORT.md) · **Architecture diagram:** [`docs/architecture.png`](docs/architecture.png)

```
Patient vitals
    ↓  (CSV / JSONL replay, key=patient_id, acks=all)
Kafka topic: vitals.raw  (3 partitions)
    ↓
Spark Structured Streaming (standalone cluster: master + worker)
    ├─ JSON parse, schema validate, drop bad timestamps
    ├─ watermark 2 min, event-time, dedupe on (patient_id, event_time, seq)
    ├─ rolling window 5 min / slide 1 min, per patient_id
    ├─ build_features() — shared with training
    └─ XGBoost risk model → Risk 0–100 + LOW/MODERATE/HIGH/CRITICAL
    ↓                                            ↓
HDFS — durability lake                    PostgreSQL — serving
  /vytals/bronze/vitals  (raw)              risk_scores (latest windows,
  /vytals/silver/risk_scores  (scored)      vitals means + risk)
  /vytals/checkpoints                           ↓
                                    FastAPI (:8000)  ←→  Next.js dashboard (:3000)
                                        ├─ /             patient ranking, distribution, critical list
                                        └─ /patients/[id] live gauges, HR trace, SpO₂+risk, raw stream
```

## Architecture

![Vytals full-stack architecture](docs/architecture.png)

> Full-size diagram: [`docs/architecture.png`](docs/architecture.png) — source: [`docs/architecture.html`](docs/architecture.html) (render it in a browser and screenshot, or regenerate with any headless browser).

| Layer | Path | Role |
|---|---|---|
| Source | any CSV/JSONL with vitals cols | `scripts/make_sample_data.py` makes a demo dataset with stable/degrading/critical archetypes; canonical CSVs written by this repo also carry recomputed `news2_score,qsofa_score` columns |
| Loader | `src/vytals/data/loader.py` | CSV/JSONL → canonical schema (`patient_id,event_time,seq,hr,spo2,rr,temp_c,sbp,dbp`); aliases, range clamp (out-of-range→NA, never fabricate), missing allowed; extra columns ignored |
| Producer | `src/vytals/ingest/producer.py` | replay into `vitals.raw`, Kafka key = `patient_id`, `acks=all`, `--speed 1/60/0`, `--loop --shift-loop` (replayed loops advance event time, otherwise the watermark drops them as late data) |
| Streaming | `src/vytals/streaming/job.py` | Spark Structured Streaming: parse → validate → watermark 2m → window 5m/1m → features → ML → HDFS parquet (bronze + silver) **and** PG `risk_scores` |
| Lake | HDFS (`/vytals`) | `bronze/vitals` = validated raw vitals, `silver/risk_scores` = scored windows (parquet, partitioned by date/patient), `checkpoints/` = stream state. Written by `spark-submit`, browsable at `:9870` |
| Features | `src/vytals/features/windowed.py` | one `build_features(df)` used by both training (`train.py`) and the stream sink — no duplicated logic. Per vital: current/mean/std/min/max/slope/delta; plus `n_rows, coverage, low_quality` |
| ML | `src/vytals/ml/` | XGBoost regressor (`reg:squarederror`), labels = transparent physiology rules + small noise, grouped-by-patient split (no leakage), top features logged |
| Model artifact | `models/xgb_risk.json` | loaded at API and stream start; falls back to rule-based score if unloadable |
| Store | Postgres `risk_scores` (+ `vitals_raw`) | serving table written by Spark (`src/vytals/store/db.py:ensure()` creates tables, host port **5434**); `src/vytals/store/queries.py` is the read path |
| API | `src/vytals/api/server.py` | FastAPI. `VYTALS_SOURCE=postgres` (full build) reads the serving table; `VYTALS_SOURCE=replay` (dev/CI) uses the in-memory engine. Same JSON contract either way. Endpoints: `/api/health`, `/api/patients`, `/api/patients/{id}/live`, `/api/patients/{id}/history`, `/api/analysis`, `POST /api/advance` (replay-only) |
| Replay sim | `src/vytals/sim/replay.py` | dev/CI fallback so the UI works with zero Kafka/Spark/HDFS running |
| UI | `web/` (Next.js 14, Tailwind, Recharts) | Apple-HIG light theme; `/` dashboard; `/patients/[id]` live route |

## Where data comes from

**Real data (open access):** PhysioNet/CinC Challenge 2012 — 4,000 real ICU stays with HR, SaO₂, respiratory rate, temperature, invasive + non-invasive BP time series (first 48 h) and in-hospital mortality outcomes. Open Data Commons Attribution license, no credentialing.

```bash
make data-real    # downloads set-a + Outcomes-a, converts -> data/physionet2012.csv
make train-real   # trains on real data (6h windows; hourly charting)
```

Mapping in `scripts/import_physionet2012.py`: `HR→hr`, `SaO2→spo2` (challenge has no pulse-ox), `RespRate→rr`, `Temp→temp_c`, `NISysABP|SysABP→sbp`, `NIDiasABP|DiasABP→dbp` (non-invasive preferred); `-1` = missing. `Outcomes-a.txt` (mortality, SAPS-I, SOFA, LOS) stays in `data/physionet2012/` for label work.

**Synthetic, pre-labeled sample (open):** [HLT-009](https://huggingface.co/datasets/xpertsystems/hlt009-sample) (XpertSystems.ai) — 25 synthetic ICU episodes at 5-min cadence: 12-stream vitals (HR, SpO₂, RR, NBP/IBP, temp, EtCO₂, CVP, CO), alarm + intervention logs, per-row `news2_score`/`qsofa_score`, and a 54-column episode summary (SOFA, APACHE-II, mortality, 6h-deterioration label). License CC-BY-NC-4.0 (fine for coursework, not commercial). Chosen for its standard, citable label definitions:

```bash
make data-hlt009    # downloads the 4 CSVs, converts -> data/hlt009.csv (canonical + score columns)
make train-hlt009   # XGBoost: NEWS2 regression + NEWS2>=5 / qSOFA>=1 classification (+ confusion matrices)
```

For the credentialed full-scale source the project abstract targets, see [MIMIC-IV](https://physionet.org/content/mimiciv/) (CITI training + DUA required) or the open [MIMIC-IV demo](https://physionet.org/content/mimic-iv-demo/) (100 patients, chartevents). [eICU-CRD](https://physionet.org/content/eicu-crd/) and [VitalDB](https://vitaldb.net) are further options.

**Synthetic fallback:** the repo also ships a generator:

```bash
make sample   # writes data/vitals.csv — 12 patients, 240 min, 30s cadence, 3 archetypes
```

| Archetype | Behavior |
|---|---|
| stable (~70%) | normal means, small noise |
| deteriorating (~20%) | HR↑, SpO₂↓, RR↑, temp↑ drift over time |
| critical (~10%) | sustained abnormal |

Point the loader/producer at any real export instead via `--input`. Canonical columns must include patient id, timestamp, and the 6 vitals (missing ok).

Every canonical CSV written by this repo (demo, PhysioNet 2012, HLT-009) also carries `news2_score,qsofa_score` recomputed from the six vitals by `src/vytals/data/clinicalscore.py` (NEWS2 Scale 1; supplemental oxygen and consciousness are not in the schema and are scored as normal — room air, alert; missing vitals score 0).

## Where the model comes from

Trained locally by this repo — no external pretrained weights:

```bash
make train        # synthetic demo  → models/xgb_risk.json
make train-real   # PhysioNet 2012  → models/xgb_risk_real.json  (6h windows, hourly charting)
make train-hlt009 # HLT-009 sample   → models/xgb_hlt009_*.json  (NEWS2/qSOFA labels, Python xgboost)
```

- Features: output of `build_features()` on per-patient time windows (`src/vytals/ml/train.py:windowize`).
- Labels: `RiskModel.rule_score()` — hand-written clinical derangement thresholds, clipped 0–100, plus Gaussian noise σ=2 so the model learns a smooth mapping.
- Split: patient-wise (`GroupShuffleSplit`) so one patient's windows never appear in both train and test.
- Eval: MAE / R² printed; naive R² on random split was inflated — honest patient-wise R² ≈ 0.88 on the demo set. Top features: `spo2_mean`, `sbp_mean`, `temp_c_mean`…
- Real data (set-a, 3,946 patients): 31,238 windows, 789 held-out patients. Rule baseline MAE 1.62. Sanity vs outcome: mean score died=25.2 vs survived=19.5 — directional signal only; the rules are not fit to mortality (see `data/physionet2012/Outcomes-a.txt` for the label work that would fit it).

### HLT-009 (synthetic) — XGBoost accuracy + confusion matrices

`make train-hlt009`: 1,897 windows (non-overlapping 60-min) × 43 shared `build_features()` features; episode-wise split (18 train / 7 test episodes, `GroupShuffleSplit` seed 42); xgboost 3.4.1, `max_depth 5`, `eta 0.08`, 300 rounds, early stop 30. Dataset: 26,677 rows / 25 episodes.

| Task | Metric | Test value |
|---|---|---|
| regression → `news2_score` | MAE / R² | **0.49** / **0.848** |
| classification → NEWS2 ≥ 5 | accuracy | **0.949** |
| | precision / recall / F1 | 1.00 / 0.91 / 0.95 |
| | ROC-AUC | 0.9994 |
| classification → qSOFA ≥ 1 | accuracy | **0.998** |
| | precision / recall / F1 | 1.00 / 1.00 / 1.00 |
| | ROC-AUC | 0.9992 |

Confusion matrices (test windows, threshold 0.5):

```
                    NEWS2 >= 5                   qSOFA >= 1
                 pred 0   pred 1              pred 0   pred 1
   actual 0         237        0       actual 0     308        0
   actual 1          27      268       actual 1       1      223
```

qSOFA never exceeds 1 in this sample (no mental-status stream), so the ≥2 cut has zero positives and ≥1 is used instead. These numbers measure how well the model reproduces the dataset's own score formulas — same vitals in, score out — **not** clinical performance. The real-data direction check above is the only outcome-linked number in the repo so far.

## How to run

### Full build — Kafka + Spark + HDFS + Postgres

Requires: Docker with the **Compose v2 plugin** (`docker compose`; the legacy v1 binary on this host is broken against Docker 29) and ~4 GB free RAM for Spark + HDFS.

```bash
make up          # zookeeper, kafka (+topic init), postgres, HDFS, Spark cluster, API
make pipeline    # demo producer (CSV → Kafka, looping with shifted event time) + Spark job (→ HDFS + Postgres)
npm --prefix web run dev   # dashboard on http://localhost:3000
```

Services: API `:8000`, web `:3000`, Spark master `:8080`, worker `:8081`, HDFS UI `:9870`, Postgres `:5434`, Kafka `:9092`.

Verify the full path:

```bash
curl -s localhost:8000/api/health         # {"ok":true,"source":"postgres"}
curl -s localhost:8000/api/patients | head -c 300
make hdfs-ls                              # /vytals/bronze/vitals, /vytals/silver/risk_scores
```

`make down` stops containers (volumes kept; `docker compose down -v` wipes HDFS + Postgres). The Spark driver runs inside the `spark-submit` container (client mode), so keep it up while the job runs; `make logs` follows all services.

### Dev fallback — replay, no infra

```bash
make install sample train   # venv + sample CSV + train model
make api                    # http://localhost:8000 (VYTALS_SOURCE=replay, 2s ticks)
npm --prefix web run dev    # http://localhost:3000
```

`make stream` still runs the job locally with pyspark against host Kafka/Postgres (`config/default.yaml`, no HDFS — checkpoints land in `./checkpoints`).

### CI/CD

`.github/workflows/ci.yml` on every push/PR (concurrency cancel-in-progress):
- backend: install, py_compile, unit tests, sample data, loader, train, predict, API smoke (`/api/health`, `/api/patients`, `/api/analysis`) with wait-loop + trap kill
- frontend: `npm ci`, `tsc --noEmit`, `next build`
- docker: `docker compose config` + `docker build -t vytal-api:ci .`

## Config

`config/default.yaml` (host dev) and `config/docker.yaml` (compose: service DNS + HDFS):

```yaml
kafka:     bootstrap_servers: localhost:9092 | kafka:29092, topic: vitals.raw, partitions: 3, acks: all
streaming: watermark: "2 minutes", window: "5 minutes", slide: "1 minute", trigger: 30s | 10s, starting_offsets: latest | earliest
hdfs:      uri: "" | hdfs://namenode:8020, base: /vytals
postgres:  host: localhost port 5434 | postgres:5432, db/user/password: vytals
model:     path: models/xgb_risk.json, version: v1
```

`VYTALS_SOURCE=postgres|replay` selects the API read path (`postgres` in compose); `VYTALS_CONFIG` points at the YAML.

## Honest limitations

- Risk "truth" is rule-based labels + noise — a demo stand-in for a clinically validated target.
- Full build serves the dashboard from Postgres; the replay engine remains only as a dev/CI fallback (`VYTALS_SOURCE=replay`). API startup runs `ensure()` so an empty database gets its tables.
- HDFS streaming sink writes many small parquet files (no compaction job yet) — fine for a demo, not for long-running production.
- `foreachBatch` is not transactional across HDFS + Postgres: a batch retried after a partial write can duplicate rows or hit the `risk_scores` primary key.
- Streaming sink approximates slope/delta as 0 and current≈window mean; training uses full trailing windows. Feature impl is shared via `build_features()` to avoid drift, but the Spark mini-window is coarser than batch.
- A window is scored once, when the watermark closes it (append mode) — a late reading never re-scores a closed window, and readings older than the watermark are dropped.
- Legacy `docker-compose` v1 on this host breaks against Docker 29 — the Makefile targets need the compose v2 plugin.
- HLT-009 accuracy/confusion numbers come from synthetic labels: they measure formula imitation (same vitals in, score out), not clinical performance. HLT-009 itself is CC-BY-NC-4.0 (non-commercial).
- Recomputed NEWS2/qSOFA assume room air + alert consciousness (absent from every schema here; oxygen and ACVPU are impossible to derive). PhysioNet's SpO₂ (SaO₂) coverage is only 3.4%, so its NEWS2 leans on HR/RR/temp/BP.
- Port map on this host: 5432 = system pg, 5433 = care stack, **5434 = vytals pg**, 9092 = vytals kafka.
