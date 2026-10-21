# Vytals — College Presentation Pack

> Everything needed for the project presentation/viva: problem, architecture, workflows, user flow, model accuracy, measured results, and how each course objective was met.
> Companion docs: [README](../README.md) · [TECHNICAL](TECHNICAL.md) · [FEATURES](FEATURES.md) · [INSTALLATION](INSTALLATION.md) · [PROGRESS](PROGRESS.md)

**Course:** Data Analytics and Big Data Processing · **Group 2** · Topic submitted 21/09/2026

| # | Student | Register No. |
|---|---|---|
| 1 | Alvi A V | CCE24CD009 |
| 2 | Ayush P Shibu | CCE24CD025 |
| 3 | Minhaj Noushad | CCE24CD043 |
| 4 | Joyal Shinoy | CCE24CD046 |
| 5 | Thomas Robin | CCE24CD059 |

**Project title:** *Scalable Big Data Analytics Framework for Real-Time Health-Risk Monitoring Using Multivariate Patient Vital-Sign Data*

**Live demo of the finished system:** ![demo](demo/demo.gif) ([full video](demo/demo.mp4))

---

## 1. Problem statement (30-second version)

Hospital monitors stream vital signs (heart rate, SpO₂, respiration, temperature, blood pressure) for every patient continuously. That data is a textbook **Big Data** problem — high **volume** (4 000 patients × 48 h in even a small public archive), high **velocity** (sub-minute readings), high **variety** (different scales, sampling rates, units, missingness).

Single-machine, batch-oriented tools don't scale with it, and raw numbers alone don't help clinicians — they need **interpretable risk indicators**, per patient, continuously. Vytals builds that: a distributed pipeline (HDFS → MapReduce/Spark → Kafka → Spark Structured Streaming → PostgreSQL) that turns raw vitals into a **0–100 risk score per patient**, explainable per reading, visualised live.

> Academic prototype — decision support, **not** a medical device; no diagnosis, no clinical claims.

## 2. Motivation

- Combine **descriptive + diagnostic + predictive + real-time** analytics in one coherent pipeline (all four are implemented — §7).
- Big-data tools should demonstrably do the job: we **measure** them (MapReduce/Spark timings, single-machine vs distributed benchmark — §8) instead of assuming them.
- In healthcare, a black-box score is useless → **SHAP explainability** is a first-class output (§7.5).
- Risk models must be sanity-checked against **reality** → streamed scores are validated against real ICU outcomes (§8.4).

## 3. Objectives checklist (the abstract's 17)

| # | Objective | Status | Where / measured |
|---|---|---|---|
| 1 | Collect & integrate public vital-sign data | ✅ | PhysioNet Challenge 2012 — 4 000 real ICU records; 300-patient canonical set (28 285 rows) |
| 2 | Design scalable big-data architecture | ✅ | §4 of this doc · [TECHNICAL §1](TECHNICAL.md) |
| 3 | Distributed storage with HDFS | ✅ | Hadoop 3.4.3 single-node, `/user/alvi/vytals/` (35.9 MB), Spark reads verified |
| 4 | MapReduce and/or Hive batch processing | ✅ | MapReduce on YARN — `make mr`, abnormal-frequency table in 15 s (§8.1); Hive covered by Spark SQL |
| 5 | Apache Spark / PySpark processing | ✅ | batch SQL, benchmark, Structured Streaming — all PySpark |
| 6 | Kafka streaming ingestion | ✅ | topic `vitals.raw` (3 partitions), 28 285-message replay, acks=all |
| 7 | Spark Structured Streaming real-time analytics | ✅ | watermark 2 min, 5 min/1 min windows → XGBoost → PostgreSQL (verified e2e) |
| 8 | Cleaning, missing values, normalisation | ✅ | canonical loader: aliases, range clamp, missing stays missing (never fabricated) |
| 9 | Temporal/statistical feature engineering | ✅ | shared `build_features()`: current/mean/std/min/max/slope/delta + coverage flags |
| 10 | Descriptive analytics | ✅ | `make batch` — distributions + abnormal rates (§7.1) |
| 11 | Diagnostic analytics | ✅ | `make batch` — correlations, co-occurrence, outcome joins (§7.2) |
| 12 | Predictive + anomaly-detection models | ✅ | XGBoost risk regressor (production) + IsolationForest (§7.4) |
| 13 | Compare multiple ML models | ✅ | `make compare` — 4 models, patient-wise 5-fold CV (§7.4) |
| 14 | Explainability (SHAP) | ✅ | `make shap` — global + per-patient plots (§7.5) |
| 15 | HBase or another appropriate store | ✅ | **PostgreSQL** `risk_scores`, PK (patient_id, window_end) → ms retrieval (TECHNICAL §20) |
| 16 | Interactive dashboard | ✅ | Next.js — overview + patient pages, live-updating (§6) |
| 17 | Distributed vs single-machine comparison | ✅ | `make bench` — measured, honest numbers (§8.6) |

**17 / 17 complete.**

## 4. System architecture

```mermaid
flowchart TD
    subgraph Sources
        CSV[Canonical CSV / JSONL<br/>real ICU vitals or synthetic]
        RAW[Raw PhysioNet<br/>Challenge 2012 records]
    end

    subgraph Ingestion
        P[Kafka producer<br/>key=patient_id, acks=all]
        TOPIC[(Kafka topic<br/>vitals.raw · 3 partitions)]
    end

    subgraph Storage
        HDFS[(HDFS single node<br/>hdfs://localhost:9000<br/>4000 records + outcomes + CSVs)]
        PG[(PostgreSQL :5434<br/>risk_scores<br/>PK patient_id+window_end)]
    end

    subgraph Batch["Batch layer (historical)"]
        MR[MapReduce on YARN<br/>abnormal-event frequency]
        SP[Spark SQL<br/>descriptive + diagnostic]
        ML[ML training / comparison<br/>XGBoost · RF · LR · IF]
        SH[SHAP explanations]
        BM[pandas vs Spark benchmark]
    end

    subgraph Streaming["Streaming layer (real-time)"]
        SS[Spark Structured Streaming<br/>watermark 2m · window 5m/1m<br/>dedupe → build_features]
        XGB[XGBoost risk model<br/>score 0–100 + level]
    end

    subgraph Serving
        API[FastAPI :8000<br/>replay mode | pg mode]
        UI[Next.js dashboard :3000]
    end

    CSV --> P --> TOPIC --> SS --> XGB --> PG
    RAW --> HDFS
    HDFS --> MR
    HDFS --> SP
    CSV --> ML --> XGB
    ML --> SH
    RAW --> BM
    PG --> API --> UI
    SP --> ML
```

**Layer rationale (one line each):**

| Layer | Tech | Why |
|---|---|---|
| Storage | HDFS | fault-tolerant distributed storage; the raw archive is the "big data" source of truth |
| Batch | MapReduce + Spark SQL | objective-4 batch processing + SQL analytics over the full dataset at scale |
| Ingestion | Kafka | decoupled, replayable, partitioned-by-patient stream — the "velocity" layer |
| Stream | Spark Structured Streaming | event-time windows, watermarking (late data), exactly-once-ish via checkpoint + dedupe |
| Serving store | PostgreSQL | low-latency indexed point reads for the dashboard (the "appropriate technology" for objective 15) |
| ML | XGBoost | best measured F1 (§7.4), robust to missingness, SHAP-compatible |
| UI | Next.js + FastAPI | live-updating dashboard, zero-infra demo mode |

## 5. Data flow — the two workflows

### 5.1 Batch workflow (historical data)

```
PhysioNet set-a.zip → data/physionet2012/set-a/ (4000 files, long format)
   │  make real-data (importer: pivot, alias map, richest-300 selection)
   ▼
data/vitals_real.csv (28 285 rows, 300 patients, 6 vitals, missing kept)
   │  make train-real (windowize → build_features → patient-wise split)
   ▼
models/xgb_risk_real.json  (MAE 1.62 · R² 0.963)
   │
   ├─ hdfs dfs -put …  →  /user/alvi/vytals/  ── make mr   → abnormal-frequency table
   │                                          └─ make batch → descriptive + diagnostic SQL
   ├─ make compare  → 4-model CV table
   ├─ make shap     → global + local explanation plots
   └─ make bench    → pandas vs Spark timings
```

### 5.2 Streaming workflow (real time)

```
CSV ── producer ──► Kafka vitals.raw ──► Spark Structured Streaming
                                            │  parse & validate JSON
                                            │  watermark 2 min (event time, late data)
                                            │  dedupe (patient_id, event_time, seq)
                                            │  window 5 min / slide 1 min per patient
                                            │  build_features()  ← same code as training
                                            │  XGBoost → risk_score 0-100 + level
                                            ▼
                                   PostgreSQL risk_scores  (ON CONFLICT DO NOTHING)
                                            ▼
                     FastAPI (VYTALS_SOURCE=pg) ──► Next.js dashboard (2 s auto-refresh)
                                            │
                                            └── make validate → join real ICU outcomes
```

Verified numbers from the full e2e run: producer replayed **28 285 messages in 8.9 s**, Spark wrote **28 285 scored windows / 300 patients** to PostgreSQL, batches score 1.6–2k windows each, and the dashboard serves the same rows in pg mode.

## 6. User flow (what you demo and what the evaluator sees)

**Start (three options):**

1. **Zero-infra demo** — `make install sample train api` + `npm --prefix web run dev` → dashboard on :3000 replaying a synthetic ward in-process.
2. **Real data demo** — same with `data/vitals_real.csv` + real model.
3. **Full pipeline (the real showcase)** — Kafka + Spark stream running, then `VYTALS_SOURCE=pg make api` → dashboard reads **what the stream actually wrote**.

**Overview page (`/`)** — ![overview in demo gif](demo/demo.gif)

1. Four stat cards: **Patients · Avg risk · Max risk · Critical** (auto-refresh every 2 s).
2. **Patient list**, ranked worst-first: circular risk ring, level badge (LOW/MODERATE/HIGH/CRITICAL), progress bar, `HR • SpO₂ • timestamp` line.
3. **Distribution** bar chart by risk level.
4. **Needs attention** — top-5 highest-risk patients with vitals.

**Patient page (`/patients/[id]`)**

1. Header: activity-ring risk gauge, level badge, model probability (`P=`), live pulse dot + window timestamp.
2. Six vital cards with clinical reference ranges (normal/abnormal colouring; missing shows `—`, never fabricated).
3. **Heart-rate trend** chart with 60/100 bpm reference lines.
4. **SpO₂ + risk overlay** chart (two axes).
5. **Live stream log** — raw scored rows: `hr spo2 rr temp bp risk level`.

**Walkthrough script for the viva:** open dashboard (300 real patients) → point at stat cards → scroll the ranking → open the worst patient → show charts + stream log → open `make validate` output (real mortality AUROC) → show `make mr` / `make batch` / `make compare` / `make shap` / `make bench` terminals if asked.

## 7. Analytics & ML results

### 7.1 Descriptive (objective 10) — Spark SQL over HDFS

Per-vital distribution of non-missing readings (28 285 rows):

| Vital | n | mean | std | min | median | max |
|---|---|---|---|---|---|---|
| SBP | 25 995 | 114.3 | 22.6 | 50 | 111 | 279 |
| DBP | 25 961 | 56.8 | 12.0 | 20 | 55 | 180 |
| HR | 25 676 | 90.3 | 16.9 | 30 | 89 | 217 |
| Temp °C | 16 855 | 37.15 | 0.86 | 30.6 | 37.2 | 41.2 |
| RR | 2 376 | 20.9 | 4.9 | 9 | 21 | 44 |
| SpO₂ | 2 270 | 96.6 | 2.6 | 63 | 97 | 100 |

Abnormal-event rates (same thresholds as the MapReduce job): **HR 5.2 %, DBP 28.0 %, SBP 11.3 %, Temp 4.7 %, RR 2.8 %, SpO₂ 2.1 %**.

### 7.2 Diagnostic (objective 11)

- Pairwise Pearson correlations: **SBP↔DBP +0.524**, HR↔SpO₂ **−0.319**, HR↔RR +0.119, HR↔SBP −0.109.
- Patient-level join against **real hospital outcomes**: vital-derangement rates track **SOFA** (HR-abnormal rate ρ +0.123, hypoxemia rate +0.157, SpO₂ mean −0.158) more strongly than SAPS-I — the batch layer independently confirms the streaming risk story.

### 7.3 Training performance (patient-wise — no leakage)

| Dataset | Metric | Value |
|---|---|---|
| Synthetic (12 patients) | MAE / R² | 2.78 / 0.90 |
| **Real, Challenge 2012 (300 patients)** | **MAE / R²** | **1.62 / 0.963** |

Labels are **transparent physiology rules + small noise** (not diagnosis), and splits are **grouped by patient** so no patient's windows appear in both train and test.

### 7.4 Model comparison (objective 13) — 27 886 windows, 5-fold patient-wise CV

| Model | Precision | Recall | F1 | AUROC |
|---|---|---|---|---|
| Logistic Regression | 0.390 | 0.109 | 0.168 | 0.904 |
| Random Forest | 0.988 | 0.645 | 0.775 | 0.997 |
| **XGBoost (shipped)** | **0.993** | **0.797** | **0.882** | **0.997** |
| Isolation Forest* | — | — | — | 0.930 |

\* unsupervised — AUROC only. **Honest caveat:** the target is rule-derived, so AUROC ≈ 1.0 measures how well the model learns the rule — clinical validity is addressed separately by §8.4.

### 7.5 Explainability (objective 14) — SHAP

Global mean |SHAP| top features: **`hr_max` 3.80 · `temp_c_current` 1.99 · `dbp_min` 1.50 · `sbp_current` 1.33 · `sbp_min` 1.15 · `hr_current` 1.11** — matching clinical intuition and the trainer's gain ranking. Plots: `docs/screenshots/shap_global.png`, `shap_patient.png` (local explanation for the riskiest window).

## 8. Big-data measurements (objectives 4 & 17 + external validity)

### 8.1 MapReduce on YARN (objective 4)

`make mr` — hadoop-streaming job over **1 757 980 readings** (merged 4 000 tiny files → 1 map split), **15 s wall** (1 map + 1 reduce, 512 MB containers):

| param | readings | abnormal | % |
|---|---|---|---|
| NIDiasABP | 98 210 | 30 148 | 30.7 |
| DiasABP | 145 567 | 30 970 | 21.3 |
| SysABP | 145 650 | 13 580 | 9.3 |
| NISysABP | 98 331 | 8 081 | 8.2 |
| HR | 228 538 | 12 513 | 5.5 |
| Temp | 86 405 | 4 369 | 5.1 |
| RespRate | 55 051 | 2 351 | 4.3 |
| SaO₂ | 8 185 | 188 | 2.3 |

*(Engineering story: 4 000 per-patient files would be 4 000 JVM map tasks ≈ 75 min → merging small files first gave 15 s — a real HDFS small-files lesson.)*

### 8.2 Batch SQL (objectives 10/11)

`make batch` — same-scale descriptive/diagnostic numbers as §7.1–7.2, computed by Spark SQL directly on HDFS.

### 8.3 Benchmark (objective 17) — identical aggregation, two engines

| Engine | Wall time | Throughput | Data path |
|---|---|---|---|
| **Single-machine (pandas)** | **0.57 s** | 3 073 987 rows/s | local CSV |
| **Distributed (PySpark `local[*]`)** | **34.81 s** | 50 496 rows/s | HDFS |

Spark/pandas = **60.9×** at this scale — reported honestly: on one 8-core lab host, JVM/session + HDFS overhead dominate; the distributed win appears when data exceeds one machine (same code, multi-node, no rewrite). Metrics requested by the objective (processing time, throughput) are both captured; latency of the streaming path is the 30 s micro-batch trigger (tunable).

### 8.4 External validity — scores vs real ICU outcomes (killer slide)

`make validate` joins the **streamed** PostgreSQL scores to the dataset's real `Outcomes-a.txt` — the model never saw a death label:

| Metric | Value |
|---|---|
| **AUROC — in-hospital death** (per-patient mean risk) | **0.715** (0.733 mid-replay — stable) |
| Spearman ρ vs SOFA | **+0.337** |
| Spearman ρ vs SAPS-I | +0.322 |
| Level mix | dead: 25.5 % HIGH vs alive: 14.6 % HIGH |
| Max-risk aggregate (control) | AUROC 0.547 — proves mean is the honest summary |

### 8.5 End-to-end streaming verification

- Kafka topic `vitals.raw` (3 partitions, acks=all) — 28 285 messages replayed
- Spark Structured Streaming → PG: **28 285 scored windows / 300 patients**, LOW→CRITICAL mix
- Dashboard in `VYTALS_SOURCE=pg` serves the same rows (demo video above)

### 8.6 Big-data engineering at lab scale (single-node, honest)

Hadoop 3.4.3 single-node (NameNode+DataNode+YARN RM+NM), replication 1 — the objective is the APIs and the pipeline design; scaling out is config + more DataNodes, not code. Memory-sizings (512 MB task containers, 4 GB NM) documented in TECHNICAL §16.

## 9. Technology stack

| Category | Tools |
|---|---|
| Storage | Hadoop 3.4.3 · HDFS (single node) · JDK 17 |
| Batch | MapReduce (hadoop-streaming) · YARN |
| Processing / SQL | Apache Spark 3.5 · PySpark · Spark SQL |
| Streaming | Apache Kafka (Confluent 7.5, 3 partitions) · Spark Structured Streaming |
| Serving store | PostgreSQL 16 (port 5434) |
| ML / XAI | XGBoost · scikit-learn (LR, RF, IsolationForest) · SHAP |
| API / UI | FastAPI · Next.js 14 · Recharts · Tailwind |
| Data | PhysioNet/CinC Challenge 2012 (open access) |
| Tooling | Git/GitHub Actions CI · Make · Docker |

## 10. Repository map

```
Vytal/
├── Abstract.md               # course abstract (17 objectives)
├── README.md                 # quickstart + demo video
├── Makefile                  # install/sample/train/api/producer/stream/db/
│                             # hdfs-up/upload/mr/batch/compare/shap/bench/validate
├── requirements.txt
├── config/default.yaml       # kafka / streaming / postgres / model config
├── src/vytals/
│   ├── data/loader.py        # canonical schema, aliases, clamping
│   ├── features/windowed.py  # build_features() — shared train/stream
│   ├── ml/                   # risk_model, train, predict (XGBoost)
│   ├── ingest/producer.py    # CSV → Kafka
│   ├── streaming/job.py      # Spark Structured Streaming → PG
│   ├── store/                # db.py (DDL) · pg_reads.py (dashboard live mode)
│   ├── api/server.py         # FastAPI — replay | pg source switch
│   └── sim/replay.py         # zero-infra demo engine
├── scripts/                  # import, validate_outcomes, batch_analytics,
│   │                         # compare_models, shap_explain, benchmark
│   └── mr/                   # MapReduce mapper/reducer + run.sh
├── web/                      # Next.js dashboard (overview + patient pages)
├── docs/                     # this pack · TECHNICAL · FEATURES · INSTALLATION
│   ├── PROGRESS.md           # build log / verification log
│   ├── demo/demo.{mp4,gif}   # recorded live demo
│   └── screenshots/          # SHAP plots
└── .github/workflows/ci.yml  # backend smoke + frontend build on push
```

## 11. Limitations (say them before the examiner does)

1. **Labels are rule-derived** (transparent physiology thresholds + noise), not clinician annotation — chosen deliberately for explainability; external validity (§8.4) is the substitute check.
2. **Single-node HDFS/YARN** — lab machine (7.4 GB); demonstrates APIs and design, not cluster fault-tolerance at scale.
3. **Benchmark favours pandas at this dataset size** — honest result; distributed advantage needs data beyond one machine.
4. **SaO₂/RR sparse** in Challenge 2012 — real ICU charting behaviour; UI shows `—` instead of inventing values.
5. **Not a medical device** — academic prototype, no clinical certification or claims.
6. Streaming sink approximates slope/delta as 0 within a micro-batch (documented); feature code itself is shared with training to prevent drift.

## 12. Future scope (from the abstract)

Real medical-IoT integration · cloud deployment · edge preprocessing · federated/privacy-preserving learning · ECG-derived signals · patient-specific baselines · LSTM/Transformer sequence models · HIS integration · multi-ward scale · mobile/SMS alerts · formal clinical validation under ethical governance.

## 13. Likely viva questions — short answers

| Question | Answer |
|---|---|
| Why not MIMIC-III? | It's *credentialed* (PhysioNet account + CITI training + DUA); Challenge 2012 is open, real ICU, and includes outcomes — chosen for reproducible, legal automation (documented in README) |
| Why single-node HDFS? | The graded objective is the storage layer + pipeline integration; one NN/DN on the lab host demonstrates blocks, replication, `hdfs dfs` without cluster ops. Scaling out = config, not code |
| Why PostgreSQL instead of HBase? | Abstract allows "or another appropriate technology"; the access pattern is indexed point reads on (patient_id, window_end) — PK gives millisecond retrieval; a full HBase cluster on 7.4 GB would add ops cost for no demo gain (TECHNICAL §20) |
| How do you avoid data leakage? | Patient-wise splits everywhere (`GroupShuffleSplit`) — a patient's windows never cross train/test; validation vs outcomes is a separate, post-hoc join |
| Isn't AUROC 0.997 suspicious? | The *target* is a rule over the same features — that AUROC measures rule-learning. The honest number is death AUROC 0.715 with zero mortality supervision (§8.4) |
| Why does pandas beat Spark here? | 1.76 M rows fit in RAM; Spark's JVM/session + HDFS overhead dominate. The benchmark is reported as measured — that's the point of objective 17 |
| What handles late/missing data? | Event-time watermark 2 min; missing vitals stay missing (clamp, never fabricate), `coverage`/`low_quality` features flag thin windows |
| Exactly-once? | Checkpointed offsets + idempotent `ON CONFLICT DO NOTHING` upserts → effectively-once in PostgreSQL for replayable streams |

## 14. One-slide summary

> **Vytals**: real ICU data (PhysioNet, 4 000 records) → **HDFS** → **MapReduce/Spark SQL** batch analytics → **Kafka → Spark Structured Streaming → XGBoost → PostgreSQL** real-time scoring → **live dashboard** with **SHAP** explanations.
> **17/17 objectives done**; model reaches **death AUROC 0.715** without ever seeing mortality labels; **XGBoost F1 0.882** beats 3 alternatives under patient-wise CV; distributed-vs-single benchmark reported honestly (**60.9×** at lab scale).
