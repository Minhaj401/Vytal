# Vytals — Build Progress

> Living status document. Update the tables/checklist as phases complete.
> Companion docs: [FEATURES.md](FEATURES.md) · [TECHNICAL.md](TECHNICAL.md) · [INSTALLATION.md](INSTALLATION.md)

**Last updated:** 2026-10-21
**Overall:** MVP ✅ shipped · Real data ✅ integrated · HDFS+YARN ✅ live · Streaming e2e ✅ verified · Outcome validation ✅ done · **all 17 abstract objectives ✅ completed** · dashboard PG live-read ✅ done

## 1. Status by area

| # | Area | Status | Notes |
|---|---|---|---|
| 1 | MVP demo (replay → risk → API → dashboard) | ✅ Done | works with zero infra |
| 2 | Synthetic data generator (archetypes) | ✅ Done | `make sample` |
| 3 | Canonical loader (aliases, clamping) | ✅ Done | CSV/JSON/JSONL |
| 4 | Shared feature engineering | ✅ Done | one impl for train/stream/replay |
| 5 | XGBoost risk model + fallback rules | ✅ Done | synthetic: MAE 2.78 / R² 0.90 |
| 6 | FastAPI backend + endpoints | ✅ Done | incl. NaN-JSON 500 fix (2026-10-19) |
| 7 | Next.js dashboard (overview + patient) | ✅ Done | Apple-HIG light theme |
| 8 | Kafka producer | ✅ Done | full replay verified 2026-10-21 (28 285 msgs, topic `vitals.raw` 3 partitions) |
| 9 | Spark Structured Streaming job | ✅ Done | e2e verified on real data 2026-10-21 (checkpoint repo-relative, psycopg2 sink) |
| 10 | Postgres schema + DDL | ✅ Done | port 5434, `make db` |
| 11 | CI (backend + frontend) | ✅ Done | GitHub Actions on push/PR |
| 12 | **Real data (PhysioNet Challenge 2012)** | ✅ Done | importer, `make real-data`, 300 patients, validated e2e (2026-10-19) |
| 13 | **Real-data model** | ✅ Done | `models/xgb_risk_real.json` — MAE 1.62 / R² 0.963 |
| 14 | **HDFS** | ✅ Done | single-node Hadoop 3.4.3 on JDK 17; Challenge 2012 (4000 records) + outcomes + CSVs uploaded; Spark reads HDFS verified |
| 15 | **Streaming e2e on real data (Kafka→Spark→PG)** | ✅ Done | verified 2026-10-21: 3-partition topic → Spark SS → XGBoost → PG `risk_scores` (LOW→CRITICAL rows growing per 30 s batch) |
| 16 | Hive / MapReduce batch layer | ✅ Done | objective #4 — `make mr`: streaming job on YARN, 1.76 M readings → abnormal-freq table in 15 s (TECHNICAL §16) |
| 17 | Risk scores → HBase (or equivalent) | ✅ Done | objective #15 — PostgreSQL `risk_scores` documented as the "appropriate technology" (PK patient_id+window_end, ms retrieval; TECHNICAL §20) |
| 18 | SHAP explainability | ✅ Done | objective #14 — `make shap`: global + local plots in `docs/screenshots/` (hr_max, temp, sbp lead) |
| 19 | Multi-model comparison (RF/LR/IF…) | ✅ Done | objective #13 — `make compare`: XGBoost F1 0.882 wins vs RF 0.775 / LR 0.168 / IF AUROC 0.930 |
| 20 | **Outcome validation (SAPS/SOFA/death labels)** | ✅ Done | `make validate` — death AUROC **0.733** (mid-replay) → **0.715** final (full replay), SOFA rho +0.337 |
| 21 | Distributed vs single-machine benchmark | ✅ Done | objective #17 — `make bench`: pandas 0.57 s vs Spark-on-HDFS 34.81 s (60.9×, honest overhead reading) |
| 22 | PG-backed live reads in dashboard | ✅ Done | `VYTALS_SOURCE=pg make api` — API serves streamed PG scores + as-of-joined vitals (`store/pg_reads.py`); replay stays default; auto-fallback if PG down |

## 2. Completed timeline

| Date | Milestone |
|---|---|
| 2026-10-06 | Abstract + architecture assets; **MVP shipped** (Kafka producer, Spark SS job, XGBoost risk, FastAPI, Next.js dashboard); Postgres port map fix (5434); GitHub Actions CI; Spark e2e fix (key col, `file://` checkpoint, PG timestamp cast) |
| 2026-10-07 | README (architecture, data source, model provenance, run guide); Makefile robustness (`python -m pip`, ensurepip fallback); nav cleanup |
| 2026-10-10 | Experiment: backend ported to Go (loader/features/GBM/API/producer/worker) |
| 2026-10-13 | Go port **reverted** — Python backend is canonical |
| 2026-10-19 | **Bug fix:** `/api/analysis` NaN → 500 (starlette `allow_nan=False`) — `json_safe()` guard; **bug fix:** `windowize` hang on 48 h sparse records (floor+groupby instead of Grouper); **real data:** Challenge 2012 downloaded (AWS mirror), importer written, `data/vitals_real.csv` (300 richest patients), `make train-real` → R² 0.963, e2e validation on real data all-green; docs set added |
| 2026-10-19 (pm) | **HDFS live:** JDK 17 installed (`jdk17-openjdk`), `JAVA_HOME` flipped in `~/.bashrc` + `~/hadoop/conf/hadoop-env.sh`; single-node cluster (NameNode+DataNode, Hadoop 3.4.3, replication 1) started on existing fsimage; **4000 Challenge 2012 records + Outcomes-a + set-a.zip + both pipeline CSVs uploaded** to `/user/alvi/vytals/{raw,csv}` (35.9 MB); Spark↔HDFS integration verified (PySpark reads HDFS, 28 285 rows / 300 patients match local); `scripts/hdfs.sh` + `make hdfs-up/down/status/upload` |
| 2026-10-19 (pm) | **streaming unblocked:** checkpoint path in `streaming/job.py` made repo-relative (`<repo>/checkpoints/vytals`, auto-created; `--checkpoint` flag + `VYTALS_CHECKPOINT` env); Kafka connector package version now derived from the installed pyspark (was hardcoded 3.4.1 vs runtime 3.5.9) |
| 2026-10-21 | **streaming e2e verified on real data:** infra via `docker run` (zk+kafka on `vytal-net`, PG :5434, topic `vitals.raw` 3 partitions), producer replaying `vitals_real.csv` @ ~10 msg/s, Spark job scoring 1.6–2k windows per 30 s batch → PG (`ON CONFLICT DO NOTHING`); fixed two pyspark-3.5-on-py3.14 killers along the way: `distutils` missing in `toPandas()` (→ `setuptools` in requirements) and `RecursionError` in `createDataFrame(pandas)`/`.rdd` (→ psycopg2 sink + JVM-side emptiness check); mid-session host reboot survived (containers `--restart unless-stopped`) |
| 2026-10-21 | **outcome validation:** `scripts/validate_outcomes.py` + `make validate` — PG risk scores vs real `Outcomes-a.txt`: **in-hospital-death AUROC 0.733** (per-patient mean risk), SOFA rho +0.351, SAPS-I +0.301, dead-vs-alive level mix separates (27.8% vs 13.8% HIGH); max-risk aggregate saturates (AUROC 0.547) — mean is the honest summary |
| 2026-10-21 (pm) | **batch/ML layer complete — all 17 objectives done:** YARN (RM+NM) added to `make hdfs-up` with lab-sized resources (4096 MB NM / 512 MB tasks — first attempt deadlocked at 1024 MB); **MapReduce** streaming job on real records (merge 4000 tiny files → 1 map, **15 s**, abnormal-freq table for 8 vitals); **Spark SQL** descriptive+diagnostic batch (`make batch` — distributions, abnormal rates, correlations, SOFA join); **model comparison** (`make compare` — XGBoost F1 0.882 > RF > LR, IF AUROC 0.930); **SHAP** (`make shap` — global+local plots); **benchmark** (`make bench` — pandas 0.57 s vs Spark 34.81 s, 60.9×); final `make validate` after full replay: death AUROC **0.715**, SOFA +0.337 |

## 3. Recent session detail (2026-10-19)

- ✅ Fixed terminal 500s: `ValueError: Out of range float values ... nan` on `/api/analysis` (missing vitals → pandas NaN → `to_dict(records)` → starlette rejects). Guard in `api/server.py`.
- ✅ Real-data assessment: MIMIC-III is credentialed → chose **PhysioNet Challenge 2012** (open, real ICU, all 6 vitals via SaO₂ for O₂-sat, real outcomes).
- ✅ Downloaded `set-a.zip` + `set-b.zip` (8 MB each, AWS `physionet-open` mirror; physionet.org throttled at ~2 KB/s) + `Outcomes-a.txt`.
- ✅ `scripts/import_physionet2012.py`: long-format pivot, SaO₂→spo2, NI→invasive BP fallback, richest-first selection → `data/vitals_real.csv` (28 285 rows, 300 patients).
- ✅ `windowize` fixed (see TECHNICAL §11.7) — real training completes in ~8 min.
- ✅ Trained `models/xgb_risk_real.json` (MAE 1.62, R² 0.963 patient-wise).
- ✅ E2E smoke on real data: loader → predict (CRITICAL 77.2 example) → API (:8001 throwaway) all endpoints 200, no log errors → killed test server; user's :8000 untouched.
- ✅ Makefile targets `real-data` / `train-real`; CI compile list includes importer; gitignore for raw+derived real data; README real-data section.
- ✅ Docs: FEATURES / TECHNICAL / PROGRESS / INSTALLATION.

## 4. Recent session detail (2026-10-19 pm) — HDFS

- ✅ Installed `jdk17-openjdk` (17.0.20.1); switched `JAVA_HOME` from Java 21 → 17 in `~/.bashrc:27` and `~/hadoop/conf/hadoop-env.sh:435` (Spark 3.5 + Hadoop 3.4 both need 8/11/17).
- ✅ Started the existing single-node cluster (fsimage from 2026-10-06 preserved — no reformat needed): NameNode + DataNode on Java 17, `dfsadmin -report` → 1 live datanode, safe mode OFF.
- ✅ Uploaded the big-data source of truth: `set-a/` (4000 ICU records, parallel `put -t 8` in background — the 5-min tool timeout killed the foreground attempt), `Outcomes-a.txt`, `set-a.zip`, `vitals.csv`, `vitals_real.csv`.
- ✅ Spark↔HDFS integration check: PySpark 3.5.9 `local[2]` session → `hdfs://localhost:9000` reads a raw record (273 rows) and `vitals_real.csv` (28 285 rows / 300 patients — identical to local).
- ✅ `scripts/hdfs.sh` (up/down/status/upload/tree, self-picking JAVA_HOME, format-once, stale-pid cleanup) + Makefile targets `hdfs-up/down/status/upload`; TECHNICAL §14.

## 5. Recent session detail (2026-10-19 pm) — checkpoint fix

- ✅ `streaming/job.py`: dropped the hardcoded `file:///home/minhaj/vytal/checkpoints/vytals` — checkpoint is now `<repo>/checkpoints/vytals` via `default_checkpoint()` (repo-root resolved from `__file__`, dir created at runtime, `checkpoints/` already gitignored), overridable with `--checkpoint DIR` or `VYTALS_CHECKPOINT`.
- ✅ `build_spark()`: `spark-sql-kafka-0-10_2.12` package version now follows the installed pyspark (3.5.9 here) instead of the hardcoded 3.4.1 — connector/runtime mismatch would have failed the e2e run.
- ✅ Verified: `py_compile`, `default_checkpoint()` → `file:///home/alvi/Projects/Vytal/checkpoints/vytals` (dir created), env override + CLI flag work, `--help` clean, no `/home/minhaj` left in `src/`.

## 6. Session detail (2026-10-21) — streaming e2e

- ✅ Infra: pulled `cp-zookeeper`/`cp-kafka` 7.5.0 + `postgres:16` (~3 GB, slow mirror); **fixed first-run crash** — zookeeper wasn't attached to `vytal-net` so `KAFKA_ZOOKEEPER_CONNECT=vytal-zk:2181` didn't resolve; recreated both on the network; topic `vitals.raw` created explicitly (3 partitions, auto-create alone wasn't enough for the Spark source).
- ✅ Bug 1: `toPandas()` died with `ModuleNotFoundError: No module named 'distutils'` (removed in py3.12+; venv is py3.14; pyspark 3.5.9 still imports it) → `setuptools` added to `requirements.txt` (shims distutils).
- ✅ Bug 2: PG sink died with `RecursionError: Stack overflow` — `spark.createDataFrame(pandas_df)` and `batch_df.rdd.isEmpty()` both take the Python-RDD pickle path on pyspark 3.5 + py3.14 → replaced with JVM-side `limit(1).count()` emptiness check and a **pandas → psycopg2** `INSERT … ON CONFLICT DO NOTHING` sink (reuses `risk_scores` PK for replay idempotency).
- ✅ Host rebooted mid-session — containers were down; `docker start` + `--restart unless-stopped` (data in container layers survived), clean restart of stream + producer.
- ✅ **E2E result (real data, XGBoost real model):** batches score 1663–2073 windows each; PG `risk_scores` grows continuously — e.g. 1903 rows / 240 patients / max risk 86.5 / levels LOW 387, MODERATE 1149, HIGH 354, CRITICAL 13 after ~2 min of replay.
- ✅ **Outcome validation** (while replay ran): `scripts/validate_outcomes.py` joins PG scores to `Outcomes-a.txt` — **death AUROC 0.733** on per-patient mean risk, SOFA rho +0.351, SAPS-I +0.301, dead patients 2× as likely to be HIGH / ⅓ as likely LOW; max-risk AUROC 0.547 (ceiling — mean is the default `--agg`).
- ✅ **MapReduce on YARN (obj 4):** brought up ResourceManager + NodeManager (`scripts/hdfs.sh up` now manages them); `mapred-site.xml` got absolute application classpaths (fixed `ClassNotFoundException: MRAppMaster`); NM sized 4096 MB/6 vcores with 512 MB tasks (1024 MB NM deadlocked AM+map); 4000 per-patient files → 4000 tiny map tasks (~75 min projected) → **merged to one 26 MB file → 1 mapper, job done in 15 s** (`scripts/mr/run.sh`, `make mr`). Abnormal-freq table: NIDiasABP 30.7 %, DiasABP 21.3 %, HR 5.5 %…
- ✅ **Spark SQL batch (obj 10/11):** `scripts/batch_analytics.py` (`make batch`) over HDFS — descriptive distributions + abnormal rates (same thresholds as MR), Pearson correlations (sbp–dbp +0.524), co-occurrence, patient-level join vs real outcomes (derangement rates track SOFA ρ up to +0.157).
- ✅ **Model comparison (obj 13):** `make compare` — patient-wise 5-fold CV on 27 886 windows: XGBoost P 0.993 / R 0.797 / F1 **0.882** / AUROC 0.997 beats RF (F1 0.775), LR (F1 0.168); IsolationForest unsupervised AUROC 0.930.
- ✅ **SHAP (obj 14):** `make shap` — TreeExplainer global plot + riskiest-window local plot (`docs/screenshots/shap_{global,patient}.png`); top |SHAP|: `hr_max` 3.80, `temp_c_current` 1.99, `dbp_min` 1.50, `sbp_current` 1.33.
- ✅ **Benchmark (obj 17):** `make bench` — identical group-by over 1 757 980 readings: pandas 0.57 s (3.07 M rows/s) vs PySpark on HDFS 34.81 s (50 k rows/s) = **60.9×**; documented honestly (JVM+HDFS overhead dominates at this scale; distributed win needs multi-node data).
- ✅ **Obj 15 decision:** PostgreSQL `risk_scores` documented as the HBase-equivalent serving store (TECHNICAL §20) — PK (patient_id, window_end) gives ms point retrieval the dashboard needs; no extra cluster on a 7.4 GB box.
- ✅ **Dashboard PG live-read (row 22):** `store/pg_reads.py:PgLive` + `VYTALS_SOURCE=pg` mode in `api/server.py` — risk from PG `risk_scores` (3 s TTL, merge_asof refresh), vitals as-of-joined from the replayed CSV; same response shapes so the frontend needed zero changes; `/advance` → 409 in pg mode; auto-fallback to replay if PG is down; verified both modes end-to-end (300 patients / 28 270 scores / no NaN leaks; replay regression green).
- ✅ **pg-mode robustness:** API now retries every 5 s until `risk_scores` has rows (start order no longer matters) and flips `source: replay → pg` automatically — added while staging the demo when an empty table at boot stranded the API in replay.
- 🔧 **Dedupe-vs-replay gotcha found:** re-producing the same CSV after a prior stream run drops every message (checkpoint `dropDuplicates` state) — fixed for the demo with a fresh `VYTALS_CHECKPOINT`; documented in INSTALLATION §4 + TECHNICAL §7/§13 + troubleshooting.
- ✅ **Demo video recorded (2026-10-21 pm):** live pipeline re-run (fresh checkpoint → producer 28 285 msgs → stream → PG full) with the dashboard in `VYTALS_SOURCE=pg`; captured 100 s via Chromium CDP screencast (puppeteer-core), encoded → `docs/demo/demo.mp4` (1.0 MB) + `demo.gif` (2.1 MB) embedded in README; frames verified (overview 300 patients, patient drill-down with charts + stream log).
- ✅ **Docs pass:** README (demo embed + batch section), FEATURES ("coming next" → done list), INSTALLATION (pg mode, YARN, new make targets, env vars, troubleshooting), TECHNICAL (§6 live mode, §13 resolved rows, dedupe caveat), PROGRESS.

## 7. Next up (ordered)

All **17 abstract objectives**, the PG live-read, the demo video, and the docs pass are complete.

1. **`docs/PRESENTATION.md`** — full college-presentation write-up (architecture, workflow, user flow, accuracy tables, benchmarks, objectives↔results).
2. **Push** — local commits to `origin/main` (GitHub: Minhaj401/Vytal).
3. Final wrap: restart HDFS/YARN, viva demo rehearsal.

## 8. Known issues / blockers

| Severity | Item | Notes |
|---|---|---|
| 🟡 Medium | `docker-compose` v1 broken vs Docker 29 | documented `docker run` workaround (README/Makefile `infra`) |
| 🟡 Medium | Dashboard reads replay, not PG | PG read path exists but unwired |
| 🟡 Medium | Disk pressure on host (was 99% full) | ~17 GB free now — Docker images (kafka/zk/pg ≈ 3 GB) + Spark jars + JDK 17 ate into it |
| 🟡 Medium | Foreground `hdfs dfs -put` of 4000 files died mid-run ("Filesystem closed") | worked as background job with `-t 8`; treat bulk uploads as long-running jobs |
| 🟢 Low | First Spark run resolves jars from Maven (network) | one-time delay |
| 🟢 Low | `make train-real` ~8 min | one-time; windowize cost on 48 h sparse data |
| 🟢 Low | Untracked root `package-lock.json` | stray artifact from an npm run at repo root — candidate for deletion |
| 🟢 Low | Kafka/zk containers stopped to free RAM after replay | `docker start vytal-zk vytal-kafka` before any new stream demo |
| ✅ Resolved | YARN AM+map deadlock (NM 1024 MB < 256 AM + 1024 map) | NM 4096 MB/6 cores, 512 MB task containers (2026-10-21) |
| ✅ Resolved | `MRAppMaster` ClassNotFoundException | absolute `mapreduce.application.classpath` in `mapred-site.xml` |
| ✅ Resolved | MapReduce 4000 tiny-file splits ≈ 75 min | merged single 26 MB input file → 1 mapper, 15 s (`scripts/mr/run.sh`) |
| ✅ Resolved | Host had Java 21 only — Spark 3.5 needs 8/11/17 | JDK 17 installed 2026-10-19, `JAVA_HOME` pinned in `.bashrc` + `hadoop-env.sh` |
| ✅ Resolved | `streaming/job.py` checkpoint hardcoded `/home/minhaj/...` | repo-relative + created at runtime (2026-10-19) |
| ✅ Resolved | Kafka package 3.4.1 vs pyspark 3.5.9 | version now derived from installed pyspark (2026-10-19) |

## 9. Verification log

| Check | Result |
|---|---|
| `py_compile` (touched modules) | ✅ |
| Synthetic train after `windowize` refactor | ✅ MAE 2.78 / R² 0.899 (unchanged) |
| Real import + loader validation | ✅ 28 285 rows, 300 patients, missing% sane |
| Real train | ✅ MAE 1.62 / R² 0.963 |
| Real replay init + step | ✅ 0.4 s, NaN → `None` |
| API on real data (6 endpoints incl. advance) | ✅ all 200, zero log errors |
| `/api/analysis` NaN regression (15× across loops) | ✅ all 200 after fix |
| JDK 17 install + JAVA_HOME flip | ✅ `java -version` → 17.0.20.1; daemons on `java-17-openjdk` |
| HDFS cluster up | ✅ 1 live datanode, safe mode OFF, 28.4 GB remaining |
| HDFS data upload | ✅ 4000/4000 records + outcomes + zip + 2 CSVs = 35.9 MB |
| `hdfs dfs -cat` raw record | ✅ chart lines returned |
| Spark read from HDFS | ✅ PySpark 3.5.9: raw record 273 rows; `vitals_real.csv` 28 285 rows / 300 patients (matches local) |
| `make hdfs-up` / `hdfs-status` / `tree` | ✅ idempotent (detected running daemons) |
| `streaming/job.py` py_compile + `--help` | ✅ |
| `default_checkpoint()` / `VYTALS_CHECKPOINT` / `--checkpoint` | ✅ creates + returns `file:///…/checkpoints/vytals`; env override works |
| `/home/minhaj` remnants in `src/` | ✅ none |
| Infra up (zk+kafka on vytal-net, PG :5434) | ✅ zookeeper network attachment fixed; topic `vitals.raw` 3 partitions |
| `toPandas()` distutils crash (py3.14) | ✅ fixed — setuptools shim in requirements |
| `createDataFrame(pandas)` RecursionError (py3.14) | ✅ fixed — psycopg2 sink |
| **E2E: producer → Kafka → Spark → XGBoost → PG (real data)** | ✅ batches score 1663–2073 windows; PG 1903 rows / 240 patients / max 86.5 / LOW→CRITICAL mix, growing per 30 s trigger |
| Host reboot resilience | ✅ containers `--restart unless-stopped`, clean resume |
| `make validate` vs Outcomes-a (300 pts, mid-replay) | ✅ death AUROC 0.733 (mean) / 0.547 (max); SOFA +0.351, SAPS-I +0.301; level mix separates |
| `make validate` after **full** replay (final) | ✅ death AUROC **0.715**; SOFA +0.337, SAPS-I +0.322, LOS +0.109; dead 25.5% HIGH vs alive 14.6% |
| `make mr` — MapReduce on YARN | ✅ 1 map + 1 reduce in 15 s → 8-row abnormal-freq table (NIDiasABP 30.7 % … SaO₂ 2.3 %) |
| `make batch` — Spark SQL over HDFS | ✅ descriptive + diagnostic tables printed (28 285 rows, correlations, SOFA join) |
| `make compare` — model comparison | ✅ XGBoost F1 0.882 / AUROC 0.997 > RF > LR; IsolationForest AUROC 0.930 (patient-wise CV) |
| `make shap` — SHAP explanations | ✅ `docs/screenshots/shap_global.png` + `shap_patient.png`; hr_max/temp/sbp lead |
| `make bench` — pandas vs Spark | ✅ 0.57 s (3.07 M rows/s) vs 34.81 s (50 k rows/s), same 1.76 M rows, 42 groups |
| `VYTALS_SOURCE=pg` API mode | ✅ health `{source:pg, n_scores:28270}`, patients 300, analysis counts, history 127 pts (risk as-of-joined), live LivePoint shape, 404/409 correct, no NaN |
| replay mode regression (default) | ✅ health/source=replay, 12 patients, analysis + history + live unchanged |
