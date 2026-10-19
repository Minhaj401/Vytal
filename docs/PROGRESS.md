# Vytals — Build Progress

> Living status document. Update the tables/checklist as phases complete.
> Companion docs: [FEATURES.md](FEATURES.md) · [TECHNICAL.md](TECHNICAL.md) · [INSTALLATION.md](INSTALLATION.md)

**Last updated:** 2026-10-19
**Overall:** MVP ✅ shipped · Real data ✅ integrated · HDFS 🔜 in progress · Distributed batch/ML extras ⬜ pending

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
| 8 | Kafka producer | ✅ Code done | needs live broker to verify |
| 9 | Spark Structured Streaming job | ⚠️ Code done | e2e verified 2026-10-06 on author machine; checkpoint path hardcoded — must fix before rerun |
| 10 | Postgres schema + DDL | ✅ Done | port 5434, `make db` |
| 11 | CI (backend + frontend) | ✅ Done | GitHub Actions on push/PR |
| 12 | **Real data (PhysioNet Challenge 2012)** | ✅ Done | importer, `make real-data`, 300 patients, validated e2e (2026-10-19) |
| 13 | **Real-data model** | ✅ Done | `models/xgb_risk_real.json` — MAE 1.62 / R² 0.963 |
| 14 | **HDFS** | 🚧 In progress | Hadoop 3.4.3 present; single-node cluster + data upload next |
| 15 | Streaming e2e on real data (Kafka→Spark→PG) | ⬜ Pending | after HDFS + checkpoint fix + JDK 17 |
| 16 | Hive / MapReduce batch layer | ⬜ Pending | abstract objective #4 |
| 17 | Risk scores → HBase (or equivalent) | ⬜ Pending | abstract objective #15 |
| 18 | SHAP explainability | ⬜ Pending | abstract objective #14 |
| 19 | Multi-model comparison (RF/LR/IF…) | ⬜ Pending | abstract objective #13 |
| 20 | Outcome validation (SAPS/SOFA/death labels) | ⬜ Pending | data already in `data/physionet2012/Outcomes-a.txt` |
| 21 | Distributed vs single-machine benchmark | ⬜ Pending | abstract objective #17 |
| 22 | PG-backed live reads in dashboard | ⬜ Pending | currently replay-served |

## 2. Completed timeline

| Date | Milestone |
|---|---|
| 2026-10-06 | Abstract + architecture assets; **MVP shipped** (Kafka producer, Spark SS job, XGBoost risk, FastAPI, Next.js dashboard); Postgres port map fix (5434); GitHub Actions CI; Spark e2e fix (key col, `file://` checkpoint, PG timestamp cast) |
| 2026-10-07 | README (architecture, data source, model provenance, run guide); Makefile robustness (`python -m pip`, ensurepip fallback); nav cleanup |
| 2026-10-10 | Experiment: backend ported to Go (loader/features/GBM/API/producer/worker) |
| 2026-10-13 | Go port **reverted** — Python backend is canonical |
| 2026-10-19 | **Bug fix:** `/api/analysis` NaN → 500 (starlette `allow_nan=False`) — `json_safe()` guard; **bug fix:** `windowize` hang on 48 h sparse records (floor+groupby instead of Grouper); **real data:** Challenge 2012 downloaded (AWS mirror), importer written, `data/vitals_real.csv` (300 richest patients), `make train-real` → R² 0.963, e2e validation on real data all-green; docs set added |

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

## 4. Next up (ordered)

1. **HDFS single-node** (in progress): pick JDK (17 for Hadoop+Spark compat), format+start NameNode/DataNode, `hdfs dfs` basics, create project dirs, upload Challenge 2012 raw records as the "big data" source of truth.
2. **Checkpoint-path fix** in `streaming/job.py` (repo-relative, created at runtime).
3. **Streaming e2e on real data**: Kafka+PG via `docker run` → `make producer` (real CSV) → `make stream` → verify `risk_scores` rows → API/PG read path.
4. **Outcome validation**: correlate replay/PG risk scores vs `Outcomes-a.txt` (death/LOS/SAPS) — a real "does the score mean anything" check.
5. **Hive** external table over HDFS data + 2–3 descriptive SQL queries (abnormal-event frequency etc.).
6. **SHAP** on the trained booster (global feature importance + per-patient explanation).
7. **Model comparison**: LR / RF / XGBoost / IsolationForest with patient-wise CV table.
8. **Benchmark**: single-machine (pandas) vs distributed (Spark) on the same batch — timing table for the report.
9. **HBase** (or documented alternative) for risk-score serving; wire dashboard PG/live read path.

## 5. Known issues / blockers

| Severity | Item | Notes |
|---|---|---|
| 🔴 High | `streaming/job.py:126` checkpoint hardcoded `/home/minhaj/vytal/...` | must be fixed before any streaming rerun |
| 🔴 High | Host had Java 21 only — Spark 3.5 needs Java 8/11/17 | JDK 17 install planned this session (space freed) |
| 🟡 Medium | `docker-compose` v1 broken vs Docker 29 | documented `docker run` workaround (README/Makefile `infra`) |
| 🟡 Medium | Dashboard reads replay, not PG | PG read path exists but unwired |
| 🟡 Medium | Disk pressure on host (was 99% full) | HDFS + JDK need space — user freed some |
| 🟢 Low | First Spark run resolves jars from Maven (network) | one-time delay |
| 🟢 Low | `make train-real` ~8 min | one-time; windowize cost on 48 h sparse data |
| 🟢 Low | Untracked root `package-lock.json` | stray artifact from an npm run at repo root — candidate for deletion |

## 6. Verification log (last session)

| Check | Result |
|---|---|
| `py_compile` (touched modules) | ✅ |
| Synthetic train after `windowize` refactor | ✅ MAE 2.78 / R² 0.899 (unchanged) |
| Real import + loader validation | ✅ 28 285 rows, 300 patients, missing% sane |
| Real train | ✅ MAE 1.62 / R² 0.963 |
| Real replay init + step | ✅ 0.4 s, NaN → `None` |
| API on real data (6 endpoints incl. advance) | ✅ all 200, zero log errors |
| `/api/analysis` NaN regression (15× across loops) | ✅ all 200 after fix |
