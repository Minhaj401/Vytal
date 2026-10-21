# Vytals — Runbook (copy-paste ready)

Paste-ready commands, in order. No explanations — those live in
[INSTALLATION](INSTALLATION.md) · [TECHNICAL](TECHNICAL.md) · [PROGRESS](PROGRESS.md).
Paths assume this repo on the lab machine; change `JAVA_HOME` if your JDK lives elsewhere.

## 0. Every session

```bash
cd ~/Projects/Vytal
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk     # macOS: $(/usr/libexec/java_home -v 17)
export HADOOP_CONF_DIR="$HOME/hadoop/conf"        # only for ad-hoc hdfs/yarn below
```

Fresh clone (one time):

```bash
git clone https://github.com/Minhaj401/Vytal.git && cd Vytal
make install
```

## 1. Quick demo — replay engine (no Docker, no Java, ~20 s)

```bash
make install sample train     # first time only: venv + synthetic data + model
```

```bash
make api                      # T1 → API  http://localhost:8000
make web                      # T2 → UI   http://localhost:3000
```

Open **http://localhost:3000**.

Same demo on real ICU data (one-time import + train):

```bash
make real-data train-real     # import 300 patients (~10 s) + train (~8 min)
```

```bash
VYTALS_DATA=data/vitals_real.csv VYTALS_MODEL=models/xgb_risk_real.json make api   # T1
make web                                                                            # T2
```

## 2. Full streaming demo — Kafka → Spark → Postgres → dashboard

Order matters: **stream (T2) before producer (T3)**. Each block = one terminal.

**T1 — infrastructure:**

```bash
docker network inspect vytal-net >/dev/null 2>&1 || docker network create vytal-net
docker start vytal-zk 2>/dev/null || docker run -d --name vytal-zk --network vytal-net -p 2181:2181 \
  -e ZOOKEEPER_CLIENT_PORT=2181 -e ZOOKEEPER_TICK_TIME=2000 confluentinc/cp-zookeeper:7.5.0
docker start vytal-kafka 2>/dev/null || docker run -d --name vytal-kafka --network vytal-net -p 9092:9092 --hostname kafka \
  -e KAFKA_BROKER_ID=1 -e KAFKA_ZOOKEEPER_CONNECT=vytal-zk:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true confluentinc/cp-kafka:7.5.0
docker start vytal-postgres 2>/dev/null || docker run -d --name vytal-postgres --restart unless-stopped -p 5434:5432 \
  -e POSTGRES_DB=vytals -e POSTGRES_USER=vytals -e POSTGRES_PASSWORD=vytals postgres:16
docker exec vytal-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic vitals.raw --partitions 3 --replication-factor 1
```

**T1 — tables + clean slate** (skip the `TRUNCATE`/`rm -rf` only if you want to keep existing scores):

```bash
make db
docker exec vytal-postgres psql -U vytals -d vytals -c "TRUNCATE risk_scores;"
rm -rf checkpoints        # fresh dedupe state — otherwise re-replayed events are silently dropped
```

**T2 — Spark stream** (wait for the `streaming localhost:9092/vitals.raw … -> postgres risk_scores` banner):

```bash
make stream
```

**T3 — feed data** (only after the T2 banner; `--speed 0` = max throughput, done in ~10 s):

```bash
PYTHONPATH=src .venv/bin/python -m vytals.ingest.producer --input data/vitals_real.csv --speed 0
# synthetic alternative: make producer
```

**T4 — API on the streamed scores:**

```bash
VYTALS_SOURCE=pg make api
curl -s localhost:8000/api/health    # "source":"pg" once scores land (polls every 5 s; "replay" meanwhile)
```

**T5 — dashboard:**

```bash
make web
```

Open **http://localhost:3000**. Expected: 300 patients, `n_scores` 28 285 after the full replay.

## 3. HDFS + batch / ML jobs

```bash
make hdfs-up          # NameNode + DataNode + YARN (first run formats; ~10 s)
make hdfs-upload      # push Challenge 2012 + CSVs into HDFS (few minutes, idempotent)
make hdfs-status      # daemons + capacity
```

```bash
make mr        # MapReduce on YARN — abnormal-event frequency table
make batch     # Spark SQL descriptive + diagnostic analytics over HDFS
make compare   # model CV table + confusion matrix → docs/screenshots/confusion_matrix.png
make shap      # SHAP global + local plots → docs/screenshots/
make bench     # pandas vs Spark timing over 1.76 M rows
make validate  # streamed scores vs real mortality (needs filled PG from §2)
```

Ad-hoc HDFS / YARN:

```bash
export HADOOP_CONF_DIR="$HOME/hadoop/conf"
hdfs dfs -ls -h /user/alvi/vytals
hdfs dfs -cat /user/alvi/vytals/mr/abnormal_freq/part-*    # MapReduce output
yarn application -list | head
```

## 4. Health checks (the numbers people ask for)

```bash
curl -s localhost:8000/api/health
curl -s localhost:8000/api/patients | head -c 300; echo
docker exec vytal-postgres psql -U vytals -d vytals -c "SELECT count(*) AS scores, count(DISTINCT patient_id) AS patients FROM risk_scores;"
curl -s -o /dev/null -w 'dashboard %{http_code}\n' localhost:3000
make hdfs-status
```

Expected after a full run: `scores 28285 · patients 300` · `dashboard 200` ·
`make validate` → death AUROC ≈ **0.715** · `make compare` → XGBoost F1 **0.882** (FP 2 / FN 56).

## 5. Restart after reboot

Postgres data survives in the `vytal_pgdata` volume; if `risk_scores` still has rows,
you can skip the producer.

```bash
docker start vytal-zk vytal-kafka vytal-postgres
make hdfs-up
make stream                         # T2 (producer first if PG was truncated)
VYTALS_SOURCE=pg make api           # T4
make web                            # T5
```

## 6. Stop everything

```bash
pkill -f '[v]ytals.streaming.job'        # stop stream
pkill -f '[u]vicorn vytals.api.server'   # stop API
pkill -f '[n]ext dev -p 3000'             # stop dashboard
bash scripts/hdfs.sh down                # stop NameNode/DataNode/YARN
docker stop vytal-kafka vytal-zk vytal-postgres
```

Full wipe:

```bash
docker rm vytal-kafka vytal-zk vytal-postgres
docker volume rm vytal_pgdata
rm -rf checkpoints .venv web/node_modules web/.next
```

## 7. If something breaks (fast list)

| Symptom | Fix |
|---|---|
| API stuck on `"source":"replay"` | stream not writing yet — check T2 for `[batch N] scored`; API self-heals within 5 s |
| PG empty after producer run | dedupe ate it: `rm -rf checkpoints` + `TRUNCATE risk_scores`, then stream **before** producer (§2 order) |
| Kafka connection refused | infra not up — §2 T1 |
| Port already in use | `ss -tlnp` and look for 3000 / 8000 / 5434 / 9092 |
| Spark job dies on Java | `java -version` must be 8 / 11 / **17**; re-export `JAVA_HOME` (§0) |
| `make mr` / YARN fails | `make hdfs-status` — daemons must be up first |

Full troubleshooting table: [INSTALLATION §9](INSTALLATION.md#9-troubleshooting).

---

Demo: [README](../README.md) · Presentation: [PRESENTATION.md](PRESENTATION.md)
