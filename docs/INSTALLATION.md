# Vytals — Installation & Running (Linux / macOS)

> **Platforms:** Linux and macOS only. Windows is not supported (if you must, use a Linux VM or WSL2 — untested).
> Companion docs: [FEATURES.md](FEATURES.md) · [TECHNICAL.md](TECHNICAL.md) · [PROGRESS.md](PROGRESS.md)

## 1. Prerequisites

### Required (demo mode)

| Tool | Version | Check | Install hint |
|---|---|---|---|
| Python | 3.10+ (3.12–3.14 OK) | `python3 --version` | apt: `sudo apt install python3 python3-venv` · brew: `brew install python` |
| Node.js | 18.17+ (20/22/26 OK) | `node --version` | https://nodejs.org or `nvm install 22` |
| make | any | `make --version` | apt: `build-essential` · preinstalled on macOS (Xcode CLT) |
| git, curl, unzip | any | `git --version` | preinstalled on macOS |

No Java, no Docker, no database needed for demo mode.

### Only for full streaming mode (Kafka + Spark + Postgres)

| Tool | Version | Why |
|---|---|---|
| **JDK** | **17** (8/11 also work; **not 21+**) | Spark 3.5 & Hadoop run on 8/11/17 only |
| Docker | 20+ (compose plugin or plain `docker run`) | Kafka / Postgres containers |
| Disk | ~2 GB free | jars, containers, HDFS data |

```bash
# JDK 17 examples
sudo apt install openjdk-17-jdk-headless        # Debian/Ubuntu
sudo apt install openjdk-17-jdk                 # some distros
brew install --cask temurin@17                  # macOS
java -version                                   # must show 17 (or 8/11)
```

If several JDKs are installed: `sudo update-alternatives --config java` (Linux) or `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` (macOS).

## 2. Get the code + install dependencies

```bash
git clone <repo-url> Vytal && cd Vytal
make install        # creates .venv, pip installs requirements.txt, npm install in web/
```

`make install` is idempotent — re-run any time.

## 3. Demo mode — run in under a minute

```bash
make install sample train     # venv + synthetic data + model
make api                      # API  → http://localhost:8000
npm --prefix web run dev      # UI   → http://localhost:3000   (new terminal)
```

Open **http://localhost:3000**. The dashboard replays patient data live (2 s ticks) through the in-process engine — no other services required.

### Same demo on real ICU data

```bash
# one-time download (8 MB; AWS mirror — physionet.org itself throttles hard)
mkdir -p data/physionet2012
curl -L -o data/physionet2012/set-a.zip \
  https://physionet-open.s3.amazonaws.com/challenge-2012/1.0.0/set-a.zip
unzip -q -d data/physionet2012 data/physionet2012/set-a.zip    # creates data/physionet2012/set-a/

make real-data                # import 300 richest patients → data/vitals_real.csv  (~10 s)
make train-real               # train real model → models/xgb_risk_real.json  (~8 min, one-time)

VYTALS_DATA=data/vitals_real.csv VYTALS_MODEL=models/xgb_risk_real.json make api
npm --prefix web run dev      # if not already running
```

## 4. Full streaming mode (Kafka + Spark + Postgres)

Prerequisites: JDK 17 on PATH, Docker running. Ports used: **9092** Kafka, **5434** Postgres
(host 5432/5433 often belong to other projects — keep 5434).

```bash
# 1) infrastructure (docker run — this host's legacy compose v1 is broken vs Docker 29)
docker run -d --name vytal-zk -p 2181:2181 \
  -e ZOOKEEPER_CLIENT_PORT=2181 -e ZOOKEEPER_TICK_TIME=2000 confluentinc/cp-zookeeper:7.5.0
docker network create vytal-net 2>/dev/null
docker run -d --name vytal-kafka --network vytal-net -p 9092:9092 --hostname kafka \
  -e KAFKA_BROKER_ID=1 -e KAFKA_ZOOKEEPER_CONNECT=vytal-zk:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true confluentinc/cp-kafka:7.5.0
docker run -d --name vytal-postgres --restart unless-stopped -p 5434:5432 \
  -e POSTGRES_DB=vytals -e POSTGRES_USER=vytals -e POSTGRES_PASSWORD=vytals postgres:16

# 2) tables
make db

# 3) feed data into Kafka (synthetic or swap --input data/vitals_real.csv)
make producer                   # CSV → vitals.raw @ 60×

# 4) Spark streaming → risk_scores in Postgres   (first run downloads jars — needs network)
#    NOTE: set the checkpoint dir in src/vytals/streaming/job.py to a writable path first
make stream

# 5) API + dashboard, now backed by Postgres
make api
npm --prefix web run dev
```

**Stop / clean up containers:**

```bash
docker stop vytal-kafka vytal-zk vytal-postgres
docker rm   vytal-kafka vytal-zk vytal-postgres
# data persists in the vytal_pgdata volume until: docker volume rm vytal_pgdata
```

## 5. Make targets

| Target | What it does |
|---|---|
| `make install` | venv + Python deps + web deps |
| `make sample` | synthetic `data/vitals.csv` (12 patients, 180 min) |
| `make real-data` | import Challenge 2012 → `data/vitals_real.csv` (needs raw files, §3) |
| `make train` | train model on synthetic → `models/xgb_risk.json` |
| `make train-real` | train model on real data → `models/xgb_risk_real.json` |
| `make predict` | batch inference on `data/vitals.csv` (prints top risks) |
| `make api` | FastAPI with reload → :8000 |
| `make web` | Next.js dev → :3000 |
| `make producer` | replay CSV into Kafka @ 60× |
| `make stream` | Spark Structured Streaming job |
| `make db` | create Postgres tables (port 5434) |
| `make infra` | start Postgres container only |

Run any Python module directly for options, e.g. `PYTHONPATH=src .venv/bin/python scripts/import_physionet2012.py --help`.

## 6. Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `VYTALS_DATA` | `data/vitals.csv` | CSV the replay engine serves |
| `VYTALS_MODEL` | `models/xgb_risk.json` | model artifact loaded at startup |
| `VYTALS_CONFIG` | `config/default.yaml` | config file path |
| `NEXT_PUBLIC_API` | same-origin | API base URL for the frontend (cross-origin setups) |

## 7. Ports

| Port | Service |
|---|---|
| 3000 | Next.js dev server |
| 8000 | FastAPI |
| 5434 | Vytals Postgres |
| 9092 | Kafka |
| 2181 | Zookeeper |

`Address already in use` → something already listens there: `ss -tlnp | grep 8000` (find PID), or run the API on another port:
`PYTHONPATH=src .venv/bin/python -m uvicorn vytals.api.server:app --port 8010`

## 8. Troubleshooting

| Symptom | Fix |
|---|---|
| `Address already in use` on `make api` | old server still running: `pkill -f 'uvicorn vytals.api.server'` (or reuse it — it has `--reload`) |
| `/api/health` says no CSV | `make sample` (or `make real-data` + `VYTALS_DATA=...`) |
| `cannot reach Kafka at localhost:9092` | start infra (§4) first; producer fails fast by design |
| Spark fails immediately mentioning Java | wrong JDK: need 8/11/17, set `JAVA_HOME`, `java -version` to confirm |
| Spark job fails on checkpoint / permission | set `checkpointLocation` in `streaming/job.py` to a writable absolute dir |
| `make db` connection refused | Postgres container not up, or wrong port (must be 5434) |
| `docker compose` KeyError / ContainerConfig | legacy compose v1 vs Docker 29 — use `docker run` (§4) |
| `make train-real` seems stuck (~8 min) | normal — windowizing 48 h sparse records; model log prints when training starts |
| Download from physionet.org crawls | use the AWS mirror URL from §3 |
| Low disk (`df -h`) | HDFS/JDK/jars need ~2 GB; `docker system prune` if Docker is bloated |
| Dashboard shows `—` for vitals | missing readings are handled by design (never fabricated) — see FEATURES |

## 9. Uninstall

```bash
rm -rf .venv web/node_modules web/.next checkpoints
docker stop vytal-kafka vytal-zk vytal-postgres && docker rm vytal-kafka vytal-zk vytal-postgres
# HDFS (once set up): stop daemons + rm the data dir per Hadoop docs
```
