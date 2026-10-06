# Vytals MVP — Real-Time Vital Risk Scoring

`Patient Vitals → Kafka Producer → vitals.raw → Spark Streaming → validate → window → build_features() → XGBoost → Risk 0–100 → PostgreSQL → Next.js`

## Quickstart (demo without Kafka)

```bash
make install sample train
make api          # :8000  (live replay engine)
npm --prefix web install; npm --prefix web run dev  # :3000
```

Open http://localhost:3000 → dashboard analysis + click any patient → `/patients/P001` live stream.

## Full streaming (with Kafka + Spark)

```bash
docker compose up -d zookeeper kafka postgres
# create topic
docker exec $(docker ps -qf "name=kafka") kafka-topics --create --topic vitals.raw --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 || true
PYTHONPATH=src python -m vytals.data.loader --input data/vitals.csv
PYTHONPATH=src python -m vytals.ingest.producer --input data/vitals.csv --speed 60 --topic vitals.raw
PYTHONPATH=src python -m vytals.streaming.job   # spark reads kafka, writes risk_scores to postgres
```

## Layout

```
src/vytals/data/loader.py      CSV/JSONL -> canonical schema
src/vytals/ingest/producer.py  Kafka replay (key=patient_id, acks=all, --speed)
src/vytals/streaming/job.py    Spark readStream kafka, watermark 2m, window 5m/1m
src/vytals/features/windowed.py  build_features(df) — shared train+stream
src/vytals/ml/{train,predict,risk_model}.py  XGBoost -> P(risk) -> 0..100 + level
src/vytals/store/db.py         Postgres risk_scores
src/vytals/api/server.py       FastAPI for Next.js (replay fallback when Kafka down)
src/vytals/sim/replay.py       in-memory replay engine
web/                           Next.js dashboard + /patients/[id] live routes
```

## Config

`config/default.yaml` — kafka, streaming watermark/window/slide, postgres, model path.
