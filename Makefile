VENV ?= .venv
PY := $(VENV)/bin/python

install:
	python3 -m venv $(VENV)
	$(VENV)/bin/python -m ensurepip --upgrade >/dev/null 2>&1 || true
	$(VENV)/bin/python -m pip install -r requirements.txt
	npm --prefix web install

sample:
	$(PY) scripts/make_sample_data.py --patients 12 --minutes 180 --out data/vitals.csv

# Real data: PhysioNet/CinC Challenge 2012 (open access, ODC-BY) — 4000 ICU stays,
# real HR/SaO2/RespRate/Temp/BP time series + in-hospital mortality outcomes.
# (S3 mirror: physionet.org direct download truncates on some networks.)
data-real:
	mkdir -p data/physionet2012
	cd data/physionet2012 && \
	  curl -sL -O https://physionet-open.s3.amazonaws.com/challenge-2012/1.0.0/set-a.tar.gz && \
	  curl -sL -O https://physionet-open.s3.amazonaws.com/challenge-2012/1.0.0/Outcomes-a.txt && \
	  tar xzf set-a.tar.gz
	$(PY) scripts/import_physionet2012.py --dir data/physionet2012/set-a --out data/physionet2012.csv

# HLT-009 synthetic ICU sample (Hugging Face, CC-BY-NC-4.0): 25 episodes with
# per-row news2_score/qsofa_score labels + 54-column episode summary.
data-hlt009:
	mkdir -p data/hlt009
	cd data/hlt009 && for f in vitals_timeseries.csv episode_summary.csv alarm_events.csv interventions.csv; do \
	  curl -sfL -O "https://huggingface.co/datasets/xpertsystems/hlt009-sample/resolve/main/$$f"; done
	$(PY) scripts/import_hlt009.py --input data/hlt009/vitals_timeseries.csv --out data/hlt009.csv

train:
	PYTHONPATH=src $(PY) -m vytals.ml.train --input data/vitals.csv --out models/xgb_risk.json

train-real:
	PYTHONPATH=src $(PY) -m vytals.ml.train --input data/physionet2012.csv --out models/xgb_risk_real.json --window 6h

train-hlt009:
	$(PY) scripts/train_hlt009.py

predict:
	PYTHONPATH=src $(PY) -m vytals.ml.predict --input data/vitals.csv --model models/xgb_risk.json

# ---------- full stack (Kafka + Spark cluster + HDFS + Postgres) ----------
# Needs the Docker Compose v2 plugin (`docker compose`, not the legacy v1 binary).
up:
	docker compose up -d --build

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

# producer + streaming job (Spark driver stays up in its container)
pipeline:
	docker compose --profile pipeline up -d spark-submit producer

hdfs:
	docker compose up -d namenode datanode hdfs-init

hdfs-ls:
	docker compose exec namenode hdfs dfs -ls -R /vytals

submit:
	docker compose --profile pipeline run --rm spark-submit

demo:
	docker compose --profile pipeline run --rm producer

# ---------- dev fallback (no infra) ----------
api:
	PYTHONPATH=src $(PY) -m uvicorn vytals.api.server:app --reload --port 8000

producer:
	PYTHONPATH=src $(PY) -m vytals.ingest.producer --input data/vitals.csv --speed 60

stream:
	PYTHONPATH=src $(PY) -m vytals.streaming.job

db:
	PYTHONPATH=src $(PY) -c "from vytals.store.db import ensure; ensure(); print('tables ok')"

test:
	$(PY) -m unittest discover -s tests

web:
	npm --prefix web run dev

# legacy local infra (no HDFS/Spark); the full build is `make up`
infra:
	docker compose up -d kafka kafka-init postgres
