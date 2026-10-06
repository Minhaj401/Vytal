VENV ?= .venv
PY := $(VENV)/bin/python

install:
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -r requirements.txt
	npm --prefix web install

sample:
	$(PY) scripts/make_sample_data.py --patients 12 --minutes 180 --out data/vitals.csv

train:
	PYTHONPATH=src $(PY) -m vytals.ml.train --input data/vitals.csv --out models/xgb_risk.json

predict:
	PYTHONPATH=src $(PY) -m vytals.ml.predict --input data/vitals.csv --model models/xgb_risk.json

api:
	PYTHONPATH=src $(VENV)/bin/uvicorn vytals.api.server:app --reload --port 8000

producer:
	PYTHONPATH=src $(PY) -m vytals.ingest.producer --input data/vitals.csv --speed 60

stream:
	PYTHONPATH=src $(PY) -m vytals.streaming.job

db:
	PYTHONPATH=src $(PY) -c "from vytals.store.db import ensure; ensure(); print('tables ok')"

web:
	npm --prefix web run dev

infra:
	docker compose up -d kafka postgres zookeeper
