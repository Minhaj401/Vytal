VENV ?= .venv
PY := $(VENV)/bin/python

install:
	python3 -m venv $(VENV)
	$(VENV)/bin/python -m ensurepip --upgrade >/dev/null 2>&1 || true
	$(VENV)/bin/python -m pip install -r requirements.txt
	npm --prefix web install

sample:
	$(PY) scripts/make_sample_data.py --patients 12 --minutes 180 --out data/vitals.csv

train:
	PYTHONPATH=src $(PY) -m vytals.ml.train --input data/vitals.csv --out models/xgb_risk.json

predict:
	PYTHONPATH=src $(PY) -m vytals.ml.predict --input data/vitals.csv --model models/xgb_risk.json

api:
	PYTHONPATH=src $(PY) -m uvicorn vytals.api.server:app --reload --port 8000

producer:
	PYTHONPATH=src $(PY) -m vytals.ingest.producer --input data/vitals.csv --speed 60

stream:
	PYTHONPATH=src $(PY) -m vytals.streaming.job

db:
	PYTHONPATH=src $(PY) -c "from vytals.store.db import ensure; ensure(); print('tables ok')"

web:
	npm --prefix web run dev

infra:
	# legacy docker-compose 1.29 is broken vs Docker 29 (KeyError ContainerConfig),
	# and 5432/5433 are taken on this host — run pg directly on 5434
	docker run -d --name vytal-postgres --restart unless-stopped \
		-e POSTGRES_DB=vytals -e POSTGRES_USER=vytals -e POSTGRES_PASSWORD=vytals \
		-p 5434:5432 -v vytal_pgdata:/var/lib/postgresql/data postgres:16
