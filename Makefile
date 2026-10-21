VENV ?= .venv
PY := $(VENV)/bin/python

install:
	python3 -m venv $(VENV)
	$(VENV)/bin/python -m ensurepip --upgrade >/dev/null 2>&1 || true
	$(VENV)/bin/python -m pip install -r requirements.txt
	npm --prefix web install

sample:
	$(PY) scripts/make_sample_data.py --patients 12 --minutes 180 --out data/vitals.csv

real-data:
	PYTHONPATH=src $(PY) scripts/import_physionet2012.py --limit 300 --out data/vitals_real.csv

train:
	PYTHONPATH=src $(PY) -m vytals.ml.train --input data/vitals.csv --out models/xgb_risk.json

train-real:
	PYTHONPATH=src $(PY) -m vytals.ml.train --input data/vitals_real.csv --out models/xgb_risk_real.json

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

# --- HDFS (single-node, JDK 17, conf in ~/hadoop/conf) ---
hdfs-up:
	bash scripts/hdfs.sh up

hdfs-down:
	bash scripts/hdfs.sh down

hdfs-status:
	bash scripts/hdfs.sh status

hdfs-upload:
	bash scripts/hdfs.sh upload

validate:
	PYTHONPATH=src $(PY) scripts/validate_outcomes.py

validate-max:
	PYTHONPATH=src $(PY) scripts/validate_outcomes.py --agg max

# --- batch analytics layer (needs: make hdfs-up && make hdfs-upload) ---
# objective 4 — MapReduce on YARN (abnormal-event frequency over raw records)
mr:
	bash scripts/mr/run.sh

# objectives 10/11 — descriptive + diagnostic Spark SQL over HDFS
batch:
	PYTHONPATH=src JAVA_HOME=$${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk} $(PY) scripts/batch_analytics.py

# objective 13 — LR / RF / XGBoost / IsolationForest patient-wise CV table
# (+ out-of-fold confusion matrix for the shipped model -> docs/screenshots/)
compare:
	PYTHONPATH=src MPLCONFIGDIR=/tmp/mpl $(PY) scripts/compare_models.py

# objective 14 — SHAP global + local explanations -> docs/screenshots/
shap:
	PYTHONPATH=src MPLCONFIGDIR=/tmp/mpl $(PY) scripts/shap_explain.py

# objective 17 — single-machine (pandas) vs distributed (Spark on HDFS) timing
bench:
	PYTHONPATH=src JAVA_HOME=$${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk} $(PY) scripts/benchmark.py
