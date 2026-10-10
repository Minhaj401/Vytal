GOBIN ?= bin
GO := go

build:
	mkdir -p $(GOBIN)
	$(GO) build -o $(GOBIN)/vytal-server   ./cmd/vytal-server
	$(GO) build -o $(GOBIN)/vytal-producer ./cmd/vytal-producer
	$(GO) build -o $(GOBIN)/vytal-stream   ./cmd/vytal-stream
	$(GO) build -o $(GOBIN)/vytal-loader   ./cmd/vytal-loader
	$(GO) build -o $(GOBIN)/vytal-train    ./cmd/vytal-train
	$(GO) build -o $(GOBIN)/vytal-predict  ./cmd/vytal-predict
	$(GO) build -o $(GOBIN)/vytal-makedata ./cmd/vytal-makedata
	$(GO) build -o $(GOBIN)/vytal-db       ./cmd/vytal-db
	npm --prefix web install

vet:
	$(GO) vet ./...

sample:
	$(GO) run ./cmd/vytal-makedata --patients 12 --minutes 180 --out data/vitals.csv

train:
	$(GO) run ./cmd/vytal-train --input data/vitals.csv --out models/risk_gbm.json

predict:
	$(GO) run ./cmd/vytal-predict --input data/vitals.csv --model models/risk_gbm.json

loader:
	$(GO) run ./cmd/vytal-loader --input data/vitals.csv

api:
	$(GO) run ./cmd/vytal-server

producer:
	$(GO) run ./cmd/vytal-producer --input data/vitals.csv --speed 60

stream:
	$(GO) run ./cmd/vytal-stream

db:
	$(GO) run ./cmd/vytal-db

web:
	npm --prefix web run dev

infra:
	# legacy docker-compose 1.29 is broken vs Docker 29 (KeyError ContainerConfig),
	# and 5432/5433 are taken on this host — run pg directly on 5434
	docker run -d --name vytal-postgres --restart unless-stopped \
		-e POSTGRES_DB=vytals -e POSTGRES_USER=vytals -e POSTGRES_PASSWORD=vytals \
		-p 5434:5432 -v vytal_pgdata:/var/lib/postgresql/data postgres:16
