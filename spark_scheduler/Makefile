PY ?= .venv/bin/python
export SPARKSCHED_API ?= http://localhost:8000

.PHONY: venv test test-e2e build up down clean logs apply demo status

venv:
	python3 -m venv .venv && .venv/bin/pip install -q -U pip && .venv/bin/pip install -q -r requirements-dev.txt && .venv/bin/pip install -q -e .

test:            ## unit + integration tests (no Docker needed)
	$(PY) -m pytest -q

build:
	docker compose build

up:              ## start the whole stack
	docker compose up -d --build
	@echo "API http://localhost:8000/docs | Spark http://localhost:8088 | MinIO http://localhost:9001 | Grafana http://localhost:3001 (admin/admin)"

down:
	docker compose down

clean:           ## stop and delete all volumes (DB, MinIO data, logs)
	docker compose down -v

logs:
	docker compose logs -f scheduler api

apply:           ## register the example jobs
	.venv/bin/sched apply -f examples/jobs.yaml

demo: apply      ## run all example jobs now as one batch and wait
	.venv/bin/sched run orders_ingest customers_merge flaky_demo slow_demo --wait

status:
	.venv/bin/sched status

test-e2e:        ## needs `make up`
	$(PY) -m pytest -q -m e2e tests/e2e
