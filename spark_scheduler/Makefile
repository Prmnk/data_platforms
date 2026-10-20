PY ?= .venv/bin/python
export SPARKSCHED_API ?= http://localhost:8000

.PHONY: venv test test-e2e build up down clean logs apply demo demo-sail status bench-up bench-data bench-baseline bench-report

venv:
	python3 -m venv .venv && .venv/bin/pip install -q -U pip && .venv/bin/pip install -q -r requirements-dev.txt && .venv/bin/pip install -q -e .

test:            ## unit + integration tests (no Docker needed)
	$(PY) -m pytest -q

build:
	docker compose build

up:              ## start the whole stack
	docker compose up -d --build
	@echo "API http://localhost:8000/docs | Spark http://localhost:8088 | Sail sc://localhost:50051 | MinIO http://localhost:9001 | Grafana http://localhost:3001 (admin/admin)"

down:
	docker compose down

clean:           ## stop and delete all volumes (DB, MinIO data, logs)
	docker compose down -v

logs:
	docker compose logs -f scheduler api sail

apply:           ## register the example jobs
	.venv/bin/sched apply -f examples/jobs.yaml

demo: apply      ## run all example jobs now as one batch and wait
	.venv/bin/sched run orders_ingest customers_merge flaky_demo slow_demo --wait

demo-sail: apply ## run the Sail versions of ingest + merge
	.venv/bin/sched run orders_ingest_sail --wait && .venv/bin/sched run customers_merge_sail --wait

status:
	.venv/bin/sched status

test-e2e:        ## needs `make up`
	$(PY) -m pytest -q -m e2e tests/e2e

# ---- Spark vs Sail benchmark (bench/README.md). Needs only MinIO; stop the rest for clean numbers.
BENCH_EVENTS ?= 2000000

bench-up:        ## images + MinIO only
	docker compose build scheduler sail
	docker compose up -d minio
	docker compose stop scheduler api spark-master spark-worker-1 spark-worker-2 sail postgres prometheus grafana 2>/dev/null || true

bench-data:      ## two landing batches of BENCH_EVENTS raw events (generated once, on Spark)
	for b in 1 2; do docker run --rm --network spark-scheduler_default --cpus 4 --memory 3g --user root \
	  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -v $(PWD)/jobs:/opt/sparksched/jobs:ro \
	  --entrypoint /opt/spark/bin/spark-submit sparksched:dev --master 'local[4]' --driver-memory 2g \
	  --conf spark.ui.enabled=false /opt/sparksched/jobs/gen_raw_orders.py --batch $$b --events $(BENCH_EVENTS); done

bench-baseline:  ## Spark at 4 CPU / 3 GB, then Sail at a fifth of the CPU with the same memory
	caffeinate -i python3 bench/sweep.py spark 4:3g:0.6 --keep-going
	caffeinate -i python3 bench/sweep.py sail 0.8:3g:0.75 --keep-going

bench-report:
	python3 bench/report.py
