PY ?= .venv/bin/python
export LAB_API ?= http://localhost:8010

.PHONY: venv test test-e2e build up down clean logs ps urls demo notebook dashboards

venv:            ## local venv for tests (the ./lab CLI itself needs nothing)
	python3 -m venv .venv && .venv/bin/pip install -q -U pip && .venv/bin/pip install -q -r requirements-dev.txt && .venv/bin/pip install -q -e .

test:            ## unit tests (no Docker needed)
	$(PY) -m pytest -q

build:
	docker compose build

up:              ## start Sail, the lab API, MinIO and the monitoring stack
	docker compose up -d --build
	@$(MAKE) --no-print-directory urls

urls:
	@echo "Grafana    http://localhost:3002   (dashboards open without login; admin/admin to edit)"
	@echo "Lab API    http://localhost:8010/docs"
	@echo "Sail       sc://localhost:50052   (Spark Connect)"
	@echo "MinIO      http://localhost:9011   (minioadmin/minioadmin)"
	@echo "Prometheus http://localhost:9097"

down:
	docker compose --profile notebook down

clean:           ## stop and delete all data (runs, MinIO tables, metrics, logs)
	docker compose --profile notebook down -v

logs:
	docker compose logs -f lab-api sail

ps:
	docker compose ps

demo:            ## generate data, then run the main examples one after another
	./lab submit gen_orders.py -w
	./lab submit hello_sail.py -w
	./lab submit orders_report.py -w
	./lab submit explore.sql -w
	./lab submit delta_lake.py -w
	./lab submit iceberg_tables.py -w
	./lab submit python_udfs.py -w
	./lab runs

notebook:        ## JupyterLab on http://localhost:8890, connected to Sail
	docker compose --profile notebook up -d --build notebook
	@echo "JupyterLab http://localhost:8890  (notebooks/getting_started.ipynb)"

dashboards:      ## regenerate grafana/dashboards/*.json (Grafana reloads them within ~10s)
	python3 grafana/build_dashboards.py

test-e2e:        ## needs `make up`
	$(PY) -m pytest -q -m e2e tests/e2e
