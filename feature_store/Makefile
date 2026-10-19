COMPOSE = docker compose
TOOLS   = $(COMPOSE) run --rm tools python

.PHONY: up ui down clean logs ps generate verify parity reload reload-follow switch-v2 switch-v1 cql test \
        redeploy-job load load-stop cases cases-cql

up:                ## build and start redpanda, scylla, flink, api, prometheus, grafana
	$(COMPOSE) up -d --build

redeploy-job:      ## stop the Flink job with a savepoint, rebuild, resume from it (never with empty state)
	./scripts/stop-with-savepoint.sh
	$(COMPOSE) up -d --build --force-recreate jobmanager taskmanager

ui:                ## also start Redpanda Console on :8080
	$(COMPOSE) --profile ui up -d console

down:
	$(COMPOSE) down

clean:             ## stop and delete all data (topics, scylla, checkpoints)
	$(COMPOSE) --profile ui --profile tools --profile load down -v

logs:
	$(COMPOSE) logs -f --tail=100 jobmanager taskmanager api

ps:
	$(COMPOSE) ps -a

generate:          ## history + 60s of live traffic
	$(TOOLS) generate.py $(ARGS)

verify:            ## end-to-end correctness + freshness check
	$(TOOLS) verify.py

parity:            ## recompute features from raw topics and compare with the API (pauses loadgen: needs a quiet system)
	-$(COMPOSE) --profile load stop loadgen
	$(TOOLS) parity.py --sample 500
	@echo "(loadgen paused; 'make load' to resume)"

load:              ## continuous live traffic (25 txn/s) for the dashboards
	$(COMPOSE) --profile load up -d loadgen

load-stop:
	$(COMPOSE) --profile load stop loadgen

cases:             ## publish the curated demo users, save inputs + served features under samples/
	$(TOOLS) scenarios.py

cases-cql:         ## run the sample CQL queries for the demo users
	$(COMPOSE) exec -T scylla cqlsh -f /schema/queries.cql

reload:            ## blue/green rebuild of the online store into fs_v2
	$(TOOLS) reload.py --target fs_v2

reload-follow:
	$(TOOLS) reload.py --target fs_v2 --follow

switch-v2:
	curl -s -XPOST localhost:8000/admin/keyspace -H 'content-type: application/json' -d '{"keyspace":"fs_v2"}'; echo

switch-v1:
	curl -s -XPOST localhost:8000/admin/keyspace -H 'content-type: application/json' -d '{"keyspace":"fs"}'; echo

cql:
	$(COMPOSE) exec scylla cqlsh

test:              ## unit tests (feature assembly); Java tests run in the image build
	cd api && python3 -m pytest -q test_features.py
