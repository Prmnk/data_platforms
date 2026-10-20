# data_platforms

Hands-on, local data-platform projects: streaming, CDC, batch orchestration and real-time
feature serving. Each project is self-contained (its own `docker-compose.yml`, README and
monitoring) and runs on a laptop with Docker.

## Projects

| Project | What it shows | Stack |
|---|---|---|
| [**feature_store**](feature_store/) | Real-time user features for fraud / credit-risk APIs: today, MTD, YTD, 12-month monthly history (seasonality), hourly velocity and profile attributes, served in single-digit milliseconds. Includes parity checks against raw events, crash recovery, blue/green rebuilds and a backfill design. | Redpanda, Flink (Java), ScyllaDB, FastAPI, Prometheus, Grafana |
| [**spark_scheduler**](spark_scheduler/) | A small self-hosted scheduler for Spark jobs without Airflow: cron schedules, per-job retry policies, timeouts and concurrency limits, REST API and CLI. Each job runs on Spark or on [Sail](https://github.com/lakehq/sail), with a [Spark vs Sail benchmark](spark_scheduler/bench/README.md) of an ingestion pipeline. | Spark 3.5 standalone, Sail 0.7, Delta Lake, MinIO, Postgres, Prometheus, Grafana |
| [**cdc_monitoring**](cdc_monitoring/) | Change Data Capture from Postgres into Redpanda with Debezium, and a Python consumer turning order events into live metrics. | Postgres, Debezium, Redpanda, Python, PushGateway, Prometheus, Grafana |
| [**kafka-local**](kafka-local/) | A 3-broker Kafka cluster with JMX metrics, used for producer experiments (`acks=0/1/all`) and to see where produce latency goes. | Kafka + ZooKeeper, JMX exporter, Prometheus, Grafana |

![Feature store dashboard](feature_store/docs/images/grafana-dashboard.png)

## How they fit together

```
 OLTP (Postgres) ──Debezium CDC──▶ Redpanda / Kafka ──▶ Flink ──▶ ScyllaDB ──▶ Feature API ──▶ fraud / credit models
   cdc_monitoring                    kafka-local          feature_store (online, real time)
                                                                       │
                                     Spark + Delta Lake on MinIO ◀─────┘ offline / batch (spark_scheduler)
```

- **cdc_monitoring** is how operational tables get onto the event bus.
- **kafka-local** looks at the bus itself: durability settings and their latency cost.
- **feature_store** turns events into low-latency features for online decisions.
- **spark_scheduler** runs the batch side: scheduled Spark/Delta jobs on object storage. The feature store's backfill design (see its README) is where the two meet.

## Running a project

Prerequisite: Docker (Docker Desktop or OrbStack). Each project is started from its own folder:

```bash
cd feature_store && make up        # see each project's README for its exact commands
```

**Run one project at a time.** They reuse the same local ports (Grafana on 3000, Prometheus on
9090, Redpanda Console on 8080, …) and each needs a good share of the Docker VM's memory. Stop one
before starting the next (`make down`, or `docker compose down`).

| Project | Docker memory | Main local ports |
|---|---|---|
| feature_store | ~4 GB | Grafana 3000, API 8000, Console 8080, Flink 8081, Prometheus 9090, CQL 9042, Kafka 19092 |
| spark_scheduler | ~6 GB; on smaller machines `MAX_CONCURRENT_RUNS=2 make up` | Grafana 3001, API 8000, Spark 8088, Sail 50051, MinIO 9000/9001, Prometheus 9095 |
| cdc_monitoring | not measured | Grafana 3000, Console 8080, Kafka Connect 8083, Prometheus 9090, PushGateway 9091, Postgres 5432, Kafka 19092 |
| kafka-local | not measured | Grafana 3000, Prometheus 9090, brokers 9091–9093, ZooKeeper 2181 |

## Repository layout

```
data_platforms/
├── feature_store/      # Redpanda → Flink → Scylla → Feature API
├── spark_scheduler/    # cron + retries for Spark/Delta jobs
├── cdc_monitoring/     # Postgres → Debezium → Redpanda → metrics
├── kafka-local/        # 3-broker Kafka + JMX metrics experiments
└── .github/workflows/  # CI (spark_scheduler: unit tests on push/PR, nightly end-to-end)
```

## Conventions

- Everything runs locally; no cloud account or credentials are needed. Default local-only
  logins (e.g. MinIO `minioadmin`) are documented in the project READMEs.
- Every project ships Prometheus metrics and a provisioned Grafana dashboard.
- Each project README lists what was tested and what is simplified compared with production.
