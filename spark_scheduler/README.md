# spark_scheduler

A small, self-hosted scheduler for **Apache Spark 3.5.8 + Delta Lake 3.3.2** jobs with **MinIO** storage,
without Airflow. It schedules jobs on cron, submits them to a Spark standalone cluster, polls them
until they finish, retries them according to each job's own policy, and exposes everything through
a REST API, Prometheus metrics, and a ready-made Grafana dashboard.

```
 jobs.yaml ─▶ sched CLI ─▶ REST API (:8000) ─▶ Postgres ◀─ Scheduler loop (single leader)
                              │  /status, /metrics            │  cron → launch → poll → retry
                              ▼                               ▼
                 Prometheus ─▶ Grafana (:3001)      spark-submit (client mode, driver in scheduler)
                                                              │
                                     Spark master (:8088) ─ worker-1, worker-2 ──s3a──▶ MinIO (Delta)
```

## Quick start

```bash
make venv          # local virtualenv with the CLI (`.venv/bin/sched`) and test deps
make test          # unit + integration tests, no Docker needed
make up            # build images and start the stack
make demo          # register examples/jobs.yaml and run all four jobs as one batch
make status        # latest state of every job
```

| UI | URL |
|---|---|
| API docs (Swagger) | http://localhost:8000/docs |
| Spark master | http://localhost:8088 |
| Driver UIs | http://localhost:4040 … 4049 (one per running attempt) |
| MinIO console | http://localhost:9001 (minioadmin / minioadmin) |
| Grafana | http://localhost:3001 (admin / admin) → *Spark Scheduler* dashboard |
| Prometheus | http://localhost:9095 |

Docker needs about 6 GB of RAM for comfortable concurrency. On smaller machines run
`MAX_CONCURRENT_RUNS=2 make up`.

## Defining jobs

```yaml
jobs:
  - name: orders_ingest
    schedule: "*/5 * * * *"          # cron, UTC; omit for manual-only jobs
    app: /opt/sparksched/jobs/delta_ingest.py
    args: ["--date", "{{ logical_date }}"]   # also {{ run_id }}, {{ attempt }}, {{ job }}
    spark_conf: {spark.cores.max: "2", spark.executor.memory: 1g}
    retry:
      max_attempts: 3                # total tries, including the first
      backoff: exponential           # or fixed
      initial_delay_s: 20
      max_delay_s: 300
      jitter: 0.1                    # ±10%
      retry_on_exit_codes: []        # empty = retry any failure
      retry_on_timeout: true
    timeout_s: 900                   # kill + count as TIMED_OUT after this long
    concurrency: 1                   # max simultaneous runs of this job
    catchup: false                   # true = backfill missed cron slots after downtime
```

Put PySpark scripts in `jobs/`. That folder is mounted into the scheduler container at `/opt/sparksched/jobs`.

### Working with many jobs at once

```bash
sched apply -f examples/jobs.yaml                  # create/update every job in one transaction
sched run orders_ingest customers_merge --wait     # one batch, poll until all finish
sched run -f examples/jobs.yaml --wait             # apply + run everything in the file
```

Every run in a batch shares a `batch_id` (`GET /runs?batch_id=...`), and each run retries by its own job's policy.

### Other commands

`sched status | runs [--job X --state DEAD] | show <run_id> | logs <run_id> [--attempt N] | cancel <run_id> | enable/disable/delete <job>`

## How it works

**Run lifecycle**

```
PENDING ─▶ RUNNING ─▶ SUCCEEDED
              │
              ├─ FAILED / TIMED_OUT / LOST ─▶ RETRY_WAIT ─▶ (backoff elapsed) ─▶ RUNNING …
              │                            └▶ DEAD   (out of attempts, or exit code not retryable)
              └─ cancel ─▶ CANCELLED
```

Each try is a row in `run_attempts` with its exit code, Spark app ID, cluster state, driver UI port and log path.

**The loop** (`scheduler/engine.py`, every `TICK_SECONDS`):
1. Heartbeat.
2. Create runs for due cron slots. A unique `dedupe_key` per job and fire time makes this idempotent.
3. Apply cancel requests.
4. Poll RUNNING attempts:
   - process exit code;
   - Spark master `/json` for the app ID and state;
   - timeout check, which kills the whole process group.
5. Launch ready runs, within `MAX_CONCURRENT_RUNS` and each job's `concurrency`.

**Why client mode:** Spark standalone can't run Python drivers in cluster deploy mode. Each driver
therefore runs as a child process of the scheduler, and executors connect back to `scheduler:<port>`.
A small shell wrapper writes the exit code to `<log>.exit`, so a restarted scheduler process can
still resolve attempts it didn't launch. In Docker, a container restart kills its drivers. Those
attempts are marked `LOST` and retried. The `Runner` interface (`scheduler/runners/base.py`) is
where a Kubernetes runner would plug in later.

**One active scheduler:** the loop holds a Postgres advisory lock. A second instance waits as a hot standby.

**Retry-safe writes:** for retries to be safe, the jobs must be idempotent. `delta_ingest.py` uses
`replaceWhere` on the logical date, and `delta_merge.py` uses `MERGE`.

## Monitoring API (for Grafana and anything else)

| Endpoint | Use |
|---|---|
| `GET /status` | Summary (run counts by state, scheduler heartbeat age) + one row per job |
| `GET /status/jobs` | Flat array, one row per job: `last_state`, `last_attempts`, `running`, `queued`, `last_success_at`, `next_fire_at`, `last_duration_s`, `succeeded_24h`, `dead_24h`, `last_error`, … |
| `GET /runs?job=&state=&batch_id=&limit=` | Recent runs |
| `GET /runs/{id}` | One run with every attempt |
| `GET /runs/{id}/attempts/{n}/log?tail=200` | Driver log tail |
| `GET /metrics` | Prometheus metrics, see below |

Prometheus metrics, computed from the DB at scrape time:
- `sparksched_job_last_run_state{job,state}`: 1 for the job's latest state
- `sparksched_runs{state}`
- `sparksched_runs_24h{job,state}`
- `sparksched_attempts_24h{job,outcome}`
- `sparksched_job_last_success_timestamp_seconds{job}`
- `sparksched_job_last_run_duration_seconds{job}`
- `sparksched_scheduler_heartbeat_timestamp_seconds`
- `sparksched_job_info{job,schedule,enabled}`

Grafana is provisioned with two datasources. **Prometheus** feeds the trend panels and alerts. **Infinity** reads
`http://api:8000/status/jobs` directly for the live job table. The dashboard lives in `grafana/dashboards/sparksched.json`.

Alerts worth adding:
- `time() - sparksched_scheduler_heartbeat_timestamp_seconds > 60`: the scheduler is down.
- `increase(sparksched_runs_24h{state="DEAD"}[15m]) > 0`: a run is out of retries.
- Hours since last success over N for critical jobs.

## Testing

| Layer | Command | Covers |
|---|---|---|
| Unit | `make test` | Spec validation, cron and catchup, retry/backoff math, the engine state machine with a scripted fake runner (retry, DEAD, timeout, cancel, global and per-job concurrency, recovery/LOST, launch failure), API and metrics |
| Integration | `make test` | Real process handling with a fake `spark-submit`: exit codes, exit files, adopting after restart, killing the process group |
| End-to-end | `make up && make test-e2e` | Real Spark + Delta + MinIO: a batch of ingest + flaky + slow jobs (retries to success, timeouts to DEAD, concurrency cap), Delta table verified by a Spark job, cancel, the status/metrics endpoints, scheduler restart mid-run |

CI is in `.github/workflows/spark-scheduler.yml`. Unit tests run on every push. The e2e suite runs nightly and on manual dispatch.

## Configuration (env vars)

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `sqlite:///sparksched.db` | Postgres in compose |
| `SPARK_MASTER_URL` / `SPARK_MASTER_UI` | `spark://spark-master:7077` / `http://spark-master:8080` | |
| `DRIVER_HOST` | hostname | Address executors use to reach drivers |
| `MAX_CONCURRENT_RUNS` | 4 (compose: 3) | Global cap |
| `TICK_SECONDS` | 5 (compose: 3) | Loop interval |
| `LOG_DIR` | `./logs` | Driver logs, `<job>/<run_id>/attempt-N.log` |
| `KILL_GRACE_SECONDS` | 10 | SIGTERM → SIGKILL grace |
| `MINIO_IMAGE` | `bitnamilegacy/minio:2025.4.22` | The official `minio/minio` image is no longer on Docker Hub |

## Next steps (not built yet)
- Job dependencies (`depends_on`) / DAGs inside a batch
- Ship driver logs to MinIO; Alembic migrations once the schema settles
- Alerting hooks (Slack/email) on DEAD
- Kubernetes runner (Spark on K8s, cluster mode, poll pod status)
- Auth on the API
