# Spark vs Sail: the same ingestion pipeline, measured

How does [Sail](https://github.com/lakehq/sail) 0.7.2 compare with Apache Spark 3.5.8 on one
ingestion pipeline? Specifically: can Sail run it with **a fifth of Spark's resources**, and what
is the **minimum** CPU and memory each engine needs?

**Short answer** (2 batches, ~4.1M raw events, MERGE into a 2.08M-row Delta table):

- **CPU: yes.** On 0.8 CPU, a fifth of the baseline, Sail finished in 381 s. Spark took 349–363 s
  on 4 CPUs. Sail used 298 CPU-seconds against Spark's ~1,100 (about 3.7× less).
- **Memory: no.** Sail's Delta MERGE needs about 1.75 GB, roughly 58% of the 3 GB baseline. At a
  fifth of the memory (614 MB), the first batch completes but the MERGE batch is OOM-killed. Spark
  itself gets by with 1.25–1.5 GB.
- **On equal resources** (4 CPU, 3 GB), Sail is 1.4× faster: 249 s against 349–363 s.
- **On equal CPU** (0.8), Sail is 1.9× faster (381 s against 726 s) and uses half the CPU-seconds.
- **Where the gain comes from:** almost all of it is reading, validating and deduplicating JSON,
  where Sail is about 4× faster. The Delta MERGE is only about 1.3× faster.
- **Output:** both engines produced identical tables, with 0 rows differing in silver and quarantine.

## The workload

`jobs/ingest_pipeline.py` runs unchanged on both engines (Spark via `spark-submit`, Sail via Spark
Connect). For each landing batch it:

1. **Reads** the batch's JSON lines with an explicit schema. Unknown fields such as a drifting
   `promo_code` are dropped.
2. **Runs schema checks:** required fields, number and timestamp formats, and allowed currency and
   status values. Failing rows go to a **quarantine** Delta table with their reasons; this table is
   rewritten per batch with `replaceWhere`.
3. **Dedups** in two steps: exact redeliveries (same `event_id`), then the latest version per
   `order_id` (`row_number` over `updated_at`).
4. **Merges** into the **silver** Delta table with `MERGE INTO`: insert new orders, and update a row
   only when the event is newer.

The input comes from `jobs/gen_raw_orders.py`, generated once on Spark so every run reads the same
files: two batches of 2M events (~325 MB of JSON each) on MinIO. The data has realistic defects:
about 3% duplicate deliveries, 0.5% missing `order_id`, 0.5% bad amounts, 0.5% unknown
currency/status values, and 0.3% unparsable timestamps. Batch 1 creates about 1.4M orders, each with
several versions. In batch 2, half the events update batch-1 orders and half create new ones, so its
MERGE does both updates and inserts.

Result after both batches: **2,081,538 silver rows and 93,396 quarantined rows**, the same on every
successful run of both engines.

## Method

`bench/run_bench.py` runs one engine in **one container** with `docker run --cpus --memory` and no
swap. Everything the engine needs runs inside that container, so the container's cgroup is the
whole cost:

| | Spark | Sail |
|---|---|---|
| Processes | `spark-submit --master local[N]` per batch (JVM + Python driver) | `sail spark server` (local mode) + the same script over Spark Connect |
| Parallelism | `local[ceil(cpus)]` | `execution.default_parallelism = ceil(cpus)` |
| Memory | heap = 60% of the container | fair memory pool = 75% of the container, spilling to `/tmp` |
| Storage | MinIO through `s3a://` | MinIO through `s3a://` (Sail's own S3 client) |

What's measured:
- **Wall time** per batch, including JVM or session start-up. Every Spark batch pays JVM start-up;
  the Sail server starts once per run.
- **CPU-seconds** and **peak memory** from the container's cgroup (`cpu.stat`, `memory.peak`).
- **OOM kills** from `memory.events`.
- **The pipeline's own counts**, from its `PIPELINE_METRICS` line.

Every run appends one JSON line to `results/runs.jsonl`; `report.py` renders the table below.
**Correctness:** `jobs/compare_tables.py` checked `exceptAll` in both directions between the Spark
baseline's tables and Sail's.

**Hardware:** a MacBook Air with an i5-5350U, which has 2 physical cores and 4 hyper-threads. Docker
Desktop gets 4 vCPUs and 3.84 GiB. **"4 CPUs" is really 2 cores with hyper-threading**, so going from
2 to 4 CPUs barely helps either engine (Spark: 387 s at 2 CPUs, 349–363 s at 4). This makes
low-CPU results look better than they would on 4 real cores. Expect larger gaps between the
CPU budgets on server hardware.

## Results

Valid runs only; the exclusions are listed under [Caveats](#caveats).

### A fifth of the resources

| Run | Batch 1 (create) | Batch 2 (MERGE) | Total | CPU-s | Peak memory | Result |
|---|---|---|---|---|---|---|
| **Spark, 4 CPU / 3 GB (baseline)** | 148–152 s | 199–210 s | **349–363 s** | 1,081–1,146 | 2.5–2.6 GB | ok |
| Sail, 0.8 CPU / 614 MB (⅕ CPU, ⅕ memory) | 73–80 s | – | – | – | 614 MB | **MERGE OOM-killed / pool exhausted** |
| **Sail, 0.8 CPU / 3 GB (⅕ CPU)** | 71 s | 307 s | **381 s** | **298** | 2.4 GB | ok |
| Sail, 0.8 CPU / 1.75 GB | 80 s | 327 s | 409 s | 320 | 1.8 GB | ok |

### Same resources

| CPU / memory | Spark | Sail | Sail speed-up | CPU-s Spark → Sail |
|---|---|---|---|---|
| 4 CPU / 3 GB | 349–363 s | 249 s | 1.4× | 1,081–1,146 → 855 |
| 2 CPU / 3 GB | 387 s | 291 s | 1.3× | 712 → 553 |
| 0.8 CPU / 3 GB | 726 s | 381 s | 1.9× | 578 → 298 |

By batch, at 0.8 CPU: batch 1 took 313 s on Spark and 71 s on Sail (4.4×); the MERGE batch took 411 s
and 307 s (1.3×). Sail's advantage is in scanning, parsing and filtering. Its Delta MERGE is not
much faster than Spark's.

### Minimum resources

| | Spark | Sail |
|---|---|---|
| Memory floor at 4 CPU | **1.5 GB** ok (266 s); 1.25 GB OOM-killed in the MERGE batch, 1 GB OOM-killed | not measured (more threads need more memory) |
| Memory floor at 0.8 CPU | **1.25 GB** ok (715 s); 1 GB not measured | **1.75 GB** ok (409 s); 1.5 GB with a 60% pool fails (pool exhausted in the MERGE's aggregate; not retried with 75%); 1 GB and below fail in the MERGE |
| CPU | completes at 0.8 CPU (726 s) | completes at 0.8 CPU (381 s) |

At 0.8 CPU, the smallest configurations that pass are:
- **Spark:** 0.8 CPU, 1.25 GB, 715 s.
- **Sail:** 0.8 CPU, 1.75 GB, 409 s. It is 1.7× faster on about 40% more memory.

### Utilisation

Average CPU utilisation is CPU-seconds divided by (wall time × CPU limit):

- **Spark** uses 77–79% of 4 CPUs.
- **Sail** uses 86% of 4 CPUs.
- **Both engines** reach 92–100% at 0.8–2 CPUs, so they are CPU-bound there.
- **Memory:** under a generous limit, peak memory mostly reflects heap and pool sizing. At 3 GB,
  Spark peaked at 1.7–2.6 GB and Sail at 2.4–2.7 GB. The memory floors above are the better measure.

### Sail's memory behaviour

- **Default pool:** Sail's default memory pool is **unbounded**. With it, Sail took 2.9 GB for
  batch 1, which runs in 614 MB under a bounded pool, and was OOM-killed in the MERGE at 2, 2.5 and
  3 GB.
- **Bounded pool:** a bounded `fair` pool with spilling (`SAIL_RUNTIME__MEMORY_POOL__TYPE=fair`,
  `..._FAIR__MAX_SIZE`) is required in any memory-limited container. The compose `sail` service sets
  one.
- **The MERGE can't spill:** its hash join (`HashJoinInput`) and an aggregate fail with
  `Failed to allocate ...` once they outgrow the pool. That sets Sail's floor for this workload.

## Compatibility notes (Sail 0.7.2)

- **JSON reader:** Spark turns a JSON number into a string when the schema says string. Sail fails
  the read (`expected string got 43156`), so the pipeline declares numeric fields as numbers.
- **Return types:** Spark Connect returns numpy integers in `Row`s, so the pipeline casts them with
  `int()` before `json.dumps`.
- **More:** see the main README's *Running jobs on Sail* section for the list of Delta, UDF and
  cluster-mode issues.

## Caveats

- **Single runs:** most configurations ran once. Repeated baselines varied by about 4% (349 s and
  363 s). The first baseline run, with a cold cache and a different heap setting (memory − 400 MB),
  took 416 s and is shown in `report.py`'s output but not used above.
- **Excluded runs:** these are kept in `runs.jsonl` with `valid: false` and a reason:
  - two runs from pipeline bugs that were fixed afterwards;
  - one from a harness hang before the server watchdog existed;
  - one killed manually.
  - **one run during which the Docker VM stalled.** When the Mac stalls the Docker VM, its kernel
    logs "Hangcheck", and a container's CPU quota timer can stay expired: threads are runnable but
    never scheduled, and even `docker kill` hangs. The harness now runs under `caffeinate`, records
    VM stalls per run (`valid` is false when there were any), and frees a stuck container by
    rewriting its `cpu.max`.
- **Scope:** local mode only, one machine, one data size. Cluster mode was not compared; Sail's
  `local-cluster` mode has a Delta bug, see the main README.

## Reproduce

```bash
make bench-up              # build images, start MinIO only
make bench-data            # landing batches 1 and 2 (BENCH_EVENTS=2000000)
make bench-baseline        # Spark 4 CPU / 3 GB, Sail 0.8 CPU / 3 GB
python3 bench/sweep.py sail 0.8:1792m:0.75 2:3g:0.75 --keep-going    # cpus:memory[:pool fraction]
python3 bench/sweep.py spark 0.8:1280m:0.6 --keep-going              # cpus:memory[:heap fraction]
make bench-report
```

Each run writes its tables to `s3a://warehouse/bench/<run_id>/`. To check that two runs produced the
same rows:

```bash
spark-submit jobs/compare_tables.py s3a://warehouse/bench/<a>/silver s3a://warehouse/bench/<b>/silver
```
