# Feature store (local): Redpanda → Flink → Scylla → Feature API

Real-time user features for fraud / credit-risk APIs: **today, MTD, YTD**, the
**last 12 months month by month** (seasonality), hourly **velocity**, and slow-changing
**profile** attributes, all served from one Scylla partition per user.

```
 transactions ─┐                                   ┌─▶ Scylla fs.user_agg (D/M/Y buckets + profile static cols)
 orders ───────┼─▶ Flink: dedupe → calendar buckets ┤   Scylla fs.user_agg_hourly (velocity, TWCS)
               │   (keyed by user, flush 100 ms)    └─▶ features.changelog (compacted = copy of online store)
 users ────────┴─▶ Flink: profile upsert ─────────────▶ Scylla static columns
                                                                     │
                     Feature API (FastAPI) ◀── 1 single-partition read + 1 hourly read, in parallel
                           └─▶ features.served (logged vectors for training / skew checks)
```

## Design in one paragraph

Flink keeps running totals per **calendar bucket** (hour, day, month, year in the
business timezone) instead of sliding windows. Today/MTD/YTD/12-month history are just
bucket lookups at read time, day rollover is a new key (no reset jobs), late events
update their own bucket. Flink writes **absolute values** with `USING TTL <remaining>
AND TIMESTAMP <version>`, so writes are idempotent and order-independent: replays after a
crash, duplicate deliveries and bulk reloads running next to the live job are all safe.
D/M/Y rows of a user go in one single-partition `UNLOGGED BATCH` (atomic + isolated),
so today/MTD/YTD are never seen out of sync. The API never reads the lake; on store
errors it returns zeros with `features_status: "degraded"`.

## See it running

Needs Docker with ~4 GB memory.

```bash
make up          # redpanda, scylla (+schema), flink job, api, prometheus, grafana
make generate    # 500 users × 400 days of history + 60 s live traffic
make load        # keep 25 txn/s flowing for the dashboards (make load-stop to stop)
make cases       # publish the 7 demo users, check them, save inputs + features to samples/
make cases-cql   # run scylla/queries.cql against the demo users
make verify      # end-to-end correctness + freshness for a fresh user
make parity      # recompute everything from raw topics, compare with the API (pauses loadgen)
make clean       # stop and delete all data
```

| What | Where |
|---|---|
| **Grafana dashboard "Feature store"** (no login) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Flink UI | http://localhost:8081 |
| Redpanda Console (`make ui`) | http://localhost:8080 |
| Feature API | `GET  localhost:8000/v1/users/{id}/features?current_txn_amt=50000&format=flat` |
| Batch API | `POST localhost:8000/v1/features {"ids": [...]}` |
| Blue/green switch | `POST localhost:8000/admin/keyspace {"keyspace": "fs_v2"}` (no auth, local only) |
| Scylla CQL | `make cql` (or `localhost:9042`) · Kafka `localhost:19092` |

Amounts are minor units (paise/cents). Timezone: `FS_TZ` (default `Asia/Kolkata`).

### Event schema (topics `transactions`, `orders`, `users`)

```jsonc
// transactions / orders, key = user_id
{"event_id": "uuid",            // dedupe key (7-day window)
 "user_id": "user-000042",
 "amount": 125000,              // minor units: Rs.1,250.00
 "event_time": 1792376095000,   // epoch ms, when it happened
 "op": "refund",                // transactions: create | refund   orders: create | cancel
 "original_event_time": 1792289695000}  // refund/cancel only: when the reversed purchase happened

// users (compacted), key = user_id
{"user_id": "user-000042", "segment": "gold", "kyc_level": "full",
 "signup_date": "2021-03-02", "updated_at": 1792376095000}
```

| op | count | amount | bucketed at |
|---|---|---|---|
| `create` | +1 | +amount | `event_time` |
| `refund` (transactions) | 0 (the purchase still happened) | −amount | `original_event_time` |
| `cancel` (orders) | −1 | −amount | `original_event_time` |

### Demo users (`make cases`)

| user | behaviour | look at |
|---|---|---|
| `demo-steady` | ~Rs.1,250 every 3 days, weekly order, 13 months | flat `monthly_12m`, TTL/WRITETIME query |
| `demo-seasonal` | 4 buys/month, 25 bigger ones/month in Oct–Dec last year | `monthly_12m`, `txn_amt_mtd_vs_same_month_last_year` |
| `demo-burst` | small weekly spend, then 12 large txns in 20 min | `velocity`, `current_txn_amt_vs_avg_txn_12m` (scored with a Rs.25,000 payment) |
| `demo-refunds` | 15 buys, 7 refunded; 6 orders over 2 days, 3 cancelled today | reversals land on the purchase's day |
| `demo-new` | signed up 2 days ago, 3 buys today | `account_age_days`, empty history |
| `demo-dupes` | 5 buys each delivered 3×, 1 malformed, 1 unknown op | counts 5, `invalid_events` metric |
| `demo-unknown` | no events | zeros, `has_history: false` |

`samples/input/*.jsonl` is exactly what was published; `samples/features/<user>.json` is the
API response, and `samples/features/flat_vectors.json` holds the model-ready flat vectors.
`make cases` only saves them after the served values match an independent re-computation
(`tools/expect.py`). Event ids are stable, so re-running within the 7-day dedupe window
doesn't double count. Re-running later shifts the timestamps, so the sample files change.

**Reversal semantics.** A refund or cancel is bucketed at the time of the **purchase it
reverses** (`original_event_time`), so "orders today" means orders placed today net of
their later cancels, and no day goes negative. Example: an order from last month cancelled
today nets last month's bucket to zero and leaves today untouched. A reversal without
`original_event_time` falls back to its own time and is counted in the
`reversals_without_original_time` metric (Grafana, Data quality panel). Fix the producer
when that is non-zero. If the original purchase's bucket has already expired (e.g. a day
bucket older than 7 days), only the still-live grains (month, year) are adjusted.

### Monitoring

Prometheus scrapes Flink (`:9249`), the API (`/metrics`), Scylla (`:9180`) and Redpanda
(`/public_metrics`) every 5 s.

![Grafana "Feature store" dashboard](docs/images/grafana-dashboard.png)

The dashboard rows:

- **Overview:** feature freshness p95, source backlog, events/s, API p99, degraded-response share, job restarts, failed checkpoints.
- **Pipeline:**
  - `e2e_latency_ms`: Kafka timestamp to Scylla ack, i.e. what the API sees, incl. the 100 ms flush.
  - Throughput before and after per-user coalescing.
  - Lag: Flink `pendingRecords` (true lag) and committed-offset lag (sawtooth, because Flink commits on checkpoint).
  - Scylla write latency and in-flight writes.
  - Data quality: duplicates dropped, expired-bucket skips, invalid ops, failed writes.
  - Busy/backpressure, checkpoints, JVM heap.
- **Feature API:** requests by status (`ok` / `no_history` / `degraded`), request and Scylla-read latency, and served users by activity recency.
- **Scylla:** CQL reads/inserts/batches, server-side coordinator latency, reactor utilization, memory, cache hit ratio.
- **Redpanda:** produced and fetched records per topic, CPU, memory.

Custom metrics are defined in `FeatureAggregator.java` (`ingest_lag_ms`, `duplicates_dropped`,
`expired_bucket_skips`, `invalid_events`), `AsyncCqlSink.java` (`e2e_latency_ms`,
`cql_write_latency_ms`, `cql_writes_ok/failed`, `cql_in_flight`) and `api/app.py` (`fs_api_*`).

### Redeploying the Flink job

```bash
make redeploy-job    # stop-with-savepoint -> rebuild -> resume from that savepoint
```

The job always starts from the newest savepoint or retained checkpoint (`flink-job/start-job.sh`).
With no state at all it reads Kafka from the **earliest** offset and rebuilds by replay.
Never start it with empty state from committed offsets: it would overwrite correct bucket
totals in Scylla with partial ones.

## Rebuilding the online store (worst case)

```bash
make reload-follow   # load fs_v2 from compacted users + features.changelog, then keep tailing
make switch-v2       # every API worker follows fs_meta.serving within 2 s
make switch-v1       # roll back
```

## Backfill (designed, not built)

Locally, history goes through the stream itself: the generator publishes 400 days of
past events to Redpanda and Flink buckets them by event time. In production Kafka
won't hold years of history, so the history has to come from the lake (raw events in
Iceberg/Parquet).

### Why Spark can't just bulk-load Scylla

Flink writes **absolute totals from its own state**. If Spark loads history into Scylla
but Flink's state starts empty, the next live event overwrites the bucket:

1. Spark writes `M 2026-10 = 41 txns` (history up to cutover time T).
2. One live transaction arrives; Flink's state for that month holds just that event.
3. Flink writes `M 2026-10 = 1` with a newer version (USING TIMESTAMP), and MTD is now wrong.

The same applies to today, YTD and any bucket that still receives events. So a backfill
must seed **Flink's keyed state**, not only Scylla. (Loading Scylla alone is fine only
for buckets that can no longer change, which is exactly what `tools/reload.py` relies on:
it copies the latest absolute values and the live job owns the state.)

### Option A: replay history through Flink (recommended)

Use Flink's `HybridSource`: a bounded `FileSource` / Iceberg source over raw events ingested before
T, then switch to the `KafkaSource` starting at timestamp T.

```
HybridSource
  ├─ 1. Iceberg/Parquet raw events  [kafka_ts < T]     (bounded)
  └─ 2. KafkaSource  OffsetsInitializer.timestamp(T)    (unbounded)
        └─▶ same keyBy(user) → FeatureAggregator → Scylla + changelog
```

- **Same code path:** the aggregator, dedupe, timezone and TTL logic are not duplicated, so Spark and Flink logic can't drift apart.
- **Cost:** throughput is bounded by the job's parallelism. State is small (~25 buckets per user plus a 7-day dedupe set), so this is usually fine. Raise parallelism for the backfill run, take a savepoint, then rescale down.
- **Only the retention window is needed:** ~13 months for M, current and previous year for Y. Older events are skipped (`expired_bucket_skips`).
- **Dedupe:** the 7-day dedupe state also covers duplicates that straddle T.
- **Steps:** land raw events in Iceberg with a separate sink job, deploy the job with the HybridSource, wait until the file phase finishes and lag is ~0, then switch serving if this is a rebuild (blue/green as below).

### Option B: Spark computes and bootstraps a Flink savepoint

For very large histories (billions of events, hundreds of millions of users):

1. **Spark** reads raw events from Iceberg and computes H/D/M/Y buckets per user for events ingested before T (Kafka timestamp), deduped by `event_id`. It uses the same bucket rules as `Buckets.java`: business timezone, labels, `expires_at = period_end + retention`, version = T in micros.
2. A **Flink State Processor API** job reads Spark's output and writes a savepoint for operator uid `bucket-aggregator`: the `agg-H/D/M/Y` MapStates (`Agg` POJOs), plus a seeded `seen-event-ids` for the last 7 days.
3. Spark (or the same job) bulk-loads the buckets into Scylla with `USING TTL <remaining> AND TIMESTAMP <version>`.
4. Start the live job **from that savepoint** with `--allowNonRestoredState`. The Kafka source uid isn't in the savepoint, so it uses `OffsetsInitializer.timestamp(T)`.

- **Pros:** fastest, fully parallel.
- **Cons:**
  - The bucket logic exists twice, so run `tools/parity.py`-style checks after every backfill.
  - The savepoint is coupled to state names and serializers: any change to `Agg` or state descriptors needs the bootstrap job updated.
  - One more job type to operate.

### Correctness checks for either option

- Parity: recompute from raw events for a sample of users and compare with the API (`tools/parity.py`).
- Boundary: no gap and no double count at T. Split on the **Kafka record timestamp** (ingest time, landed in the lake with each raw event), the same clock `OffsetsInitializer.timestamp(T)` uses. Splitting on `event_time` would double count a late event produced after T with an event time before T.
- Late events: an event before T that arrives after the cutover is still handled, because the live job updates the bucket of the event's own time.

## What was tested locally (4-CPU laptop, Docker 3.8 GB)

| Test | Result |
|---|---|
| Unit: bucket labels, timezone/DST, year boundary (Java, runs in image build) | pass |
| Unit: feature assembly, rollover, velocity, unknown user (`api/test_features.py`) | 5 pass |
| `verify.py`: dedupe, refund, cancel, late event, last-year month, profile, velocity | pass; features correct **0.24–0.34 s** after publish, API **3–8 ms** |
| 153k-event history backlog | drained in ~2 min, 0 job exceptions |
| Parity: 500 users × 30 values vs. recompute from raw topics | **0 mismatches** |
| Kill TaskManager during live traffic, recover from checkpoint, parity | **0 mismatches** (no double counting) |
| Blue/green rebuild into `fs_v2` with follow, switch all workers, parity + verify | 0 mismatches, pass |
| `make redeploy-job` (savepoint → new jar → resume), parity | **0 mismatches** |
| Demo cases vs. independent expectation (`make cases`) | 7/7 match |
| All 46 dashboard queries evaluated against live Prometheus | all return data |
| Under 25–38 events/s: produce → Scylla ack (`e2e_latency_ms`) | p50 ~185–215 ms, p95 ~245–480 ms, p99 ~0.35–1.1 s |
| Reload throughput | **~1.6–2k writes/s** (single Python process + 1-core Scylla) |

Notes:
- Right after restarting every container at once, 13 API requests (~1%) hit the 200 ms Scylla timeout and were served degraded. Warm runs had 0.
- Parity needs a quiet system. With `loadgen` running, the API is a few events ahead of parity's snapshot of the topics, which shows up as false mismatches. `make parity` pauses it.
- The reload number is a local floor, not a production estimate: production would use parallel loaders (Spark or many consumers) or SSTable load-and-stream on a real cluster. Measure it in a drill on target hardware.

## Simplified here vs. production

- RF=1, single node; production: RF=3 per DC, racks = AZs, `LOCAL_QUORUM`, Scylla Manager repairs/backups.
- One business timezone; per-user timezone would come from the profile.
- Profiles come from a `users` topic; production: Debezium CDC from the OLTP DB (compacted topic).
- No offline store yet: `features.changelog` and the raw topics are where an Iceberg sink job would attach (separate job, so lake slowness can't backpressure the online path).
- Expired buckets are filtered by the loader, not tombstoned in the changelog topic.
- Dedupe window is 7 days (`FS_DEDUP_TTL_HOURS`); a redelivery older than that would double count.
- Hashmap state backend; production: RocksDB with incremental checkpoints on S3.
- `LeveledCompactionStrategy` on `user_agg`; check ICS availability for your Scylla edition.
- `/admin/keyspace` has no auth.
- Changing a bucketing rule (like the reversal rule) changes the meaning of stored aggregates: existing buckets must be recomputed by replay (Backfill, Option A) into a new keyspace and switched blue/green. Locally the stack was simply rebuilt.
- The Grafana dashboard JSON is generated; edit it in Grafana (`allowUiUpdates`) and export, or edit the file.
