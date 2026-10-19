"""Online feature-serving API.

Reads ONLY the online store (Scylla). One single-partition read for D/M/Y buckets +
profile, and one for hourly velocity buckets, issued in parallel. On store errors it
returns defaults with features_status="degraded" instead of failing or falling back
to the lake; the calling risk service decides what degraded means.
"""
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import dict_factory
from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Histogram, generate_latest, multiprocess
from pydantic import BaseModel

from features import assemble, flatten

log = logging.getLogger("feature-api")

HOSTS = os.getenv("FS_SCYLLA_HOSTS", "localhost").split(",")
LOCAL_DC = os.getenv("FS_SCYLLA_DC", "datacenter1")
TZ = ZoneInfo(os.getenv("FS_TZ", "Asia/Kolkata"))
TIMEOUT_S = float(os.getenv("FS_REQUEST_TIMEOUT_S", "0.2"))
BROKERS = os.getenv("FS_BROKERS")
SERVED_TOPIC = os.getenv("FS_SERVED_TOPIC")
CONFIG_POLL_S = float(os.getenv("FS_CONFIG_POLL_S", "2"))

# Multiprocess mode (PROMETHEUS_MULTIPROC_DIR): every uvicorn worker writes, /metrics aggregates.
LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.0075, 0.01, 0.015, 0.025, 0.05, 0.1, 0.25, 0.5, 1)
REQUESTS = Counter("fs_api_requests_total", "Feature requests served", ["status"])
REQUEST_SECONDS = Histogram("fs_api_request_seconds", "End-to-end feature request time", buckets=LATENCY_BUCKETS)
STORE_SECONDS = Histogram("fs_api_store_read_seconds", "Scylla read time (both queries, parallel)", buckets=LATENCY_BUCKETS)
FEATURE_AGE = Histogram("fs_api_feature_age_seconds", "Now minus the user's latest bucket update (user activity recency)",
                        buckets=(1, 10, 60, 600, 3600, 86400, 7 * 86400, 30 * 86400))


class Store:
    def __init__(self) -> None:
        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            request_timeout=TIMEOUT_S,
            row_factory=dict_factory,
        )
        self.cluster = Cluster(HOSTS, execution_profiles={EXEC_PROFILE_DEFAULT: profile}, protocol_version=4)
        self.session = None
        self.lock = threading.Lock()
        self.keyspace = None

    def connect(self, default_keyspace: str) -> None:
        for attempt in range(30):
            try:
                self.session = self.cluster.connect()
                break
            except Exception as e:  # noqa: BLE001 - startup retry
                log.warning("scylla not ready (%s): %s", attempt, e)
                time.sleep(2)
        else:
            raise RuntimeError("could not connect to scylla")
        self.default_keyspace = default_keyspace
        self.get_live = self.session.prepare("SELECT active_ks FROM fs_meta.serving WHERE name = 'online'")
        self.set_live = self.session.prepare(
            "INSERT INTO fs_meta.serving (name, active_ks, updated_at) VALUES ('online', ?, toTimestamp(now()))")
        self.use(self.live_keyspace())
        threading.Thread(target=self._watch_config, daemon=True).start()

    def live_keyspace(self) -> str:
        row = self.session.execute(self.get_live, timeout=2).one()
        return row["active_ks"] if row else self.default_keyspace

    def _watch_config(self) -> None:
        """Every worker/instance follows the shared serving config, so a switch reaches all of them."""
        while True:
            time.sleep(CONFIG_POLL_S)
            try:
                ks = self.live_keyspace()
                if ks != self.keyspace:
                    self.use(ks)
                    log.warning("switched serving keyspace to %s", ks)
            except Exception as e:  # noqa: BLE001 - keep serving the current keyspace
                log.warning("serving-config poll failed: %s", e)

    def publish_live(self, keyspace: str) -> None:
        self.use(keyspace)  # fails fast if the keyspace/tables don't exist
        self.session.execute(self.set_live, [keyspace], timeout=2)

    def use(self, keyspace: str) -> None:
        """Blue/green switch: prepare against another keyspace, then swap atomically."""
        agg = self.session.prepare(
            f"SELECT grain, bucket, txn_cnt, txn_amt, ord_cnt, ord_amt, updated_at, "
            f"segment, kyc_level, signup_date, profile_ts FROM {keyspace}.user_agg WHERE user_id = ?"
        )
        hourly = self.session.prepare(
            f"SELECT hour, txn_cnt, txn_amt, ord_cnt, ord_amt, updated_at "
            f"FROM {keyspace}.user_agg_hourly WHERE user_id = ? AND hour >= ?"
        )
        with self.lock:
            self.agg_stmt, self.hourly_stmt, self.keyspace = agg, hourly, keyspace

    def fetch(self, user_id: str, now_utc: datetime):
        with self.lock:
            agg_stmt, hourly_stmt = self.agg_stmt, self.hourly_stmt
        since = now_utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=23)
        f_agg = self.session.execute_async(agg_stmt, [user_id])
        f_hourly = self.session.execute_async(hourly_stmt, [user_id, since])
        return list(f_agg.result()), list(f_hourly.result())


class ServedLog:
    """Best-effort log of every served vector (training data / skew checks). Never blocks serving."""

    def __init__(self) -> None:
        self.producer = None
        if BROKERS and SERVED_TOPIC:
            from confluent_kafka import Producer

            self.producer = Producer({"bootstrap.servers": BROKERS, "linger.ms": 50, "acks": "1"})

    def log(self, payload: dict) -> None:
        if not self.producer:
            return
        try:
            self.producer.produce(SERVED_TOPIC, key=payload["user_id"], value=json.dumps(payload, default=str))
            self.producer.poll(0)
        except Exception as e:  # noqa: BLE001
            log.warning("served-feature logging failed: %s", e)


store = Store()
served = ServedLog()


@asynccontextmanager
async def lifespan(_: FastAPI):
    store.connect(os.getenv("FS_KEYSPACE", "fs"))
    yield
    if served.producer:
        served.producer.flush(2)
    store.cluster.shutdown()


app = FastAPI(title="feature-api", lifespan=lifespan)


def serve(user_id: str, current_txn_amt: Optional[int], fmt: str) -> dict:
    started = time.perf_counter()
    now = datetime.now(timezone.utc)
    try:
        with STORE_SECONDS.time():
            agg_rows, hourly_rows = store.fetch(user_id, now)
        resp = assemble(user_id, agg_rows, hourly_rows, now, TZ, current_txn_amt)
    except Exception as e:  # noqa: BLE001 - degrade, never fail the caller
        log.warning("online store read failed for %s: %s", user_id, e)
        resp = assemble(user_id, [], [], now, TZ, current_txn_amt, status="degraded")
    out = flatten(resp) if fmt == "flat" else resp
    elapsed = time.perf_counter() - started
    out["served_in_ms"] = round(elapsed * 1000, 2)
    REQUEST_SECONDS.observe(elapsed)
    status = resp["features_status"]
    if status == "ok" and not resp["has_history"]:
        status = "no_history"
    REQUESTS.labels(status).inc()
    if resp["freshness_sec"] is not None:
        FEATURE_AGE.observe(resp["freshness_sec"])
    served.log({"user_id": user_id, "served_at": now.isoformat(), "features": flatten(resp)})
    return out


@app.get("/metrics")
def metrics():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health():
    return {"status": "ok", "keyspace": store.keyspace}


@app.get("/v1/users/{user_id}/features")
def get_features(user_id: str, current_txn_amt: Optional[int] = None, format: str = "nested"):
    return serve(user_id, current_txn_amt, format)


class BatchRequest(BaseModel):
    ids: List[str]
    format: str = "flat"


@app.post("/v1/features")
def batch_features(req: BatchRequest):
    if len(req.ids) > 500:
        raise HTTPException(400, "max 500 ids per request")
    return {"results": [serve(uid, None, req.format) for uid in req.ids]}


class KeyspaceSwitch(BaseModel):
    keyspace: str


@app.post("/admin/keyspace")
def switch_keyspace(req: KeyspaceSwitch):
    # local demo only (no auth): writes the shared serving config; all workers follow within FS_CONFIG_POLL_S
    if not req.keyspace.replace("_", "").isalnum():
        raise HTTPException(400, "invalid keyspace name")
    try:
        store.publish_live(req.keyspace)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(400, f"cannot serve from {req.keyspace}: {e}")
    return {"keyspace": req.keyspace, "propagates_within_s": CONFIG_POLL_S}
