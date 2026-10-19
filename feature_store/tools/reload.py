"""Rebuild the online store into a fresh keyspace from the compacted topics (blue/green).

    python reload.py --target fs_v2            # load to end of topics, then exit
    python reload.py --target fs_v2 --follow   # keep tailing: acts as the dual-write until the switch

Every row is written with its original version as USING TIMESTAMP and its remaining
TTL, so order does not matter and running alongside the live job is safe.
Then: curl -XPOST localhost:8000/admin/keyspace -d '{"keyspace":"fs_v2"}' -H 'content-type: application/json'
"""
import argparse
import json
import os
import re
import threading
import time
import uuid
from datetime import date, datetime, timezone

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from confluent_kafka import Consumer, KafkaError


def create_keyspace(session, target: str) -> None:
    with open("/schema/schema.cql") as f:
        cql = f.read()
    cql = "\n".join(l for l in cql.splitlines() if not l.strip().startswith("--"))
    cql = cql.replace("EXISTS fs\n", f"EXISTS {target}\n").replace("fs.", f"{target}.")
    for stmt in filter(None, (s.strip() for s in cql.split(";"))):
        session.execute(stmt, timeout=60)


class Writer:
    """Bounded-concurrency async writer; raises on the first failed write."""

    def __init__(self, session, concurrency: int):
        self.session = session
        self.sem = threading.Semaphore(concurrency)
        self.concurrency = concurrency
        self.error = None
        self.done = 0

    def submit(self, stmt, params) -> None:
        if self.error:
            raise self.error
        self.sem.acquire()
        f = self.session.execute_async(stmt, params)
        f.add_callbacks(self._ok, self._fail)

    def _ok(self, _):
        self.done += 1
        self.sem.release()

    def _fail(self, exc):
        self.error = exc
        self.sem.release()

    def drain(self) -> None:
        for _ in range(self.concurrency):
            self.sem.acquire()
        for _ in range(self.concurrency):
            self.sem.release()
        if self.error:
            raise self.error


def consume(handlers: dict, follow: bool, on_caught_up=None) -> dict:
    """Read all topics from the beginning; stop at the end unless following."""
    c = Consumer({"bootstrap.servers": os.getenv("FS_BROKERS", "localhost:19092"),
                  "group.id": f"reload-{uuid.uuid4().hex[:8]}", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False, "enable.partition.eof": True})
    meta = c.list_topics(timeout=10).topics
    parts = sum(len(meta[t].partitions) for t in handlers)
    c.subscribe(list(handlers))
    eof, counts, caught_up = set(), {t: 0 for t in handlers}, False
    try:
        while True:
            for m in c.consume(1000, timeout=1.0):
                if m.error():
                    if m.error().code() == KafkaError._PARTITION_EOF:
                        eof.add((m.topic(), m.partition()))
                        continue
                    raise RuntimeError(m.error())
                eof.discard((m.topic(), m.partition()))
                if m.value() is not None:
                    handlers[m.topic()](json.loads(m.value()))
                    counts[m.topic()] += 1
            if len(eof) == parts and not caught_up:
                caught_up = True
                if on_caught_up:
                    on_caught_up(counts)
                if not follow:
                    return counts
    finally:
        c.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True)
    ap.add_argument("--concurrency", type=int, default=256)
    ap.add_argument("--follow", action="store_true")
    args = ap.parse_args()
    if not re.fullmatch(r"[a-z][a-z0-9_]*", args.target):
        raise SystemExit("invalid keyspace name")

    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=os.getenv("FS_SCYLLA_DC", "datacenter1"))),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM, request_timeout=10)
    cluster = Cluster(os.getenv("FS_SCYLLA_HOSTS", "localhost").split(","),
                      execution_profiles={EXEC_PROFILE_DEFAULT: profile}, protocol_version=4)
    session = cluster.connect()
    create_keyspace(session, args.target)
    ks = args.target
    ins_agg = session.prepare(f"INSERT INTO {ks}.user_agg (user_id, grain, bucket, txn_cnt, txn_amt, ord_cnt, "
                              f"ord_amt, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? AND TIMESTAMP ?")
    ins_hourly = session.prepare(f"INSERT INTO {ks}.user_agg_hourly (user_id, hour, txn_cnt, txn_amt, ord_cnt, "
                                 f"ord_amt, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL ? AND TIMESTAMP ?")
    upd_profile = session.prepare(f"UPDATE {ks}.user_agg USING TIMESTAMP ? SET segment = ?, kyc_level = ?, "
                                  f"signup_date = ?, profile_ts = ? WHERE user_id = ?")
    w = Writer(session, args.concurrency)
    skipped = 0

    def ts(ms_: int) -> datetime:
        return datetime.fromtimestamp(ms_ / 1000, timezone.utc)

    def on_profile(p: dict) -> None:
        signup = date.fromisoformat(p["signup_date"]) if p.get("signup_date") else None
        w.submit(upd_profile, [p["updated_at"] * 1000, p.get("segment"), p.get("kyc_level"), signup,
                               ts(p["updated_at"]), p["user_id"]])

    def on_bucket(r: dict) -> None:
        nonlocal skipped
        ttl = int((r["expires_at_ms"] - time.time() * 1000) / 1000)
        if ttl <= 0:
            skipped += 1
            return
        vals = [r["txn_cnt"], r["txn_amt"], r["ord_cnt"], r["ord_amt"], ts(r["updated_at_ms"]), ttl, r["version"]]
        if r["grain"] == "H":
            w.submit(ins_hourly, [r["user_id"], ts(r["bucket_start_ms"]), *vals])
        else:
            w.submit(ins_agg, [r["user_id"], r["grain"], r["bucket"], *vals])

    started = time.time()

    def report(counts: dict) -> None:
        w.drain()
        elapsed = time.time() - started
        print(f"reloaded into {ks}: {counts['users']} profiles, {counts['features.changelog'] - skipped} bucket rows "
              f"({skipped} already expired, skipped) in {elapsed:.1f}s = {w.done / elapsed:,.0f} writes/s", flush=True)
        if args.follow:
            print(f"caught up; tailing users + features.changelog into {ks} (Ctrl-C to stop)", flush=True)

    consume({"users": on_profile, "features.changelog": on_bucket}, args.follow, report)
    cluster.shutdown()


if __name__ == "__main__":
    main()
