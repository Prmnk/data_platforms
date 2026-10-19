"""Online/offline parity check: recompute features from the raw topics (the source of
truth) and compare with what the API serves. In production this runs daily over the
lake for a sample of users; here it replays the topics.

    python parity.py --sample 200
"""
import argparse
import json
import os
import random
import sys
import urllib.request
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from confluent_kafka import Consumer, KafkaError

from expect import compare, expected_windows

TZ = ZoneInfo(os.getenv("FS_TZ", "Asia/Kolkata"))
API = os.getenv("FS_API", "http://localhost:8000")


def read_all(topics):
    c = Consumer({"bootstrap.servers": os.getenv("FS_BROKERS", "localhost:19092"),
                  "group.id": f"parity-{uuid.uuid4().hex[:8]}", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False, "enable.partition.eof": True})
    meta = c.list_topics(timeout=10).topics
    parts = sum(len(meta[t].partitions) for t in topics)
    c.subscribe(topics)
    eof = set()
    try:
        while len(eof) < parts:
            for m in c.consume(5000, timeout=1.0):
                if m.error():
                    if m.error().code() == KafkaError._PARTITION_EOF:
                        eof.add((m.topic(), m.partition()))
                        continue
                    raise RuntimeError(m.error())
                try:
                    yield m.topic(), json.loads(m.value())
                except (TypeError, ValueError):
                    continue  # malformed records are dropped by the job too
    finally:
        c.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=200)
    ap.add_argument("--prefix", default="user-", help="only check users with this id prefix")
    args = ap.parse_args()

    expected, unique_events = expected_windows(read_all(["transactions", "orders"]), TZ, datetime.now(timezone.utc))
    users = sorted(u for u in expected if u.startswith(args.prefix))
    sample = random.Random(1).sample(users, min(args.sample, len(users)))
    mismatches = 0
    degraded = 0
    for u in sample:
        # a degraded response (store timeout) is an availability event, not a data mismatch: retry it
        for _ in range(3):
            with urllib.request.urlopen(f"{API}/v1/users/{u}/features") as resp:
                r = json.load(resp)
            if r["features_status"] == "ok":
                break
            degraded += 1
        else:
            print(f"DEGRADED {u}: store unavailable after 3 attempts")
            mismatches += 1
            continue
        for problem in compare(u, r, expected[u]):
            mismatches += 1
            print("MISMATCH", problem)

    checks = len(sample) * (6 + 24)
    print(f"parity: {len(sample)} users, {checks} feature values compared, {mismatches} mismatches "
          f"({unique_events} unique events replayed; {degraded} degraded responses retried)")
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
