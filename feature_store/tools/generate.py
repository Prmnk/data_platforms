"""Synthetic users, transactions and orders.

1. publishes a profile per user to `users`
2. publishes --history-days of past events (seasonal: Nov/Dec busier) so month/year buckets fill up
3. streams live events at --rate events/sec for --live-seconds

Amounts are minor units. ~2% refunds/cancels, ~1% duplicate deliveries (deduped by the job).
"""
import argparse
import json
import os
import random
import time
import uuid
from datetime import date, timedelta

from confluent_kafka import Producer

SEASONALITY = {1: 0.8, 2: 0.8, 3: 0.9, 4: 0.9, 5: 1.0, 6: 1.0, 7: 1.0, 8: 1.1, 9: 1.0, 10: 1.2, 11: 1.5, 12: 1.7}


def producer() -> Producer:
    return Producer({"bootstrap.servers": os.getenv("FS_BROKERS", "localhost:19092"),
                     "linger.ms": 20, "batch.num.messages": 10000, "acks": "all"})


def send(p: Producer, topic: str, key: str, value: dict) -> None:
    while True:
        try:
            p.produce(topic, key=key, value=json.dumps(value))
            return
        except BufferError:
            p.poll(0.1)


def make_events(user: str, ts_ms: int, rng: random.Random):
    """One transaction, plus sometimes an order, a refund/cancel or a duplicate."""
    out = []
    txn = {"event_id": str(uuid.uuid4()), "user_id": user, "amount": rng.randint(5_000, 500_000),
           "event_time": ts_ms, "op": "create"}
    out.append(("transactions", txn))
    if rng.random() < 0.01:
        out.append(("transactions", txn))  # duplicate delivery
    if rng.random() < 0.02:
        out.append(("transactions", {**txn, "event_id": str(uuid.uuid4()), "op": "refund",
                                     "event_time": ts_ms + 60_000, "original_event_time": ts_ms}))
    if rng.random() < 0.5:
        order = {"event_id": str(uuid.uuid4()), "user_id": user, "amount": rng.randint(10_000, 800_000),
                 "event_time": ts_ms, "op": "create"}
        out.append(("orders", order))
        if rng.random() < 0.03:
            out.append(("orders", {**order, "event_id": str(uuid.uuid4()), "op": "cancel",
                                   "event_time": ts_ms + 120_000, "original_event_time": ts_ms}))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, default=500)
    ap.add_argument("--history-days", type=int, default=400)
    ap.add_argument("--txn-per-day", type=float, default=0.5, help="average per user")
    ap.add_argument("--live-seconds", type=int, default=60)
    ap.add_argument("--rate", type=int, default=50, help="live transactions/sec")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    p = producer()
    users = [f"user-{i:06d}" for i in range(args.users)]
    now_ms = int(time.time() * 1000)

    for u in users:
        signup = date.today() - timedelta(days=rng.randint(30, 3000))
        send(p, "users", u, {"user_id": u, "segment": rng.choice(["standard", "gold", "platinum"]),
                             "kyc_level": rng.choice(["min", "full"]), "signup_date": signup.isoformat(),
                             "updated_at": now_ms})

    sent = 0
    start_ms = now_ms - args.history_days * 86_400_000
    for u in users:
        n = int(args.history_days * args.txn_per_day * rng.uniform(0.3, 1.7))
        k = 0
        while k < n:
            ts = rng.randint(start_ms, now_ms)
            month = time.gmtime(ts / 1000).tm_mon
            if rng.random() > SEASONALITY[month] / 1.7:  # rejection sampling for seasonality
                continue
            k += 1
            for topic, ev in make_events(u, ts, rng):
                send(p, topic, u, ev)
                sent += 1
        p.poll(0)
    p.flush()
    print(f"history: {args.users} profiles, {sent} events over {args.history_days} days", flush=True)

    live = 0
    deadline = time.time() + args.live_seconds
    while time.time() < deadline:
        tick = time.time()
        for _ in range(args.rate):
            u = rng.choice(users)
            for topic, ev in make_events(u, int(time.time() * 1000), rng):
                send(p, topic, u, ev)
                live += 1
        p.poll(0)
        time.sleep(max(0.0, 1 - (time.time() - tick)))
    p.flush()
    print(f"live: {live} events in {args.live_seconds}s", flush=True)


if __name__ == "__main__":
    main()
