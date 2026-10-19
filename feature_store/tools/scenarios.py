"""Curated demo users with known behaviour, published through the real pipeline.

    python scenarios.py            # publish, wait until served features match, save samples
    python scenarios.py --no-publish   # only re-capture served features

Writes:
  /samples/input/{users,transactions,orders}.jsonl   exactly what was published
  /samples/features/<user>.json                       API response (nested) per user
  /samples/features/flat_vectors.json                 model-ready flat vectors for all users

Event ids are stable (uuid5 of user + sequence), so re-running within the 7-day dedupe
window does not double count.
"""
import argparse
import json
import os
import random
import sys
import time
import urllib.request
import uuid
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from confluent_kafka import Producer

from expect import compare, expected_windows

TZ = ZoneInfo(os.getenv("FS_TZ", "Asia/Kolkata"))
API = os.getenv("FS_API", "http://localhost:8000")
OUT = os.getenv("FS_SAMPLES", "/samples")
NS = uuid.UUID("5f0c2a3e-8a3b-4c1e-9a43-3b7d1f0e2c11")

CASES = {
    "demo-steady": "Regular customer: a ~Rs.1,250 purchase every 3 days and a weekly order for 13 months.",
    "demo-seasonal": "Festive shopper: 4 purchases a month, but 25 bigger ones a month in Oct-Dec last year.",
    "demo-burst": "Fraud-like: small weekly spend for 10 months, then 12 large card transactions in the last 20 minutes.",
    "demo-refunds": "Refund-heavy: 15 purchases this month, 7 refunded; 6 orders over 2 days, 3 cancelled today.",
    "demo-new": "New user: signed up 2 days ago, 3 small purchases today, no history.",
    "demo-dupes": "Producer retries: 5 purchases today, each delivered 3 times, plus 1 malformed and 1 unknown-op event.",
    "demo-unknown": "Never seen: no events at all (API returns zeros, has_history=false).",
}


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def month_start(local: datetime, months_back: int) -> datetime:
    y, m = divmod(local.year * 12 + local.month - 1 - months_back, 12)
    return local.replace(year=y, month=m + 1, day=1, hour=12, minute=0, second=0, microsecond=0)


def build(now_local: datetime):
    """Returns (profiles, [(topic, event)]) for every case."""
    profiles, events = [], []
    seq = {}

    def ev(topic, user, amount, when, op="create", eid=None, original=None):
        seq[user] = seq.get(user, 0) + 1
        e = {"event_id": eid or str(uuid.uuid5(NS, f"{user}/{seq[user]}")), "user_id": user,
             "amount": amount, "event_time": ms(when), "op": op}
        if original is not None:
            e["original_event_time"] = original["event_time"]
        events.append((topic, e))
        return e

    def profile(user, segment, kyc, signup: date):
        profiles.append({"user_id": user, "segment": segment, "kyc_level": kyc,
                         "signup_date": signup.isoformat(), "updated_at": ms(now_local)})

    today = now_local.date()

    # steady
    u, rng = "demo-steady", random.Random(1)
    profile(u, "standard", "full", date(2023, 5, 10))
    for d in range(0, 395, 3):
        ev("transactions", u, rng.randint(100_000, 150_000), now_local - timedelta(days=d, hours=1))
    for d in range(0, 395, 7):
        ev("orders", u, rng.randint(200_000, 300_000), now_local - timedelta(days=d, hours=2))

    # seasonal
    u, rng = "demo-seasonal", random.Random(2)
    profile(u, "gold", "full", date(2022, 1, 15))
    for k in range(0, 13):
        start = month_start(now_local, k)
        festive = start.month in (10, 11, 12) and start.year == now_local.year - 1
        n, lo, hi = (25, 300_000, 800_000) if festive else (4, 100_000, 300_000)
        last_day = today.day if k == 0 else 28
        for _ in range(n):
            when = start.replace(day=rng.randint(1, last_day))
            if when > now_local:
                when = now_local - timedelta(minutes=30)
            ev("transactions", u, rng.randint(lo, hi), when)
        if k == 0:
            for _ in range(6):  # this month so far, to compare against last year's festive month
                ev("transactions", u, rng.randint(lo, hi), now_local - timedelta(hours=rng.randint(1, 72)))

    # burst
    u, rng = "demo-burst", random.Random(3)
    profile(u, "standard", "min", date(2025, 12, 1))
    for d in range(1, 300, 4):
        ev("transactions", u, rng.randint(50_000, 90_000), now_local - timedelta(days=d, hours=3))
    for i in range(12):
        ev("transactions", u, rng.randint(900_000, 1_500_000), now_local - timedelta(minutes=20 - i * 1.5))

    # refunds
    u, rng = "demo-refunds", random.Random(4)
    profile(u, "gold", "full", date(2024, 2, 20))
    purchases = [ev("transactions", u, rng.randint(200_000, 500_000),
                    now_local - timedelta(hours=rng.randint(1, max(2, (today.day - 1) * 24 + now_local.hour))))
                 for _ in range(15)]
    for p in purchases[:7]:
        ev("transactions", u, p["amount"], datetime.fromtimestamp(p["event_time"] / 1000, TZ) + timedelta(minutes=30),
           op="refund", original=p)
    orders = [ev("orders", u, rng.randint(300_000, 900_000), now_local - timedelta(hours=rng.randint(1, 48)))
              for _ in range(6)]
    for o in orders[:3]:
        ev("orders", u, o["amount"], now_local - timedelta(minutes=10), op="cancel", original=o)

    # new
    u, rng = "demo-new", random.Random(5)
    profile(u, "standard", "min", today - timedelta(days=2))
    for i in range(3):
        ev("transactions", u, rng.randint(20_000, 200_000), now_local - timedelta(minutes=40 - i * 10))

    # dupes
    u, rng = "demo-dupes", random.Random(6)
    profile(u, "platinum", "full", date(2020, 8, 1))
    for i in range(5):
        e = ev("transactions", u, rng.randint(100_000, 300_000), now_local - timedelta(minutes=50 - i * 5))
        events.extend([("transactions", dict(e)), ("transactions", dict(e))])  # redelivered twice
    events.append(("transactions", {"event_id": str(uuid.uuid5(NS, "malformed")), "amount": 999,
                                    "event_time": ms(now_local), "op": "create"}))  # no user_id
    ev("transactions", u, 777_700, now_local, op="chargeback")  # unknown op -> invalid_events metric
    return profiles, events


def fetch(user, current_txn_amt=None, fmt="nested"):
    q = []
    if current_txn_amt:
        q.append(f"current_txn_amt={current_txn_amt}")
    if fmt != "nested":
        q.append(f"format={fmt}")
    url = f"{API}/v1/users/{user}/features" + ("?" + "&".join(q) if q else "")
    with urllib.request.urlopen(url) as r:
        return url, json.load(r)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-publish", action="store_true")
    args = ap.parse_args()

    now_local = datetime.now(TZ).replace(microsecond=0)
    profiles, events = build(now_local)
    os.makedirs(f"{OUT}/input", exist_ok=True)
    os.makedirs(f"{OUT}/features", exist_ok=True)

    if not args.no_publish:
        p = Producer({"bootstrap.servers": os.getenv("FS_BROKERS", "localhost:19092"), "acks": "all"})
        with open(f"{OUT}/input/users.jsonl", "w") as f:
            for pr in profiles:
                p.produce("users", key=pr["user_id"], value=json.dumps(pr))
                f.write(json.dumps(pr) + "\n")
        files = {t: open(f"{OUT}/input/{t}.jsonl", "w") for t in ("transactions", "orders")}
        for topic, e in events:
            p.produce(topic, key=e.get("user_id", ""), value=json.dumps(e))
            files[topic].write(json.dumps(e) + "\n")
        for f in files.values():
            f.close()
        p.flush()
        print(f"published {len(profiles)} profiles and {len(events)} events")

    expected, _ = expected_windows(events, TZ, datetime.now(timezone.utc))
    users = [u for u in CASES if u != "demo-unknown"]
    deadline = time.time() + 90
    while True:
        problems = []
        for u in users:
            problems += compare(u, fetch(u)[1], expected[u])
        if not problems or time.time() > deadline:
            break
        time.sleep(0.5)
    if problems:
        print("served features do not match expectation:")
        print("\n".join("  " + p for p in problems[:20]))
        return 1

    flat = {}
    for u, desc in CASES.items():
        amt = 2_500_000 if u == "demo-burst" else None  # score a Rs.25,000 card payment for the burst user
        url, resp = fetch(u, amt)
        resp.pop("served_in_ms", None)
        with open(f"{OUT}/features/{u}.json", "w") as f:
            json.dump({"case": desc, "request": url.replace(API, ""), "response": resp}, f, indent=2)
        flat[u] = {k: v for k, v in fetch(u, amt, "flat")[1].items() if k != "served_in_ms"}
    with open(f"{OUT}/features/flat_vectors.json", "w") as f:
        json.dump(flat, f, indent=2)

    print("all cases match the independently computed expectation\n")
    rupees = lambda paise: f"{paise / 100:,.0f}"
    print(f"{'user':14} {'today txn':>14} {'MTD txn':>16} {'YTD txn':>18} {'last 24h':>9} {'MTD vs LY':>9}")
    for u in CASES:
        r = json.load(open(f"{OUT}/features/{u}.json"))["response"]
        t, m, y = (r[w]["transactions"] for w in ("today", "mtd", "ytd"))
        ratio = r["derived"]["txn_amt_mtd_vs_same_month_last_year"]
        print(f"{u:14} {t['count']:>3} Rs.{rupees(t['amount']):>8} {m['count']:>4} Rs.{rupees(m['amount']):>9} "
              f"{y['count']:>4} Rs.{rupees(y['amount']):>11} {r['velocity']['last_24h']['transactions']['count']:>9} "
              f"{'-' if ratio is None else ratio:>9}")
    print(f"\nsaved to {OUT}/input and {OUT}/features")
    return 0


if __name__ == "__main__":
    sys.exit(main())
