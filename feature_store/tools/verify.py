"""End-to-end check: publish known events for a fresh user, poll the API until the
served features equal the independently computed expectation, report freshness.

Covers: today/MTD/YTD, last-12-month history, refunds, order cancels (incl. one
reversing last month's order), duplicate delivery, a late (10-days-old) event,
profile static columns, hourly velocity.
"""
import json
import os
import sys
import time
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from confluent_kafka import Producer

from expect import compare, expected_windows

TZ = ZoneInfo(os.getenv("FS_TZ", "Asia/Kolkata"))
API = os.getenv("FS_API", "http://localhost:8000")


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def shift_month(local: datetime, months: int) -> datetime:
    y, m = divmod(local.year * 12 + local.month - 1 - months, 12)
    return local.replace(year=y, month=m + 1, day=min(local.day, 28), hour=12, minute=0)


def main() -> int:
    user = f"verify-{uuid.uuid4().hex[:8]}"
    now = datetime.now(TZ)
    t_now = ms(now)

    def ev(kind, amount, ts, op="create", eid=None, original=None):
        e = {"event_id": eid or str(uuid.uuid4()), "user_id": user, "amount": amount,
             "event_time": ts, "op": op}
        if original is not None:
            e["original_event_time"] = original
        return kind, e

    dup_id = str(uuid.uuid4())
    last_month = ms(shift_month(now, 1))
    events = [
        ev("transactions", 10_000, t_now),
        ev("transactions", 20_000, t_now, eid=dup_id),
        ev("transactions", 20_000, t_now, eid=dup_id),           # duplicate delivery
        ev("transactions", 30_000, t_now),
        ev("transactions", 5_000, t_now, op="refund", original=t_now),
        ev("orders", 15_000, t_now),
        ev("orders", 25_000, t_now),
        ev("orders", 15_000, t_now, op="cancel", original=t_now),
        ev("transactions", 40_000, last_month),                  # last month
        ev("orders", 33_000, last_month),
        ev("orders", 33_000, t_now, op="cancel", original=last_month),  # reversal lands last month, not today
        ev("transactions", 70_000, ms(shift_month(now, 12))),    # same month last year
        ev("transactions", 11_000, ms(now - timedelta(days=10))),  # late event: day bucket expired
    ]

    # expectation, computed independently of the job
    exp = expected_windows(events, TZ, datetime.now(timezone.utc))[0][user]

    p = Producer({"bootstrap.servers": os.getenv("FS_BROKERS", "localhost:19092"), "acks": "all"})
    p.produce("users", key=user, value=json.dumps({"user_id": user, "segment": "gold", "kyc_level": "full",
                                                    "signup_date": "2021-03-02", "updated_at": t_now}))
    for topic, e in events:
        p.produce(topic, key=user, value=json.dumps(e))
    p.flush()
    published = time.time()
    print(f"published {len(events)} events for {user}")

    last_month_label = shift_month(now, 1).strftime("%Y-%m")

    def check(r):
        problems = compare(user, r, exp)
        # fixed assertions for the reversal rule, independent of expect.py
        if r["today"]["orders"] != {"count": 1, "amount": 25_000}:
            problems.append(f"today.orders should ignore last month's cancel: {r['today']['orders']}")
        lm = [m for m in r["monthly_12m"] if m["month"] == last_month_label][0]
        if (lm["ord_cnt"], lm["ord_amt"]) != (0, 0):
            problems.append(f"{last_month_label} orders should be netted to 0 by the cancel: {lm}")
        if not r["profile"] or r["profile"]["segment"] != "gold":
            problems.append(f"profile: {r['profile']}")
        if r["velocity"]["last_24h"]["transactions"]["count"] < 3:
            problems.append(f"velocity: {r['velocity']}")
        return problems

    problems = ["not polled yet"]
    r = None
    deadline = time.time() + 120
    while time.time() < deadline:
        with urllib.request.urlopen(f"{API}/v1/users/{user}/features?current_txn_amt=50000") as resp:
            r = json.load(resp)
        problems = check(r)
        if not problems:
            break
        time.sleep(0.2)

    if problems:
        print("FAIL")
        for line in problems:
            print("  -", line)
        print(json.dumps(r, indent=2))
        return 1
    print(f"PASS: features correct {time.time() - published:.2f}s after publish "
          f"(includes {os.getenv('FS_FLUSH_MS', '100')}ms flush window), API served in {r['served_in_ms']}ms")
    print(json.dumps({k: r[k] for k in ("today", "mtd", "ytd", "derived", "profile")}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
