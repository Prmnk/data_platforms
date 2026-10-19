"""Independent re-implementation of the bucket rules, used to check what the API serves.

Deliberately not shared with the Flink job: a bug in one should show up as a mismatch.
"""
from collections import defaultdict
from datetime import datetime
from typing import Dict, Iterable, List, Tuple
from zoneinfo import ZoneInfo

DELTAS = {"create": 1, "refund": 0, "cancel": -1}


def expected_windows(events: Iterable[Tuple[str, dict]], tz: ZoneInfo, now: datetime) -> Tuple[Dict, int]:
    """events: (topic, event). Returns ({user: {(window, kind): [count, amount]}}, unique events) where
    window is today | mtd | ytd | month:YYYY-MM and kind is transactions | orders. Dedupes on event_id."""
    local_now = now.astimezone(tz)
    out = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    seen = set()
    for topic, e in events:
        if not e.get("event_id") or not e.get("user_id") or e.get("op", "create") not in DELTAS:
            continue  # the job drops these too
        if e["event_id"] in seen:
            continue
        seen.add(e["event_id"])
        cnt = DELTAS[e.get("op", "create")]
        # reversals are bucketed at the purchase they reverse
        t = e["event_time"]
        if cnt != 1 and e.get("original_event_time"):
            t = e["original_event_time"]
        local = datetime.fromtimestamp(t / 1000, tz)
        amt = e["amount"] if cnt == 1 else -e["amount"]
        windows: List[str] = ["month:" + local.strftime("%Y-%m")]
        if local.date() == local_now.date():
            windows.append("today")
        if local.strftime("%Y-%m") == local_now.strftime("%Y-%m"):
            windows.append("mtd")
        if local.year == local_now.year:
            windows.append("ytd")
        for w in windows:
            acc = out[e["user_id"]][(w, topic)]
            acc[0] += cnt
            acc[1] += amt
    return out, len(seen)


def compare(user: str, served: dict, exp: dict) -> List[str]:
    """Differences between an API response (nested format) and the expectation for one user."""
    problems = []
    for w in ("today", "mtd", "ytd"):
        for kind in ("transactions", "orders"):
            want = exp.get((w, kind), [0, 0])
            got = served[w][kind]
            if [got["count"], got["amount"]] != want:
                problems.append(f"{user} {w}.{kind}: got {got} want {want}")
    for m in served["monthly_12m"]:
        for kind, c, a in (("transactions", "txn_cnt", "txn_amt"), ("orders", "ord_cnt", "ord_amt")):
            want = exp.get(("month:" + m["month"], kind), [0, 0])
            if [m[c], m[a]] != want:
                problems.append(f"{user} {m['month']}.{kind}: got {[m[c], m[a]]} want {want}")
    return problems
