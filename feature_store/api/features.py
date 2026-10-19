"""Read-time assembly of features from online-store rows.

Pure functions only (no I/O) so the same code can run over offline rows for
training/backfill and be unit tested. Bucket labels must match the Flink job's
Buckets.java: D='YYYY-MM-DD', M='YYYY-MM', Y='YYYY', hourly rows keyed by hour start.
Amounts are minor units (paise / cents).
"""
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional
from zoneinfo import ZoneInfo

METRICS = ("txn_cnt", "txn_amt", "ord_cnt", "ord_amt")
PROFILE_FIELDS = ("segment", "kyc_level", "signup_date", "profile_ts")


def month_label(d: date, months_back: int = 0) -> str:
    y, m = divmod(d.year * 12 + d.month - 1 - months_back, 12)
    return f"{y:04d}-{m + 1:02d}"


def _metrics(row: Optional[Mapping[str, Any]]) -> Dict[str, int]:
    if not row:
        return {k: 0 for k in METRICS}
    return {k: int(row.get(k) or 0) for k in METRICS}


def _utc(ts: Optional[datetime]) -> Optional[datetime]:
    # the driver returns naive UTC datetimes
    if ts is None:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def _as_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    if isinstance(v, date):
        return v
    if hasattr(v, "date"):  # cassandra.util.Date
        return v.date()
    return date.fromisoformat(str(v))


def _ratio(num: float, den: float) -> Optional[float]:
    return round(num / den, 4) if den else None


def _split(m: Dict[str, int]) -> Dict[str, Dict[str, int]]:
    return {
        "transactions": {"count": m["txn_cnt"], "amount": m["txn_amt"]},
        "orders": {"count": m["ord_cnt"], "amount": m["ord_amt"]},
    }


def assemble(
    user_id: str,
    agg_rows: Iterable[Mapping[str, Any]],
    hourly_rows: Iterable[Mapping[str, Any]],
    now_utc: datetime,
    tz: ZoneInfo,
    current_txn_amt: Optional[int] = None,
    status: str = "ok",
) -> Dict[str, Any]:
    agg_rows = list(agg_rows)
    hourly_rows = list(hourly_rows)
    local = now_utc.astimezone(tz)
    today = local.date()

    by_key = {(r["grain"], r["bucket"]): r for r in agg_rows if r.get("grain")}
    day = _metrics(by_key.get(("D", today.isoformat())))
    mtd = _metrics(by_key.get(("M", month_label(today))))
    ytd = _metrics(by_key.get(("Y", str(today.year))))

    # oldest first: [12 months ago ... last month]
    months = [month_label(today, i) for i in range(12, 0, -1)]
    hist = [{"month": m, **_metrics(by_key.get(("M", m)))} for m in months]

    hour_start = local.replace(minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    this_hour = {k: 0 for k in METRICS}
    last_24h = {k: 0 for k in METRICS}
    for r in hourly_rows:
        h = _utc(r["hour"])
        if h >= hour_start - timedelta(hours=23):
            for k in METRICS:
                last_24h[k] += int(r.get(k) or 0)
        if h == hour_start:
            this_hour = _metrics(r)

    profile = None
    if agg_rows and agg_rows[0].get("profile_ts") is not None:
        p = agg_rows[0]  # static columns are repeated on every row
        signup = _as_date(p.get("signup_date"))
        profile = {
            "segment": p.get("segment"),
            "kyc_level": p.get("kyc_level"),
            "signup_date": signup.isoformat() if signup else None,
            "account_age_days": (today - signup).days if signup else None,
        }

    updated = [_utc(r.get("updated_at")) for r in agg_rows + hourly_rows if r.get("updated_at")]
    freshness = round((now_utc - max(updated)).total_seconds(), 3) if updated else None

    amt_12m = sum(h["txn_amt"] for h in hist)
    cnt_12m = sum(h["txn_cnt"] for h in hist)
    derived: Dict[str, Any] = {
        "avg_monthly_txn_amt_12m": round(amt_12m / 12, 2),
        "avg_txn_amt_12m": _ratio(amt_12m, cnt_12m),
        "txn_amt_mtd_vs_same_month_last_year": _ratio(mtd["txn_amt"], hist[0]["txn_amt"]),
        "ord_amt_mtd_vs_same_month_last_year": _ratio(mtd["ord_amt"], hist[0]["ord_amt"]),
    }
    if current_txn_amt is not None:
        # the transaction being scored is not in the stream yet: request-time features
        derived["current_txn_amt_vs_avg_txn_12m"] = _ratio(current_txn_amt, derived["avg_txn_amt_12m"] or 0)
        derived["txn_amt_today_incl_current"] = day["txn_amt"] + current_txn_amt
        derived["txn_cnt_today_incl_current"] = day["txn_cnt"] + 1

    return {
        "user_id": user_id,
        "as_of": local.isoformat(),
        "features_status": status,
        "has_history": bool(agg_rows),
        "freshness_sec": freshness,
        "profile": profile,
        "today": _split(day),
        "mtd": _split(mtd),
        "ytd": _split(ytd),
        "velocity": {"this_hour": _split(this_hour), "last_24h": _split(last_24h)},
        "monthly_12m": hist,
        "derived": derived,
    }


def flatten(resp: Dict[str, Any]) -> Dict[str, Any]:
    """Stable flat names for model consumption, e.g. txn_amt_mtd, txn_amt_m_minus_12."""
    flat: Dict[str, Any] = {"user_id": resp["user_id"], "has_history": resp["has_history"]}
    short = {"transactions": "txn", "orders": "ord"}
    for window in ("today", "mtd", "ytd"):
        for kind, vals in resp[window].items():
            flat[f"{short[kind]}_cnt_{window}"] = vals["count"]
            flat[f"{short[kind]}_amt_{window}"] = vals["amount"]
    for window, block in resp["velocity"].items():
        for kind, vals in block.items():
            flat[f"{short[kind]}_cnt_{window}"] = vals["count"]
            flat[f"{short[kind]}_amt_{window}"] = vals["amount"]
    n = len(resp["monthly_12m"])
    for i, m in enumerate(resp["monthly_12m"]):
        for k in METRICS:
            flat[f"{k}_m_minus_{n - i}"] = m[k]
    flat.update(resp["derived"])
    if resp["profile"]:
        flat.update({f"profile_{k}": v for k, v in resp["profile"].items()})
    return flat
