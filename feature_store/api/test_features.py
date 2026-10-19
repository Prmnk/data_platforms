from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from features import assemble, flatten, month_label

IST = ZoneInfo("Asia/Kolkata")
NOW = datetime(2026, 10, 19, 9, 0, tzinfo=timezone.utc)  # 14:30 IST


def row(grain, bucket, tc=0, ta=0, oc=0, oa=0, **extra):
    return {"grain": grain, "bucket": bucket, "txn_cnt": tc, "txn_amt": ta,
            "ord_cnt": oc, "ord_amt": oa, "updated_at": datetime(2026, 10, 19, 8, 59, 58), **extra}


def test_month_label_wraps_years():
    assert month_label(date(2026, 1, 15), 1) == "2025-12"
    assert month_label(date(2026, 10, 19), 12) == "2025-10"
    assert month_label(date(2026, 10, 19), 0) == "2026-10"


def test_today_mtd_ytd_and_history():
    rows = [
        row("D", "2026-10-19", 3, 600, 1, 100, segment="gold", kyc_level="full",
            signup_date=date(2021, 3, 2), profile_ts=datetime(2026, 1, 1)),
        row("D", "2026-10-18", 9, 9999),       # yesterday: not today
        row("M", "2026-10", 10, 2000, 4, 400),
        row("M", "2026-09", 7, 1500),
        row("M", "2025-10", 5, 1000),          # same month last year
        row("M", "2025-09", 99, 99999),        # 13 months ago: outside 12m window
        row("Y", "2026", 50, 20000, 20, 4000),
    ]
    r = assemble("u1", rows, [], NOW, IST, current_txn_amt=300)
    assert r["today"]["transactions"] == {"count": 3, "amount": 600}
    assert r["mtd"]["transactions"] == {"count": 10, "amount": 2000}
    assert r["ytd"]["orders"] == {"count": 20, "amount": 4000}
    assert [m["month"] for m in r["monthly_12m"]][0] == "2025-10"
    assert [m["month"] for m in r["monthly_12m"]][-1] == "2026-09"
    assert sum(m["txn_amt"] for m in r["monthly_12m"]) == 2500
    assert r["derived"]["txn_amt_mtd_vs_same_month_last_year"] == 2.0
    assert r["derived"]["avg_txn_amt_12m"] == round(2500 / 12, 4)
    assert r["derived"]["txn_amt_today_incl_current"] == 900
    assert r["profile"]["account_age_days"] == (date(2026, 10, 19) - date(2021, 3, 2)).days
    assert r["freshness_sec"] == 2.0
    flat = flatten(r)
    assert flat["txn_amt_mtd"] == 2000 and flat["txn_amt_m_minus_12"] == 1000 and flat["txn_amt_m_minus_1"] == 1500


def test_day_rollover_uses_local_timezone():
    # 19:00Z on Oct 18 is already Oct 19 00:30 IST -> yesterday's bucket is not "today"
    now = datetime(2026, 10, 18, 19, 0, tzinfo=timezone.utc)
    r = assemble("u1", [row("D", "2026-10-18", 4, 400)], [], now, IST)
    assert r["today"]["transactions"]["count"] == 0


def test_velocity_from_hourly_rows():
    # IST is +05:30, so the local hour 14:00 starts at 08:30Z
    hourly = [
        {"hour": datetime(2026, 10, 19, 8, 30), "txn_cnt": 2, "txn_amt": 50, "ord_cnt": 0, "ord_amt": 0},
        {"hour": datetime(2026, 10, 18, 12, 30), "txn_cnt": 5, "txn_amt": 500, "ord_cnt": 0, "ord_amt": 0},
        {"hour": datetime(2026, 10, 18, 8, 30), "txn_cnt": 100, "txn_amt": 1, "ord_cnt": 0, "ord_amt": 0},  # 24h ago: out
    ]
    r = assemble("u1", [], hourly, NOW, IST)
    assert r["velocity"]["this_hour"]["transactions"]["count"] == 2
    assert r["velocity"]["last_24h"]["transactions"]["count"] == 7


def test_unknown_user_gets_zeros():
    r = assemble("nobody", [], [], NOW, IST)
    assert r["has_history"] is False
    assert r["ytd"]["transactions"] == {"count": 0, "amount": 0}
    assert r["profile"] is None
    assert r["derived"]["txn_amt_mtd_vs_same_month_last_year"] is None
