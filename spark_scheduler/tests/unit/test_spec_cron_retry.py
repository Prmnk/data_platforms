from datetime import datetime

import pytest
from pydantic import ValidationError

from scheduler import retry
from scheduler.cron import due_times, next_fire
from scheduler.spec import JobSpec, RetryPolicy, load_job_file

T0 = datetime(2026, 1, 1, 12, 0)


def test_load_job_file_and_defaults():
    jf = load_job_file("""
jobs:
  - name: a
    app: /x/a.py
    schedule: "*/5 * * * *"
    spark_conf: {spark.executor.memory: 1g, spark.cores.max: 2}
    retry: {max_attempts: 3, backoff: fixed, initial_delay_s: 10}
  - name: b
    app: /x/b.py
    engine: sail
""")
    a, b = jf.jobs
    assert a.engine == "spark" and b.engine == "sail"
    assert a.spark_conf["spark.cores.max"] == "2"
    assert a.retry.max_attempts == 3 and a.retry.backoff == "fixed"
    assert b.schedule is None and b.retry.max_attempts == 1 and b.concurrency == 1


@pytest.mark.parametrize("bad", [
    {"name": "bad name", "app": "x"},
    {"name": "a", "app": "x", "schedule": "not cron"},
    {"name": "a", "app": "x", "retry": {"backoff": "linear"}},
    {"name": "a", "app": "x", "retry": {"max_attempts": 0}},
    {"name": "a", "app": "x", "engine": "flink"},
])
def test_invalid_specs(bad):
    with pytest.raises(ValidationError):
        JobSpec.model_validate(bad)


def test_duplicate_names_rejected():
    with pytest.raises(ValidationError):
        load_job_file("jobs: [{name: a, app: x}, {name: a, app: y}]")


def test_render_args():
    s = JobSpec(name="j", app="x", args=["--date", "{{ logical_date }}", "--try={{attempt}}", "{{ job }}"])
    assert s.render_args(logical_date="2026-01-01", attempt="2", job="j") == ["--date", "2026-01-01", "--try=2", "j"]


def test_due_times_no_catchup_returns_latest_only():
    now = T0.replace(hour=13)
    assert due_times("*/15 * * * *", T0, now, catchup=False) == [now]


def test_due_times_catchup_and_limit():
    now = T0.replace(hour=13)
    times = due_times("*/15 * * * *", T0, now, catchup=True)
    assert len(times) == 4 and times[0] == T0.replace(minute=15) and times[-1] == now
    assert len(due_times("* * * * *", T0, now, catchup=True, limit=10)) == 10


def test_due_times_none_due():
    assert due_times("0 * * * *", T0, T0.replace(minute=59), catchup=False) == []
    assert next_fire("0 * * * *", T0) == T0.replace(hour=13)
    assert next_fire(None, T0) is None


def test_should_retry():
    p = RetryPolicy(max_attempts=3)
    assert retry.should_retry(p, 1, retry.FAILED, 1)
    assert retry.should_retry(p, 2, retry.LOST, None)
    assert not retry.should_retry(p, 3, retry.FAILED, 1)
    assert not retry.should_retry(p, 1, retry.CANCELLED, None)
    p2 = RetryPolicy(max_attempts=3, retry_on_exit_codes=[137], retry_on_timeout=False)
    assert retry.should_retry(p2, 1, retry.FAILED, 137)
    assert not retry.should_retry(p2, 1, retry.FAILED, 1)
    assert not retry.should_retry(p2, 1, retry.TIMED_OUT, None)


def test_backoff():
    exp = RetryPolicy(backoff="exponential", initial_delay_s=10, max_delay_s=60, jitter=0)
    assert [retry.backoff_delay(exp, n) for n in (1, 2, 3, 4, 5)] == [10, 20, 40, 60, 60]
    fixed = RetryPolicy(backoff="fixed", initial_delay_s=7, jitter=0)
    assert retry.backoff_delay(fixed, 4) == 7
    jit = RetryPolicy(backoff="fixed", initial_delay_s=100, jitter=0.1)
    assert retry.backoff_delay(jit, 1, rand=lambda: 0.0) == pytest.approx(90)
    assert retry.backoff_delay(jit, 1, rand=lambda: 1.0) == pytest.approx(110)
