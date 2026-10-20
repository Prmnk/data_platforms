import pytest
from sqlalchemy import select

from scheduler import service
from scheduler.engine import Scheduler
from scheduler.models import Job, JobRun, RunAttempt
from scheduler.spec import JobSpec
from tests.conftest import job

NO_JITTER = {"backoff": "fixed", "initial_delay_s": 10, "jitter": 0}


def apply(sf, clock, *jobs):
    with sf() as s:
        service.apply_jobs(s, [JobSpec.model_validate(j) for j in jobs], now=clock())


def trigger(sf, clock, *names):
    with sf() as s:
        return service.trigger_runs(s, names, now=clock())


def runs(sf, **where):
    with sf() as s:
        q = select(JobRun).order_by(JobRun.id)
        for k, v in where.items():
            q = q.where(getattr(JobRun, k) == v)
        return s.scalars(q).all()


def attempts(sf, run_id):
    with sf() as s:
        return s.scalars(select(RunAttempt).where(RunAttempt.run_id == run_id)
                         .order_by(RunAttempt.attempt_no)).all()


def test_scheduled_job_runs_and_succeeds(sf, sched, runner, clock):
    apply(sf, clock, job("hourly", schedule="0 * * * *"))
    sched.tick()
    assert runs(sf) == []
    clock.advance(3600)
    sched.tick()  # creates + launches
    [r] = runs(sf)
    assert r.state == "RUNNING" and r.trigger == "schedule" and r.logical_date == clock()
    sched.tick()  # polls: exit 0
    [r] = runs(sf)
    assert r.state == "SUCCEEDED" and r.attempt_count == 1 and r.finished_at == clock()
    [a] = attempts(sf, r.id)
    assert a.state == "SUCCEEDED" and a.exit_code == 0 and a.app_id == "app-0"


def test_schedule_is_idempotent_and_catchup(sf, sched, clock):
    apply(sf, clock, job("a", schedule="*/10 * * * *"), job("b", schedule="*/10 * * * *", catchup=True))
    clock.advance(30 * 60)
    sched.tick()
    sched.tick()
    assert len(runs(sf, job_name="a")) == 1
    assert len(runs(sf, job_name="b")) == 3


def test_disabled_job_is_not_scheduled(sf, sched, clock):
    apply(sf, clock, job("off", schedule="* * * * *", enabled=False))
    clock.advance(600)
    sched.tick()
    assert runs(sf) == []


def test_retry_then_success(sf, sched, runner, clock):
    apply(sf, clock, job("flaky", retry={"max_attempts": 3, **NO_JITTER}))
    runner.script["flaky"] = [1, 1, 0]
    [r] = trigger(sf, clock, "flaky")["runs"]
    sched.tick()  # launch #1
    sched.tick()  # #1 fails -> RETRY_WAIT
    [run] = runs(sf)
    assert run.state == "RETRY_WAIT" and (run.next_attempt_at - clock()).total_seconds() == 10
    sched.tick()  # still waiting: nothing launched
    assert len(runner.launched) == 1
    clock.advance(10)
    sched.tick()  # launch #2
    sched.tick()  # #2 fails
    clock.advance(10)
    sched.tick()  # launch #3
    sched.tick()  # #3 succeeds
    [run] = runs(sf)
    assert run.state == "SUCCEEDED" and run.attempt_count == 3
    assert [a.state for a in attempts(sf, run.id)] == ["FAILED", "FAILED", "SUCCEEDED"]
    assert [q.attempt_no for q in runner.launched] == [1, 2, 3]


def test_dead_after_max_attempts(sf, sched, runner, clock):
    apply(sf, clock, job("bad", retry={"max_attempts": 2, **NO_JITTER}))
    runner.script["bad"] = [2]
    trigger(sf, clock, "bad")
    for _ in range(5):  # launch, fail, (wait) launch, fail
        sched.tick()
        clock.advance(10)
    [run] = runs(sf)
    assert run.state == "DEAD" and run.attempt_count == 2 and run.last_error == "exit code 2"


def test_non_retryable_exit_code(sf, sched, runner, clock):
    apply(sf, clock, job("x", retry={"max_attempts": 5, "retry_on_exit_codes": [137], **NO_JITTER}))
    runner.script["x"] = [1]
    trigger(sf, clock, "x")
    sched.tick()
    sched.tick()
    assert runs(sf)[0].state == "DEAD"


def test_timeout_kills_and_retries(sf, sched, runner, clock):
    apply(sf, clock, job("slow", timeout_s=60, retry={"max_attempts": 2, **NO_JITTER}))
    runner.script["slow"] = ["hang", 0]
    trigger(sf, clock, "slow")
    sched.tick()
    clock.advance(30)
    sched.tick()
    assert runs(sf)[0].state == "RUNNING"
    clock.advance(31)
    sched.tick()
    [run] = runs(sf)
    assert run.state == "RETRY_WAIT" and runner.killed == ["slow__r1__a1"]
    assert attempts(sf, run.id)[0].state == "TIMED_OUT"
    clock.advance(10)
    sched.tick()
    sched.tick()
    assert runs(sf)[0].state == "SUCCEEDED"


def test_cancel_running_and_queued(sf, sched, runner, clock, settings):
    settings.max_concurrent_runs = 1
    apply(sf, clock, job("a"), job("b"))
    runner.script["a"] = ["hang"]
    res = trigger(sf, clock, "a", "b")
    sched.tick()
    ra, rb = runs(sf)
    assert ra.state == "RUNNING" and rb.state == "PENDING"
    with sf() as s:
        service.request_cancel(s, ra.id)
        service.request_cancel(s, rb.id)
    sched.tick()
    ra, rb = runs(sf)
    assert ra.state == "CANCELLED" and rb.state == "CANCELLED"
    assert runner.killed == ["a__r1__a1"] and len(runner.launched) == 1
    assert attempts(sf, ra.id)[0].state == "CANCELLED"
    assert res["batch_id"] == ra.batch_id == rb.batch_id


def test_global_and_per_job_concurrency(sf, sched, runner, clock, settings):
    settings.max_concurrent_runs = 2
    names = [f"j{i}" for i in range(5)]
    apply(sf, clock, *[job(n) for n in names])
    for n in names:
        runner.script[n] = ["hang"]
    trigger(sf, clock, *names)
    sched.tick()
    assert len(runs(sf, state="RUNNING")) == 2 and len(runs(sf, state="PENDING")) == 3

    settings.max_concurrent_runs = 10
    apply(sf, clock, job("solo", concurrency=1))
    runner.script["solo"] = ["hang"]
    trigger(sf, clock, "solo")
    trigger(sf, clock, "solo")
    sched.tick()
    assert len(runs(sf, job_name="solo", state="RUNNING")) == 1
    assert len(runs(sf, state="RUNNING")) == 6


def test_sail_attempts_do_not_take_a_ui_port(sf, sched, runner, clock):
    apply(sf, clock, job("on_spark"), job("on_sail", engine="sail"))
    runner.script.update(on_spark=["hang"], on_sail=["hang"])
    trigger(sf, clock, "on_spark", "on_sail")
    sched.tick()
    ports = {r.spec.name: r.ui_port for r in runner.launched}
    assert ports == {"on_spark": 4040, "on_sail": None}


def test_recovery_adopts_running_attempts(sf, runner, settings, clock):
    apply(sf, clock, job("long", retry={"max_attempts": 2, **NO_JITTER}))
    runner.script["long"] = ["hang", 0]
    first = Scheduler(sf, runner, settings, clock=clock)
    trigger(sf, clock, "long")
    first.tick()
    # scheduler restarts; the driver is still alive
    second = Scheduler(sf, runner, settings, clock=clock)
    second.recover()
    second.tick()
    assert runs(sf)[0].state == "RUNNING"
    # ...and then the driver disappears without an exit code
    runner.forget("long__r1__a1")
    second.tick()
    [run] = runs(sf)
    assert run.state == "RETRY_WAIT" and attempts(sf, run.id)[0].state == "LOST"


def test_launch_failure_counts_as_failed_attempt(sf, sched, runner, clock):
    apply(sf, clock, job("x", retry={"max_attempts": 2, **NO_JITTER}))
    runner.fail_launch = True
    trigger(sf, clock, "x")
    sched.tick()
    [run] = runs(sf)
    assert run.state == "RETRY_WAIT" and "launch failed" in run.last_error


def test_deleted_job_cancels_pending_run(sf, sched, clock):
    apply(sf, clock, job("gone"))
    trigger(sf, clock, "gone")
    with sf() as s, pytest.raises(service.Conflict):
        service.delete_job(s, "gone")  # the API refuses while a run is active
    with sf() as s:  # but if the row disappears anyway, the run is cancelled
        s.delete(s.get(Job, "gone"))
        s.commit()
    sched.tick()
    assert runs(sf)[0].state == "CANCELLED"


def test_job_status_rows(sf, sched, runner, clock):
    apply(sf, clock, job("ok", schedule="0 * * * *"), job("ko", engine="sail", retry={"max_attempts": 1}))
    runner.script["ko"] = [1]
    trigger(sf, clock, "ok", "ko")
    sched.tick()
    sched.tick()
    with sf() as s:
        rows = {r["job"]: r for r in service.job_status(s, now=clock())}
        summ = service.summary(s, now=clock())
    assert rows["ok"]["last_state"] == "SUCCEEDED" and rows["ok"]["succeeded_24h"] == 1
    assert rows["ok"]["next_fire_at"] == "2026-01-01T13:00:00Z"
    assert rows["ko"]["last_state"] == "DEAD" and rows["ko"]["dead_24h"] == 1
    assert rows["ok"]["engine"] == "spark" and rows["ko"]["engine"] == "sail"
    assert summ["runs_by_state"]["SUCCEEDED"] == 1 and summ["scheduler"]["heartbeat_age_s"] == 0


def test_loop_exits_when_leader_lock_is_lost(sf, runner, settings):
    s = Scheduler(sf, runner, settings, leader_check=lambda: False)
    with pytest.raises(SystemExit):
        s.run_forever()
