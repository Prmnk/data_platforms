"""Exercise SailRunner and EngineRouter with a fake Python standing in for the Sail client."""
import os
import stat
import time
from datetime import datetime

import pytest

from scheduler.config import Settings
from scheduler.runners.base import LaunchRequest
from scheduler.runners.router import EngineRouter
from scheduler.runners.sail import SailRunner, session_file_for, session_id_for
from scheduler.runners.sail_bootstrap import parse
from scheduler.runners.standalone import SparkSubmitRunner
from scheduler.spec import JobSpec

# Fake `python3 -m scheduler.runners.sail_bootstrap ...`: --interrupt records SPARK_REMOTE,
# otherwise the last arg decides behaviour (like the fake spark-submit).
FAKE = """#!/bin/sh
echo "remote: $SPARK_REMOTE"
echo "args: $@"
case " $* " in
  *" --interrupt "*) echo "$SPARK_REMOTE" >> "$INTERRUPTS"; exit 0 ;;
esac
eval last=\\${$#}
case "$last" in
  sleep*) sleep "${last#sleep}" ;;
  exit*)  exit "${last#exit}" ;;
esac
"""


@pytest.fixture
def interrupts(tmp_path, monkeypatch):
    path = tmp_path / "interrupts"
    monkeypatch.setenv("INTERRUPTS", str(path))
    return path


@pytest.fixture
def settings(tmp_path, interrupts):
    fake = tmp_path / "python3"
    fake.write_text(FAKE)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    return Settings(sail_python=str(fake), sail_remote="sc://sail:50051", spark_submit=str(fake),
                    log_dir=str(tmp_path / "logs"), driver_host="sched", kill_grace_seconds=2)


@pytest.fixture
def runner(settings):
    return SailRunner(settings)


@pytest.fixture
def router(settings):
    return EngineRouter({"spark": SparkSubmitRunner(settings), "sail": SailRunner(settings)})


def req(arg, run_id=7, engine="sail", **kw):
    spec = JobSpec(name="j", engine=engine, app="/jobs/j.py", args=["--date={{ logical_date }}", arg],
                   spark_conf={"spark.sql.shuffle.partitions": 4}, **kw)
    return LaunchRequest(spec=spec, run_id=run_id, attempt_no=2, logical_date=datetime(2026, 1, 1))


def wait(runner, h, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = runner.poll(h)
        if r.status != "running":
            return r
        time.sleep(0.05)
    raise AssertionError("still running")


def interrupted(path):
    return path.read_text().split() if path.exists() else []


def test_command_line(runner, settings):
    cmd = runner.build_command(req("x", py_files=["/jobs/lib.zip"]))
    assert cmd[:3] == [settings.sail_python, "-m", "scheduler.runners.sail_bootstrap"]
    assert cmd[3:7] == ["--conf", "spark.app.name=j__r7__a2", "--conf", "spark.sql.shuffle.partitions=4"]
    assert cmd[7:9] == ["--py-file", "/jobs/lib.zip"]
    assert cmd[9:] == ["--", "/jobs/j.py", "--date=2026-01-01T00:00:00", "x"]


def test_launch_uses_its_own_session(runner):
    a, b = runner.launch(req("exit0", run_id=1)), runner.launch(req("exit4", run_id=2))
    assert wait(runner, a).exit_code == 0 and wait(runner, b).exit_code == 4
    sid = session_id_for(a.log_path)
    assert sid == a.extra["session_id"] and sid != session_id_for(b.log_path)
    assert f"remote: sc://sail:50051/;session_id={sid}" in open(a.log_path).read()


def test_kill_interrupts_session_then_kills_process(runner, interrupts):
    h = runner.launch(req("sleep30"))
    time.sleep(0.2)
    runner.kill(h)
    assert interrupted(interrupts) == [f"sc://sail:50051/;session_id={h.extra['session_id']}"]
    assert runner.poll(h).status == "exited"
    with pytest.raises(ProcessLookupError):
        os.killpg(h.pid, 0)
    assert "interrupting Sail session" in open(h.log_path).read()


def test_lost_driver_interrupts_its_session(runner, interrupts):
    h = runner.launch(req("sleep30"))
    adopted = runner.adopt(h.pid, h.app_name, h.log_path)
    assert runner.poll(adopted).status == "running" and interrupted(interrupts) == []
    os.killpg(h.pid, 9)
    h.proc.wait()
    assert runner.poll(adopted).status == "lost"
    assert len(interrupted(interrupts)) == 1


def test_router_dispatches_and_adopts_by_engine(router, interrupts):
    spark = router.launch(req("exit0", run_id=1, engine="spark"))
    sail = router.launch(req("sleep30", run_id=2))
    assert spark.extra["engine"] == "spark" and sail.extra["engine"] == "sail"
    assert not session_file_for(spark.log_path).exists()
    assert router.poll(spark).status in ("running", "exited")

    # after a scheduler restart only pid, app name and log path are known
    assert router.adopt(spark.pid, spark.app_name, spark.log_path).extra["engine"] == "spark"
    assert router.adopt(sail.pid, sail.app_name, sail.log_path).extra["engine"] == "sail"
    router.kill(sail)  # the launched handle, so the test process can reap its child
    assert len(interrupted(interrupts)) == 1


def test_router_without_runner_for_engine(settings):
    with pytest.raises(RuntimeError, match="no runner configured for engine 'sail'"):
        EngineRouter({"spark": SparkSubmitRunner(settings)}).launch(req("exit0"))


def test_bootstrap_args():
    a = parse(["--conf", "a=b=c", "--py-file", "x.zip", "--", "app.py", "--date", "d", "-v"])
    assert (a.conf, a.py_files, a.app, a.app_args) == (["a=b=c"], ["x.zip"], "app.py", ["--date", "d", "-v"])
    assert parse(["--interrupt"]).interrupt
    for bad in (["--conf", "nokv", "--", "app.py"], []):
        with pytest.raises(SystemExit):
            parse(bad)
