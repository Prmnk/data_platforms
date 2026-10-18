"""Exercise the real process handling of SparkSubmitRunner with a fake spark-submit."""
import os
import stat
import time
from datetime import datetime

import pytest

from scheduler.config import Settings
from scheduler.runners.base import LaunchRequest
from scheduler.runners.standalone import SparkSubmitRunner, exit_file_for
from scheduler.spec import JobSpec

FAKE = """#!/bin/sh
# Fake spark-submit: the last arg decides behaviour.
echo "args: $@"
eval last=\\${$#}
case "$last" in
  sleep*) sleep "${last#sleep}" ;;
  exit*)  exit "${last#exit}" ;;
esac
"""


@pytest.fixture
def runner(tmp_path):
    fake = tmp_path / "spark-submit"
    fake.write_text(FAKE)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    return SparkSubmitRunner(Settings(spark_submit=str(fake), log_dir=str(tmp_path / "logs"),
                                      driver_host="sched", kill_grace_seconds=2))


def req(arg, run_id=7, **kw):
    spec = JobSpec(name="j", app="/jobs/j.py", args=["--date={{ logical_date }}", arg],
                   spark_conf={"spark.cores.max": 1}, **kw)
    return LaunchRequest(spec=spec, run_id=run_id, attempt_no=2, logical_date=datetime(2026, 1, 1), ui_port=4041)


def wait(runner, h, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = runner.poll(h)
        if r.status != "running":
            return r
        time.sleep(0.05)
    raise AssertionError("still running")


def test_command_line(runner):
    cmd = runner.build_command(req("x"))
    assert cmd[1:9] == ["--master", "spark://spark-master:7077", "--deploy-mode", "client",
                        "--name", "j__r7__a2", "--conf", "spark.driver.host=sched"]
    assert "spark.ui.port=4041" in cmd and "spark.cores.max=1" in cmd
    assert cmd[-3:] == ["/jobs/j.py", "--date=2026-01-01T00:00:00", "x"]


def test_exit_codes_and_logs(runner):
    ok = runner.launch(req("exit0", run_id=1))
    bad = runner.launch(req("exit3", run_id=2))
    assert wait(runner, ok).exit_code == 0
    assert wait(runner, bad).exit_code == 3
    assert "args:" in open(bad.log_path).read()
    assert exit_file_for(bad.log_path).read_text().strip() == "3"


def test_adopted_handle_uses_exit_file(runner):
    h = runner.launch(req("exit5"))
    wait(runner, h)
    adopted = runner.adopt(h.pid, h.app_name, h.log_path)
    assert runner.poll(adopted).exit_code == 5


def test_adopted_running_then_lost(runner, tmp_path):
    h = runner.launch(req("sleep30"))
    adopted = runner.adopt(h.pid, h.app_name, h.log_path)
    assert runner.poll(adopted).status == "running"
    os.killpg(h.pid, 9)
    h.proc.wait()
    assert runner.poll(adopted).status == "lost"  # no exit file was written


def test_kill_takes_down_process_group(runner):
    h = runner.launch(req("sleep30"))
    time.sleep(0.2)
    t = time.time()
    runner.kill(h)
    assert time.time() - t < 3
    assert runner.poll(h).status == "exited"
    with pytest.raises(ProcessLookupError):
        os.killpg(h.pid, 0)
