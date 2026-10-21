import json
import time

import pytest

from saillab.runner import Submission, SubmitError, bootstrap_command, describe
from saillab.store import Run

from .conftest import wait_final


def test_success_failure_and_logs(runner):
    ok = runner.submit(Submission(job="ok.py"))
    boom = runner.submit(Submission(job="boom.py"))
    assert wait_final(runner, ok.id).status == "succeeded"
    b = wait_final(runner, boom.id)
    assert (b.status, b.exit_code, b.error) == ("failed", 3, "exit code 3")
    log = runner.log_path(ok.id).read_text()
    assert "hello from ok" in log and "[lab] run succeeded" in log
    assert b.max_rss_bytes and b.max_rss_bytes > 1_000_000


def test_runs_wait_for_a_free_slot(runner):
    first = runner.submit(Submission(job="sleepy.py", args=["0.5"]))
    second = runner.submit(Submission(job="ok.py"))
    time.sleep(0.2)
    assert runner.store.get(second.id).status == "queued"
    a, b = wait_final(runner, first.id), wait_final(runner, second.id)
    assert b.started_at >= a.finished_at - 0.1


def test_cancel_queued_and_running(runner):
    running = runner.submit(Submission(job="sleepy.py", args=["30"]))
    queued = runner.submit(Submission(job="ok.py"))
    time.sleep(0.3)
    assert runner.cancel(queued.id).status == "cancelled"
    runner.cancel(running.id)
    r = wait_final(runner, running.id)
    assert r.status == "cancelled" and r.error == "cancelled by user"


def test_timeout(runner):
    run = runner.submit(Submission(job="sleepy.py", args=["30"], timeout_seconds=1))
    r = wait_final(runner, run.id)
    assert r.status == "timed_out" and r.error == "timed out after 1s"


def test_upload_sql_and_result_file(runner, settings):
    up = runner.submit(Submission(script='print("uploaded")', filename="../../etc/try me.py"))
    assert up.kind == "upload" and up.target.endswith("try_me.py") and str(settings.uploads_dir) in up.target
    assert up.job == "try_me"
    (settings.results_dir / f"{up.id}.json").write_text(json.dumps({"rows": 42}))
    assert wait_final(runner, up.id).result == {"rows": 42}
    sql = runner.submit(Submission(sql="SELECT 1"))
    assert sql.kind == "sql" and sql.job == "sql"


@pytest.mark.parametrize("sub", [
    Submission(),
    Submission(job="ok.py", sql="SELECT 1"),
    Submission(job="../conftest.py"),
    Submission(job="missing.py"),
    Submission(script="x", filename="x.sh"),
    Submission(job="ok.py", timeout_seconds=0),
])
def test_rejects_bad_submissions(runner, sub):
    with pytest.raises(SubmitError):
        runner.submit(sub)


def test_restart_marks_running_runs_lost(settings):
    from saillab.runner import Runner
    from saillab.store import Store
    store = Store(settings.db_path)
    store.insert(Run(id="11111111-0000-0000-0000-000000000000", job="x", kind="job", target="x.py", status="running"))
    r = Runner(settings, store)
    r.start()
    r.stop()
    assert store.get("1111").status == "lost"


def test_list_jobs_and_describe(runner):
    jobs = {j["job"]: j for j in runner.list_jobs()}
    assert set(jobs) == {"ok.py", "boom.py", "sleepy.py", "query.sql"}
    assert jobs["ok.py"]["summary"] == "Prints and exits 0."
    assert jobs["query.sql"]["summary"] == "Counts things."


def test_bootstrap_command(settings):
    run = Run(id="abc", job="q", kind="sql", target="/x/q.sql", conf={"spark.sql.shuffle.partitions": "4"}, args=["--max-rows", "5"])
    cmd = bootstrap_command(settings, run, settings.results_dir / "abc.json")
    assert cmd[1:3] == ["-m", "saillab.bootstrap"]
    assert ["--conf", "spark.sql.shuffle.partitions=4"] == cmd[cmd.index("--conf"):cmd.index("--conf") + 2]
    assert cmd[-5:] == ["saillab.sql_runner", "--", "/x/q.sql", "--max-rows", "5"]
