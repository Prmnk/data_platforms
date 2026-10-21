import sys
import time
from pathlib import Path

import pytest

from saillab.config import Settings
from saillab.runner import Runner
from saillab.store import FINAL, Store


def direct_command(s, run, result_file):
    """Run the script itself instead of the Spark Connect bootstrap (no Sail needed)."""
    return [sys.executable, run.target, *run.args]


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    (jobs / "ok.py").write_text('"""Prints and exits 0."""\nprint("hello from ok")\n')
    (jobs / "boom.py").write_text('"""Exits 3."""\nimport sys\nprint("boom")\nsys.exit(3)\n')
    (jobs / "sleepy.py").write_text('"""Sleeps."""\nimport time, sys\ntime.sleep(float(sys.argv[1]))\n')
    (jobs / "query.sql").write_text("-- Counts things.\nSELECT 1;\n")
    (jobs / "_helper.py").write_text("X = 1\n")
    return Settings(jobs_dir=jobs, data_dir=tmp_path / "data", log_dir=tmp_path / "logs",
                    max_concurrent_runs=1, kill_grace_seconds=1, python=sys.executable, poll_seconds=0.05)


@pytest.fixture
def runner(settings):
    r = Runner(settings, Store(settings.db_path), command=direct_command)
    # interrupting a Sail session needs pyspark; tests only check the signal path
    r._interrupt_session = lambda run: None
    r.start()
    yield r
    r.stop()


def wait_final(runner, run_id, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = runner.store.get(run_id)
        if run.status in FINAL:
            return run
        time.sleep(0.05)
    raise AssertionError(f"run {run_id} still {runner.store.get(run_id).status}")
