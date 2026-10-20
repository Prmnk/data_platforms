"""End-to-end tests against the running compose stack: `make up && make test-e2e`."""
import os
import subprocess
import time
from pathlib import Path

import httpx
import pytest

from scheduler.spec import load_job_file

pytestmark = pytest.mark.e2e

ROOT = Path(__file__).resolve().parents[2]
API = os.environ.get("SPARKSCHED_API", "http://localhost:8000")
TERMINAL = {"SUCCEEDED", "DEAD", "CANCELLED"}


@pytest.fixture(scope="module")
def api():
    c = httpx.Client(base_url=API, timeout=30)
    deadline = time.time() + 120
    while True:
        try:
            if c.get("/health").status_code == 200:
                break
        except httpx.HTTPError:
            pass
        if time.time() > deadline:
            pytest.fail(f"API not reachable at {API}")
        time.sleep(2)
    jobs = load_job_file((ROOT / "examples/e2e_jobs.yaml").read_text())
    c.post("/jobs", json=jobs.model_dump()).raise_for_status()
    yield c
    c.close()


def trigger(api, *names):
    r = api.post("/runs", json={"jobs": list(names)})
    r.raise_for_status()
    return r.json()


def wait_batch(api, batch_id, timeout=900, on_poll=None):
    deadline = time.time() + timeout
    while time.time() < deadline:
        runs = api.get("/runs", params={"batch_id": batch_id, "limit": 100}).json()
        if on_poll:
            on_poll(runs)
        if runs and all(r["state"] in TERMINAL for r in runs):
            return {r["job"]: api.get(f"/runs/{r['run_id']}").json() for r in runs}
        time.sleep(3)
    pytest.fail(f"batch {batch_id} did not finish in {timeout}s")


def test_batch_retry_timeout_and_concurrency(api):
    max_running = []
    batch = trigger(api, "e2e_ingest", "e2e_flaky", "e2e_slow")

    def track(runs):
        max_running.append(sum(r["state"] == "RUNNING" for r in runs))

    res = wait_batch(api, batch["batch_id"], on_poll=track)

    assert res["e2e_ingest"]["state"] == "SUCCEEDED"

    flaky = res["e2e_flaky"]
    assert flaky["state"] == "SUCCEEDED" and flaky["attempts"] == 3
    assert [a["state"] for a in flaky["attempt_details"]] == ["FAILED", "FAILED", "SUCCEEDED"]
    assert all(a["app_id"] for a in flaky["attempt_details"]), "app ids correlated from the master"

    slow = res["e2e_slow"]
    assert slow["state"] == "DEAD"
    assert [a["state"] for a in slow["attempt_details"]] == ["TIMED_OUT", "TIMED_OUT"]

    assert max(max_running) <= int(os.environ.get("MAX_CONCURRENT_RUNS", 3))

    # Data actually landed in MinIO as a readable Delta table.
    verify = wait_batch(api, trigger(api, "e2e_verify")["batch_id"])["e2e_verify"]
    assert verify["state"] == "SUCCEEDED", api.get(f"/runs/{verify['run_id']}/attempts/1/log").text


def test_cancel_running_job(api):
    batch = trigger(api, "e2e_slow")
    run_id = batch["runs"][0]["run_id"]
    deadline = time.time() + 120
    while api.get(f"/runs/{run_id}").json()["state"] != "RUNNING":
        assert time.time() < deadline
        time.sleep(2)
    api.post(f"/runs/{run_id}/cancel").raise_for_status()
    res = wait_batch(api, batch["batch_id"], timeout=120)["e2e_slow"]
    assert res["state"] == "CANCELLED" and res["attempt_details"][0]["state"] == "CANCELLED"


def test_status_endpoints_for_dashboards(api):
    status = api.get("/status").json()
    assert status["scheduler"]["heartbeat_age_s"] < 30
    rows = {r["job"]: r for r in api.get("/status/jobs").json()}
    assert rows["e2e_flaky"]["last_state"] == "SUCCEEDED"
    metrics = api.get("/metrics").text
    assert 'sparksched_job_last_run_state{job="e2e_flaky",state="SUCCEEDED"} 1.0' in metrics


@pytest.mark.skipif(os.environ.get("E2E_SKIP_RESTART") == "1", reason="restart test disabled")
def test_scheduler_restart_mid_run(api):
    """Kill the scheduler container while a job runs. The driver dies with it, so the attempt
    is marked LOST on startup and retried; the run still succeeds and is not duplicated."""
    batch = trigger(api, "e2e_restart")
    run_id = batch["runs"][0]["run_id"]
    deadline = time.time() + 120
    while api.get(f"/runs/{run_id}").json()["state"] != "RUNNING":
        assert time.time() < deadline
        time.sleep(2)
    time.sleep(5)
    subprocess.run(["docker", "compose", "restart", "scheduler"], cwd=ROOT, check=True)
    res = wait_batch(api, batch["batch_id"], timeout=600)["e2e_restart"]
    assert res["state"] == "SUCCEEDED"
    assert [a["state"] for a in res["attempt_details"]] == ["LOST", "SUCCEEDED"]
    assert len(api.get("/runs", params={"batch_id": batch["batch_id"]}).json()) == 1


def test_sail_engine_batch(api):
    """The same scripts on Sail: retries, timeout (which must interrupt the Sail session),
    Delta ingest + MERGE, and Spark reading the table Sail wrote."""
    res = wait_batch(api, trigger(api, "e2e_sail_ingest", "e2e_sail_flaky", "e2e_sail_slow")["batch_id"])

    assert res["e2e_sail_ingest"]["state"] == "SUCCEEDED"
    flaky = res["e2e_sail_flaky"]
    assert [a["state"] for a in flaky["attempt_details"]] == ["FAILED", "FAILED", "SUCCEEDED"]
    assert all(a["ui_port"] is None and a["app_id"] is None for a in flaky["attempt_details"])

    slow = res["e2e_sail_slow"]
    assert [a["state"] for a in slow["attempt_details"]] == ["TIMED_OUT", "TIMED_OUT"]
    log = api.get(f"/runs/{slow['run_id']}/attempts/1/log", params={"tail": 50}).text
    assert "interrupted 1 operation(s)" in log, log

    for name in ("e2e_sail_merge", "e2e_sail_verify", "e2e_spark_reads_sail_table"):
        run = wait_batch(api, trigger(api, name)["batch_id"])[name]
        assert run["state"] == "SUCCEEDED", api.get(f"/runs/{run['run_id']}/attempts/1/log").text


def test_cancel_running_sail_job(api):
    batch = trigger(api, "e2e_sail_slow")
    run_id = batch["runs"][0]["run_id"]
    deadline = time.time() + 120
    while api.get(f"/runs/{run_id}").json()["state"] != "RUNNING":
        assert time.time() < deadline
        time.sleep(2)
    time.sleep(5)
    api.post(f"/runs/{run_id}/cancel").raise_for_status()
    res = wait_batch(api, batch["batch_id"], timeout=120)["e2e_sail_slow"]
    assert res["state"] == "CANCELLED"
    assert "interrupted 1 operation(s)" in api.get(f"/runs/{run_id}/attempts/1/log").text
