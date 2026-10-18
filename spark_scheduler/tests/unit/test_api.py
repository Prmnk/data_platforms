import pytest
from fastapi.testclient import TestClient

from scheduler.api import create_app
from scheduler.engine import Scheduler
from tests.conftest import job


@pytest.fixture
def client(sf):
    return TestClient(create_app(sf))


@pytest.fixture
def sched(sf, runner, settings):
    # The API stamps runs with the wall clock, so the scheduler must use it too.
    return Scheduler(sf, runner, settings)


def test_apply_trigger_status_flow(client, sched):
    r = client.post("/jobs", json={"jobs": [job("a", schedule="*/5 * * * *"), job("b")]})
    assert r.json() == {"a": "created", "b": "created"}
    assert client.post("/jobs", json={"jobs": [job("a", schedule="*/5 * * * *")]}).json() == {"a": "unchanged"}

    batch = client.post("/runs", json={"jobs": ["a", "b"]}).json()
    assert [x["job"] for x in batch["runs"]] == ["a", "b"]

    sched.tick()
    sched.tick()
    runs = client.get("/runs", params={"batch_id": batch["batch_id"]}).json()
    assert {r["state"] for r in runs} == {"SUCCEEDED"}

    detail = client.get(f"/runs/{runs[0]['run_id']}").json()
    assert detail["attempt_details"][0]["state"] == "SUCCEEDED"

    status = client.get("/status").json()
    assert status["jobs"] == 2 and status["runs_by_state"]["SUCCEEDED"] == 2
    assert {r["job"]: r["last_state"] for r in status["job_status"]} == {"a": "SUCCEEDED", "b": "SUCCEEDED"}
    assert client.get("/status/jobs").json()[0]["job"] == "a"


def test_validation_and_errors(client):
    assert client.post("/jobs", json={"jobs": [{"name": "x", "app": "a", "schedule": "nope"}]}).status_code == 422
    assert client.post("/runs", json={"jobs": ["missing"]}).status_code == 404
    assert client.get("/runs/999").status_code == 404
    assert client.patch("/jobs/missing", json={"enabled": False}).status_code == 404


def test_enable_disable_cancel_delete(client, sched):
    client.post("/jobs", json={"jobs": [job("a")]})
    assert client.patch("/jobs/a", json={"enabled": False}).json() == {"job": "a", "enabled": False}
    run_id = client.post("/runs", json={"jobs": ["a"]}).json()["runs"][0]["run_id"]
    assert client.delete("/jobs/a").status_code == 409  # active run
    assert client.post(f"/runs/{run_id}/cancel").status_code == 200
    sched.tick()
    assert client.get(f"/runs/{run_id}").json()["state"] == "CANCELLED"
    assert client.post(f"/runs/{run_id}/cancel").status_code == 409
    assert client.delete("/jobs/a").status_code == 204


def test_metrics(client, sched):
    client.post("/jobs", json={"jobs": [job("a")]})
    client.post("/runs", json={"jobs": ["a"]})
    sched.tick()
    sched.tick()
    body = client.get("/metrics").text
    assert 'sparksched_job_last_run_state{job="a",state="SUCCEEDED"} 1.0' in body
    assert 'sparksched_runs{state="SUCCEEDED"} 1.0' in body
    assert "sparksched_scheduler_heartbeat_timestamp_seconds" in body
    assert 'sparksched_attempts_24h{job="a",outcome="SUCCEEDED"} 1.0' in body
