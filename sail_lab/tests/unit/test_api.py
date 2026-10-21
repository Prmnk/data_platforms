import time

from fastapi.testclient import TestClient

from saillab.api import create_app

from .conftest import direct_command


def make_client(settings):
    from saillab.runner import Runner
    from saillab.store import Store
    r = Runner(settings, Store(settings.db_path), command=direct_command)
    return TestClient(create_app(settings, r))


def test_submit_follow_log_and_metrics(settings):
    with make_client(settings) as c:
        assert {j["job"] for j in c.get("/jobs").json()} >= {"ok.py", "query.sql"}
        run = c.post("/runs", json={"job": "ok.py", "name": "smoke"}).json()
        assert run["status"] == "queued" and run["job"] == "smoke"
        assert f"var-run={run['id']}&from=" in run["grafana_url"]
        offset, text = 0, ""
        for _ in range(200):
            chunk = c.get(f"/runs/{run['short_id']}/log", params={"offset": offset}).json()
            text += chunk["text"]
            offset = chunk["next_offset"]
            if chunk["done"]:
                break
            time.sleep(0.05)
        assert chunk["done"] and "hello from ok" in text
        assert c.get(f"/runs/{run['id']}").json()["status"] == "succeeded"
        assert [r["id"] for r in c.get("/runs", params={"job": "smoke"}).json()] == [run["id"]]
        m = c.get("/metrics").text
        assert 'lab_runs_finished_total{lab_job="smoke",status="succeeded"}' in m
        assert f'lab_run_info{{lab_job="smoke",run_id="{run["id"]}",status="succeeded"}}' in m


def test_errors(settings):
    with make_client(settings) as c:
        assert c.post("/runs", json={}).status_code == 400
        assert c.post("/runs", json={"job": "nope.py"}).status_code == 400
        assert c.get("/runs/doesnotexist").status_code == 404
        assert c.post("/runs/doesnotexist/cancel").status_code == 404
