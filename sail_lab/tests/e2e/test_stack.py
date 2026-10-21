"""End-to-end checks against a running stack (make up). Each test submits real jobs to Sail."""
import json
import os
import time
import urllib.parse
import urllib.request

import pytest

pytestmark = pytest.mark.e2e

API = os.environ.get("LAB_API", "http://localhost:8010")
PROM = os.environ.get("PROMETHEUS", "http://localhost:9097")


def call(method, path, body=None):
    req = urllib.request.Request(API + path, json.dumps(body).encode() if body is not None else None,
                                 {"Content-Type": "application/json"}, method=method)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def wait(run_id, timeout=600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = call("GET", f"/runs/{run_id}")
        if run["status"] not in ("queued", "running"):
            return run
        time.sleep(2)
    raise AssertionError(f"run {run_id} did not finish in {timeout}s")


def prom(expr):
    with urllib.request.urlopen(f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": expr}), timeout=10) as r:
        return json.load(r)["data"]["result"]


def eventually(fn, timeout=60):
    deadline = time.time() + timeout
    while True:
        result = fn()
        if result or time.time() > deadline:
            return result
        time.sleep(3)


def test_job_runs_on_sail_and_reports_everywhere():
    run = wait(call("POST", "/runs", {"job": "hello_sail.py", "args": ["--rows", "200000"]})["id"])
    assert run["status"] == "succeeded", call("GET", f"/runs/{run['id']}/log")["text"][-2000:]
    assert run["result"]["rows"] == 200000 and run["result"]["session_ready_seconds"] > 0
    log = call("GET", f"/runs/{run['id']}/log?max_bytes=4000000")["text"]
    assert "AggregateExec" in log  # Sail's own physical plan was printed
    # Sail's engine metrics carry the run id as session_id
    assert eventually(lambda: prom(f'execution_output_row_count{{session_id="{run["id"]}"}}'))
    assert eventually(lambda: prom(f'lab_run_result{{run_id="{run["id"]}",key="rows"}}'))


def test_sql_upload_failure_and_cancel():
    sql = wait(call("POST", "/runs", {"sql": "SELECT 21 * 2 AS answer"})["id"])
    assert sql["status"] == "succeeded" and "42" in call("GET", f"/runs/{sql['id']}/log")["text"]

    up = wait(call("POST", "/runs", {"script": {"filename": "bad.py", "content":
              "from pyspark.sql import SparkSession\nSparkSession.builder.getOrCreate().range(1).select('nope').show()\n"}})["id"])
    assert up["status"] == "failed" and up["exit_code"] != 0

    long = call("POST", "/runs", {"job": "long_running.py", "args": ["--seconds", "300"]})
    deadline = time.time() + 120
    while call("GET", f"/runs/{long['id']}")["status"] != "running" and time.time() < deadline:
        time.sleep(1)
    time.sleep(5)
    call("POST", f"/runs/{long['id']}/cancel")
    assert wait(long["id"], timeout=120)["status"] == "cancelled"
