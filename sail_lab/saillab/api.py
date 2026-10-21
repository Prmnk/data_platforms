"""REST API for submitting and inspecting runs. Interactive docs at /docs."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from .config import Settings
from .runner import Runner, Submission, SubmitError
from .store import FINAL, AmbiguousId, Run, Store

log = logging.getLogger(__name__)


class Script(BaseModel):
    filename: str = Field(examples=["my_job.py"])
    content: str


class SubmitRequest(BaseModel):
    job: Optional[str] = Field(None, description="Job under jobs/, e.g. hello_sail.py", examples=["hello_sail.py"])
    script: Optional[Script] = Field(None, description="Upload a .py or .sql file instead of using jobs/")
    sql: Optional[str] = Field(None, description="SQL text; several statements separated by ;")
    args: List[str] = Field(default_factory=list, description="Arguments passed to the script")
    conf: Dict[str, str] = Field(default_factory=dict, description="Spark session configs")
    timeout_seconds: Optional[int] = None
    name: Optional[str] = Field(None, description="Label for metrics; defaults to the file name")


def create_app(settings: Optional[Settings] = None, runner: Optional[Runner] = None) -> FastAPI:
    s = settings or Settings.from_env()
    r = runner or Runner(s, Store(s.db_path))

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        r.start()
        yield
        r.stop()

    app = FastAPI(title="Sail Lab", version="0.1.0", lifespan=lifespan,
                  description="Submit PySpark and SQL jobs to a LakeSail server. "
                              "Every run id is also its Spark Connect session id.")

    def view(run: Run) -> dict:
        d = run.to_dict()
        # open the run dashboard on the run's own time window (a minute either side)
        start = int((run.submitted_at - 60) * 1000)
        end = int((run.finished_at + 60) * 1000) if run.finished_at else "now"
        d["grafana_url"] = f"{s.grafana_url}/d/sail-lab-run/sail-lab-run?var-run={run.id}&from={start}&to={end}"
        return d

    def lookup(run_id: str) -> Run:
        try:
            run = r.store.get(run_id)
        except AmbiguousId:
            raise HTTPException(409, f"run id prefix {run_id!r} is ambiguous")
        if run is None:
            raise HTTPException(404, f"run {run_id!r} not found")
        return run

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse("/docs")

    @app.get("/healthz")
    def healthz():
        return {"ok": True, "sail_remote": s.sail_remote}

    @app.get("/jobs")
    def jobs():
        return r.list_jobs()

    @app.post("/runs", status_code=201)
    def submit(req: SubmitRequest):
        try:
            run = r.submit(Submission(
                job=req.job, script=req.script.content if req.script else None,
                filename=req.script.filename if req.script else None, sql=req.sql,
                args=req.args, conf=req.conf, timeout_seconds=req.timeout_seconds, name=req.name))
        except SubmitError as e:
            raise HTTPException(400, str(e))
        return view(run)

    @app.get("/runs")
    def runs(limit: int = Query(50, ge=1, le=1000), status: Optional[str] = None, job: Optional[str] = None):
        return [view(x) for x in r.store.list(limit=limit, status=status, job=job)]

    @app.get("/runs/{run_id}")
    def get_run(run_id: str):
        return view(lookup(run_id))

    @app.get("/runs/{run_id}/log")
    def get_log(run_id: str, offset: int = Query(0, ge=0), max_bytes: int = Query(256_000, ge=1, le=4_000_000)):
        """A chunk of the run's log from `offset`; poll with next_offset until done is true."""
        run = lookup(run_id)
        path = r.log_path(run.id)
        try:
            with open(path, "rb") as f:
                f.seek(offset)
                data = f.read(max_bytes)
        except FileNotFoundError:
            data = b""
        next_offset = offset + len(data)
        size = path.stat().st_size if path.exists() else 0
        return {"run_id": run.id, "status": run.status, "text": data.decode("utf-8", "replace"),
                "next_offset": next_offset, "done": run.status in FINAL and next_offset >= size}

    @app.post("/runs/{run_id}/cancel")
    def cancel(run_id: str):
        return view(r.cancel(lookup(run_id).id))

    @app.get("/metrics", include_in_schema=False)
    def prom():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app
