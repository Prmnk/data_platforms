"""REST API: manage jobs, trigger batches of runs, and poll statuses (also for Grafana)."""
from __future__ import annotations

from collections import deque
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import Body, Depends, FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from . import service
from .metrics import DbCollector
from .models import Job, JobRun, RunAttempt
from .spec import JobFile, JobSpec


class TriggerRequest(BaseModel):
    jobs: List[str] = Field(..., min_length=1)


class EnabledRequest(BaseModel):
    enabled: bool


def create_app(session_factory: sessionmaker[Session]) -> FastAPI:
    app = FastAPI(title="sparksched", version="0.1.0")
    registry = CollectorRegistry()
    registry.register(DbCollector(session_factory))

    def db():
        with session_factory() as s:
            yield s

    def _nf(e: Exception):
        raise HTTPException(404, f"not found: {e}")

    @app.get("/health")
    def health():
        return {"ok": True}

    # ---------------- jobs
    @app.get("/jobs")
    def list_jobs(s: Session = Depends(db)) -> List[Dict]:
        return service.job_status(s)

    @app.get("/jobs/{name}")
    def get_job(name: str, s: Session = Depends(db)) -> Dict:
        rows = [r for r in service.job_status(s) if r["job"] == name]
        if not rows:
            _nf(Exception(name))
        return {**rows[0], "spec": s.get(Job, name).spec}

    @app.post("/jobs")
    def apply_jobs(body: JobFile, s: Session = Depends(db)) -> Dict[str, str]:
        """Create or update many jobs at once: {"jobs": [JobSpec, ...]}."""
        return service.apply_jobs(s, body.jobs)

    @app.put("/jobs/{name}")
    def put_job(name: str, spec: JobSpec, s: Session = Depends(db)) -> Dict[str, str]:
        if spec.name != name:
            raise HTTPException(400, "name in path and body differ")
        return service.apply_jobs(s, [spec])

    @app.patch("/jobs/{name}")
    def patch_job(name: str, body: EnabledRequest, s: Session = Depends(db)) -> Dict:
        try:
            job = service.set_enabled(s, name, body.enabled)
        except service.NotFound as e:
            _nf(e)
        return {"job": job.name, "enabled": job.enabled}

    @app.delete("/jobs/{name}", status_code=204)
    def delete_job(name: str, s: Session = Depends(db)):
        try:
            service.delete_job(s, name)
        except service.NotFound as e:
            _nf(e)
        except service.Conflict as e:
            raise HTTPException(409, str(e))
        return Response(status_code=204)

    # ---------------- runs
    @app.post("/runs")
    def trigger(body: TriggerRequest, s: Session = Depends(db)) -> Dict:
        """Queue a manual run of each listed job now (one batch)."""
        try:
            return service.trigger_runs(s, body.jobs)
        except service.NotFound as e:
            _nf(e)

    @app.get("/runs")
    def list_runs(
        job: Optional[str] = None,
        state: Optional[str] = None,
        batch_id: Optional[str] = None,
        limit: int = Query(50, ge=1, le=1000),
        s: Session = Depends(db),
    ) -> List[Dict]:
        q = select(JobRun).order_by(JobRun.id.desc()).limit(limit)
        if job:
            q = q.where(JobRun.job_name == job)
        if state:
            q = q.where(JobRun.state == state.upper())
        if batch_id:
            q = q.where(JobRun.batch_id == batch_id)
        return [service.run_to_dict(r) for r in s.scalars(q)]

    @app.get("/runs/{run_id}")
    def get_run(run_id: int, s: Session = Depends(db)) -> Dict:
        run = s.get(JobRun, run_id)
        if run is None:
            _nf(Exception(run_id))
        return service.run_to_dict(run, with_attempts=True)

    @app.post("/runs/{run_id}/cancel")
    def cancel(run_id: int, s: Session = Depends(db)) -> Dict:
        try:
            run = service.request_cancel(s, run_id)
        except service.NotFound as e:
            _nf(e)
        except service.Conflict as e:
            raise HTTPException(409, str(e))
        return {"run_id": run.id, "cancel_requested": True}

    @app.get("/runs/{run_id}/attempts/{attempt_no}/log", response_class=PlainTextResponse)
    def attempt_log(run_id: int, attempt_no: int, tail: int = Query(200, ge=1, le=20000),
                    s: Session = Depends(db)) -> str:
        a = s.scalars(select(RunAttempt).where(RunAttempt.run_id == run_id,
                                               RunAttempt.attempt_no == attempt_no)).first()
        if a is None or not a.log_path or not Path(a.log_path).exists():
            _nf(Exception(f"log for run {run_id} attempt {attempt_no}"))
        with open(a.log_path, errors="replace") as f:
            return "".join(deque(f, maxlen=tail))

    # ---------------- status (dashboards)
    @app.get("/status")
    def status(s: Session = Depends(db)) -> Dict:
        """Everything a dashboard needs in one call: summary + one row per job."""
        return {**service.summary(s), "job_status": service.job_status(s)}

    @app.get("/status/jobs")
    def status_jobs(s: Session = Depends(db)) -> List[Dict]:
        """Flat array of per-job rows (handy as a Grafana Infinity table source)."""
        return service.job_status(s)

    @app.get("/metrics")
    def metrics():
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    return app
