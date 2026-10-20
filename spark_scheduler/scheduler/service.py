"""Operations shared by the API and the CLI: apply jobs, trigger runs, cancel, status."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from . import models as m
from .cron import next_fire
from .models import Job, JobRun, SchedulerHeartbeat, utcnow
from .spec import JobSpec


class NotFound(Exception):
    pass


class Conflict(Exception):
    pass


def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() + "Z" if dt else None


def apply_jobs(s: Session, specs: Sequence[JobSpec], now: Optional[datetime] = None) -> Dict[str, str]:
    """Create or update jobs. Returns {name: "created" | "updated" | "unchanged"}."""
    now = now or utcnow()
    result = {}
    for spec in specs:
        data = spec.model_dump()
        job = s.get(Job, spec.name)
        if job is None:
            s.add(Job(name=spec.name, spec=data, schedule=spec.schedule, enabled=spec.enabled,
                      last_scheduled_at=now, created_at=now, updated_at=now))
            result[spec.name] = "created"
        elif job.spec != data:
            if job.schedule != spec.schedule:
                job.last_scheduled_at = now  # don't backfill under a new schedule
            job.spec, job.schedule, job.enabled = data, spec.schedule, spec.enabled
            job.version += 1
            result[spec.name] = "updated"
        else:
            result[spec.name] = "unchanged"
    s.commit()
    return result


def set_enabled(s: Session, name: str, enabled: bool, now: Optional[datetime] = None) -> Job:
    job = s.get(Job, name)
    if job is None:
        raise NotFound(name)
    if enabled and not job.enabled:
        job.last_scheduled_at = now or utcnow()  # resume from now, no backfill
    job.enabled = enabled
    job.spec = {**job.spec, "enabled": enabled}
    s.commit()
    return job


def delete_job(s: Session, name: str) -> None:
    job = s.get(Job, name)
    if job is None:
        raise NotFound(name)
    active = s.scalar(select(func.count()).select_from(JobRun).where(
        JobRun.job_name == name, JobRun.state.notin_(m.TERMINAL_RUN_STATES)))
    if active:
        raise Conflict(f"{name} has {active} active run(s); cancel them first")
    s.delete(job)
    s.commit()


def create_run(s: Session, job: Job, logical_date: datetime, trigger: str,
               batch_id: Optional[str] = None, now: Optional[datetime] = None) -> JobRun:
    spec = JobSpec.model_validate(job.spec)
    dedupe = f"{job.name}@{logical_date.isoformat()}" if trigger == "schedule" else None
    run = JobRun(job_name=job.name, logical_date=logical_date, dedupe_key=dedupe, trigger=trigger,
                 batch_id=batch_id,
                 state=m.PENDING, max_attempts=spec.retry.max_attempts,
                 next_attempt_at=now or utcnow(), created_at=now or utcnow())
    s.add(run)
    return run


def trigger_runs(s: Session, names: Sequence[str], now: Optional[datetime] = None) -> Dict:
    """Queue one manual run for each job, all sharing a batch id."""
    now = now or utcnow()
    missing = [n for n in names if s.get(Job, n) is None]
    if missing:
        raise NotFound(", ".join(missing))
    batch_id = uuid.uuid4().hex[:12]
    runs = []
    for n in dict.fromkeys(names):
        runs.append(create_run(s, s.get(Job, n), now, "manual", batch_id, now))
    s.commit()
    return {"batch_id": batch_id, "runs": [{"run_id": r.id, "job": r.job_name} for r in runs]}


def request_cancel(s: Session, run_id: int) -> JobRun:
    run = s.get(JobRun, run_id)
    if run is None:
        raise NotFound(str(run_id))
    if run.state in m.TERMINAL_RUN_STATES:
        raise Conflict(f"run {run_id} is already {run.state}")
    run.cancel_requested = True
    s.commit()
    return run


# ---------- read models ----------

def run_to_dict(r: JobRun, with_attempts: bool = False) -> Dict:
    d = {
        "run_id": r.id, "job": r.job_name, "state": r.state, "trigger": r.trigger,
        "batch_id": r.batch_id, "logical_date": iso(r.logical_date),
        "attempts": r.attempt_count, "max_attempts": r.max_attempts,
        "next_attempt_at": iso(r.next_attempt_at) if r.state in m.QUEUED_RUN_STATES else None,
        "created_at": iso(r.created_at), "started_at": iso(r.started_at),
        "finished_at": iso(r.finished_at),
        "duration_s": (r.finished_at - r.started_at).total_seconds()
        if r.started_at and r.finished_at else None,
        "cancel_requested": r.cancel_requested, "last_error": r.last_error,
    }
    if with_attempts:
        d["attempt_details"] = [{
            "attempt": a.attempt_no, "state": a.state, "exit_code": a.exit_code,
            "app_id": a.app_id, "cluster_state": a.cluster_state, "ui_port": a.ui_port,
            "pid": a.pid, "started_at": iso(a.started_at), "finished_at": iso(a.finished_at),
            "error": a.error,
        } for a in r.attempts]
    return d


def job_status(s: Session, now: Optional[datetime] = None) -> List[Dict]:
    """One flat row per job, for dashboards (Grafana Infinity table) and `sched status`."""
    now = now or utcnow()
    since = now - timedelta(hours=24)
    counts: Dict[str, Dict[str, int]] = {}
    for name, state, n in s.execute(
        select(JobRun.job_name, JobRun.state, func.count())
        .where(JobRun.created_at >= since).group_by(JobRun.job_name, JobRun.state)
    ):
        counts.setdefault(name, {})[state] = n
    active = {name: n for name, n in s.execute(
        select(JobRun.job_name, func.count()).where(JobRun.state == m.RUNNING).group_by(JobRun.job_name))}
    queued = {name: n for name, n in s.execute(
        select(JobRun.job_name, func.count()).where(JobRun.state.in_(m.QUEUED_RUN_STATES))
        .group_by(JobRun.job_name))}

    rows = []
    for job in s.scalars(select(Job).order_by(Job.name)):
        last = s.scalars(select(JobRun).where(JobRun.job_name == job.name)
                         .order_by(JobRun.created_at.desc(), JobRun.id.desc()).limit(1)).first()
        last_ok = s.scalars(select(JobRun).where(JobRun.job_name == job.name, JobRun.state == m.SUCCEEDED)
                            .order_by(JobRun.finished_at.desc()).limit(1)).first()
        c = counts.get(job.name, {})
        rows.append({
            "job": job.name,
            "enabled": job.enabled,
            "engine": job.spec.get("engine", "spark"),
            "schedule": job.schedule,
            "next_fire_at": iso(next_fire(job.schedule, max(now, job.last_scheduled_at or now)))
            if job.enabled else None,
            "last_run_id": last.id if last else None,
            "last_state": last.state if last else None,
            "last_attempts": last.attempt_count if last else None,
            "last_started_at": iso(last.started_at) if last else None,
            "last_finished_at": iso(last.finished_at) if last else None,
            "last_duration_s": (last.finished_at - last.started_at).total_seconds()
            if last and last.started_at and last.finished_at else None,
            "last_error": last.last_error if last else None,
            "last_success_at": iso(last_ok.finished_at) if last_ok else None,
            "running": active.get(job.name, 0),
            "queued": queued.get(job.name, 0),
            "succeeded_24h": c.get(m.SUCCEEDED, 0),
            "dead_24h": c.get(m.DEAD, 0),
            "cancelled_24h": c.get(m.CANCELLED, 0),
        })
    return rows


def summary(s: Session, now: Optional[datetime] = None) -> Dict:
    now = now or utcnow()
    by_state = {st: 0 for st in m.RUN_STATES}
    for st, n in s.execute(select(JobRun.state, func.count()).group_by(JobRun.state)):
        by_state[st] = n
    hb = s.get(SchedulerHeartbeat, 1)
    return {
        "jobs": s.scalar(select(func.count()).select_from(Job)),
        "jobs_enabled": s.scalar(select(func.count()).select_from(Job).where(Job.enabled.is_(True))),
        "runs_by_state": by_state,
        "scheduler": {
            "holder": hb.holder if hb else None,
            "last_heartbeat": iso(hb.beat_at) if hb else None,
            "heartbeat_age_s": (now - hb.beat_at).total_seconds() if hb else None,
        },
    }
