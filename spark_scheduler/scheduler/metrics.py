"""Prometheus metrics computed from the database at scrape time."""
from __future__ import annotations

from datetime import datetime, timedelta

from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from . import models as m
from .models import Job, JobRun, RunAttempt, SchedulerHeartbeat, utcnow


def _ts(dt: datetime) -> float:
    return (dt - datetime(1970, 1, 1)).total_seconds()


class DbCollector(Collector):
    def __init__(self, session_factory: sessionmaker[Session]):
        self.sf = session_factory

    def collect(self):
        now = utcnow()
        since = now - timedelta(hours=24)
        with self.sf() as s:
            info = GaugeMetricFamily("sparksched_job_info", "Registered jobs", labels=["job", "schedule", "enabled"])
            last_state = GaugeMetricFamily(
                "sparksched_job_last_run_state", "1 for the state of the job's latest run",
                labels=["job", "state"])
            last_ok = GaugeMetricFamily(
                "sparksched_job_last_success_timestamp_seconds", "Finish time of the last successful run",
                labels=["job"])
            last_dur = GaugeMetricFamily(
                "sparksched_job_last_run_duration_seconds", "Duration of the latest finished run",
                labels=["job"])
            for job in s.scalars(select(Job)):
                info.add_metric([job.name, job.schedule or "", str(job.enabled).lower()], 1)
                last = s.scalars(select(JobRun).where(JobRun.job_name == job.name)
                                 .order_by(JobRun.created_at.desc(), JobRun.id.desc()).limit(1)).first()
                for st in m.RUN_STATES:
                    last_state.add_metric([job.name, st], 1 if last and last.state == st else 0)
                ok = s.scalar(select(func.max(JobRun.finished_at)).where(
                    JobRun.job_name == job.name, JobRun.state == m.SUCCEEDED))
                if ok:
                    last_ok.add_metric([job.name], _ts(ok))
                fin = s.scalars(select(JobRun).where(JobRun.job_name == job.name, JobRun.finished_at.isnot(None),
                                                     JobRun.started_at.isnot(None))
                                .order_by(JobRun.finished_at.desc()).limit(1)).first()
                if fin:
                    last_dur.add_metric([job.name], (fin.finished_at - fin.started_at).total_seconds())
            yield from (info, last_state, last_ok, last_dur)

            runs = GaugeMetricFamily("sparksched_runs", "Runs currently in each state", labels=["state"])
            by_state = dict(s.execute(select(JobRun.state, func.count()).group_by(JobRun.state)).all())
            for st in m.RUN_STATES:
                runs.add_metric([st], by_state.get(st, 0))
            yield runs

            runs24 = GaugeMetricFamily("sparksched_runs_24h", "Runs created in the last 24h by job and state",
                                       labels=["job", "state"])
            for job, st, n in s.execute(select(JobRun.job_name, JobRun.state, func.count())
                                        .where(JobRun.created_at >= since)
                                        .group_by(JobRun.job_name, JobRun.state)):
                runs24.add_metric([job, st], n)
            yield runs24

            att24 = GaugeMetricFamily("sparksched_attempts_24h", "Attempts finished in the last 24h by outcome",
                                      labels=["job", "outcome"])
            for job, st, n in s.execute(select(JobRun.job_name, RunAttempt.state, func.count())
                                        .join(JobRun, RunAttempt.run_id == JobRun.id)
                                        .where(RunAttempt.finished_at >= since)
                                        .group_by(JobRun.job_name, RunAttempt.state)):
                att24.add_metric([job, st], n)
            yield att24

            hb = s.get(SchedulerHeartbeat, 1)
            g = GaugeMetricFamily("sparksched_scheduler_heartbeat_timestamp_seconds",
                                  "Last time the scheduler loop ticked")
            if hb:
                g.add_metric([], _ts(hb.beat_at))
            yield g
