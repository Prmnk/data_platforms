"""The scheduler loop: create due runs, launch, poll, time out, retry, cancel, recover."""
from __future__ import annotations

import logging
import socket
import threading
from datetime import datetime, timedelta
from typing import Callable, Dict, Optional

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from . import models as m
from . import retry
from .config import Settings
from .cron import due_times
from .models import Job, JobRun, RunAttempt, SchedulerHeartbeat, utcnow
from .runners.base import Handle, LaunchRequest, Runner
from .service import create_run
from .spec import JobSpec

log = logging.getLogger(__name__)


class Scheduler:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        runner: Runner,
        settings: Settings,
        clock: Callable[[], datetime] = utcnow,
        leader_check: Optional[Callable[[], bool]] = None,
    ):
        self.sf = session_factory
        self.runner = runner
        self.s = settings
        self.clock = clock
        self.leader_check = leader_check
        self.holder = f"{socket.gethostname()}"
        self._live: Dict[int, Handle] = {}  # attempt id -> handle
        self._stop = threading.Event()

    # ------------------------------------------------------------------ lifecycle

    def recover(self) -> None:
        """Re-attach to attempts left RUNNING by a previous scheduler process."""
        with self.sf() as s:
            for a in s.scalars(select(RunAttempt).where(RunAttempt.state == m.RUNNING)):
                if a.pid and a.app_name and a.log_path:
                    self._live[a.id] = self.runner.adopt(a.pid, a.app_name, a.log_path)
                    log.info("adopted attempt %s (pid %s)", a.app_name, a.pid)
        # The next poll resolves each adopted attempt (running / exited / lost).

    def run_forever(self) -> None:
        self.recover()
        while not self._stop.is_set():
            if self.leader_check and not self.leader_check():
                log.error("lost the leader lock; exiting so a standby can take over")
                raise SystemExit(1)
            try:
                self.tick()
            except Exception:  # noqa: BLE001 - keep the loop alive
                log.exception("tick failed")
            self._stop.wait(self.s.tick_seconds)

    def stop(self) -> None:
        self._stop.set()

    def tick(self) -> None:
        now = self.clock()
        with self.sf() as s:
            self._heartbeat(s, now)
            self._schedule_due(s, now)
            self._handle_cancels(s, now)
            self._poll_running(s, now)
            self._launch_ready(s, now)

    # ------------------------------------------------------------------ steps

    def _heartbeat(self, s: Session, now: datetime) -> None:
        hb = s.get(SchedulerHeartbeat, 1)
        if hb is None:
            s.add(SchedulerHeartbeat(id=1, holder=self.holder, beat_at=now))
        else:
            hb.holder, hb.beat_at = self.holder, now
        s.commit()

    def _schedule_due(self, s: Session, now: datetime) -> None:
        jobs = s.scalars(select(Job).where(Job.enabled.is_(True), Job.schedule.isnot(None))).all()
        for job in jobs:
            spec = JobSpec.model_validate(job.spec)
            last = job.last_scheduled_at or job.created_at
            times = due_times(job.schedule, last, now, spec.catchup, self.s.max_catchup_runs)
            if not times:
                continue
            for t in times:
                try:
                    with s.begin_nested():
                        create_run(s, job, t, "schedule", now=now)
                except IntegrityError:
                    log.info("run for %s @ %s already exists", job.name, t)
            job.last_scheduled_at = times[-1]
            s.commit()
            log.info("scheduled %d run(s) of %s", len(times), job.name)

    def _handle_cancels(self, s: Session, now: datetime) -> None:
        runs = s.scalars(select(JobRun).where(
            JobRun.cancel_requested.is_(True), JobRun.state.notin_(m.TERMINAL_RUN_STATES))).all()
        for run in runs:
            if run.state == m.RUNNING:
                attempt = self._current_attempt(s, run)
                if attempt is not None:
                    self._kill(attempt)
                    self._finish_attempt(s, run, attempt, retry.CANCELLED, None, "cancelled by user", now)
                    continue
            run.state, run.finished_at = m.CANCELLED, now
            s.commit()
            log.info("run %s cancelled", run.id)

    def _poll_running(self, s: Session, now: datetime) -> None:
        attempts = s.scalars(select(RunAttempt).where(RunAttempt.state == m.RUNNING)).all()
        if not attempts:
            return
        cluster = self.runner.cluster_apps()
        for a in attempts:
            run = a.run
            handle = self._live.get(a.id)
            if handle is None:
                handle = self.runner.adopt(a.pid or 0, a.app_name or "", a.log_path or "")
                self._live[a.id] = handle
            info = cluster.get(a.app_name or "")
            if info is not None:
                a.app_id, a.cluster_state = info.app_id, info.state
            a.last_polled_at = now

            spec = self._spec(s, run.job_name)
            timeout = spec.timeout_s if spec else None
            res = self.runner.poll(handle)
            if res.status == "running":
                if timeout and now - a.started_at > timedelta(seconds=timeout):
                    log.warning("%s exceeded timeout of %ss", a.app_name, timeout)
                    self._kill(a)
                    self._finish_attempt(s, run, a, retry.TIMED_OUT, None, f"timed out after {timeout}s", now)
                else:
                    s.commit()
            elif res.status == "exited":
                if res.exit_code == 0:
                    self._finish_attempt(s, run, a, retry.SUCCEEDED, 0, None, now)
                else:
                    self._finish_attempt(s, run, a, retry.FAILED, res.exit_code,
                                         f"exit code {res.exit_code}", now)
            else:
                self._finish_attempt(s, run, a, retry.LOST, None, "driver process disappeared", now)

    def _launch_ready(self, s: Session, now: datetime) -> None:
        running_total = s.scalar(select(func.count()).select_from(JobRun).where(JobRun.state == m.RUNNING))
        capacity = self.s.max_concurrent_runs - running_total
        if capacity <= 0:
            return
        per_job = dict(s.execute(select(JobRun.job_name, func.count())
                                 .where(JobRun.state == m.RUNNING).group_by(JobRun.job_name)).all())
        used_ports = set(s.scalars(select(RunAttempt.ui_port).where(
            RunAttempt.state == m.RUNNING, RunAttempt.ui_port.isnot(None))).all())

        candidates = s.scalars(
            select(JobRun)
            .where(JobRun.state.in_(m.QUEUED_RUN_STATES), JobRun.next_attempt_at <= now,
                   JobRun.cancel_requested.is_(False))
            .order_by(JobRun.next_attempt_at, JobRun.id)
            .with_for_update(skip_locked=True)
        ).all()
        for run in candidates:
            if capacity <= 0:
                break
            spec = self._spec(s, run.job_name)
            if spec is None:
                run.state, run.finished_at, run.last_error = m.CANCELLED, now, "job was deleted"
                s.commit()
                continue
            if per_job.get(run.job_name, 0) >= spec.concurrency:
                continue
            port = self._free_port(used_ports) if spec.engine == "spark" else None  # no Spark UI on Sail
            attempt_no = run.attempt_count + 1
            req = LaunchRequest(spec=spec, run_id=run.id, attempt_no=attempt_no,
                                logical_date=run.logical_date, ui_port=port)
            attempt = RunAttempt(run_id=run.id, attempt_no=attempt_no, state=m.RUNNING,
                                 app_name=req.app_name, ui_port=port, started_at=now)
            run.attempt_count = attempt_no
            run.state = m.RUNNING
            run.started_at = run.started_at or now
            s.add(attempt)
            s.flush()
            try:
                handle = self.runner.launch(req)
            except Exception as e:  # noqa: BLE001
                log.exception("launch failed for %s", req.app_name)
                self._finish_attempt(s, run, attempt, retry.FAILED, None, f"launch failed: {e}", now)
                continue
            attempt.pid, attempt.log_path = handle.pid, handle.log_path
            self._live[attempt.id] = handle
            s.commit()
            capacity -= 1
            per_job[run.job_name] = per_job.get(run.job_name, 0) + 1
            if port:
                used_ports.add(port)
        s.commit()  # release row locks held by skipped candidates

    # ------------------------------------------------------------------ helpers

    def _finish_attempt(self, s: Session, run: JobRun, a: RunAttempt, outcome: str,
                        exit_code: Optional[int], error: Optional[str], now: datetime) -> None:
        a.state, a.exit_code, a.error, a.finished_at = outcome, exit_code, error, now
        self._live.pop(a.id, None)
        run.last_error = error
        spec = self._spec(s, run.job_name)
        if outcome == retry.SUCCEEDED:
            run.state, run.finished_at = m.SUCCEEDED, now
        elif outcome == retry.CANCELLED or run.cancel_requested:
            run.state, run.finished_at = m.CANCELLED, now
        elif spec is not None and retry.should_retry(
                spec.retry.model_copy(update={"max_attempts": run.max_attempts}),
                a.attempt_no, outcome, exit_code):
            delay = retry.backoff_delay(spec.retry, a.attempt_no)
            run.state, run.next_attempt_at = m.RETRY_WAIT, now + timedelta(seconds=delay)
            log.info("run %s attempt %s %s; retrying in %.0fs", run.id, a.attempt_no, outcome, delay)
        else:
            run.state, run.finished_at = m.DEAD, now
            log.warning("run %s is DEAD after %s attempt(s): %s", run.id, a.attempt_no, error)
        s.commit()

    def _kill(self, a: RunAttempt) -> None:
        handle = self._live.get(a.id) or self.runner.adopt(a.pid or 0, a.app_name or "", a.log_path or "")
        if handle.pid:
            self.runner.kill(handle)

    def _current_attempt(self, s: Session, run: JobRun) -> Optional[RunAttempt]:
        return s.scalars(select(RunAttempt).where(
            RunAttempt.run_id == run.id, RunAttempt.state == m.RUNNING)).first()

    def _spec(self, s: Session, job_name: str) -> Optional[JobSpec]:
        job = s.get(Job, job_name)
        return JobSpec.model_validate(job.spec) if job else None

    def _free_port(self, used: set) -> Optional[int]:
        for p in range(self.s.ui_port_start, self.s.ui_port_start + self.s.ui_port_count):
            if p not in used:
                return p
        return None  # let Spark pick (it retries upward from 4040)

