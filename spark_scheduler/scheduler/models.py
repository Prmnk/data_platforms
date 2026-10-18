from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import JSON, ForeignKey, Index, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    """Naive UTC; every timestamp in the DB uses this convention."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass


# Run states
PENDING = "PENDING"
RUNNING = "RUNNING"
RETRY_WAIT = "RETRY_WAIT"
SUCCEEDED = "SUCCEEDED"
DEAD = "DEAD"  # failed and out of retries
CANCELLED = "CANCELLED"

RUN_STATES = [PENDING, RUNNING, RETRY_WAIT, SUCCEEDED, DEAD, CANCELLED]
TERMINAL_RUN_STATES = {SUCCEEDED, DEAD, CANCELLED}
QUEUED_RUN_STATES = {PENDING, RETRY_WAIT}


class Job(Base):
    __tablename__ = "jobs"

    name: Mapped[str] = mapped_column(String(100), primary_key=True)
    spec: Mapped[dict] = mapped_column(JSON)
    schedule: Mapped[Optional[str]] = mapped_column(String(100))
    enabled: Mapped[bool] = mapped_column(default=True)
    version: Mapped[int] = mapped_column(default=1)
    # Logical time of the most recent scheduled run that was created.
    last_scheduled_at: Mapped[Optional[datetime]]
    created_at: Mapped[datetime] = mapped_column(default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(default=utcnow, onupdate=utcnow)


class JobRun(Base):
    __tablename__ = "job_runs"
    __table_args__ = (
        Index("ix_runs_state_next", "state", "next_attempt_at"),
        Index("ix_runs_job_created", "job_name", "created_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    job_name: Mapped[str] = mapped_column(String(100), index=True)
    logical_date: Mapped[datetime]
    # "<job>@<logical_date>" for scheduled runs so a fire time is never queued twice;
    # NULL for manual runs, which may repeat freely.
    dedupe_key: Mapped[Optional[str]] = mapped_column(String(160), unique=True)
    trigger: Mapped[str] = mapped_column(String(20))  # schedule | manual
    batch_id: Mapped[Optional[str]] = mapped_column(String(40), index=True)
    state: Mapped[str] = mapped_column(String(20), default=PENDING)
    attempt_count: Mapped[int] = mapped_column(default=0)
    max_attempts: Mapped[int] = mapped_column(default=1)
    next_attempt_at: Mapped[Optional[datetime]]
    cancel_requested: Mapped[bool] = mapped_column(default=False)
    last_error: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)
    started_at: Mapped[Optional[datetime]]
    finished_at: Mapped[Optional[datetime]]

    attempts: Mapped[List["RunAttempt"]] = relationship(
        back_populates="run", order_by="RunAttempt.attempt_no", cascade="all, delete-orphan"
    )


class RunAttempt(Base):
    __tablename__ = "run_attempts"
    __table_args__ = (Index("ix_attempts_state", "state"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("job_runs.id", ondelete="CASCADE"))
    attempt_no: Mapped[int]
    state: Mapped[str] = mapped_column(String(20))  # RUNNING | SUCCEEDED | FAILED | TIMED_OUT | LOST | CANCELLED
    pid: Mapped[Optional[int]]
    app_name: Mapped[Optional[str]] = mapped_column(String(200))
    app_id: Mapped[Optional[str]] = mapped_column(String(100))
    cluster_state: Mapped[Optional[str]] = mapped_column(String(30))
    ui_port: Mapped[Optional[int]]
    exit_code: Mapped[Optional[int]]
    log_path: Mapped[Optional[str]] = mapped_column(Text)
    error: Mapped[Optional[str]] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(default=utcnow)
    finished_at: Mapped[Optional[datetime]]
    last_polled_at: Mapped[Optional[datetime]]

    run: Mapped[JobRun] = relationship(back_populates="attempts")


class SchedulerHeartbeat(Base):
    __tablename__ = "scheduler_heartbeat"

    id: Mapped[int] = mapped_column(primary_key=True)
    holder: Mapped[str] = mapped_column(String(200))
    beat_at: Mapped[datetime]
