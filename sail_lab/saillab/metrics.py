"""Prometheus metrics of the submit service. Sail's own engine metrics arrive separately
(Sail -> OTLP -> OTel Collector -> Prometheus) with session_id = run id."""
from __future__ import annotations

from typing import Iterable

from prometheus_client import Counter, Gauge, Histogram

from .store import Run

DURATION_BUCKETS = (1, 2, 5, 10, 20, 30, 60, 120, 300, 600, 1200, 3600)

RUNS_SUBMITTED = Counter("lab_runs_submitted_total", "Runs submitted", ["lab_job"])
RUNS_FINISHED = Counter("lab_runs_finished_total", "Runs that reached a final status", ["lab_job", "status"])
RUN_DURATION = Histogram("lab_run_duration_seconds", "Wall time from process start to exit",
                         ["lab_job", "status"], buckets=DURATION_BUCKETS)
RUN_QUEUE = Histogram("lab_run_queue_seconds", "Time a run waited for a free slot", ["lab_job"],
                      buckets=(0.1, 0.5, 1, 5, 10, 30, 60, 300, 900))
RUN_CLIENT_RSS = Histogram("lab_run_client_max_rss_bytes", "Peak memory of the PySpark client process",
                           ["lab_job"], buckets=tuple(m * 2**20 for m in (64, 128, 256, 512, 1024, 2048)))
RUNS_ACTIVE = Gauge("lab_runs_active", "Runs currently queued or running", ["state"])
SLOTS = Gauge("lab_run_slots", "Maximum concurrent runs")

# One series per recent run, so Grafana can offer a run picker and per-run tables straight
# from Prometheus. Rebuilt on every transition and capped at Settings.recent_runs.
RUN_INFO = Gauge("lab_run_info", "Recent runs; value is duration in seconds (0 while queued)",
                 ["run_id", "lab_job", "status"])
RUN_RESULT = Gauge("lab_run_result", "Numeric values a recent run reported via saillab.sdk.report",
                   ["run_id", "lab_job", "key"])


def refresh(recent: Iterable[Run], queued: int, running: int) -> None:
    RUN_INFO.clear()
    RUN_RESULT.clear()
    for r in recent:
        RUN_INFO.labels(r.id, r.job, r.status).set(r.duration_seconds or 0)
        for k, v in r.result.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                RUN_RESULT.labels(r.id, r.job, k).set(v)
    RUNS_ACTIVE.labels("queued").set(queued)
    RUNS_ACTIVE.labels("running").set(running)


def finished(run: Run) -> None:
    RUNS_FINISHED.labels(run.job, run.status).inc()
    if run.duration_seconds is not None:
        RUN_DURATION.labels(run.job, run.status).observe(run.duration_seconds)
    if run.max_rss_bytes:
        RUN_CLIENT_RSS.labels(run.job).observe(run.max_rss_bytes)
