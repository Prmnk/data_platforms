"""Runs submitted jobs as PySpark client processes against the Sail server.

There is no scheduler: a run starts as soon as one of MAX_CONCURRENT_RUNS slots is free,
in submission order. Each run is a child process

    python -m saillab.bootstrap --remote sc://sail:50051 --session <run id> ... -- <script> [args]

with stdout/stderr in <log_dir>/<run id>.log. Sail keeps executing a query after its client
dies, so cancel and timeout first interrupt the run's Spark Connect session from a fresh
client, then signal the process group.
"""
from __future__ import annotations

import json
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional

from . import metrics
from .config import Settings
from .store import Run, Store

log = logging.getLogger(__name__)

SAFE_NAME = re.compile(r"[^A-Za-z0-9_.-]+")


class SubmitError(ValueError):
    pass


@dataclass
class Submission:
    job: Optional[str] = None             # path relative to jobs_dir (.py or .sql)
    script: Optional[str] = None          # uploaded script content ...
    filename: Optional[str] = None        # ... and its file name (.py or .sql)
    sql: Optional[str] = None             # SQL text, one or more statements
    args: Optional[List[str]] = None
    conf: Optional[Dict[str, str]] = None
    timeout_seconds: Optional[int] = None
    name: Optional[str] = None            # metrics/grouping label; defaults to the file stem


@dataclass
class _Proc:
    pid: int
    popen: subprocess.Popen
    stop_reason: Optional[str] = None     # cancelled | timed_out
    kill_at: Optional[float] = None


def _maxrss_bytes(ru_maxrss: int) -> int:
    return ru_maxrss if sys.platform == "darwin" else ru_maxrss * 1024


class Runner:
    def __init__(self, settings: Settings, store: Store,
                 command: Optional[Callable[[Settings, Run, Path], List[str]]] = None):
        self.s = settings
        self.store = store
        self.command = command or bootstrap_command
        self._procs: Dict[str, _Proc] = {}
        self._lock = threading.RLock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_refresh = 0.0
        for d in (self.s.log_dir, self.s.uploads_dir, self.s.results_dir):
            d.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        # Child processes do not survive a restart of this service; their runs are lost.
        for run in self.store.by_status("running"):
            self.store.update(run.id, status="lost", finished_at=time.time(),
                              error="submit service restarted while the run was active")
        metrics.SLOTS.set(self.s.max_concurrent_runs)
        self._refresh(force=True)
        self._thread = threading.Thread(target=self._loop, name="runner", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=5)

    # ------------------------------------------------------------------ API
    def log_path(self, run_id: str) -> Path:
        return self.s.log_dir / f"{run_id}.log"

    def submit(self, sub: Submission) -> Run:
        given = [x for x in (sub.job, sub.script, sub.sql) if x]
        if len(given) != 1:
            raise SubmitError("give exactly one of: job, script, sql")
        run_id = str(uuid.uuid4())
        if sub.job:
            target = self._job_path(sub.job)
            kind, stem = "job", target.stem
        elif sub.script:
            fname = SAFE_NAME.sub("_", Path(sub.filename or "script.py").name)
            if not fname.endswith((".py", ".sql")):
                raise SubmitError("uploaded scripts must be .py or .sql files")
            target = self.s.uploads_dir / run_id / fname
            target.parent.mkdir(parents=True)
            target.write_text(sub.script)
            kind, stem = "upload", target.stem
        else:
            target = self.s.uploads_dir / run_id / "query.sql"
            target.parent.mkdir(parents=True)
            target.write_text(sub.sql)
            kind, stem = "sql", "sql"
        if sub.timeout_seconds is not None and sub.timeout_seconds <= 0:
            raise SubmitError("timeout_seconds must be positive")
        run = Run(id=run_id, job=SAFE_NAME.sub("_", sub.name or stem), kind=kind, target=str(target),
                  args=list(sub.args or []), conf={str(k): str(v) for k, v in (sub.conf or {}).items()},
                  timeout_seconds=sub.timeout_seconds)
        self.store.insert(run)
        metrics.RUNS_SUBMITTED.labels(run.job).inc()
        self._refresh(force=True)
        self._wake.set()
        return run

    def cancel(self, run_id: str) -> Run:
        run = self.store.get(run_id)
        if run is None:
            raise KeyError(run_id)
        with self._lock:
            if run.status == "queued":
                self._finish(run, "cancelled", error="cancelled before it started")
            elif run.status == "running" and run.id in self._procs:
                self._stop_proc(run, "cancelled")
        return self.store.get(run.id)

    def list_jobs(self) -> List[Dict[str, str]]:
        out = []
        for p in sorted(self.s.jobs_dir.rglob("*")):
            if p.suffix not in (".py", ".sql") or p.name.startswith("_") or "__pycache__" in p.parts:
                continue
            out.append({"job": str(p.relative_to(self.s.jobs_dir)), "kind": p.suffix[1:],
                        "summary": describe(p)})
        return out

    # ------------------------------------------------------------------ internals
    def _job_path(self, job: str) -> Path:
        root = self.s.jobs_dir.resolve()
        p = (root / job).resolve()
        if root not in p.parents or p.suffix not in (".py", ".sql"):
            raise SubmitError(f"unknown job {job!r}; see GET /jobs")
        if not p.is_file():
            raise SubmitError(f"job {job!r} not found in {root}")
        return p

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception:  # keep the loop alive; one bad run must not stop the service
                log.exception("runner tick failed")
            self._wake.wait(self.s.poll_seconds)
            self._wake.clear()

    def _tick(self) -> None:
        with self._lock:
            self._reap()
            self._enforce_deadlines()
            free = self.s.max_concurrent_runs - len(self._procs)
            for run in self.store.by_status("queued")[:max(free, 0)]:
                self._launch(run)
        self._refresh()

    def _launch(self, run: Run) -> None:
        log_path = self.log_path(run.id)
        try:
            cmd = self.command(self.s, run, self.s.results_dir / f"{run.id}.json")
            # jobs/ is a bind mount, slow to stat on Docker Desktop. It must not be the working
            # directory or on PYTHONPATH, where every pandas/pyspark import would search it
            # first; the bootstrap appends it to the end of sys.path instead.
            env = {**os.environ, "LAB_RUN_ID": run.id, "LAB_JOB": run.job, "LAB_JOBS_DIR": str(self.s.jobs_dir)}
            with open(log_path, "ab") as logf:
                popen = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                         cwd=str(self.s.data_dir), env=env, start_new_session=True)
        except OSError as e:
            self._finish(run, "failed", error=f"could not start: {e}")
            return
        self._procs[run.id] = _Proc(popen.pid, popen)
        now = time.time()
        self.store.update(run.id, status="running", started_at=now, pid=popen.pid)
        metrics.RUN_QUEUE.labels(run.job).observe(now - run.submitted_at)
        log.info("run %s (%s) started, pid %s", run.id, run.job, popen.pid)

    def _reap(self) -> None:
        for run_id, proc in list(self._procs.items()):
            try:
                pid, status, rusage = os.wait4(proc.pid, os.WNOHANG)
            except ChildProcessError:
                pid, status, rusage = proc.pid, 0, None
            if pid == 0:
                continue
            code = os.waitstatus_to_exitcode(status)
            proc.popen.returncode = code  # we reaped it; stop Popen from trying again
            del self._procs[run_id]
            run = self.store.get(run_id)
            final = proc.stop_reason or ("succeeded" if code == 0 else "failed")
            error = {"cancelled": "cancelled by user",
                     "timed_out": f"timed out after {run.timeout_seconds}s"}.get(final)
            if final == "failed":
                error = f"exit code {code}"
            self._finish(run, final, exit_code=code, error=error,
                         max_rss_bytes=_maxrss_bytes(rusage.ru_maxrss) if rusage else None)

    def _enforce_deadlines(self) -> None:
        now = time.time()
        for run_id, proc in list(self._procs.items()):
            if proc.kill_at and now >= proc.kill_at:
                log.warning("run %s ignored SIGTERM, sending SIGKILL", run_id)
                self._signal(proc, signal.SIGKILL)
                proc.kill_at = None
            elif not proc.stop_reason:
                run = self.store.get(run_id)
                if run.timeout_seconds and run.started_at and now - run.started_at > run.timeout_seconds:
                    self._stop_proc(run, "timed_out")

    def _stop_proc(self, run: Run, reason: str) -> None:
        proc = self._procs[run.id]
        if proc.stop_reason:
            return
        proc.stop_reason = reason
        proc.kill_at = time.time() + self.s.kill_grace_seconds + 30  # interrupt + grace

        def _do() -> None:
            self._interrupt_session(run)
            self._signal(proc, signal.SIGTERM)
            proc.kill_at = time.time() + self.s.kill_grace_seconds
        threading.Thread(target=_do, name=f"stop-{run.id[:8]}", daemon=True).start()

    def _interrupt_session(self, run: Run) -> None:
        """Interrupt every operation of the run's Sail session. Best effort."""
        cmd = [self.s.python, "-m", "saillab.bootstrap", "--remote", self.s.sail_remote,
               "--session", run.id, "--interrupt"]
        try:
            with open(self.log_path(run.id), "ab") as logf:
                logf.write(f"\n[lab] interrupting Sail session {run.id}\n".encode())
                logf.flush()
                subprocess.run(cmd, stdout=logf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                               timeout=30, check=False)
        except (OSError, subprocess.TimeoutExpired) as e:
            log.warning("could not interrupt session of run %s: %s", run.id, e)

    @staticmethod
    def _signal(proc: _Proc, sig: int) -> None:
        try:
            os.killpg(proc.pid, sig)
        except (ProcessLookupError, PermissionError):
            pass

    def _finish(self, run: Run, status: str, **fields) -> None:
        result = dict(run.result)
        rf = self.s.results_dir / f"{run.id}.json"
        try:
            result.update(json.loads(rf.read_text()))
        except (OSError, ValueError):
            pass
        self.store.update(run.id, status=status, finished_at=time.time(), result=result, **fields)
        done = self.store.get(run.id)
        metrics.finished(done)
        with open(self.log_path(run.id), "ab") as logf:
            logf.write(f"\n[lab] run {status}"
                       f"{' (' + done.error + ')' if done.error else ''}"
                       f"{f' in {done.duration_seconds:.1f}s' if done.duration_seconds else ''}\n".encode())
        log.info("run %s (%s) %s", run.id, run.job, status)
        self._refresh(force=True)

    def _refresh(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_refresh < 5:
            return
        self._last_refresh = now
        queued = len(self.store.by_status("queued"))
        metrics.refresh(self.store.list(limit=self.s.recent_runs), queued=queued, running=len(self._procs))


def bootstrap_command(s: Settings, run: Run, result_file: Path) -> List[str]:
    cmd = [s.python, "-m", "saillab.bootstrap", "--remote", s.sail_remote, "--session", run.id,
           "--result-file", str(result_file), "--app-name", f"lab:{run.job}"]
    for k, v in run.conf.items():
        cmd += ["--conf", f"{k}={v}"]
    if run.target.endswith(".sql"):
        return cmd + ["--module", "saillab.sql_runner", "--", run.target, *run.args]
    return cmd + ["--", run.target, *run.args]


def describe(path: Path) -> str:
    """First line of a job's docstring (.py) or of its leading -- comments (.sql)."""
    import ast
    try:
        text = path.read_text()
    except OSError:
        return ""
    if path.suffix == ".py":
        try:
            doc = ast.get_docstring(ast.parse(text)) or ""
        except SyntaxError:
            return ""
        return doc.strip().splitlines()[0] if doc.strip() else ""
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("--") and line.lstrip("- ").strip():
            return line.lstrip("- ").strip()
        if line:
            break
    return ""
