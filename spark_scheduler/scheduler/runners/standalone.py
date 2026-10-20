"""Runs jobs with `spark-submit --deploy-mode client` against a Spark standalone master.

Standalone mode cannot run PySpark drivers in cluster deploy mode, so the driver runs
as a child process of the scheduler. Each attempt is wrapped in a tiny shell that
records the exit code to a file, so an attempt can still be resolved after the
scheduler process itself restarts.
"""
from __future__ import annotations

import logging
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional

import httpx

from ..config import Settings
from .base import ClusterApp, Handle, LaunchRequest, PollResult

log = logging.getLogger(__name__)

_WRAPPER = '"$@"; rc=$?; echo "$rc" > "$SPARKSCHED_EXIT_FILE"; exit "$rc"'


def exit_file_for(log_path: str) -> Path:
    return Path(log_path).with_suffix(".exit")


def _pid_alive(pid: int, app_name: str) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    # Guard against pid reuse: on Linux confirm the process is really our attempt.
    cmdline = Path(f"/proc/{pid}/cmdline")
    if cmdline.exists():
        try:
            return app_name.encode() in cmdline.read_bytes()
        except OSError:
            return False
    return True


class SparkSubmitRunner:
    def __init__(self, settings: Settings):
        self.s = settings

    def build_command(self, req: LaunchRequest) -> List[str]:
        spec = req.spec
        cmd = [
            self.s.spark_submit,
            "--master", self.s.spark_master_url,
            "--deploy-mode", "client",
            "--name", req.app_name,
            "--conf", f"spark.driver.host={self.s.driver_host}",
            "--conf", "spark.driver.bindAddress=0.0.0.0",
        ]
        if req.ui_port:
            cmd += ["--conf", f"spark.ui.port={req.ui_port}"]
        for k, v in spec.spark_conf.items():
            cmd += ["--conf", f"{k}={v}"]
        if spec.py_files:
            cmd += ["--py-files", ",".join(spec.py_files)]
        cmd.append(spec.app)
        cmd += spec.render_args(
            logical_date=req.logical_date.isoformat(),
            run_id=str(req.run_id),
            attempt=str(req.attempt_no),
            job=spec.name,
        )
        return cmd

    def log_path_for(self, req: LaunchRequest) -> Path:
        log_dir = Path(self.s.log_dir) / req.spec.name / str(req.run_id)
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir / f"attempt-{req.attempt_no}.log"

    def launch(self, req: LaunchRequest) -> Handle:
        return self.spawn(req, self.build_command(req), self.log_path_for(req))

    def spawn(self, req: LaunchRequest, cmd: List[str], log_path: Path,
              extra_env: Optional[Dict[str, str]] = None) -> Handle:
        """Start `cmd` in its own process group, wrapped so its exit code lands in <log>.exit."""
        exit_file = exit_file_for(str(log_path))
        exit_file.unlink(missing_ok=True)

        env = dict(os.environ)
        env.update(
            SPARKSCHED_EXIT_FILE=str(exit_file),
            SPARKSCHED_JOB=req.spec.name,
            SPARKSCHED_RUN_ID=str(req.run_id),
            SPARKSCHED_ATTEMPT=str(req.attempt_no),
        )
        env.update(extra_env or {})
        with open(log_path, "ab") as logf:
            logf.write(("$ " + " ".join(cmd) + "\n").encode())
            logf.flush()
            proc = subprocess.Popen(
                ["/bin/sh", "-c", _WRAPPER, "sparksched", *cmd],
                stdout=logf,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,  # own process group, so kill() takes the JVM down too
                env=env,
            )
        log.info("launched %s pid=%s log=%s", req.app_name, proc.pid, log_path)
        return Handle(pid=proc.pid, app_name=req.app_name, log_path=str(log_path), proc=proc)

    def adopt(self, pid: int, app_name: str, log_path: str) -> Handle:
        return Handle(pid=pid, app_name=app_name, log_path=log_path, proc=None)

    def poll(self, handle: Handle) -> PollResult:
        if handle.proc is not None:
            rc = handle.proc.poll()
            return PollResult("running") if rc is None else PollResult("exited", rc)
        exit_file = exit_file_for(handle.log_path)
        if exit_file.exists():
            try:
                return PollResult("exited", int(exit_file.read_text().strip()))
            except ValueError:
                pass
        if _pid_alive(handle.pid, handle.app_name):
            return PollResult("running")
        return PollResult("lost")

    def kill(self, handle: Handle) -> None:
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(handle.pid, sig)
            except ProcessLookupError:
                return
            deadline = time.monotonic() + self.s.kill_grace_seconds
            while time.monotonic() < deadline:
                if handle.proc is not None:
                    if handle.proc.poll() is not None:
                        return
                elif not _pid_alive(handle.pid, handle.app_name):
                    return
                time.sleep(0.2)
            log.warning("pid %s ignored %s", handle.pid, sig.name)

    def cluster_apps(self) -> Dict[str, ClusterApp]:
        try:
            data = httpx.get(f"{self.s.spark_master_ui}/json/", timeout=3).json()
        except Exception as e:  # noqa: BLE001 - monitoring is best effort
            log.debug("master /json unavailable: %s", e)
            return {}
        apps: Dict[str, ClusterApp] = {}
        for key in ("completedapps", "activeapps"):  # active wins on name clash
            for a in data.get(key, []):
                apps[a.get("name", "")] = ClusterApp(app_id=a.get("id", ""), state=a.get("state", ""))
        return apps
