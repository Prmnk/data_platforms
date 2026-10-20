"""Runs jobs on a Sail server (https://github.com/lakehq/sail) over Spark Connect.

The PySpark script runs as a plain Python process (no spark-submit, no JVM) with
SPARK_REMOTE pointing at Sail. Process handling (exit file, adopt, kill) is shared
with SparkSubmitRunner.

Each attempt gets its own Spark Connect session id, saved next to the log as
<log>.sail before the process starts. Sail keeps executing a query after its client
dies, so kill() and a lost driver first interrupt that session from a fresh client;
the session file is what lets a restarted scheduler do this for attempts it adopted.
"""
from __future__ import annotations

import logging
import os
import subprocess
import uuid
from pathlib import Path
from typing import Dict, List, Optional

from .base import ClusterApp, Handle, LaunchRequest, PollResult
from .standalone import SparkSubmitRunner

log = logging.getLogger(__name__)

BOOTSTRAP = "scheduler.runners.sail_bootstrap"


def session_file_for(log_path: str) -> Path:
    return Path(log_path).with_suffix(".sail")


def session_id_for(log_path: str) -> Optional[str]:
    try:
        return session_file_for(log_path).read_text().strip() or None
    except OSError:
        return None


class SailRunner(SparkSubmitRunner):
    def remote(self, session_id: str) -> str:
        return f"{self.s.sail_remote.rstrip('/')}/;session_id={session_id}"

    def build_command(self, req: LaunchRequest) -> List[str]:
        spec = req.spec
        cmd = [self.s.sail_python, "-m", BOOTSTRAP, "--conf", f"spark.app.name={req.app_name}"]
        for k, v in spec.spark_conf.items():
            cmd += ["--conf", f"{k}={v}"]
        for f in spec.py_files:
            cmd += ["--py-file", f]
        cmd += ["--", spec.app]
        cmd += spec.render_args(
            logical_date=req.logical_date.isoformat(),
            run_id=str(req.run_id),
            attempt=str(req.attempt_no),
            job=spec.name,
        )
        return cmd

    def launch(self, req: LaunchRequest) -> Handle:
        log_path = self.log_path_for(req)
        session_id = str(uuid.uuid4())
        session_file_for(str(log_path)).write_text(session_id + "\n")
        handle = self.spawn(req, self.build_command(req), log_path,
                            extra_env={"SPARK_REMOTE": self.remote(session_id)})
        handle.extra["session_id"] = session_id
        return handle

    def poll(self, handle: Handle) -> PollResult:
        res = super().poll(handle)
        if res.status == "lost":
            self.interrupt(handle)  # the client is gone, its query may not be
        return res

    def kill(self, handle: Handle) -> None:
        self.interrupt(handle)
        super().kill(handle)

    def interrupt(self, handle: Handle) -> None:
        """Interrupt every operation of the attempt's Sail session. Best effort."""
        session_id = session_id_for(handle.log_path)
        if not session_id:
            return
        cmd = [self.s.sail_python, "-m", BOOTSTRAP, "--interrupt"]
        try:
            with open(handle.log_path, "ab") as logf:
                logf.write(f"\n[sparksched] interrupting Sail session {session_id}\n".encode())
                logf.flush()
                subprocess.run(cmd, stdout=logf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                               env={**os.environ, "SPARK_REMOTE": self.remote(session_id)},
                               timeout=self.s.kill_grace_seconds + 30, check=False)
        except (OSError, subprocess.TimeoutExpired) as e:
            log.warning("could not interrupt Sail session %s of %s: %s", session_id, handle.app_name, e)

    def cluster_apps(self) -> Dict[str, ClusterApp]:
        return {}  # Sail has no application list like the standalone master's /json
