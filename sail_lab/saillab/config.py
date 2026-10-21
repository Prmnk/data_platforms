from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    sail_remote: str = "sc://sail:50051"
    jobs_dir: Path = Path("/opt/lab/jobs")
    data_dir: Path = Path("/var/lib/lab")       # SQLite DB, uploaded scripts, result files
    log_dir: Path = Path("/var/log/lab/runs")   # one <run_id>.log per run, tailed into Loki
    max_concurrent_runs: int = 2
    kill_grace_seconds: int = 10
    recent_runs: int = 50                        # runs exported as lab_run_info / lab_run_result
    python: str = field(default=sys.executable)
    grafana_url: str = "http://localhost:3002"
    poll_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            sail_remote=_env("SAIL_REMOTE", cls.sail_remote),
            jobs_dir=Path(_env("JOBS_DIR", str(cls.jobs_dir))),
            data_dir=Path(_env("DATA_DIR", str(cls.data_dir))),
            log_dir=Path(_env("LOG_DIR", str(cls.log_dir))),
            max_concurrent_runs=int(_env("MAX_CONCURRENT_RUNS", str(cls.max_concurrent_runs))),
            kill_grace_seconds=int(_env("KILL_GRACE_SECONDS", str(cls.kill_grace_seconds))),
            recent_runs=int(_env("RECENT_RUNS", str(cls.recent_runs))),
            grafana_url=_env("GRAFANA_PUBLIC_URL", cls.grafana_url),
        )

    @property
    def db_path(self) -> Path:
        return self.data_dir / "lab.db"

    @property
    def uploads_dir(self) -> Path:
        return self.data_dir / "uploads"

    @property
    def results_dir(self) -> Path:
        return self.data_dir / "results"
