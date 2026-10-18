from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


@dataclass
class Settings:
    database_url: str = field(
        default_factory=lambda: os.environ.get("DATABASE_URL", "sqlite:///sparksched.db")
    )
    spark_master_url: str = field(
        default_factory=lambda: os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
    )
    spark_master_ui: str = field(
        default_factory=lambda: os.environ.get("SPARK_MASTER_UI", "http://spark-master:8080")
    )
    spark_submit: str = field(
        default_factory=lambda: os.environ.get("SPARK_SUBMIT", "/opt/spark/bin/spark-submit")
    )
    # Hostname executors use to reach drivers running inside the scheduler container.
    driver_host: str = field(
        default_factory=lambda: os.environ.get("DRIVER_HOST", socket.gethostname())
    )
    log_dir: str = field(default_factory=lambda: os.environ.get("LOG_DIR", "./logs"))
    tick_seconds: float = field(default_factory=lambda: float(os.environ.get("TICK_SECONDS", 5)))
    max_concurrent_runs: int = field(default_factory=lambda: _env_int("MAX_CONCURRENT_RUNS", 4))
    ui_port_start: int = field(default_factory=lambda: _env_int("UI_PORT_START", 4040))
    ui_port_count: int = field(default_factory=lambda: _env_int("UI_PORT_COUNT", 10))
    kill_grace_seconds: float = field(
        default_factory=lambda: float(os.environ.get("KILL_GRACE_SECONDS", 10))
    )
    # Max scheduled runs created per job per tick when catchup is enabled.
    max_catchup_runs: int = field(default_factory=lambda: _env_int("MAX_CATCHUP_RUNS", 50))


def get_settings() -> Settings:
    return Settings()
