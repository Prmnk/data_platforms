"""Job specifications: what users write in jobs.yaml."""
from __future__ import annotations

import re
from typing import Dict, List, Literal, Optional

import yaml
from croniter import croniter
from pydantic import BaseModel, Field, field_validator, model_validator

NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")


class RetryPolicy(BaseModel):
    max_attempts: int = Field(1, ge=1, le=50, description="Total tries, including the first.")
    backoff: str = Field("exponential", pattern="^(fixed|exponential)$")
    initial_delay_s: float = Field(30, ge=0)
    max_delay_s: float = Field(600, ge=0)
    jitter: float = Field(0.1, ge=0, le=1, description="+/- fraction of the delay.")
    # Empty list = retry on any non-zero exit code.
    retry_on_exit_codes: List[int] = Field(default_factory=list)
    retry_on_timeout: bool = True


class JobSpec(BaseModel):
    name: str
    description: str = ""
    # Cron expression (UTC). None = only runs when triggered manually.
    schedule: Optional[str] = None
    enabled: bool = True
    # spark: spark-submit to the standalone cluster. sail: plain Python over Spark Connect to Sail.
    engine: Literal["spark", "sail"] = "spark"
    app: str = Field(..., description="Path to the PySpark script (local to the scheduler image).")
    py_files: List[str] = Field(default_factory=list)
    args: List[str] = Field(default_factory=list)
    spark_conf: Dict[str, str] = Field(default_factory=dict)
    retry: RetryPolicy = Field(default_factory=RetryPolicy)
    timeout_s: Optional[int] = Field(None, ge=1)
    concurrency: int = Field(1, ge=1, description="Max simultaneous runs of this job.")
    catchup: bool = False
    tags: List[str] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        if not NAME_RE.match(v):
            raise ValueError("name must match " + NAME_RE.pattern)
        return v

    @field_validator("schedule")
    @classmethod
    def _schedule(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not croniter.is_valid(v):
            raise ValueError(f"invalid cron expression: {v!r}")
        return v

    @field_validator("spark_conf", mode="before")
    @classmethod
    def _conf_to_str(cls, v):
        return {k: str(val) for k, val in (v or {}).items()}

    def render_args(self, **ctx: str) -> List[str]:
        """Substitute {{ key }} placeholders (logical_date, run_id, attempt, job)."""
        out = []
        for a in self.args:
            for k, val in ctx.items():
                a = re.sub(r"\{\{\s*" + re.escape(k) + r"\s*\}\}", str(val), a)
            out.append(a)
        return out


class JobFile(BaseModel):
    jobs: List[JobSpec]

    @model_validator(mode="after")
    def _unique(self) -> "JobFile":
        names = [j.name for j in self.jobs]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"duplicate job names: {sorted(dupes)}")
        return self


def load_job_file(text: str) -> JobFile:
    data = yaml.safe_load(text) or {}
    if isinstance(data, list):
        data = {"jobs": data}
    return JobFile.model_validate(data)
