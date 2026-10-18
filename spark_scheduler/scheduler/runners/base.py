"""Runner interface: how a job attempt is started, polled and killed."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, Protocol

from ..spec import JobSpec


@dataclass
class LaunchRequest:
    spec: JobSpec
    run_id: int
    attempt_no: int
    logical_date: datetime
    ui_port: Optional[int] = None

    @property
    def app_name(self) -> str:
        # Unique per attempt so it can be correlated with the Spark master's app list.
        return f"{self.spec.name}__r{self.run_id}__a{self.attempt_no}"


@dataclass
class Handle:
    pid: int
    app_name: str
    log_path: str
    proc: Any = None  # subprocess.Popen when we launched it; None when adopted after restart
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PollResult:
    status: str  # "running" | "exited" | "lost"
    exit_code: Optional[int] = None


@dataclass
class ClusterApp:
    app_id: str
    state: str  # RUNNING | WAITING | FINISHED | KILLED | FAILED ...


class Runner(Protocol):
    def launch(self, req: LaunchRequest) -> Handle: ...

    def adopt(self, pid: int, app_name: str, log_path: str) -> Handle:
        """Rebuild a handle for an attempt started by a previous scheduler process."""
        ...

    def poll(self, handle: Handle) -> PollResult: ...

    def kill(self, handle: Handle) -> None: ...

    def cluster_apps(self) -> Dict[str, ClusterApp]:
        """App name -> app info, as reported by the cluster manager. Best effort."""
        ...
