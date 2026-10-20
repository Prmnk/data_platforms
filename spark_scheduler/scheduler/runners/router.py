"""Dispatches each attempt to the runner for its job's engine (JobSpec.engine)."""
from __future__ import annotations

from typing import Dict

from .base import ClusterApp, Handle, LaunchRequest, PollResult, Runner
from .sail import session_file_for


class EngineRouter:
    def __init__(self, runners: Dict[str, Runner], default: str = "spark"):
        self.runners = runners
        self.default = default

    def _runner(self, engine: str) -> Runner:
        try:
            return self.runners[engine]
        except KeyError:
            raise RuntimeError(f"no runner configured for engine {engine!r}") from None

    def launch(self, req: LaunchRequest) -> Handle:
        handle = self._runner(req.spec.engine).launch(req)
        handle.extra["engine"] = req.spec.engine
        return handle

    def adopt(self, pid: int, app_name: str, log_path: str) -> Handle:
        # Sail attempts leave <log>.sail behind before they start; everything else is Spark.
        engine = "sail" if log_path and session_file_for(log_path).exists() else self.default
        handle = self._runner(engine).adopt(pid, app_name, log_path)
        handle.extra["engine"] = engine
        return handle

    def poll(self, handle: Handle) -> PollResult:
        return self._runner(handle.extra.get("engine", self.default)).poll(handle)

    def kill(self, handle: Handle) -> None:
        self._runner(handle.extra.get("engine", self.default)).kill(handle)

    def cluster_apps(self) -> Dict[str, ClusterApp]:
        apps: Dict[str, ClusterApp] = {}
        for r in self.runners.values():
            apps.update(r.cluster_apps())
        return apps
