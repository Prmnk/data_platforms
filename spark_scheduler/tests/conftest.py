from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pytest

from scheduler.config import Settings
from scheduler.db import init_db, make_engine, make_session_factory
from scheduler.engine import Scheduler
from scheduler.runners.base import ClusterApp, Handle, LaunchRequest, PollResult


class Clock:
    def __init__(self, start: datetime):
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class FakeRunner:
    """Scripted runner. `script[job]` is a list of outcomes, one per attempt:
    an int exit code (applied on the next poll), or "hang" (runs until killed)."""

    def __init__(self):
        self.script: Dict[str, List] = {}
        self.launched: List[LaunchRequest] = []
        self.killed: List[str] = []
        self._outcome: Dict[str, object] = {}
        self._dead: set = set()
        self.fail_launch = False

    def launch(self, req: LaunchRequest) -> Handle:
        if self.fail_launch:
            raise RuntimeError("spark-submit not found")
        self.launched.append(req)
        outcomes = self.script.get(req.spec.name, [0])
        self._outcome[req.app_name] = outcomes[min(req.attempt_no - 1, len(outcomes) - 1)]
        return Handle(pid=1000 + len(self.launched), app_name=req.app_name, log_path=f"/tmp/{req.app_name}.log")

    def adopt(self, pid: int, app_name: str, log_path: str) -> Handle:
        return Handle(pid=pid, app_name=app_name, log_path=log_path)

    def poll(self, handle: Handle) -> PollResult:
        if handle.app_name in self._dead:
            return PollResult("lost")
        out = self._outcome.get(handle.app_name)
        if out == "hang" or handle.app_name in self.killed:
            return PollResult("running")
        if out is None:
            return PollResult("lost")
        return PollResult("exited", int(out))

    def kill(self, handle: Handle) -> None:
        self.killed.append(handle.app_name)

    def cluster_apps(self) -> Dict[str, ClusterApp]:
        return {r.app_name: ClusterApp(app_id=f"app-{i}", state="RUNNING") for i, r in enumerate(self.launched)}

    def forget(self, app_name: str) -> None:
        """Simulate a driver that vanished (e.g. container restart)."""
        self._dead.add(app_name)


@pytest.fixture
def clock():
    return Clock(datetime(2026, 1, 1, 12, 0, 0))


@pytest.fixture
def settings(tmp_path):
    return Settings(database_url="sqlite://", log_dir=str(tmp_path / "logs"), tick_seconds=0.01,
                    max_concurrent_runs=4, driver_host="localhost", kill_grace_seconds=1)


@pytest.fixture
def sf(settings):
    engine = make_engine(settings.database_url)
    init_db(engine)
    return make_session_factory(engine)


@pytest.fixture
def runner():
    return FakeRunner()


@pytest.fixture
def sched(sf, runner, settings, clock):
    return Scheduler(sf, runner, settings, clock=clock)


def job(name: str, **kw) -> dict:
    d = {"name": name, "app": f"/opt/jobs/{name}.py"}
    d.update(kw)
    return d
