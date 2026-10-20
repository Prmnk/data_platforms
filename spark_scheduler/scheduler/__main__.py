"""Server entrypoints:

    python -m scheduler loop   # the scheduler loop (single active leader)
    python -m scheduler api    # REST API + /metrics
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time

from .config import get_settings
from .db import init_db, lock_alive, make_engine, make_session_factory, try_acquire_leader

log = logging.getLogger("scheduler")


def _setup_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def run_loop() -> None:
    from .engine import Scheduler
    from .runners.router import EngineRouter
    from .runners.sail import SailRunner
    from .runners.standalone import SparkSubmitRunner

    settings = get_settings()
    engine = make_engine(settings.database_url)
    init_db(engine)

    lock = try_acquire_leader(engine)
    while lock is None:
        log.info("another scheduler holds the leader lock; standing by")
        time.sleep(10)
        lock = try_acquire_leader(engine)
    log.info("acquired leader lock")

    runner = EngineRouter({"spark": SparkSubmitRunner(settings), "sail": SailRunner(settings)})
    sched = Scheduler(make_session_factory(engine), runner, settings,
                      leader_check=lambda: lock_alive(lock))
    # Leave running drivers alone on shutdown; the next scheduler adopts them.
    signal.signal(signal.SIGTERM, lambda *_: sched.stop())
    signal.signal(signal.SIGINT, lambda *_: sched.stop())
    sched.run_forever()
    log.info("scheduler stopped")


def run_api() -> None:
    import uvicorn

    from .api import create_app

    settings = get_settings()
    engine = make_engine(settings.database_url)
    init_db(engine)
    app = create_app(make_session_factory(engine))
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("API_PORT", 8000)))


def main() -> None:
    _setup_logging()
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "loop":
        run_loop()
    elif cmd == "api":
        run_api()
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
