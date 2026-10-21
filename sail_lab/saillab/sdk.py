"""Optional helpers for job scripts. Values passed to report() show up in `lab show <run>`,
in GET /runs/<id> and, when numeric, as lab_run_result in Prometheus/Grafana.

    from saillab.sdk import report, timed

    with timed("write"):            # reports write_seconds
        df.write.parquet(path)
    report(rows=df.count())

Outside a lab run (no LAB_RESULT_FILE, e.g. running a script by hand) both are no-ops
apart from the timing itself, so jobs stay runnable anywhere.
"""
from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from typing import Any, Iterator


def report(**values: Any) -> None:
    path = os.environ.get("LAB_RESULT_FILE")
    if not path:
        return
    try:
        with open(path) as f:
            current = json.load(f)
    except (OSError, ValueError):
        current = {}
    current.update(values)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(current, f, default=str)
    os.replace(tmp, path)


@contextmanager
def timed(name: str) -> Iterator[None]:
    t0 = time.monotonic()
    try:
        yield
    finally:
        dt = time.monotonic() - t0
        print(f"[lab] {name}: {dt:.2f}s", flush=True)
        report(**{f"{name}_seconds": round(dt, 3)})
