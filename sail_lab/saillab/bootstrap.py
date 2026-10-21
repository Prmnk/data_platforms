"""Client-side entry point of every run (a plain Python process: no spark-submit, no JVM).

    python -m saillab.bootstrap --remote sc://sail:50051 --session <uuid> [--conf k=v]...
                            [--result-file f] [--module m] -- app.py|args...
        Create the Spark Connect session (session id = run id) with the given configs, then
        run app.py (or module m) as __main__. The job's own SparkSession.builder.getOrCreate()
        returns this session, and SPARK_REMOTE points at it for any code that reads it.

    python -m saillab.bootstrap --remote ... --session <uuid> --interrupt
        Interrupt every running operation of that session (used by cancel and timeouts).

pyspark is imported lazily so the submit service itself does not need it to start.
"""
from __future__ import annotations

import argparse
import os
import runpy
import sys
import time
from typing import List, Optional


def parse(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="saillab.bootstrap")
    p.add_argument("--remote", required=True)
    p.add_argument("--session", required=True)
    p.add_argument("--conf", action="append", default=[], metavar="KEY=VALUE")
    p.add_argument("--result-file")
    p.add_argument("--app-name")
    p.add_argument("--module")
    p.add_argument("--interrupt", action="store_true")
    p.add_argument("app_args", nargs=argparse.REMAINDER)
    a = p.parse_args(argv)
    if a.app_args[:1] == ["--"]:
        a.app_args = a.app_args[1:]
    if not a.interrupt and not a.module and not a.app_args:
        p.error("an app script or --module is required")
    for kv in a.conf:
        if "=" not in kv:
            p.error(f"--conf expects KEY=VALUE, got {kv!r}")
    return a


def remote_url(remote: str, session: str) -> str:
    return f"{remote.rstrip('/')}/;session_id={session}"


def main(argv: Optional[List[str]] = None) -> None:
    a = parse(sys.argv[1:] if argv is None else argv)
    os.environ["SPARK_REMOTE"] = remote_url(a.remote, a.session)
    if a.result_file:
        os.environ["LAB_RESULT_FILE"] = a.result_file

    from pyspark.sql import SparkSession

    if a.interrupt:
        ops = SparkSession.builder.getOrCreate().interruptAll()
        print(f"[lab] interrupted {len(ops)} operation(s)", flush=True)
        return

    t0 = time.monotonic()
    builder = SparkSession.builder
    if a.app_name:
        builder = builder.appName(a.app_name)
    for kv in a.conf:
        k, v = kv.split("=", 1)
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.range(1).collect()  # the session exists on the server only after the first request
    ready = time.monotonic() - t0
    print(f"[lab] session {a.session} ready on {a.remote} in {ready:.2f}s "
          f"(pyspark {spark.version}, python {sys.version.split()[0]})", flush=True)

    from saillab.sdk import report
    report(session_ready_seconds=round(ready, 3))

    # Helpers next to the script and in jobs/ stay importable. Appended, not prepended, so the
    # (possibly slow, bind-mounted) directories are searched only after site-packages.
    for d in (os.environ.get("LAB_JOBS_DIR"), os.path.dirname(os.path.abspath(a.app_args[0])) if a.app_args and not a.module else None):
        if d and d not in sys.path:
            sys.path.append(d)
    if a.module:
        sys.argv = [a.module, *a.app_args]
        runpy.run_module(a.module, run_name="__main__", alter_sys=True)
    else:
        app, *args = a.app_args
        sys.argv = [app, *args]
        runpy.run_path(app, run_name="__main__")


if __name__ == "__main__":
    main()
