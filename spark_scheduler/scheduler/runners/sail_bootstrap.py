"""Driver-side entry point for Sail attempts. SPARK_REMOTE names the Sail server and session.

    python -m scheduler.runners.sail_bootstrap [--conf k=v]... [--py-file f]... -- app.py [args...]
        Create the Spark Connect session with the job's spark_conf, then run app.py as
        __main__. The script's own SparkSession.builder.getOrCreate() returns this session.

    python -m scheduler.runners.sail_bootstrap --interrupt
        Interrupt every running operation of the SPARK_REMOTE session (used by kill).

pyspark is imported lazily so the scheduler package does not depend on it.
"""
from __future__ import annotations

import argparse
import os
import runpy
import sys
from typing import List, Optional


def parse(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="sail_bootstrap")
    p.add_argument("--conf", action="append", default=[], metavar="KEY=VALUE")
    p.add_argument("--py-file", action="append", default=[], dest="py_files")
    p.add_argument("--interrupt", action="store_true")
    p.add_argument("app", nargs="?")
    p.add_argument("app_args", nargs=argparse.REMAINDER)
    a = p.parse_args(argv)
    if not a.interrupt and not a.app:
        p.error("app is required")
    for kv in a.conf:
        if "=" not in kv:
            p.error(f"--conf expects KEY=VALUE, got {kv!r}")
    return a


def main(argv: Optional[List[str]] = None) -> None:
    a = parse(sys.argv[1:] if argv is None else argv)
    if not os.environ.get("SPARK_REMOTE"):
        sys.exit("SPARK_REMOTE is not set")

    from pyspark.sql import SparkSession

    if a.interrupt:
        ops = SparkSession.builder.getOrCreate().interruptAll()
        print(f"[sparksched] interrupted {len(ops)} operation(s)", flush=True)
        return

    builder = SparkSession.builder
    for kv in a.conf:
        k, v = kv.split("=", 1)
        builder = builder.config(k, v)
    builder.getOrCreate()

    for f in a.py_files:  # driver-side imports only; nothing is shipped to the server
        sys.path.insert(0, f)
    sys.argv = [a.app, *a.app_args]
    sys.path.insert(0, os.path.dirname(os.path.abspath(a.app)))
    runpy.run_path(a.app, run_name="__main__")


if __name__ == "__main__":
    main()
