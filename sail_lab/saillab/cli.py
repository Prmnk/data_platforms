"""Command line client for the Sail Lab API. Standard library only, so `./lab` works with
any python3 without installing anything.

    ./lab jobs                                   list jobs under jobs/
    ./lab submit hello_sail.py -f                run a job from jobs/ and follow its log
    ./lab submit ~/scratch/try.py -- --n 10      upload and run any local .py/.sql file
    ./lab sql "SELECT 1 AS x" -f                 run SQL text
    ./lab runs | show <id> | logs <id> [-f] | cancel <id>

Run ids can be shortened to any unique prefix. LAB_API overrides http://localhost:8010.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional

API = os.environ.get("LAB_API", "http://localhost:8010")
JOBS_DIR = Path(__file__).resolve().parent.parent / "jobs"
FINAL = {"succeeded", "failed", "cancelled", "timed_out", "lost"}


def call(method: str, path: str, body: Optional[dict] = None) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        detail = e.read().decode()
        try:
            detail = json.loads(detail).get("detail", detail)
        except ValueError:
            pass
        sys.exit(f"error: {detail}")
    except urllib.error.URLError as e:
        sys.exit(f"error: cannot reach {API} ({e.reason}). Is the stack up? (make up)")


def parse_conf(pairs: List[str]) -> Dict[str, str]:
    out = {}
    for kv in pairs:
        if "=" not in kv:
            sys.exit(f"error: --conf expects KEY=VALUE, got {kv!r}")
        k, v = kv.split("=", 1)
        out[k] = v
    return out


def fmt_duration(sec: Optional[float]) -> str:
    if sec is None:
        return "-"
    return f"{sec:.1f}s" if sec < 120 else f"{sec / 60:.1f}m"


def print_runs(runs: List[dict]) -> None:
    print(f"{'RUN':<10}{'JOB':<24}{'STATUS':<11}{'SUBMITTED':<21}{'DURATION':>9}")
    for r in runs:
        print(f"{r['short_id']:<10}{r['job'][:23]:<24}{r['status']:<11}"
              f"{(r['submitted'] or '')[:19].replace('T', ' '):<21}{fmt_duration(r['duration_seconds']):>9}")


def follow(run_id: str) -> dict:
    offset = 0
    while True:
        chunk = call("GET", f"/runs/{run_id}/log?offset={offset}")
        if chunk["text"]:
            sys.stdout.write(chunk["text"])
            sys.stdout.flush()
        offset = chunk["next_offset"]
        if chunk["done"]:
            return call("GET", f"/runs/{run_id}")
        if not chunk["text"]:
            time.sleep(1)


def wait(run_id: str) -> dict:
    while True:
        run = call("GET", f"/runs/{run_id}")
        if run["status"] in FINAL:
            return run
        time.sleep(1)


def finish(run: dict, args: argparse.Namespace) -> None:
    if args.follow:
        run = follow(run["id"])
    elif args.wait:
        run = wait(run["id"])
    else:
        print(f"submitted {run['short_id']} ({run['job']})  status={run['status']}")
        print(f"  logs:    ./lab logs {run['short_id']} -f")
        print(f"  grafana: {run['grafana_url']}")
        return
    print(f"\n{run['short_id']} {run['status']} in {fmt_duration(run['duration_seconds'])}  {run['grafana_url']}")
    sys.exit(0 if run["status"] == "succeeded" else 1)


def submit_body(args: argparse.Namespace) -> dict:
    body: Dict[str, Any] = {"args": args.args, "conf": parse_conf(args.conf),
                            "timeout_seconds": args.timeout, "name": args.name}
    return {k: v for k, v in body.items() if v not in (None, [], {})}


def cmd_submit(args: argparse.Namespace) -> None:
    body = submit_body(args)
    local = Path(args.target).expanduser()
    if local.is_file():
        resolved = local.resolve()
        if JOBS_DIR in resolved.parents:  # mounted into the service; submit by name
            body["job"] = str(resolved.relative_to(JOBS_DIR))
        else:
            body["script"] = {"filename": local.name, "content": local.read_text()}
    else:
        body["job"] = args.target
    finish(call("POST", "/runs", body), args)


def cmd_sql(args: argparse.Namespace) -> None:
    body = submit_body(args)
    body["sql"] = Path(args.query[1:]).read_text() if args.query.startswith("@") else args.query
    finish(call("POST", "/runs", body), args)


def cmd_jobs(_: argparse.Namespace) -> None:
    for j in call("GET", "/jobs"):
        print(f"{j['job']:<28}{j['summary']}")


def cmd_runs(args: argparse.Namespace) -> None:
    q = {k: v for k, v in {"limit": args.limit, "status": args.status, "job": args.job}.items() if v}
    print_runs(call("GET", "/runs?" + urllib.parse.urlencode(q)))


def cmd_show(args: argparse.Namespace) -> None:
    print(json.dumps(call("GET", f"/runs/{args.run}"), indent=2))


def cmd_logs(args: argparse.Namespace) -> None:
    if args.follow:
        follow(args.run)
    else:
        sys.stdout.write(call("GET", f"/runs/{args.run}/log?max_bytes=4000000")["text"])


def cmd_cancel(args: argparse.Namespace) -> None:
    run = call("POST", f"/runs/{args.run}/cancel")
    print(f"{run['short_id']} {run['status']}" + (" (stopping)" if run["status"] == "running" else ""))


def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser(prog="lab", description="Submit and inspect Sail Lab runs.")
    sub = p.add_subparsers(dest="cmd", required=True)

    def run_opts(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--conf", action="append", default=[], metavar="KEY=VALUE", help="Spark session config")
        sp.add_argument("--timeout", type=int, help="seconds before the run is cancelled")
        sp.add_argument("--name", help="label for metrics (default: file name)")
        g = sp.add_mutually_exclusive_group()
        g.add_argument("-f", "--follow", action="store_true", help="stream the log until the run ends")
        g.add_argument("-w", "--wait", action="store_true", help="wait for the run to end")

    sp = sub.add_parser("submit", help="run a job from jobs/ or any local .py/.sql file")
    sp.add_argument("target", help="job name under jobs/, or a path to a local file")
    run_opts(sp)
    sp.set_defaults(fn=cmd_submit)

    sp = sub.add_parser("sql", help="run SQL text (or @file.sql)")
    sp.add_argument("query")
    run_opts(sp)
    sp.set_defaults(fn=cmd_sql)

    sub.add_parser("jobs", help="list jobs").set_defaults(fn=cmd_jobs)

    sp = sub.add_parser("runs", help="list recent runs")
    sp.add_argument("-n", "--limit", type=int, default=20)
    sp.add_argument("--status")
    sp.add_argument("--job")
    sp.set_defaults(fn=cmd_runs)

    for name, fn, helptext in (("show", cmd_show, "run details as JSON"), ("cancel", cmd_cancel, "cancel a run")):
        sp = sub.add_parser(name, help=helptext)
        sp.add_argument("run")
        sp.set_defaults(fn=fn)

    sp = sub.add_parser("logs", help="print a run's log")
    sp.add_argument("run")
    sp.add_argument("-f", "--follow", action="store_true")
    sp.set_defaults(fn=cmd_logs)

    argv = list(sys.argv[1:] if argv is None else argv)
    script_args: List[str] = []
    if "--" in argv:  # everything after -- goes to the script untouched
        i = argv.index("--")
        argv, script_args = argv[:i], argv[i + 1:]
    args = p.parse_args(argv)
    args.args = script_args
    args.fn(args)


if __name__ == "__main__":
    main()
