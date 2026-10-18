"""`sched` command-line client. Talks to the API (SPARKSCHED_API, default http://localhost:8000)."""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

import httpx
import typer

from .models import TERMINAL_RUN_STATES
from .spec import load_job_file

app = typer.Typer(help="Schedule, trigger and monitor Spark jobs.", no_args_is_help=True)


def _client() -> httpx.Client:
    return httpx.Client(base_url=os.environ.get("SPARKSCHED_API", "http://localhost:8000"), timeout=30)


def _ok(r: httpx.Response):
    if r.status_code >= 400:
        typer.secho(f"{r.status_code}: {r.text}", fg="red", err=True)
        raise typer.Exit(1)
    return r.json() if r.content else None


def _table(rows: List[dict], cols: List[str]) -> None:
    if not rows:
        typer.echo("(none)")
        return
    widths = {c: max(len(c), *(len(str(r.get(c) if r.get(c) is not None else "-")) for r in rows)) for c in cols}
    typer.echo("  ".join(c.upper().ljust(widths[c]) for c in cols))
    for r in rows:
        typer.echo("  ".join(str(r.get(c) if r.get(c) is not None else "-").ljust(widths[c]) for c in cols))


@app.command()
def apply(file: Path = typer.Option(..., "-f", "--file", exists=True, help="jobs.yaml")):
    """Create or update every job in a YAML file (one transaction)."""
    jobs = load_job_file(file.read_text())  # validate locally for fast feedback
    with _client() as c:
        res = _ok(c.post("/jobs", json=jobs.model_dump()))
    for name, what in res.items():
        typer.echo(f"{name}: {what}")


@app.command()
def run(
    jobs: Optional[List[str]] = typer.Argument(None, help="Job names to run now"),
    file: Optional[Path] = typer.Option(None, "-f", "--file", exists=True, help="Run every job in this file"),
    wait: bool = typer.Option(False, "--wait", help="Poll until all runs finish"),
    poll: float = typer.Option(5.0, help="Poll interval in seconds with --wait"),
):
    """Trigger one or many jobs now, as one batch."""
    names = list(jobs or [])
    if file:
        jf = load_job_file(file.read_text())
        with _client() as c:
            _ok(c.post("/jobs", json=jf.model_dump()))
        names += [j.name for j in jf.jobs]
    if not names:
        typer.secho("give job names or -f jobs.yaml", fg="red", err=True)
        raise typer.Exit(2)
    with _client() as c:
        res = _ok(c.post("/runs", json={"jobs": names}))
    typer.echo(f"batch {res['batch_id']}: " + ", ".join(f"{r['job']}#{r['run_id']}" for r in res["runs"]))
    if wait:
        sys.exit(_wait_batch(res["batch_id"], poll))


def _wait_batch(batch_id: str, poll: float) -> int:
    seen = {}
    with _client() as c:
        while True:
            try:
                runs = _ok(c.get("/runs", params={"batch_id": batch_id, "limit": 1000}))
            except httpx.TransportError as e:  # API restarting or overloaded: keep polling
                typer.secho(f"poll failed ({e.__class__.__name__}); retrying", fg="yellow", err=True)
                time.sleep(poll)
                continue
            for r in runs:
                key = (r["state"], r["attempts"])
                if seen.get(r["run_id"]) != key:
                    seen[r["run_id"]] = key
                    typer.echo(f"{r['job']}#{r['run_id']}: {r['state']} (attempt {r['attempts']}/{r['max_attempts']})")
            if runs and all(r["state"] in TERMINAL_RUN_STATES for r in runs):
                return 0 if all(r["state"] == "SUCCEEDED" for r in runs) else 1
            time.sleep(poll)


@app.command()
def status(as_json: bool = typer.Option(False, "--json")):
    """Latest state of every job."""
    with _client() as c:
        rows = _ok(c.get("/jobs"))
    if as_json:
        typer.echo(json.dumps(rows, indent=2))
        return
    _table(rows, ["job", "enabled", "schedule", "last_state", "last_attempts", "running", "queued",
                  "last_success_at", "next_fire_at"])


@app.command()
def runs(job: Optional[str] = None, state: Optional[str] = None, batch: Optional[str] = None,
         limit: int = 20):
    """Recent runs."""
    with _client() as c:
        rows = _ok(c.get("/runs", params={k: v for k, v in
                                          {"job": job, "state": state, "batch_id": batch, "limit": limit}.items() if v}))
    _table(rows, ["run_id", "job", "state", "trigger", "attempts", "max_attempts", "started_at",
                  "duration_s", "last_error"])


@app.command()
def show(run_id: int):
    """One run with all its attempts."""
    with _client() as c:
        typer.echo(json.dumps(_ok(c.get(f"/runs/{run_id}")), indent=2))


@app.command()
def logs(run_id: int, attempt: int = typer.Option(0, help="Attempt number; default latest"), tail: int = 200):
    """Tail the driver log of an attempt."""
    with _client() as c:
        if not attempt:
            attempt = _ok(c.get(f"/runs/{run_id}"))["attempts"]
        r = c.get(f"/runs/{run_id}/attempts/{attempt}/log", params={"tail": tail})
        if r.status_code >= 400:
            _ok(r)
        typer.echo(r.text)


@app.command()
def cancel(run_id: int):
    """Cancel a queued or running run."""
    with _client() as c:
        _ok(c.post(f"/runs/{run_id}/cancel"))
    typer.echo(f"cancel requested for run {run_id}")


@app.command()
def enable(name: str):
    with _client() as c:
        _ok(c.patch(f"/jobs/{name}", json={"enabled": True}))
    typer.echo(f"{name} enabled")


@app.command()
def disable(name: str):
    with _client() as c:
        _ok(c.patch(f"/jobs/{name}", json={"enabled": False}))
    typer.echo(f"{name} disabled")


@app.command()
def delete(name: str):
    with _client() as c:
        r = c.delete(f"/jobs/{name}")
        if r.status_code >= 400:
            _ok(r)
    typer.echo(f"{name} deleted")


if __name__ == "__main__":
    app()
