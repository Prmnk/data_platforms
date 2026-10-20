"""Run the ingest pipeline on Spark or Sail inside one resource-capped container and measure it.

    python3 bench/run_bench.py --engine spark --cpus 4 --mem 3g
    python3 bench/run_bench.py --engine sail  --cpus 0.8 --mem 600m

Each run processes landing batches 1..N (batch 1 creates the tables, later batches MERGE) into
fresh tables under s3a://warehouse/bench/<run_id>/, in a container on the compose network
(MinIO must be up) with `docker run --cpus --memory` and no swap. Everything the engine needs
runs inside that container, so its cgroup is the whole cost:
  spark: spark-submit --master local[ceil(cpus)] per batch, heap = memory - --jvm-overhead
         (or memory * --heap-frac)
  sail:  `sail spark server` (local mode) + the same script over Spark Connect; parallelism
         = ceil(cpus); Sail's default unbounded memory pool unless --sail-pool is set

Measured: wall time per batch, CPU seconds and peak memory from the container's cgroup
(cpu.stat, memory.peak), a 1 s docker-stats timeline, OOM kills, and the pipeline's own
PIPELINE_METRICS. One JSON line per run is appended to bench/results/runs.jsonl. A run during
which the Docker VM stalled (kernel "Hangcheck" message) is recorded with valid=false.
Run sweeps under `caffeinate -i` so the Mac does not idle-sleep the VM.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "bench" / "results"
NETWORK = "spark-scheduler_default"
JOBS = "/opt/sparksched/jobs"
S3_ENV = {"AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin", "AWS_REGION": "us-east-1"}

# Prints cgroup totals for the whole container after the batches ran.
CGROUP_REPORT = ('cat /tmp/samples.txt; echo "CGROUP cpu_usec=$(awk \'/^usage_usec/{print $2}\' /sys/fs/cgroup/cpu.stat)'
                 ' mem_peak=$(cat /sys/fs/cgroup/memory.peak)'
                 ' oom_kills=$(awk \'/^oom_kill /{print $2}\' /sys/fs/cgroup/memory.events)"')


def parse_mem(s: str) -> int:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([kmg]?)b?", s.lower())
    if not m:
        raise argparse.ArgumentTypeError(f"bad memory size {s!r}")
    return int(float(m.group(1)) * {"": 1, "k": 2**10, "m": 2**20, "g": 2**30}[m.group(2)])


def batch_script(engine: str, a, threads: int, mem: int, run_id: str) -> str:
    tables = f"s3a://warehouse/bench/{run_id}"
    args = lambda b: (f"--batch {b} --landing s3a://warehouse/landing/orders "
                      f"--silver {tables}/silver --quarantine {tables}/quarantine")
    # 1 s samples of the container's own cgroup: cumulative CPU microseconds and memory in use.
    lines = ["set -u",
             "( while true; do echo \"SAMPLE $(date +%s.%N) $(awk '/^usage_usec/{print $2}' /sys/fs/cgroup/cpu.stat)"
             " $(cat /sys/fs/cgroup/memory.current)\"; sleep 1; done ) > /tmp/samples.txt 2>/dev/null &"]
    if engine == "spark":
        heap = mem * a.heap_frac if a.heap_frac else mem - parse_mem(a.jvm_overhead)
        heap_mb = max(128, int(heap) // 2**20)
        submit = (f"/opt/spark/bin/spark-submit --master 'local[{threads}]' --driver-memory {heap_mb}m "
                  f"--conf spark.ui.enabled=false --conf spark.sql.shuffle.partitions={a.shuffle_partitions} "
                  f"--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 {JOBS}/ingest_pipeline.py")
        run = lambda b: f"{submit} {args(b)}"
    else:
        lines += ["sail spark server --ip 127.0.0.1 --port 50051 > /tmp/sail.log 2>&1 &",
                  "SAIL_PID=$!",
                  "for i in $(seq 1 100); do (echo > /dev/tcp/127.0.0.1/50051) 2>/dev/null && break; sleep 0.1; done"]
        # If the server dies (e.g. OOM-killed) the client would keep retrying: stop it instead.
        run = lambda b: (f"( python3 {JOBS}/ingest_pipeline.py {args(b)} & C=$!; "
                         "while kill -0 $C 2>/dev/null; do "
                         "kill -0 $SAIL_PID 2>/dev/null || { echo 'SAIL SERVER DIED'; kill $C; exit 137; }; "
                         "sleep 1; done; wait $C )")
    for b in range(1, a.batches + 1):
        lines += [f'echo "BATCH_START {b} $(date +%s.%N)"',
                  f"{run(b)} > /tmp/batch{b}.log 2>&1",
                  "rc=$?",
                  f"grep PIPELINE_METRICS /tmp/batch{b}.log",
                  f'[ "$rc" = 0 ] || tail -25 /tmp/batch{b}.log',
                  f'echo "BATCH_END {b} $(date +%s.%N) rc=$rc"',
                  '[ "$rc" = 0 ] || { ' + CGROUP_REPORT + '; exit "$rc"; }']
    lines.append(CGROUP_REPORT)
    return "\n".join(lines)


def docker_cmd(engine: str, a, threads: int, mem: int, name: str, script: str) -> list:
    cmd = ["docker", "run", "--name", name, "--network", NETWORK,
           f"--cpus={a.cpus}", f"--memory={mem}", f"--memory-swap={mem}",
           "-v", f"{ROOT / 'jobs'}:{JOBS}:ro"]
    env = dict(S3_ENV)
    if engine == "spark":
        image = a.spark_image
        cmd += ["--user", "root"]
    else:
        image = a.sail_image
        env.update(AWS_ENDPOINT="http://minio:9000", AWS_ALLOW_HTTP="true", SAIL_MODE="local",
                   SPARK_REMOTE="sc://127.0.0.1:50051",
                   SAIL_EXECUTION__DEFAULT_PARALLELISM=str(threads))
        if a.sail_pool > 0:  # bounded pool that spills; 0 = Sail's default (unbounded, no spill)
            env.update(SAIL_RUNTIME__MEMORY_POOL__TYPE="fair",
                       SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE=str(int(mem * a.sail_pool)),
                       SAIL_RUNTIME__TEMPORARY_FILES__PATHS='["/tmp/sail-spill"]')
            if a.sail_no_spill:  # fail with an error instead of spilling
                env["SAIL_RUNTIME__TEMPORARY_FILES__MAX_SIZE"] = "0"
            script = "mkdir -p /tmp/sail-spill\n" + script
    for k, v in env.items():
        cmd += ["-e", f"{k}={v}"]
    return cmd + ["--entrypoint", "bash", image, "-c", script]


def privileged(script: str, timeout: int = 60) -> str:
    """Run a shell snippet in the Docker VM (host cgroup namespace and kernel log)."""
    return subprocess.run(["docker", "run", "--rm", "--privileged", "--cgroupns=host", "alpine", "sh", "-c", script],
                          capture_output=True, text=True, timeout=timeout).stdout


def vm_hangchecks() -> int:
    """Times the Docker VM noticed it had not been scheduled (e.g. the Mac slept)."""
    return int(privileged("dmesg | grep -c 'Hangcheck: hangcheck value past margin' || true").strip() or 0)


def unstick_cpu_quota(name: str) -> None:
    """After a VM stall the cgroup's CPU quota timer can stay expired: threads are runnable
    but never run (even `docker exec`/`kill` hang). Rewriting cpu.max restarts it."""
    cid = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", name], capture_output=True, text=True).stdout.strip()
    privileged(f"f=/sys/fs/cgroup/docker/{cid}/cpu.max; q=$(cat $f); echo 'max 100000' > $f; sleep 1; echo \"$q\" > $f")


def sample(name: str, out: list, stop: threading.Event, stall_s: int, stalled: list) -> None:
    """1 s docker-stats timeline. A container at ~0% CPU for stall_s is unstuck and killed;
    the run is then invalid (its timings and CPU accounting include the stall)."""
    t0 = time.time()
    while not stop.is_set():
        idle = [x for x in out if x["t"] > (time.time() - t0) - stall_s]
        if len(out) > stall_s and idle and all(x["cpu_pct"] < 1 for x in idle):
            stalled.append("cpu_quota_stuck")
            try:
                unstick_cpu_quota(name)
                subprocess.run(["docker", "kill", name], capture_output=True, timeout=30)
            except subprocess.TimeoutExpired:
                stalled.append("unkillable")
            return
        r = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}};{{.MemUsage}}", name],
                           capture_output=True, text=True)
        if r.returncode == 0 and r.stdout.strip():
            cpu, mem = r.stdout.strip().split(";")
            try:
                out.append({"t": round(time.time() - t0, 1), "cpu_pct": float(cpu.rstrip("%")),
                            "mem_mb": round(parse_mem(mem.split("/")[0].strip().replace("i", "")) / 2**20)})
            except (ValueError, argparse.ArgumentTypeError):
                pass
        stop.wait(1)


def run(a) -> dict:
    engine, mem = a.engine, parse_mem(a.mem)
    threads = max(1, math.ceil(a.cpus))
    run_id = f"{engine}-{a.cpus}c-{a.mem}-{uuid.uuid4().hex[:6]}"
    name = f"bench-{run_id}"
    script = batch_script(engine, a, threads, mem, run_id)
    timeline, stop, stalled = [], threading.Event(), []
    sampler = threading.Thread(target=sample, args=(name, timeline, stop, a.stall_s, stalled), daemon=True)

    hangchecks_before = vm_hangchecks()
    started = time.time()
    proc = subprocess.Popen(docker_cmd(engine, a, threads, mem, name, script),
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    sampler.start()
    try:
        out, _ = proc.communicate(timeout=a.timeout)
        timed_out = False
    except subprocess.TimeoutExpired:
        try:
            subprocess.run(["docker", "kill", name], capture_output=True, timeout=30)
            out, _ = proc.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            stalled.append("unkillable")
            proc.kill()
            out = ""
        timed_out = True
    total_s = time.time() - started
    stop.set()
    sampler.join()
    try:
        state = json.loads(subprocess.run(["docker", "inspect", "-f", "{{json .State}}", name],
                                          capture_output=True, text=True, timeout=30).stdout or "{}")
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=60)
    except subprocess.TimeoutExpired:
        state = {}
        stalled.append("unkillable")

    vm_stalls = vm_hangchecks() - hangchecks_before
    batches, starts, samples = {}, {}, []
    for line in out.splitlines():
        if line.startswith("BATCH_START"):
            _, b, ts = line.split()
            starts[b] = float(ts)
        elif line.startswith("BATCH_END"):
            _, b, ts, rc = line.split()
            batches.setdefault(b, {}).update(wall_s=round(float(ts) - starts[b], 1), rc=int(rc.split("=")[1]))
        elif line.startswith("SAMPLE"):
            parts = line.split()
            if len(parts) == 4 and parts[2].isdigit() and parts[3].isdigit():
                samples.append((float(parts[1]), int(parts[2]), int(parts[3])))
        elif line.startswith("PIPELINE_METRICS"):
            m = json.loads(line.split(" ", 1)[1])
            batches.setdefault(str(m["batch"]), {})["metrics"] = m
    cg = dict(kv.split("=") for kv in re.findall(r"\w+=\d+", next(
        (l for l in out.splitlines() if l.startswith("CGROUP")), "")))
    ok = (not timed_out and proc.returncode == 0 and len(batches) == a.batches
          and all(b.get("rc") == 0 and "metrics" in b for b in batches.values()))
    cpu_s = int(cg.get("cpu_usec", 0)) / 1e6
    # cores busy per interval and memory, from the in-container cgroup samples
    usage = [{"t": round(t1 - samples[0][0], 1), "cores": round((u1 - u0) / 1e6 / (t1 - t0), 2),
              "mem_mb": round(m1 / 2**20)}
             for (t0, u0, _), (t1, u1, m1) in zip(samples, samples[1:]) if t1 > t0]
    result = {
        "run_id": run_id, "engine": engine, "cpus": a.cpus, "mem": a.mem, "mem_bytes": mem, "threads": threads,
        "ok": ok and not stalled, "timed_out": timed_out, "stalled": stalled,
        # A VM stall during the run inflates wall time and cgroup CPU time: don't use the run.
        "valid": not stalled and vm_stalls == 0, "vm_stalls": vm_stalls,
        "oom_killed": state.get("OOMKilled", False) or int(cg.get("oom_kills", 0)) > 0,
        "exit_code": proc.returncode, "total_s": round(total_s, 1),
        "batch_wall_s": {b: v.get("wall_s") for b, v in sorted(batches.items())},
        "cpu_s": round(cpu_s, 1), "mem_peak_mb": round(int(cg.get("mem_peak", 0)) / 2**20),
        "avg_cpu_util": round(cpu_s / total_s / a.cpus, 2) if total_s else None,
        "metrics": {b: v.get("metrics") for b, v in sorted(batches.items())},
        "tables": f"s3a://warehouse/bench/{run_id}",
        "config": {k: getattr(a, k) for k in ("jvm_overhead", "heap_frac", "sail_pool", "sail_no_spill",
                                               "shuffle_partitions")},
        "usage": usage,
        "batch_bounds": {b: [round(starts[b] - samples[0][0], 1), round(starts[b] - samples[0][0] + v["wall_s"], 1)]
                         for b, v in batches.items() if b in starts and "wall_s" in v and samples},
        "timeline": timeline,
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tail": [l for l in out.splitlines() if not l.startswith("SAMPLE")][-12:] if not ok else [],
    }
    RESULTS.mkdir(parents=True, exist_ok=True)
    with open(RESULTS / "runs.jsonl", "a") as f:
        f.write(json.dumps(result) + "\n")
    return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--engine", choices=["spark", "sail"], required=True)
    p.add_argument("--cpus", type=float, required=True)
    p.add_argument("--mem", required=True, help="container memory, e.g. 3g, 600m")
    p.add_argument("--batches", type=int, default=2)
    p.add_argument("--timeout", type=int, default=1800)
    p.add_argument("--jvm-overhead", default="400m", help="spark: container memory not given to the heap")
    p.add_argument("--heap-frac", type=float, default=0,
                   help="spark: heap as a fraction of container memory (overrides --jvm-overhead)")
    p.add_argument("--sail-pool", type=float, default=0,
                   help="sail: fair memory pool (spills) as a fraction of memory; 0 = Sail default")
    p.add_argument("--sail-no-spill", action="store_true", help="sail: disable spilling to disk")
    p.add_argument("--stall-s", type=int, default=90, help="unstick + kill the run after this long at ~0%% CPU")
    p.add_argument("--shuffle-partitions", type=int, default=8)
    p.add_argument("--spark-image", default="sparksched:dev")
    p.add_argument("--sail-image", default="sparksched-sail:0.7.2")
    r = run(p.parse_args())
    print(json.dumps({k: v for k, v in r.items() if k not in ("timeline", "metrics")}, indent=1))


if __name__ == "__main__":
    main()
