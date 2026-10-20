"""Run run_bench.py over a list of resource configs, largest first.

    python3 bench/sweep.py spark 4:2g 4:1536m 4:1g 4:768m      # stops at the first failure
    python3 bench/sweep.py sail 0.8:3g:0.5 --keep-going          # cpus:mem[:sail_pool or heap_frac]

Stops the whole sweep if a container could not be killed (a wedged Docker VM needs a restart).
"""
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main() -> int:
    engine, *rest = sys.argv[1:]
    keep_going = "--keep-going" in rest
    for cfg in (c for c in rest if not c.startswith("--")):
        cpus, mem, *pool = cfg.split(":")
        cmd = [sys.executable, str(HERE / "run_bench.py"), "--engine", engine, "--cpus", cpus, "--mem", mem,
               "--timeout", "2400"]
        if pool:  # sail: memory pool fraction; spark: heap fraction
            cmd += ["--sail-pool" if engine == "sail" else "--heap-frac", pool[0]]
        subprocess.run(cmd, capture_output=True)
        last = json.loads((HERE / "results" / "runs.jsonl").read_text().splitlines()[-1])
        print(subprocess.run([sys.executable, str(HERE / "report.py"), "--last", "1"],
                             capture_output=True, text=True).stdout.splitlines()[-1], flush=True)
        if "unkillable" in last.get("stalled", []):
            print("container could not be killed; restart Docker before continuing", flush=True)
            return 2
        if not last["ok"] and not keep_going:
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
