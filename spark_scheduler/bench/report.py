"""Summarise bench/results/runs.jsonl as a markdown table (one row per run, oldest first).

    python3 bench/report.py [--engine sail] [--last 20]
"""
import argparse
import json
from pathlib import Path

RUNS = Path(__file__).resolve().parent / "results" / "runs.jsonl"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--engine")
    p.add_argument("--last", type=int)
    a = p.parse_args()
    runs = [json.loads(l) for l in RUNS.read_text().splitlines() if l.strip()]
    for r in runs:  # runs before the validity check: CPU-seconds above the quota mean a VM stall
        r.setdefault("valid", not r.get("stalled") and r["cpu_s"] <= r["cpus"] * r["total_s"] * 1.05)
    runs = [r for r in runs if r["valid"]]
    if a.engine:
        runs = [r for r in runs if r["engine"] == a.engine]
    if a.last:
        runs = runs[-a.last:]
    print("| engine | cpus | memory | tuning | result | batch 1 s | batch 2 s | total s | CPU s | avg CPU util "
          "| peak mem MB | silver rows |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in runs:
        result = "ok" if r["ok"] else ("stalled" if r.get("stalled") else "OOM-killed" if r["oom_killed"] else
                                       "timeout" if r["timed_out"] else f"failed ({r['exit_code']})")
        m = r.get("metrics") or {}
        last = m.get(str(max(map(int, m))) if m else "") or {}
        bw = r.get("batch_wall_s") or {}
        c = r.get("config") or {}
        if r["engine"] == "spark":
            tuning = f"heap {c['heap_frac']:.0%}" if c.get("heap_frac") else f"heap = mem - {c.get('jvm_overhead', '400m')}"
        else:
            tuning = (f"fair pool {c['sail_pool']:.0%}" + (", no spill" if c.get("sail_no_spill") else "")
                      if c.get("sail_pool") else "default pool")
        print(f"| {r['engine']} | {r['cpus']} | {r['mem']} | {tuning} | {result} | {bw.get('1', '-')} | {bw.get('2', '-')} | "
              f"{r['total_s']} | {r['cpu_s']} | {r['avg_cpu_util']} | {r['mem_peak_mb']} | "
              f"{last.get('silver_rows', '-')} |")


if __name__ == "__main__":
    main()
