"""Per-container CPU and memory for this compose project, read from cgroup v2 files.

cAdvisor cannot map containers on Docker Desktop for macOS, so this reads what it would:
the Docker socket for container names, /sys/fs/cgroup (mounted at CGROUP_ROOT) for usage.

    python -m saillab containers    # serves /metrics on :9200
"""
from __future__ import annotations

import http.client
import json
import logging
import os
import socket
import time
import urllib.parse
from pathlib import Path
from typing import Dict, Iterator, List, Optional

from prometheus_client import start_http_server
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, REGISTRY

log = logging.getLogger(__name__)


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str):
        super().__init__("localhost", timeout=5)
        self.socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(5)
        self.sock.connect(self.socket_path)


def list_containers(socket_path: str, project: str) -> List[dict]:
    filters = json.dumps({"label": [f"com.docker.compose.project={project}"]})
    conn = _UnixHTTPConnection(socket_path)
    try:
        conn.request("GET", "/containers/json?filters=" + urllib.parse.quote(filters))
        resp = conn.getresponse()
        if resp.status != 200:
            raise OSError(f"docker API returned {resp.status}")
        return json.loads(resp.read())
    finally:
        conn.close()


def cgroup_dir(root: Path, container_id: str) -> Optional[Path]:
    for p in (root / "docker" / container_id,                            # Docker Desktop, cgroupfs driver
              root / "system.slice" / f"docker-{container_id}.scope"):   # systemd driver (most Linux hosts)
        if p.is_dir():
            return p
    return None


def read_kv(path: Path) -> Dict[str, int]:
    out = {}
    for line in path.read_text().splitlines():
        k, _, v = line.partition(" ")
        if v.strip().isdigit():
            out[k] = int(v)
    return out


def read_int(path: Path) -> Optional[int]:
    try:
        v = path.read_text().strip()
    except OSError:
        return None
    return int(v) if v.isdigit() else None  # memory.max is "max" when unlimited


def sample(cg: Path) -> Dict[str, Optional[float]]:
    cpu = read_kv(cg / "cpu.stat")
    mem_stat = read_kv(cg / "memory.stat")
    current = read_int(cg / "memory.current") or 0
    return {
        "cpu_seconds": cpu.get("usage_usec", 0) / 1e6,
        "memory_bytes": current,
        # what the OOM killer and `docker stats` look at: usage minus reclaimable page cache
        "working_set_bytes": max(current - mem_stat.get("inactive_file", 0), 0),
        "limit_bytes": read_int(cg / "memory.max"),
        "peak_bytes": read_int(cg / "memory.peak"),
    }


class ContainerCollector:
    def __init__(self, socket_path: str, project: str, cgroup_root: Path):
        self.socket_path, self.project, self.root = socket_path, project, cgroup_root

    def collect(self) -> Iterator:
        cpu = CounterMetricFamily("lab_container_cpu_seconds", "CPU time used", labels=["service"])
        mem = GaugeMetricFamily("lab_container_memory_bytes", "Memory in use incl. page cache", labels=["service"])
        ws = GaugeMetricFamily("lab_container_memory_working_set_bytes",
                               "Memory in use minus inactive page cache", labels=["service"])
        lim = GaugeMetricFamily("lab_container_memory_limit_bytes", "Memory limit (mem_limit)", labels=["service"])
        peak = GaugeMetricFamily("lab_container_memory_peak_bytes", "Peak memory since start", labels=["service"])
        up = GaugeMetricFamily("lab_container_stats_up", "1 if the Docker socket was readable")
        try:
            containers = list_containers(self.socket_path, self.project)
            up.add_metric([], 1)
        except OSError as e:
            log.warning("cannot list containers: %s", e)
            containers = []
            up.add_metric([], 0)
        for c in containers:
            service = c.get("Labels", {}).get("com.docker.compose.service", c["Id"][:12])
            cg = cgroup_dir(self.root, c["Id"])
            if cg is None:
                continue
            try:
                s = sample(cg)
            except OSError:
                continue  # container stopped between listing and reading
            cpu.add_metric([service], s["cpu_seconds"])
            mem.add_metric([service], s["memory_bytes"])
            ws.add_metric([service], s["working_set_bytes"])
            if s["limit_bytes"] is not None:
                lim.add_metric([service], s["limit_bytes"])
            if s["peak_bytes"] is not None:
                peak.add_metric([service], s["peak_bytes"])
        yield from (cpu, mem, ws, lim, peak, up)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    REGISTRY.register(ContainerCollector(
        os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock"),
        os.environ.get("COMPOSE_PROJECT", "sail-lab"),
        Path(os.environ.get("CGROUP_ROOT", "/host/cgroup")),
    ))
    port = int(os.environ.get("PORT", "9200"))
    start_http_server(port)
    log.info("container stats on :%s", port)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
