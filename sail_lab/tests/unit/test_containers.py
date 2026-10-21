from saillab.containers import ContainerCollector, cgroup_dir, sample


def write_cgroup(d, limit="2147483648"):
    d.mkdir(parents=True)
    (d / "cpu.stat").write_text("usage_usec 2500000\nuser_usec 2000000\nsystem_usec 500000\n")
    (d / "memory.current").write_text("300\n")
    (d / "memory.stat").write_text("anon 200\ninactive_file 120\nactive_file 0\n")
    (d / "memory.max").write_text(limit + "\n")


def test_sample_and_paths(tmp_path):
    write_cgroup(tmp_path / "docker" / "abc")
    write_cgroup(tmp_path / "system.slice" / "docker-def.scope", limit="max")
    assert cgroup_dir(tmp_path, "abc") == tmp_path / "docker" / "abc"
    assert cgroup_dir(tmp_path, "def") == tmp_path / "system.slice" / "docker-def.scope"
    assert cgroup_dir(tmp_path, "zzz") is None
    s = sample(tmp_path / "docker" / "abc")
    assert s == {"cpu_seconds": 2.5, "memory_bytes": 300, "working_set_bytes": 180,
                 "limit_bytes": 2147483648, "peak_bytes": None}
    assert sample(tmp_path / "system.slice" / "docker-def.scope")["limit_bytes"] is None


def test_collector(tmp_path, monkeypatch):
    write_cgroup(tmp_path / "docker" / "abc")
    monkeypatch.setattr("saillab.containers.list_containers", lambda sock, project: [
        {"Id": "abc", "Labels": {"com.docker.compose.service": "sail"}}, {"Id": "gone", "Labels": {}}])
    fams = {f.name: f for f in ContainerCollector("/x.sock", "sail-lab", tmp_path).collect()}
    assert [(s.labels, s.value) for s in fams["lab_container_cpu_seconds"].samples if s.name.endswith("_total")] \
        == [({"service": "sail"}, 2.5)]
    assert fams["lab_container_stats_up"].samples[0].value == 1
