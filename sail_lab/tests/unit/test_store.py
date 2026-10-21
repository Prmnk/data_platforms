import pytest

from saillab.store import AmbiguousId, Run, Store


def test_roundtrip_prefix_and_filters(tmp_path):
    s = Store(tmp_path / "db" / "lab.db")
    s.insert(Run(id="aaaa1111-x", job="a", kind="job", target="a.py", args=["--n", "1"], conf={"k": "v"}))
    s.insert(Run(id="aaaa2222-x", job="b", kind="sql", target="q.sql", status="failed"))
    assert s.get("aaaa1").args == ["--n", "1"] and s.get("aaaa1").conf == {"k": "v"}
    with pytest.raises(AmbiguousId):
        s.get("aaaa")
    assert s.get("nope") is None
    assert s.get("a%") is None  # LIKE wildcards are not honoured
    s.update("aaaa1111-x", status="succeeded", result={"rows": 3})
    assert s.get("aaaa1111-x").result == {"rows": 3}
    assert [r.job for r in s.list(status="failed")] == ["b"]
    assert [r.id for r in s.by_status("succeeded", "failed")] == ["aaaa1111-x", "aaaa2222-x"]


def test_to_dict_has_short_id_and_session():
    d = Run(id="0123456789", job="j", kind="job", target="t", submitted_at=100, started_at=101, finished_at=104).to_dict()
    assert (d["short_id"], d["session_id"], d["duration_seconds"], d["queue_seconds"]) == ("01234567", "0123456789", 3, 1)
