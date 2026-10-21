"""Run history in SQLite. A run's id is also its Spark Connect session id, so the same
UUID finds the run in Sail's metrics (session_id), traces (session.id) and Loki (run_id)."""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

ACTIVE = ("queued", "running")
FINAL = ("succeeded", "failed", "cancelled", "timed_out", "lost")

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id              TEXT PRIMARY KEY,
    job             TEXT NOT NULL,
    kind            TEXT NOT NULL,      -- job | upload | sql
    target          TEXT NOT NULL,      -- script/sql file the run executes
    args            TEXT NOT NULL,      -- JSON list
    conf            TEXT NOT NULL,      -- JSON object of Spark session configs
    timeout_seconds INTEGER,
    status          TEXT NOT NULL,
    submitted_at    REAL NOT NULL,
    started_at      REAL,
    finished_at     REAL,
    exit_code       INTEGER,
    pid             INTEGER,
    max_rss_bytes   INTEGER,            -- peak memory of the client process
    result          TEXT,               -- JSON object reported by the job (saillab.sdk.report)
    error           TEXT
);
CREATE INDEX IF NOT EXISTS runs_submitted ON runs (submitted_at DESC);
"""


def _iso(ts: Optional[float]) -> Optional[str]:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds") if ts else None


@dataclass
class Run:
    id: str
    job: str
    kind: str
    target: str
    args: List[str] = field(default_factory=list)
    conf: Dict[str, str] = field(default_factory=dict)
    timeout_seconds: Optional[int] = None
    status: str = "queued"
    submitted_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    exit_code: Optional[int] = None
    pid: Optional[int] = None
    max_rss_bytes: Optional[int] = None
    result: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if not self.started_at:
            return None
        return (self.finished_at or time.time()) - self.started_at

    @property
    def queue_seconds(self) -> Optional[float]:
        return (self.started_at - self.submitted_at) if self.started_at else None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d.update(
            short_id=self.id[:8],
            session_id=self.id,
            duration_seconds=round(self.duration_seconds, 3) if self.duration_seconds is not None else None,
            queue_seconds=round(self.queue_seconds, 3) if self.queue_seconds is not None else None,
            submitted=_iso(self.submitted_at),
            started=_iso(self.started_at),
            finished=_iso(self.finished_at),
        )
        return d


class AmbiguousId(LookupError):
    pass


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(SCHEMA)
        self._lock = threading.Lock()

    @staticmethod
    def _row(r: sqlite3.Row) -> Run:
        d = dict(r)
        d["args"] = json.loads(d["args"])
        d["conf"] = json.loads(d["conf"])
        d["result"] = json.loads(d["result"]) if d["result"] else {}
        return Run(**d)

    def insert(self, run: Run) -> Run:
        d = asdict(run)
        d["args"], d["conf"], d["result"] = json.dumps(run.args), json.dumps(run.conf), json.dumps(run.result)
        cols = ", ".join(d)
        with self._lock:
            self._db.execute(f"INSERT INTO runs ({cols}) VALUES ({', '.join('?' * len(d))})", list(d.values()))
        return run

    def update(self, run_id: str, **fields: Any) -> None:
        if "result" in fields:
            fields["result"] = json.dumps(fields["result"])
        sets = ", ".join(f"{k} = ?" for k in fields)
        with self._lock:
            self._db.execute(f"UPDATE runs SET {sets} WHERE id = ?", [*fields.values(), run_id])

    def get(self, id_or_prefix: str) -> Optional[Run]:
        """Exact id, or a unique prefix of one (like docker container ids)."""
        if not id_or_prefix:
            return None
        with self._lock:
            rows = self._db.execute("SELECT * FROM runs WHERE id = ?", (id_or_prefix,)).fetchall() or \
                self._db.execute("SELECT * FROM runs WHERE substr(id, 1, ?) = ? LIMIT 2",
                                 (len(id_or_prefix), id_or_prefix)).fetchall()
        if len(rows) > 1:
            raise AmbiguousId(id_or_prefix)
        return self._row(rows[0]) if rows else None

    def list(self, limit: int = 50, status: Optional[str] = None, job: Optional[str] = None) -> List[Run]:
        where, params = [], []
        if status:
            where.append("status = ?")
            params.append(status)
        if job:
            where.append("job = ?")
            params.append(job)
        sql = "SELECT * FROM runs"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY submitted_at DESC LIMIT ?"
        with self._lock:
            rows = self._db.execute(sql, [*params, limit]).fetchall()
        return [self._row(r) for r in rows]

    def by_status(self, *statuses: str) -> List[Run]:
        with self._lock:
            rows = self._db.execute(
                f"SELECT * FROM runs WHERE status IN ({', '.join('?' * len(statuses))}) ORDER BY submitted_at",
                statuses).fetchall()
        return [self._row(r) for r in rows]
