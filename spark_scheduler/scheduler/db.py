from __future__ import annotations

import logging
import time
from typing import Optional

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from .models import Base

log = logging.getLogger(__name__)

LEADER_LOCK_KEY = 0x5C4ED  # arbitrary, constant across scheduler instances


def make_engine(url: str) -> Engine:
    if url.startswith("sqlite"):
        kwargs = {"connect_args": {"check_same_thread": False}}
        if url in ("sqlite://", "sqlite:///:memory:"):
            kwargs["poolclass"] = StaticPool
        engine = create_engine(url, **kwargs)

        @event.listens_for(engine, "connect")
        def _fk_on(dbapi_conn, _):
            dbapi_conn.execute("PRAGMA foreign_keys=ON")

        return engine
    return create_engine(url, pool_pre_ping=True)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)


def init_db(engine: Engine, attempts: int = 10) -> None:
    """Create tables. Retries because the DB may still be starting, and the scheduler
    and API containers may race to create the same tables."""
    for i in range(attempts):
        try:
            Base.metadata.create_all(engine)
            return
        except OperationalError as e:
            if i == attempts - 1:
                raise
            log.warning("init_db failed (%s); retrying", e.__class__.__name__)
            time.sleep(2)
        except (IntegrityError, ProgrammingError):
            time.sleep(1)  # another process created a table concurrently


def try_acquire_leader(engine: Engine) -> Optional[Connection]:
    """Hold a Postgres session-level advisory lock so only one scheduler loop is active.

    Returns the connection holding the lock (keep it open), or None if another
    instance holds it. On non-Postgres databases this is a no-op that always wins.
    """
    conn = engine.connect()
    if engine.dialect.name != "postgresql":
        return conn
    got = conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": LEADER_LOCK_KEY}).scalar()
    conn.commit()
    if got:
        return conn
    conn.close()
    return None


def lock_alive(conn: Connection) -> bool:
    """True while the connection holding the leader lock is still usable."""
    try:
        conn.execute(text("SELECT 1"))
        conn.commit()
        return True
    except Exception:  # noqa: BLE001
        return False
