"""Cron helpers. All datetimes are naive UTC."""
from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from croniter import croniter


def due_times(
    schedule: str, last: datetime, now: datetime, catchup: bool, limit: int = 50
) -> List[datetime]:
    """Fire times in (last, now]. Without catchup only the most recent one is returned."""
    it = croniter(schedule, last)
    times: List[datetime] = []
    while True:
        t = it.get_next(datetime)
        if t > now:
            break
        times.append(t)
        if not catchup and len(times) > 1:
            times.pop(0)
        elif len(times) >= limit:
            break
    return times


def next_fire(schedule: Optional[str], after: datetime) -> Optional[datetime]:
    if not schedule:
        return None
    return croniter(schedule, after).get_next(datetime)
