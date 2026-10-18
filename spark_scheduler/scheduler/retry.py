"""Retry decisions and backoff math, kept pure for testing."""
from __future__ import annotations

import random
from typing import Callable, Optional

from .spec import RetryPolicy

# Attempt outcomes
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
TIMED_OUT = "TIMED_OUT"
LOST = "LOST"  # process vanished without an exit code (e.g. scheduler host restart)
CANCELLED = "CANCELLED"


def should_retry(
    policy: RetryPolicy, attempt_no: int, outcome: str, exit_code: Optional[int]
) -> bool:
    if attempt_no >= policy.max_attempts:
        return False
    if outcome == FAILED:
        return not policy.retry_on_exit_codes or exit_code in policy.retry_on_exit_codes
    if outcome == TIMED_OUT:
        return policy.retry_on_timeout
    if outcome == LOST:
        return True
    return False


def backoff_delay(
    policy: RetryPolicy, attempt_no: int, rand: Callable[[], float] = random.random
) -> float:
    """Delay before attempt `attempt_no + 1`, given that `attempt_no` just failed."""
    if policy.backoff == "fixed":
        base = policy.initial_delay_s
    else:
        base = policy.initial_delay_s * (2 ** (attempt_no - 1))
    base = min(base, policy.max_delay_s)
    if policy.jitter:
        base *= 1 + policy.jitter * (2 * rand() - 1)
    return max(0.0, base)
