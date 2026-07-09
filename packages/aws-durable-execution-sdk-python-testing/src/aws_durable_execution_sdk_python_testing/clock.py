"""Clock sources for scheduler-driven durable timers.

A clock decides how long the local runner actually waits before a
scheduled wake fires, and supplies the "now" used to stamp operation
and invocation timestamps. All time reads in the runner go through one
clock so history events share a single, totally ordered clock domain.
"""

from __future__ import annotations

import datetime
import threading
from typing import Protocol


def real_now() -> datetime.datetime:
    """Return the current wall-clock time in UTC."""
    return datetime.datetime.now(datetime.UTC)  # noqa: TID251


class Clock(Protocol):
    """Time source used to arm timers and stamp timestamps."""

    def now(self) -> datetime.datetime:
        """Return the current time on this clock."""
        ...

    def arm(self, wake: datetime.datetime) -> float:
        """Return the real seconds to wait before firing at ``wake``."""
        ...


class RealClock:
    """Wall-clock time source. Timers wait the real modeled duration."""

    def now(self) -> datetime.datetime:
        return real_now()

    def arm(self, wake: datetime.datetime) -> float:
        return max((wake - self.now()).total_seconds(), 0.0)


class SkipClock:
    """Virtual clock that skips over armed timer horizons.

    Tracks the total duration skipped so far: ``now`` returns real time
    plus the accumulated skip, so the clock advances with real time
    between jumps and every read is distinct and monotonic. Arming a
    wake beyond virtual now adds the remaining gap to the accumulated
    skip and returns a zero delay, so the wake fires on the next
    event-loop turn rather than after the modeled duration. Guards its
    state with a lock because ``arm`` and ``now`` run on different
    threads.
    """

    def __init__(self) -> None:
        self._skipped: datetime.timedelta = datetime.timedelta(0)
        self._lock: threading.Lock = threading.Lock()

    def now(self) -> datetime.datetime:
        with self._lock:
            return real_now() + self._skipped

    def arm(self, wake: datetime.datetime) -> float:
        with self._lock:
            gap: datetime.timedelta = wake - (real_now() + self._skipped)
            if gap > datetime.timedelta(0):
                self._skipped += gap
        return 0.0
