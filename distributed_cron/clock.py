from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    """Injectable time source. All datetimes are timezone-aware UTC."""

    def now(self) -> datetime: ...

    def sleep(self, seconds: float) -> None: ...


class SystemClock:
    """Wall-clock implementation backed by :mod:`time`."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


class ManualClock:
    """Deterministic clock for tests: time only moves when :meth:`advance` is called.

    ``sleep`` advances the virtual clock instead of blocking the real thread.
    """

    def __init__(self, start: datetime | None = None):
        self._now = start or datetime(2026, 1, 1, tzinfo=timezone.utc)
        if self._now.tzinfo is None:
            raise ValueError("ManualClock start must be timezone-aware")

    def now(self) -> datetime:
        return self._now

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)

    def advance(self, seconds: float) -> datetime:
        if seconds < 0:
            raise ValueError("time cannot be advanced by a negative amount")
        from datetime import timedelta

        self._now = self._now + timedelta(seconds=seconds)
        return self._now

    def set(self, moment: datetime) -> None:
        """Force the clock to ``moment``; supports simulating clock rollback."""
        if moment.tzinfo is None:
            raise ValueError("ManualClock moment must be timezone-aware")
        self._now = moment
