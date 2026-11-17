from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class CoalescePolicy(str, Enum):
    """How to collapse multiple missed firings of the same job."""

    ALWAYS = "always"
    """Never coalesce: replay up to ``max_misfires`` missed instances."""

    ON_COUNT = "on_count"
    """Coalesce only when more than ``max_misfires`` instances were missed."""

    NEVER = "never"
    """Same as :attr:`ALWAYS`'s opposite: replay every missed instance.

    The number of replays is still hard-capped by ``max_misfires`` so a long
    outage can never produce an unbounded catch-up storm.
    """


class InstanceState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass
class Job:
    """A cron job definition.

    All planning happens in ``timezone``; ``cron`` is a standard five-field
    expression (minute hour day-of-month month day-of-week).
    """

    id: str
    cron: str
    func: Any
    timezone: Any
    max_misfires: int = 1
    coalesce: CoalescePolicy = CoalescePolicy.ALWAYS
    max_attempts: int = 1
    retry_backoff_seconds: float = 0.0
    lease_seconds: float = 30.0
    heartbeat_interval_seconds: float = 10.0
    enabled: bool = True
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.max_misfires < 1:
            raise ValueError("max_misfires must be >= 1")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be > 0")


@dataclass
class JobState:
    """Mutable per-job watermark persisted by the storage layer."""

    job_id: str
    last_planned: datetime | None = None


@dataclass
class JobInstance:
    """A single planned firing of a job, identified by ``(job_id, fire_time)``."""

    job_id: str
    fire_time: datetime
    id: str = ""
    state: InstanceState = InstanceState.PENDING
    owner: str | None = None
    fencing_token: int = 0
    lease_expires_at: datetime | None = None
    attempts: int = 0
    not_before: datetime | None = None
    last_error: str | None = None

    def __post_init__(self) -> None:
        if not self.id:
            self.id = f"{self.job_id}:{self.fire_time.isoformat()}"


@dataclass
class LeaderLease:
    holder: str
    fencing_token: int
    expires_at: datetime


@dataclass
class RunContext:
    job: Job
    instance: JobInstance
    attempt: int
    cancel_event: Any


@dataclass
class RunOutcome:
    success: bool
    error: str | None = None
    timed_out: bool = False


@dataclass
class PlanResult:
    """Result of one planning tick, mainly useful for tests/observability."""

    created: list[JobInstance] = field(default_factory=list)
    coalesced: int = 0
    dropped: int = 0
    clock_rolled_back: bool = False
