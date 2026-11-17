"""Storage abstraction plus a thread-safe in-memory implementation.

A real deployment would re-implement :class:`Storage` on top of a database
(e.g. Postgres ``SELECT ... FOR UPDATE`` or a transactional KV store). The
contract that every backend must honour is documented on each method; the
in-memory version is suitable for single-process tests and demos.

Concurrency safety relies on two mechanisms:

* **Leases** -- a running instance is owned by one worker until its lease
  expires; an expired lease may be stolen by another worker.
* **Fencing tokens** -- every (re)claim bumps a monotonic token; stale owners
  are rejected when they try to renew, complete or fail the instance, which
  guarantees that *at most one owner is valid* for any planned instance even
  under GC pauses and network partitions.
"""

from __future__ import annotations

import threading
from datetime import datetime
from typing import Iterable, Protocol

from .models import InstanceState, Job, JobInstance, JobState, LeaderLease

LEADER_LOCK_KEY = "scheduler:leader"


class Storage(Protocol):
    # -- jobs ---------------------------------------------------------------
    def upsert_job(self, job: Job, now: datetime) -> JobState: ...

    def get_job(self, job_id: str) -> Job | None: ...

    def list_jobs(self) -> list[Job]: ...

    def get_job_state(self, job_id: str) -> JobState | None: ...

    def compare_and_set_planned(
        self, job_id: str, expected: datetime | None, new: datetime
    ) -> bool: ...

    # -- instances ----------------------------------------------------------
    def create_instance(self, instance: JobInstance) -> bool: ...

    def get_instance(self, instance_id: str) -> JobInstance | None: ...

    def list_instances(self, job_id: str | None = None) -> list[JobInstance]: ...

    def list_claimable(self, now: datetime) -> list[JobInstance]: ...

    def claim_instance(
        self, instance_id: str, worker_id: str, now: datetime, lease_seconds: float
    ) -> JobInstance | None: ...

    def renew_lease(
        self,
        instance_id: str,
        worker_id: str,
        fencing_token: int,
        now: datetime,
        lease_seconds: float,
    ) -> bool: ...

    def complete_instance(
        self, instance_id: str, worker_id: str, fencing_token: int
    ) -> bool: ...

    def fail_instance(
        self,
        instance_id: str,
        worker_id: str,
        fencing_token: int,
        error: str,
        now: datetime,
        max_attempts: int,
        retry_backoff_seconds: float,
    ) -> bool: ...

    # -- leader election ----------------------------------------------------
    def acquire_leader(
        self, holder: str, now: datetime, ttl_seconds: float
    ) -> LeaderLease | None: ...

    def renew_leader(
        self, holder: str, fencing_token: int, now: datetime, ttl_seconds: float
    ) -> bool: ...

    def release_leader(self, holder: str, fencing_token: int) -> bool: ...


class DuplicateInstanceError(RuntimeError):
    pass


class InMemoryStorage:
    """Reference storage backend, safe for concurrent use within one process."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._jobs: dict[str, Job] = {}
        self._states: dict[str, JobState] = {}
        self._instances: dict[str, JobInstance] = {}
        self._leader: LeaderLease | None = None
        self._next_leader_token = 1

    # -- jobs ---------------------------------------------------------------
    def upsert_job(self, job: Job, now: datetime) -> JobState:
        with self._lock:
            if job.id not in self._jobs:
                self._jobs[job.id] = job
                state = JobState(job_id=job.id, last_planned=now)
                self._states[job.id] = state
                return state
            self._jobs[job.id] = job
            return self._states[job.id]

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    def get_job_state(self, job_id: str) -> JobState | None:
        with self._lock:
            return self._states.get(job_id)

    def compare_and_set_planned(
        self, job_id: str, expected: datetime | None, new: datetime
    ) -> bool:
        with self._lock:
            state = self._states.get(job_id)
            if state is None:
                return False
            if state.last_planned != expected:
                return False
            state.last_planned = new
            return True

    # -- instances ----------------------------------------------------------
    def create_instance(self, instance: JobInstance) -> bool:
        with self._lock:
            if instance.id in self._instances:
                return False
            self._instances[instance.id] = instance
            return True

    def get_instance(self, instance_id: str) -> JobInstance | None:
        with self._lock:
            return self._instances.get(instance_id)

    def list_instances(self, job_id: str | None = None) -> list[JobInstance]:
        with self._lock:
            values = list(self._instances.values())
            if job_id is not None:
                values = [item for item in values if item.job_id == job_id]
            return sorted(values, key=lambda item: item.fire_time)

    def list_claimable(self, now: datetime) -> list[JobInstance]:
        with self._lock:
            claimable: list[JobInstance] = []
            for instance in self._instances.values():
                if instance.state is InstanceState.PENDING and (
                    instance.not_before is None or instance.not_before <= now
                ):
                    claimable.append(instance)
                elif (
                    instance.state is InstanceState.RUNNING
                    and instance.lease_expires_at is not None
                    and instance.lease_expires_at <= now
                ):
                    claimable.append(instance)
            return sorted(claimable, key=lambda item: item.fire_time)

    def claim_instance(
        self, instance_id: str, worker_id: str, now: datetime, lease_seconds: float
    ) -> JobInstance | None:
        with self._lock:
            instance = self._instances.get(instance_id)
            if instance is None:
                return None
            if instance.state is InstanceState.PENDING:
                if instance.not_before is not None and instance.not_before > now:
                    return None
            elif instance.state is InstanceState.RUNNING:
                if (
                    instance.lease_expires_at is None
                    or instance.lease_expires_at > now
                ):
                    # Actively leased to another (or the same) worker.
                    return None
            else:
                return None

            instance.state = InstanceState.RUNNING
            instance.owner = worker_id
            # Per-instance monotonic token: every ownership change must be
            # strictly greater than the previous owner's token.
            instance.fencing_token += 1
            instance.lease_expires_at = self._expiry(now, lease_seconds)
            instance.attempts += 1
            instance.not_before = None
            instance.last_error = None
            return instance

    def renew_lease(
        self,
        instance_id: str,
        worker_id: str,
        fencing_token: int,
        now: datetime,
        lease_seconds: float,
    ) -> bool:
        with self._lock:
            instance = self._instances.get(instance_id)
            if instance is None:
                return False
            if not self._is_valid_owner(instance, worker_id, fencing_token):
                return False
            instance.lease_expires_at = self._expiry(now, lease_seconds)
            return True

    def complete_instance(
        self, instance_id: str, worker_id: str, fencing_token: int
    ) -> bool:
        with self._lock:
            instance = self._instances.get(instance_id)
            if instance is None:
                return False
            if not self._is_valid_owner(instance, worker_id, fencing_token):
                return False
            instance.state = InstanceState.SUCCEEDED
            instance.lease_expires_at = None
            return True

    def fail_instance(
        self,
        instance_id: str,
        worker_id: str,
        fencing_token: int,
        error: str,
        now: datetime,
        max_attempts: int,
        retry_backoff_seconds: float,
    ) -> bool:
        with self._lock:
            instance = self._instances.get(instance_id)
            if instance is None:
                return False
            if not self._is_valid_owner(instance, worker_id, fencing_token):
                return False
            instance.last_error = error
            if instance.attempts < max_attempts:
                instance.state = InstanceState.PENDING
                instance.owner = None
                instance.lease_expires_at = None
                instance.not_before = self._expiry(
                    now, retry_backoff_seconds * instance.attempts
                )
            else:
                instance.state = InstanceState.FAILED
                instance.lease_expires_at = None
            return True

    # -- leader election ----------------------------------------------------
    def acquire_leader(
        self, holder: str, now: datetime, ttl_seconds: float
    ) -> LeaderLease | None:
        with self._lock:
            lock = self._leader
            if (
                lock is not None
                and lock.holder != holder
                and lock.expires_at > now
            ):
                return None
            if lock is not None and lock.holder == holder:
                token = lock.fencing_token
            else:
                token = self._next_leader_token
                self._next_leader_token += 1
            lease = LeaderLease(
                holder=holder,
                fencing_token=token,
                expires_at=self._expiry(now, ttl_seconds),
            )
            self._leader = lease
            return lease

    def renew_leader(
        self, holder: str, fencing_token: int, now: datetime, ttl_seconds: float
    ) -> bool:
        with self._lock:
            lock = self._leader
            if lock is None or lock.holder != holder or lock.fencing_token != fencing_token:
                return False
            lock.expires_at = self._expiry(now, ttl_seconds)
            return True

    def release_leader(self, holder: str, fencing_token: int) -> bool:
        with self._lock:
            lock = self._leader
            if lock is None or lock.holder != holder or lock.fencing_token != fencing_token:
                return False
            self._leader = None
            return True

    # -- helpers ------------------------------------------------------------
    @staticmethod
    def _is_valid_owner(
        instance: JobInstance, worker_id: str, fencing_token: int
    ) -> bool:
        return (
            instance.state is InstanceState.RUNNING
            and instance.owner == worker_id
            and instance.fencing_token == fencing_token
        )

    @staticmethod
    def _expiry(now: datetime, seconds: float) -> datetime:
        from datetime import timedelta

        return now + timedelta(seconds=seconds)
