"""Distributed cron scheduler core.

Two responsibilities are intentionally separable so they can be tested
deterministically:

* :meth:`Scheduler.plan_tick` -- leader-only. Scans every job's cron
  expression against the injected clock and materialises bounded,
  deduplicated :class:`JobInstance` rows.
* :meth:`Scheduler.worker_tick` -- runs on every node. Atomically claims
  due/expired instances and executes them under a renewable lease.

All nodes can call both ticks; leader election simply makes planning a
no-op on followers. A single node therefore loses nothing when the leader
dies -- the next elected node resumes planning from the persisted watermark.
"""

from __future__ import annotations

import logging
import threading
import uuid

from .clock import Clock, SystemClock
from .models import Job, JobInstance, JobState, RunContext, RunOutcome
from .planner import plan_job
from .runner import TaskRunner, ThreadPoolTaskRunner
from .storage import Storage

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(
        self,
        storage: Storage,
        clock: Clock | None = None,
        runner: TaskRunner | None = None,
        worker_id: str | None = None,
        leader_ttl_seconds: float = 15.0,
        tick_interval_seconds: float = 1.0,
    ):
        self.storage = storage
        self.clock = clock or SystemClock()
        self.runner = runner or ThreadPoolTaskRunner()
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.leader_ttl_seconds = leader_ttl_seconds
        self.tick_interval_seconds = tick_interval_seconds
        self._leader_token: int | None = None
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # -- jobs ---------------------------------------------------------------
    def add_job(self, job: Job) -> JobState:
        return self.storage.upsert_job(job, self.clock.now())

    # -- leader election ----------------------------------------------------
    def ensure_leader(self) -> bool:
        """Acquire or renew the planning lock. Returns True while leading."""
        now = self.clock.now()
        if self._leader_token is None:
            lease = self.storage.acquire_leader(
                self.worker_id, now, self.leader_ttl_seconds
            )
            if lease is not None:
                self._leader_token = lease.fencing_token
                logger.info("worker %s became leader (token=%d)", self.worker_id, lease.fencing_token)
            return lease is not None

        renewed = self.storage.renew_leader(
            self.worker_id, self._leader_token, now, self.leader_ttl_seconds
        )
        if not renewed:
            logger.warning("worker %s lost leadership", self.worker_id)
            self._leader_token = None
        return renewed

    def release_leadership(self) -> None:
        if self._leader_token is not None:
            self.storage.release_leader(self.worker_id, self._leader_token)
            self._leader_token = None

    # -- planning -----------------------------------------------------------
    def plan_tick(self) -> dict[str, object]:
        """Leader tick: create all due instances. No-op for followers."""
        summary = {"leader": False, "jobs": 0, "created": 0, "coalesced": 0, "dropped": 0,
                   "rolled_back": 0}
        if not self.ensure_leader():
            return summary
        summary["leader"] = True

        now = self.clock.now()
        for job in self.storage.list_jobs():
            result = plan_job(job, self.storage, now)
            summary["jobs"] += 1
            summary["created"] += len(result.created)
            summary["coalesced"] += result.coalesced
            summary["dropped"] += result.dropped
            if result.clock_rolled_back:
                summary["rolled_back"] += 1
        return summary

    # -- execution ----------------------------------------------------------
    def claim_and_run(self, instance: JobInstance) -> RunOutcome | None:
        """Atomically claim an instance and execute it once.

        Returns ``None`` when another worker won the claim race.
        """
        job = self.storage.get_job(instance.job_id)
        if job is None:
            return None

        now = self.clock.now()
        claimed = self.storage.claim_instance(
            instance.id, self.worker_id, now, job.lease_seconds
        )
        if claimed is None:
            return None

        def renew() -> bool:
            return self.storage.renew_lease(
                claimed.id,
                self.worker_id,
                claimed.fencing_token,
                self.clock.now(),
                job.lease_seconds,
            )

        # Heartbeats must be much shorter than the lease so transient slowness
        # does not cause spurious fail-over.
        heartbeat_interval = min(
            job.heartbeat_interval_seconds, max(job.lease_seconds / 3.0, 0.01)
        )
        context = RunContext(
            job=job,
            instance=claimed,
            attempt=claimed.attempts,
            cancel_event=threading.Event(),
        )
        outcome = self.runner.run(context, renew, heartbeat_interval)
        self._record_outcome(job, claimed, outcome)
        return outcome

    def _record_outcome(self, job: Job, instance: JobInstance, outcome: RunOutcome) -> None:
        if outcome.success:
            accepted = self.storage.complete_instance(
                instance.id, self.worker_id, instance.fencing_token
            )
            if not accepted:
                logger.warning(
                    "completion of %s fenced off (lease lost to another worker)",
                    instance.id,
                )
            return

        accepted = self.storage.fail_instance(
            instance.id,
            self.worker_id,
            instance.fencing_token,
            outcome.error or "unknown error",
            self.clock.now(),
            job.max_attempts,
            job.retry_backoff_seconds,
        )
        if not accepted:
            logger.warning(
                "failure report of %s fenced off; another worker owns the instance now",
                instance.id,
            )

    def worker_tick(self, limit: int | None = None) -> int:
        """Claim and run due instances. Returns the number of executions."""
        now = self.clock.now()
        claimable = self.storage.list_claimable(now)
        if limit is not None:
            claimable = claimable[:limit]
        executed = 0
        for instance in claimable:
            if self._stop_event.is_set():
                break
            if self.claim_and_run(instance) is not None:
                executed += 1
        return executed

    # -- background loop ----------------------------------------------------
    def run_forever(self) -> None:
        """Blocking loop combining planning and execution ticks."""
        while not self._stop_event.is_set():
            try:
                self.plan_tick()
                self.worker_tick()
            except Exception:  # pragma: no cover - defensive
                logger.exception("scheduler tick failed; retrying after interval")
            self.clock.sleep(self.tick_interval_seconds)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self.run_forever, name=f"scheduler-{self.worker_id}", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float | None = None) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._thread = None

    # -- helpers for tests / observability ----------------------------------
    def is_leader(self) -> bool:
        return self._leader_token is not None
