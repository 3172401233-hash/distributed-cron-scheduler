"""Task runners: execute job callables and keep the storage lease alive.

The runner is injected like the clock and storage, which lets tests use
:class:`FakeRunner` for fully deterministic execution while production uses
:class:`ThreadPoolTaskRunner` with real threads.
"""

from __future__ import annotations

import logging
import threading
from typing import Callable, Protocol

from .models import RunContext, RunOutcome

logger = logging.getLogger(__name__)


class TaskRunner(Protocol):
    def run(
        self,
        context: RunContext,
        renew: Callable[[], bool],
        heartbeat_interval: float,
    ) -> RunOutcome: ...


def _heartbeat_loop(
    renew: Callable[[], bool],
    stop_event: threading.Event,
    interval: float,
) -> None:
    while not stop_event.wait(interval):
        try:
            still_owner = renew()
        except Exception:  # pragma: no cover - defensive
            logger.exception("lease renewal raised; treating as lease loss")
            still_owner = False
        if not still_owner:
            logger.warning("lease lost; execution will be fenced off on completion")
            return


class ThreadPoolTaskRunner:
    """Runs each job in a worker thread with a lease-heartbeat thread.

    Python cannot forcibly kill a thread, so ``timeout`` is cooperative: when
    the deadline passes the runner stops heartbeating (the lease then expires
    and another worker may take over) and reports ``timed_out``. The orphaned
    thread keeps running until the callable returns; any late completion is
    rejected by the storage fencing check, so it cannot double-write results.
    """

    def __init__(self, timeout_seconds: float | None = None):
        self.timeout_seconds = timeout_seconds

    def run(
        self,
        context: RunContext,
        renew: Callable[[], bool],
        heartbeat_interval: float,
    ) -> RunOutcome:
        outcome_box: dict[str, RunOutcome] = {}

        def target() -> None:
            try:
                context.job.func(context)
                outcome_box["outcome"] = RunOutcome(success=True)
            except Exception as exc:  # noqa: BLE001 - jobs may raise anything
                logger.exception(
                    "job %s instance %s failed on attempt %d",
                    context.job.id,
                    context.instance.id,
                    context.attempt,
                )
                outcome_box["outcome"] = RunOutcome(
                    success=False, error=f"{type(exc).__name__}: {exc}"
                )

        worker = threading.Thread(
            target=target,
            name=f"cron-job-{context.job.id}",
            daemon=True,
        )
        stop_heartbeat = threading.Event()
        heartbeat = threading.Thread(
            target=_heartbeat_loop,
            args=(renew, stop_heartbeat, heartbeat_interval),
            name=f"cron-heartbeat-{context.job.id}",
            daemon=True,
        )
        worker.start()
        heartbeat.start()
        worker.join(self.timeout_seconds)

        if worker.is_alive():
            stop_heartbeat.set()
            return RunOutcome(
                success=False,
                error=f"execution exceeded timeout {self.timeout_seconds}s",
                timed_out=True,
            )

        stop_heartbeat.set()
        heartbeat.join(timeout=1.0)
        return outcome_box.get(
            "outcome",
            RunOutcome(success=False, error="worker thread ended without a result"),
        )


class FakeRunner:
    """Deterministic runner driven by pluggable callables (no real waiting)."""

    def __init__(
        self,
        behavior: Callable[[RunContext], RunOutcome] | None = None,
        renew_calls: list[bool] | None = None,
    ):
        self.behavior = behavior or (lambda ctx: RunOutcome(success=True))
        self.renew_calls = renew_calls if renew_calls is not None else []

    def run(
        self,
        context: RunContext,
        renew: Callable[[], bool],
        heartbeat_interval: float,
    ) -> RunOutcome:
        # One simulated heartbeat; a False result means the lease was stolen.
        self.renew_calls.append(renew())
        return self.behavior(context)
