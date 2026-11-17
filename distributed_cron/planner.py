"""Planning logic: turn cron expressions into bounded, deduped fire times.

The planner is pure: given a job, its persisted watermark and the current
(injected) time it decides which instances must be materialised. All side
effects go through the injected :class:`~distributed_cron.storage.Storage`.
"""

from __future__ import annotations

import logging
from datetime import datetime

from .cron import CronExpression
from .models import CoalescePolicy, Job, JobInstance, PlanResult
from .storage import Storage

logger = logging.getLogger(__name__)


def due_fires(
    cron: CronExpression,
    after: datetime,
    now: datetime,
    *,
    max_iterations: int = 200_000,
) -> list[datetime]:
    """Return every cron minute strictly after ``after`` and not later than ``now``.

    ``max_iterations`` is a hard safety bound so a pathological expression can
    never loop forever; combined with the job-level ``max_misfires`` cap the
    scheduler is guaranteed to produce a bounded number of instances per tick.
    """
    if now <= after:
        return []
    fires: list[datetime] = []
    candidate = cron.next_after(after)
    iterations = 0
    while candidate <= now:
        fires.append(candidate)
        iterations += 1
        if iterations >= max_iterations:
            logger.warning(
                "cron scan hit max_iterations=%d; remaining missed fires are dropped",
                max_iterations,
            )
            break
        candidate = cron.next_after(candidate)
    return fires


def apply_misfire_policy(
    fires: list[datetime], job: Job
) -> tuple[list[datetime], int, int]:
    """Apply ``max_misfires`` and the coalesce policy to missed fire times.

    Returns ``(kept_fires, coalesced_count, dropped_count)``.

    * :attr:`CoalescePolicy.ALWAYS` -- any backlog collapses to the most
      recent missed firing (one catch-up instance, at the newest time).
    * :attr:`CoalescePolicy.ON_COUNT` -- up to ``max_misfires`` instances are
      replayed individually; if more were missed, they coalesce into one.
    * :attr:`CoalescePolicy.NEVER` -- replay individually, still hard-capped
      by ``max_misfires``; the excess is dropped.
    """
    total = len(fires)
    if total == 0:
        return [], 0, 0

    if job.coalesce is CoalescePolicy.ALWAYS:
        kept = fires[-1:]
        return kept, total - len(kept), 0

    if total <= job.max_misfires:
        return list(fires), 0, 0

    if job.coalesce is CoalescePolicy.ON_COUNT:
        # Replay the most recent missed firing as a single coalesced instance.
        kept = fires[-1:]
        return kept, total - len(kept), 0

    # CoalescePolicy.NEVER: replay the newest max_misfires, drop the rest.
    kept = fires[-job.max_misfires :]
    dropped = total - len(kept)
    return kept, 0, dropped


def plan_job(job: Job, storage: Storage, now: datetime) -> PlanResult:
    """Materialise due instances for one job and advance its watermark."""
    result = PlanResult()
    if not job.enabled:
        return result

    state = storage.get_job_state(job.id)
    if state is None:
        # Unknown job state means the job was never registered properly.
        return result

    watermark = state.last_planned
    if watermark is None:
        return result

    if now < watermark:
        # Clock rollback: never re-plan already materialised instances.
        logger.warning(
            "clock rollback detected for job %s: now=%s < watermark=%s",
            job.id,
            now.isoformat(),
            watermark.isoformat(),
        )
        result.clock_rolled_back = True
        return result

    cron = CronExpression(job.cron)
    # Evaluate the expression in the job's own timezone.
    local_now = now.astimezone(job.timezone)
    local_watermark = watermark.astimezone(job.timezone)
    local_fires = due_fires(cron, local_watermark, local_now)

    kept_local, coalesced, dropped = apply_misfire_policy(local_fires, job)
    result.coalesced = coalesced
    result.dropped = dropped

    # Convert back to UTC instants so instance identity is timezone-safe
    # (ambiguous local times during DST still map to unique UTC moments).
    kept = [fire.astimezone(now.tzinfo) for fire in kept_local]
    newest = local_fires[-1] if local_fires else local_now
    new_watermark = newest.astimezone(now.tzinfo)

    for fire_time in kept:
        instance = JobInstance(job_id=job.id, fire_time=fire_time)
        if storage.create_instance(instance):
            result.created.append(instance)

    # Advance the watermark past everything we scanned, even coalesced/dropped
    # fires, so the next tick never replays them. CAS guards against two
    # planners racing on the same job.
    storage.compare_and_set_planned(job.id, watermark, new_watermark)
    return result
