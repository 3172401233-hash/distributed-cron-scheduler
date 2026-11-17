from datetime import datetime, timedelta, timezone
from unittest import TestCase

from distributed_cron import (
    CoalescePolicy,
    CronExpression,
    InMemoryStorage,
    Job,
    ManualClock,
    apply_misfire_policy,
    due_fires,
    plan_job,
)

UTC = timezone.utc


def make_job(**kwargs):
    defaults = dict(
        id="j1",
        cron="* * * * *",
        func=lambda ctx: None,
        timezone=UTC,
        max_misfires=1,
        coalesce=CoalescePolicy.ALWAYS,
    )
    defaults.update(kwargs)
    return Job(**defaults)


class DueFiresTest(TestCase):
    def test_returns_all_minutes_between_watermark_and_now(self):
        cron = CronExpression("* * * * *")
        start = datetime(2026, 11, 17, 10, 0, tzinfo=UTC)
        now = start + timedelta(minutes=5)
        fires = due_fires(cron, start, now)
        self.assertEqual(
            fires,
            [start + timedelta(minutes=i) for i in range(1, 6)],
        )

    def test_no_fires_when_now_before_watermark(self):
        cron = CronExpression("* * * * *")
        start = datetime(2026, 11, 17, 10, 0, tzinfo=UTC)
        self.assertEqual(due_fires(cron, start, start - timedelta(minutes=1)), [])

    def test_iteration_cap_is_bounded(self):
        cron = CronExpression("* * * * *")
        start = datetime(2020, 1, 1, tzinfo=UTC)
        now = start + timedelta(days=400)
        fires = due_fires(cron, start, now, max_iterations=100)
        self.assertEqual(len(fires), 100)


class MisfirePolicyTest(TestCase):
    def _fires(self, count):
        base = datetime(2026, 11, 17, 10, 0, tzinfo=UTC)
        return [base + timedelta(minutes=i) for i in range(1, count + 1)]

    def test_always_coalesce_backlog_to_latest(self):
        job = make_job(coalesce=CoalescePolicy.ALWAYS)
        kept, coalesced, dropped = apply_misfire_policy(self._fires(5), job)
        self.assertEqual(kept, self._fires(5)[-1:])
        self.assertEqual(coalesced, 4)
        self.assertEqual(dropped, 0)

    def test_on_count_replays_when_within_cap(self):
        job = make_job(max_misfires=3, coalesce=CoalescePolicy.ON_COUNT)
        kept, coalesced, dropped = apply_misfire_policy(self._fires(3), job)
        self.assertEqual(len(kept), 3)
        self.assertEqual((coalesced, dropped), (0, 0))

    def test_on_count_coalesces_when_cap_exceeded(self):
        job = make_job(max_misfires=2, coalesce=CoalescePolicy.ON_COUNT)
        kept, coalesced, dropped = apply_misfire_policy(self._fires(5), job)
        self.assertEqual(kept, self._fires(5)[-1:])
        self.assertEqual(coalesced, 4)

    def test_never_coalesce_is_still_capped(self):
        job = make_job(max_misfires=2, coalesce=CoalescePolicy.NEVER)
        kept, coalesced, dropped = apply_misfire_policy(self._fires(5), job)
        self.assertEqual(kept, self._fires(5)[-2:])
        self.assertEqual(dropped, 3)
        self.assertEqual(coalesced, 0)


class PlanJobTest(TestCase):
    def test_creates_due_instances_and_advances_watermark(self):
        storage = InMemoryStorage()
        clock = ManualClock(datetime(2026, 11, 17, 10, 0, tzinfo=UTC))
        job = make_job()
        storage.upsert_job(job, clock.now())

        clock.advance(60 * 3)
        result = plan_job(job, storage, clock.now())
        self.assertEqual(len(result.created), 1)  # coalesced to latest minute
        self.assertEqual(result.coalesced, 2)
        state = storage.get_job_state("j1")
        self.assertEqual(
            state.last_planned,
            datetime(2026, 11, 17, 10, 3, tzinfo=UTC),
        )

    def test_repeated_ticks_do_not_duplicate_instances(self):
        storage = InMemoryStorage()
        clock = ManualClock(datetime(2026, 11, 17, 10, 0, tzinfo=UTC))
        job = make_job(coalesce=CoalescePolicy.NEVER, max_misfires=10)
        storage.upsert_job(job, clock.now())

        clock.advance(120)
        first = plan_job(job, storage, clock.now())
        second = plan_job(job, storage, clock.now())
        self.assertEqual(len(first.created), 2)
        self.assertEqual(second.created, [])
        self.assertEqual(len(storage.list_instances("j1")), 2)

    def test_clock_rollback_never_replans(self):
        storage = InMemoryStorage()
        clock = ManualClock(datetime(2026, 11, 17, 10, 0, tzinfo=UTC))
        job = make_job()
        storage.upsert_job(job, clock.now())
        clock.advance(300)
        plan_job(job, storage, clock.now())

        clock.set(datetime(2026, 11, 17, 10, 2, tzinfo=UTC))
        result = plan_job(job, storage, clock.now())
        self.assertTrue(result.clock_rolled_back)
        self.assertEqual(result.created, [])
        # Watermark untouched by the rollback tick.
        self.assertEqual(
            storage.get_job_state("j1").last_planned,
            datetime(2026, 11, 17, 10, 5, tzinfo=UTC),
        )

    def test_long_outage_never_catches_up_unboundedly(self):
        storage = InMemoryStorage()
        clock = ManualClock(datetime(2026, 11, 17, 10, 0, tzinfo=UTC))
        job = make_job(coalesce=CoalescePolicy.NEVER, max_misfires=5)
        storage.upsert_job(job, clock.now())

        clock.advance(60 * 60)  # 60 missed minutes
        result = plan_job(job, storage, clock.now())
        self.assertEqual(len(result.created), 5)
        self.assertEqual(result.dropped, 55)
        # Watermark still jumped to the newest fire, so no later catch-up.
        clock.advance(60)
        again = plan_job(job, storage, clock.now())
        self.assertEqual(len(again.created), 1)
        self.assertEqual(len(storage.list_instances("j1")), 6)

    def test_timezone_aware_planning(self):
        # Fixed +08:00 offset keeps the test independent of system tzdata.
        from datetime import timezone as tz

        plus8 = tz(timedelta(hours=8))
        storage = InMemoryStorage()
        start = datetime(2026, 11, 17, 2, 0, tzinfo=UTC)  # 10:00 local
        clock = ManualClock(start)
        job = make_job(cron="0 9 * * *", timezone=plus8)
        storage.upsert_job(job, clock.now())

        clock.advance(24 * 3600)
        result = plan_job(job, storage, clock.now())
        self.assertEqual(len(result.created), 1)
        # 09:00 +08:00 == 01:00 UTC on 2026-11-18.
        self.assertEqual(
            result.created[0].fire_time,
            datetime(2026, 11, 18, 1, 0, tzinfo=UTC),
        )
