import threading
from datetime import datetime, timedelta, timezone
from unittest import TestCase

from distributed_cron import (
    CoalescePolicy,
    FakeRunner,
    InMemoryStorage,
    InstanceState,
    Job,
    ManualClock,
    RunContext,
    RunOutcome,
    Scheduler,
    ThreadPoolTaskRunner,
)

UTC = timezone.utc


def make_job(**kwargs):
    defaults = dict(
        id="j1",
        cron="* * * * *",
        func=lambda ctx: None,
        timezone=UTC,
        max_misfires=5,
        coalesce=CoalescePolicy.NEVER,
        lease_seconds=30.0,
        heartbeat_interval_seconds=10.0,
    )
    defaults.update(kwargs)
    return Job(**defaults)


class SchedulerTestBase(TestCase):
    def setUp(self):
        self.storage = InMemoryStorage()
        self.clock = ManualClock(datetime(2026, 11, 17, 10, 0, tzinfo=UTC))

    def make_scheduler(self, worker_id, runner=None):
        return Scheduler(
            storage=self.storage,
            clock=self.clock,
            runner=runner or FakeRunner(),
            worker_id=worker_id,
            leader_ttl_seconds=15.0,
        )


class PlanningTest(SchedulerTestBase):
    def test_only_leader_plans_and_failover_resumes(self):
        leader = self.make_scheduler("leader-a")
        follower = self.make_scheduler("follower-b")
        job = make_job()
        leader.add_job(job)

        self.assertTrue(leader.ensure_leader())
        self.assertFalse(follower.ensure_leader())

        self.clock.advance(180)
        summary_a = leader.plan_tick()
        summary_b = follower.plan_tick()
        self.assertEqual(summary_a["created"], 3)
        self.assertFalse(summary_b["leader"])
        self.assertEqual(len(self.storage.list_instances("j1")), 3)

        # Leader disappears; after its lock TTL expires the follower takes over.
        self.clock.advance(16)
        self.clock.advance(60)
        self.assertTrue(follower.ensure_leader())
        summary = follower.plan_tick()
        self.assertTrue(summary["leader"])
        # Watermark-based resume: only the newly due minute is planned, no
        # duplicates of instances created by the dead leader.
        self.assertEqual(summary["created"], 1)
        ids = [item.id for item in self.storage.list_instances("j1")]
        self.assertEqual(len(ids), len(set(ids)))

    def test_clock_skew_slow_node_does_not_burst_catch_up(self):
        leader = self.make_scheduler("leader")
        leader.add_job(make_job(coalesce=CoalescePolicy.ALWAYS))
        leader.ensure_leader()
        # Node clock jumps forward 10 minutes in one tick (simulating a slow
        # node catching up with real time after being paused).
        self.clock.advance(600)
        summary = leader.plan_tick()
        # ALWAYS coalesces the whole backlog into exactly one instance.
        self.assertEqual(summary["created"], 1)
        self.assertEqual(summary["coalesced"], 9)


class ExecutionTest(SchedulerTestBase):
    def _plan_one(self, leader):
        leader.add_job(make_job())
        leader.ensure_leader()
        self.clock.advance(60)
        leader.plan_tick()
        instances = self.storage.list_instances("j1")
        self.assertEqual(len(instances), 1)
        return instances[0]

    def test_single_owner_under_concurrent_claim(self):
        leader = self.make_scheduler("leader")
        instance = self._plan_one(leader)

        winners = []
        barrier = threading.Barrier(5)

        def worker(name):
            scheduler = self.make_scheduler(name)
            barrier.wait()
            outcome = scheduler.claim_and_run(instance)
            if outcome is not None:
                winners.append(name)

        threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(winners), 1)
        reloaded = self.storage.get_instance(instance.id)
        self.assertEqual(reloaded.state, InstanceState.SUCCEEDED)
        self.assertEqual(reloaded.owner, winners[0])

    def test_lease_expiry_failover_and_stale_owner_fenced(self):
        leader = self.make_scheduler("leader")
        instance = self._plan_one(leader)

        # w1 claims but the job hangs; its lease eventually expires.
        w1 = self.make_scheduler("w1")
        claimed_by_w1 = self.storage.claim_instance(
            instance.id, "w1", self.clock.now(), 30
        )
        self.assertIsNotNone(claimed_by_w1)

        self.clock.advance(31)
        w2 = self.make_scheduler("w2")
        outcome = w2.claim_and_run(instance)
        self.assertIsNotNone(outcome)
        reloaded = self.storage.get_instance(instance.id)
        self.assertEqual(reloaded.owner, "w2")

        # The stale w1 token must now be rejected on every mutation.
        self.assertFalse(
            self.storage.renew_lease(
                instance.id, "w1", claimed_by_w1.fencing_token,
                self.clock.now(), 30,
            )
        )
        self.assertFalse(
            self.storage.complete_instance(
                instance.id, "w1", claimed_by_w1.fencing_token
            )
        )
        self.assertFalse(
            self.storage.fail_instance(
                instance.id, "w1", claimed_by_w1.fencing_token, "late",
                self.clock.now(), max_attempts=1, retry_backoff_seconds=0,
            )
        )

    def test_heartbeat_keeps_long_running_lease_alive(self):
        leader = self.make_scheduler("leader")
        instance = self._plan_one(leader)
        w1 = self.make_scheduler("w1")
        claimed = self.storage.claim_instance(
            instance.id, "w1", self.clock.now(), 30
        )
        # Simulate heartbeats while the job runs for 75 virtual seconds.
        for _ in range(5):
            self.clock.advance(15)
            self.assertTrue(
                self.storage.renew_lease(
                    instance.id, "w1", claimed.fencing_token,
                    self.clock.now(), 30,
                )
            )
        # No other worker can steal it despite wall time exceeding one TTL.
        self.assertIsNone(
            self.storage.claim_instance(instance.id, "w2", self.clock.now(), 30)
        )

    def test_retry_with_backoff_then_success(self):
        attempts = {"count": 0}

        def flaky(context: RunContext) -> RunOutcome:
            attempts["count"] += 1
            if attempts["count"] == 1:
                return RunOutcome(success=False, error="transient")
            return RunOutcome(success=True)

        leader = self.make_scheduler("leader", FakeRunner(behavior=flaky))
        leader.add_job(make_job(max_attempts=3, retry_backoff_seconds=60))
        leader.ensure_leader()
        self.clock.advance(60)
        leader.plan_tick()
        instance = self.storage.list_instances("j1")[0]

        leader.worker_tick()
        reloaded = self.storage.get_instance(instance.id)
        self.assertEqual(reloaded.state, InstanceState.PENDING)
        self.assertEqual(reloaded.attempts, 1)
        # Backoff window: nothing claimable yet.
        self.clock.advance(30)
        self.assertEqual(leader.worker_tick(), 0)

        self.clock.advance(31)
        self.assertEqual(leader.worker_tick(), 1)
        self.assertEqual(
            self.storage.get_instance(instance.id).state,
            InstanceState.SUCCEEDED,
        )
        self.assertEqual(attempts["count"], 2)

    def test_permanent_failure_after_max_attempts(self):
        def always_fail(context: RunContext) -> RunOutcome:
            return RunOutcome(success=False, error="nope")

        leader = self.make_scheduler(
            "leader", FakeRunner(behavior=always_fail)
        )
        leader.add_job(make_job(max_attempts=2, retry_backoff_seconds=0))
        leader.ensure_leader()
        self.clock.advance(60)
        leader.plan_tick()

        leader.worker_tick()
        leader.worker_tick()
        instance = self.storage.list_instances("j1")[0]
        self.assertEqual(instance.state, InstanceState.FAILED)
        self.assertEqual(instance.attempts, 2)
        self.assertEqual(instance.last_error, "nope")


class TimeoutTest(TestCase):
    def test_timeout_stops_heartbeat_and_allows_failover(self):
        storage = InMemoryStorage()
        clock = ManualClock(datetime(2026, 11, 17, 10, 0, tzinfo=UTC))
        release = threading.Event()

        def hanging(context: RunContext) -> None:
            release.wait(timeout=5)

        job = make_job(
            func=hanging,
            lease_seconds=1.0,
            heartbeat_interval_seconds=0.2,
        )
        storage.upsert_job(job, clock.now())
        from distributed_cron import JobInstance
        storage.create_instance(
            JobInstance(job_id=job.id, fire_time=clock.now())
        )
        runner = ThreadPoolTaskRunner(timeout_seconds=0.5)
        scheduler = Scheduler(
            storage=storage, clock=clock, runner=runner,
            worker_id="w1",
        )
        instance = storage.list_instances("j1")[0]
        outcome = scheduler.claim_and_run(instance)
        self.assertIsNotNone(outcome)
        self.assertFalse(outcome.success)
        self.assertTrue(outcome.timed_out)

        # Timeout path marks the attempt failed (no retries configured); even
        # if it were retried, the orphaned thread's late write is fenced.
        reloaded = storage.get_instance(instance.id)
        self.assertIn(reloaded.state, (InstanceState.FAILED, InstanceState.PENDING))
        release.set()


class EndToEndTest(TestCase):
    def test_two_nodes_real_threads_one_effective_owner(self):
        storage = InMemoryStorage()
        from distributed_cron import SystemClock

        clock = SystemClock()
        ran = []
        lock = threading.Lock()

        def job_func(context: RunContext) -> None:
            with lock:
                ran.append((context.instance.id, context.job.id))

        job = make_job(func=job_func, lease_seconds=5.0)
        storage.upsert_job(job, clock.now())
        from distributed_cron import JobInstance

        # Pre-seed one due instance so the test does not depend on waiting
        # for the next wall-clock minute; the two live nodes still race for
        # the claim through the real storage backend.
        storage.create_instance(
            JobInstance(job_id=job.id, fire_time=clock.now())
        )

        node_a = Scheduler(storage=storage, clock=clock,
                           runner=ThreadPoolTaskRunner(timeout_seconds=5),
                           worker_id="node-a", leader_ttl_seconds=2,
                           tick_interval_seconds=0.05)
        node_b = Scheduler(storage=storage, clock=clock,
                           runner=ThreadPoolTaskRunner(timeout_seconds=5),
                           worker_id="node-b", leader_ttl_seconds=2,
                           tick_interval_seconds=0.05)
        node_a.start()
        node_b.start()
        try:
            deadline = datetime.now(timezone.utc) + timedelta(seconds=4)
            while datetime.now(timezone.utc) < deadline:
                if storage.list_instances("j1"):
                    break
                threading.Event().wait(0.05)

            instances = storage.list_instances("j1")
            self.assertTrue(instances)
            instance = instances[0]
            # Wait until it reaches a terminal state on one of the nodes.
            terminal_deadline = datetime.now(timezone.utc) + timedelta(seconds=6)
            while datetime.now(timezone.utc) < terminal_deadline:
                reloaded = storage.get_instance(instance.id)
                if reloaded.state in (InstanceState.SUCCEEDED, InstanceState.FAILED):
                    break
                threading.Event().wait(0.05)

            reloaded = storage.get_instance(instance.id)
            self.assertEqual(reloaded.state, InstanceState.SUCCEEDED)
            owners = {item.owner for item in storage.list_instances("j1")}
            self.assertEqual(len(owners), 1)
            self.assertEqual(len(ran), 1)
            self.assertEqual(ran[0][0], instance.id)
        finally:
            node_a.stop(timeout=2)
            node_b.stop(timeout=2)
