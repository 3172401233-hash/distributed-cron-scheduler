from datetime import datetime, timedelta, timezone
from unittest import TestCase

from distributed_cron import InMemoryStorage, InstanceState, Job, JobInstance

UTC = timezone.utc


def make_instance(job_id="j1", fire_time=None):
    return JobInstance(
        job_id=job_id,
        fire_time=fire_time or datetime(2026, 11, 17, 10, 0, tzinfo=UTC),
    )


def make_job(**kwargs):
    defaults = dict(
        id="j1",
        cron="* * * * *",
        func=lambda ctx: None,
        timezone=UTC,
    )
    defaults.update(kwargs)
    return Job(**defaults)


class InMemoryStorageTest(TestCase):
    def setUp(self):
        self.storage = InMemoryStorage()
        self.now = datetime(2026, 11, 17, 10, 0, tzinfo=UTC)
        self.storage.upsert_job(make_job(), self.now)

    def test_create_instance_is_idempotent(self):
        instance = make_instance()
        self.assertTrue(self.storage.create_instance(instance))
        self.assertFalse(self.storage.create_instance(make_instance()))

    def test_claim_is_exclusive_and_bumps_fencing_token(self):
        instance = make_instance()
        self.storage.create_instance(instance)
        first = self.storage.claim_instance(instance.id, "w1", self.now, 30)
        second = self.storage.claim_instance(instance.id, "w2", self.now, 30)
        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertEqual(first.owner, "w1")
        self.assertGreater(first.fencing_token, 0)
        self.assertEqual(first.state, InstanceState.RUNNING)

    def test_expired_lease_can_be_stolen_with_new_token(self):
        instance = make_instance()
        self.storage.create_instance(instance)
        first = self.storage.claim_instance(instance.id, "w1", self.now, 30)

        later = self.now + timedelta(seconds=31)
        first_token = first.fencing_token
        second = self.storage.claim_instance(instance.id, "w2", later, 30)
        self.assertIsNotNone(second)
        self.assertEqual(second.owner, "w2")
        self.assertGreater(second.fencing_token, first_token)

        # The stale owner is fenced off everywhere.
        self.assertFalse(
            self.storage.renew_lease(
                instance.id, "w1", first.fencing_token, later, 30
            )
        )
        self.assertFalse(
            self.storage.complete_instance(
                instance.id, "w1", first.fencing_token
            )
        )
        # The new owner can complete.
        self.assertTrue(
            self.storage.complete_instance(
                instance.id, "w2", second.fencing_token
            )
        )
        self.assertEqual(
            self.storage.get_instance(instance.id).state,
            InstanceState.SUCCEEDED,
        )

    def test_heartbeat_renewal_extends_lease(self):
        instance = make_instance()
        self.storage.create_instance(instance)
        claimed = self.storage.claim_instance(instance.id, "w1", self.now, 30)
        renewed = self.storage.renew_lease(
            instance.id, "w1", claimed.fencing_token,
            self.now + timedelta(seconds=20), 30,
        )
        self.assertTrue(renewed)
        # Another worker cannot steal before the extended expiry.
        self.assertIsNone(
            self.storage.claim_instance(
                instance.id, "w2", self.now + timedelta(seconds=40), 30
            )
        )

    def test_list_claimable_includes_pending_and_expired(self):
        pending = make_instance(fire_time=self.now)
        running = JobInstance(
            job_id="j1",
            fire_time=self.now - timedelta(minutes=1),
            state=InstanceState.RUNNING,
            owner="w1",
            fencing_token=1,
            lease_expires_at=self.now - timedelta(seconds=1),
        )
        self.storage.create_instance(pending)
        self.storage.create_instance(running)
        claimable = self.storage.list_claimable(self.now)
        ids = {item.id for item in claimable}
        self.assertIn(pending.id, ids)
        self.assertIn(running.id, ids)

    def test_fail_requeues_with_backoff_then_fails_permanently(self):
        instance = make_instance()
        self.storage.create_instance(instance)
        claimed = self.storage.claim_instance(instance.id, "w1", self.now, 30)

        self.assertTrue(
            self.storage.fail_instance(
                instance.id, "w1", claimed.fencing_token, "boom",
                self.now, max_attempts=2, retry_backoff_seconds=10,
            )
        )
        reloaded = self.storage.get_instance(instance.id)
        self.assertEqual(reloaded.state, InstanceState.PENDING)
        self.assertEqual(reloaded.attempts, 1)
        self.assertEqual(reloaded.not_before, self.now + timedelta(seconds=10))
        # Not claimable before the backoff deadline.
        self.assertEqual(self.storage.list_claimable(self.now + timedelta(seconds=9)), [])

        second = self.storage.claim_instance(
            instance.id, "w2", self.now + timedelta(seconds=11), 30
        )
        self.assertIsNotNone(second)
        self.storage.fail_instance(
            instance.id, "w2", second.fencing_token, "boom again",
            self.now + timedelta(seconds=11), max_attempts=2,
            retry_backoff_seconds=10,
        )
        self.assertEqual(
            self.storage.get_instance(instance.id).state,
            InstanceState.FAILED,
        )
        self.assertEqual(
            self.storage.list_claimable(self.now + timedelta(hours=1)), []
        )

    def test_compare_and_set_planned(self):
        self.assertTrue(
            self.storage.compare_and_set_planned("j1", self.now, self.now + timedelta(minutes=1))
        )
        self.assertFalse(
            self.storage.compare_and_set_planned("j1", self.now, self.now + timedelta(minutes=2))
        )

    def test_leader_election_exclusive_and_expires(self):
        first = self.storage.acquire_leader("w1", self.now, ttl_seconds=15)
        self.assertIsNotNone(first)
        self.assertIsNone(self.storage.acquire_leader("w2", self.now, ttl_seconds=15))

        # Same holder can renew; another cannot renew with its token.
        self.assertTrue(
            self.storage.renew_leader("w1", first.fencing_token,
                                      self.now + timedelta(seconds=10), 15)
        )
        self.assertFalse(
            self.storage.renew_leader("w2", first.fencing_token,
                                      self.now + timedelta(seconds=10), 15)
        )

        # After TTL expiry a new holder takes over with a higher token.
        takeover = self.storage.acquire_leader(
            "w2", self.now + timedelta(seconds=26), ttl_seconds=15
        )
        self.assertIsNotNone(takeover)
        self.assertGreater(takeover.fencing_token, first.fencing_token)
        self.assertFalse(
            self.storage.renew_leader("w1", first.fencing_token,
                                      self.now + timedelta(seconds=26), 15)
        )
