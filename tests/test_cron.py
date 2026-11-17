from datetime import datetime, timedelta, timezone
from unittest import TestCase

from distributed_cron.cron import CronError, CronExpression

UTC = timezone.utc


class CronExpressionTest(TestCase):
    def test_basic_next_after(self):
        cron = CronExpression("*/15 * * * *")
        moment = datetime(2026, 11, 17, 10, 7, tzinfo=UTC)
        self.assertEqual(
            cron.next_after(moment),
            datetime(2026, 11, 17, 10, 15, tzinfo=UTC),
        )

    def test_inclusive_match(self):
        cron = CronExpression("0 9 * * *")
        moment = datetime(2026, 11, 17, 9, 0, tzinfo=UTC)
        self.assertEqual(cron.next_after(moment, inclusive=True), moment)
        self.assertEqual(
            cron.next_after(moment),
            datetime(2026, 11, 18, 9, 0, tzinfo=UTC),
        )

    def test_weekday_ranges_and_names(self):
        cron = CronExpression("0 0 * * MON-FRI")
        # 2026-11-17 is a Tuesday; next midnight is Wednesday the 18th.
        tuesday = datetime(2026, 11, 17, 12, 0, tzinfo=UTC)
        nxt = cron.next_after(tuesday)
        self.assertEqual(nxt.weekday(), 2)
        self.assertEqual(nxt.day, 18)
        # From Saturday afternoon the next weekday firing is Monday the 23rd.
        saturday = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
        monday = cron.next_after(saturday)
        self.assertEqual(monday.weekday(), 0)
        self.assertEqual(monday.day, 23)

    def test_monthly_and_month_names(self):
        cron = CronExpression("0 0 1 JAN,JUL *")
        start = datetime(2026, 2, 1, tzinfo=UTC)
        self.assertEqual(cron.next_after(start), datetime(2026, 7, 1, tzinfo=UTC))

    def test_dom_dow_or_semantics(self):
        # 1st of month OR Monday.
        cron = CronExpression("0 0 1 * 1")
        first = datetime(2026, 11, 1, 0, 0, tzinfo=UTC)
        self.assertTrue(cron.matches(first))
        monday = datetime(2026, 11, 2, 0, 0, tzinfo=UTC)
        self.assertTrue(cron.matches(monday))
        tuesday = datetime(2026, 11, 3, 0, 0, tzinfo=UTC)
        self.assertFalse(cron.matches(tuesday))

    def test_macros(self):
        self.assertEqual(
            CronExpression("@hourly").expression,
            "0 * * * *",
        )
        cron = CronExpression("@daily")
        start = datetime(2026, 11, 17, 8, 0, tzinfo=UTC)
        self.assertEqual(
            cron.next_after(start),
            datetime(2026, 11, 18, 0, 0, tzinfo=UTC),
        )

    def test_sunday_seven_equals_zero(self):
        cron = CronExpression("0 0 * * 7")
        saturday = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
        nxt = cron.next_after(saturday)
        self.assertEqual(nxt.weekday(), 6)

    def test_step_and_list(self):
        cron = CronExpression("0,30 6,18 * * *")
        moment = datetime(2026, 11, 17, 7, 0, tzinfo=UTC)
        self.assertEqual(
            cron.next_after(moment),
            datetime(2026, 11, 17, 18, 0, tzinfo=UTC),
        )

    def test_invalid_expressions(self):
        for bad in ("* * *", "61 * * * *", "* * 32 * *", "* * * * 9", "a b c d e"):
            with self.assertRaises(CronError):
                CronExpression(bad)

    def test_requires_aware_datetime(self):
        with self.assertRaises(CronError):
            CronExpression("* * * * *").next_after(datetime(2026, 1, 1))

    def test_iterate_whole_hour(self):
        cron = CronExpression("* * * * *")
        start = datetime(2026, 11, 17, 10, 0, tzinfo=UTC)
        nxt = start
        for _ in range(60):
            nxt = cron.next_after(nxt)
        self.assertEqual(nxt, start + timedelta(hours=1))
