"""Minimal dependency-free cron expression parser.

Supported syntax (five fields, whitespace separated):

    minute        0-59
    hour          0-23
    day of month  1-31
    month         1-12 (or JAN-DEC)
    day of week   0-7  (0 and 7 == Sunday, or SUN-SAT)

Each field supports ``*``, ``,`` lists, ``-`` ranges and ``/`` steps.
Macros ``@every_minute``, ``@hourly``, ``@daily``, ``@weekly``,
``@monthly`` and ``@yearly`` are accepted.

Day-of-month and day-of-week follow Vixie-cron semantics: if both are
restricted (not ``*``), a day matching *either* field triggers.
"""

from __future__ import annotations

from datetime import datetime, timedelta

_FIELD_RANGES = (
    (0, 59),   # minute
    (0, 23),   # hour
    (1, 31),   # day of month
    (1, 12),   # month
    (0, 6),    # day of week (normalised: Sunday == 0)
)

_MONTH_NAMES = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_WEEKDAY_NAMES = {
    "SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
}

_MACROS = {
    "@every_minute": "* * * * *",
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}

# Safety bound for forward iteration: roughly six years of minutes.
_MAX_MINUTES = 6 * 366 * 24 * 60


class CronError(ValueError):
    pass


def _resolve_token(token: str, names: dict[str, int]) -> int:
    upper = token.upper()
    if upper in names:
        return names[upper]
    if not token.lstrip("-").isdigit():
        raise CronError(f"invalid cron token {token!r}")
    return int(token)


def _parse_field(spec: str, lo: int, hi: int, names: dict[str, int] | None) -> set[int]:
    values: set[int] = set()
    for part in spec.split(","):
        if not part:
            raise CronError(f"empty cron field component in {spec!r}")
        step = 1
        if "/" in part:
            range_part, step_part = part.split("/", 1)
            if not step_part.isdigit() or int(step_part) <= 0:
                raise CronError(f"invalid step {step_part!r}")
            step = int(step_part)
        else:
            range_part = part

        if range_part == "*" or range_part == "?":
            start, end = lo, hi
        elif "-" in range_part:
            start_tok, end_tok = range_part.split("-", 1)
            start = _resolve_token(start_tok, names or {})
            end = _resolve_token(end_tok, names or {})
        else:
            start = _resolve_token(range_part, names or {})
            end = hi if "/" in part else start

        if not (lo <= start <= hi) or not (lo <= end <= hi) or start > end:
            raise CronError(
                f"cron value out of range [{lo},{hi}] in {part!r}"
            )
        values.update(range(start, end + 1, step))
    return values


class CronExpression:
    def __init__(self, expression: str):
        expression = expression.strip()
        expression = _MACROS.get(expression, expression)
        fields = expression.split()
        if len(fields) != 5:
            raise CronError(
                f"cron expression must have 5 fields, got {len(fields)}"
            )

        minute_spec, hour_spec, dom_spec, month_spec, dow_spec = fields
        self.minutes = _parse_field(minute_spec, 0, 59, None)
        self.hours = _parse_field(hour_spec, 0, 23, None)
        self.days_of_month = _parse_field(dom_spec, 1, 31, None)
        self.months = _parse_field(month_spec, 1, 12, _MONTH_NAMES)

        dow_raw = _parse_field(dow_spec, 0, 7, _WEEKDAY_NAMES)
        # 7 means Sunday just like 0.
        if 7 in dow_raw:
            dow_raw.discard(7)
            dow_raw.add(0)
        self.days_of_week = dow_raw

        self.dom_restricted = dom_spec not in ("*", "?")
        self.dow_restricted = dow_spec not in ("*", "?")
        self.expression = " ".join(fields)

    def matches(self, moment: datetime) -> bool:
        if moment.minute not in self.minutes:
            return False
        if moment.hour not in self.hours:
            return False
        if moment.month not in self.months:
            return False
        if self.dom_restricted and self.dow_restricted:
            # Vixie cron: OR semantics when both are restricted.
            day_ok = moment.day in self.days_of_month
            week_ok = (moment.weekday() + 1) % 7 in self.days_of_week
            return day_ok or week_ok
        if self.dom_restricted:
            return moment.day in self.days_of_month
        if self.dow_restricted:
            return (moment.weekday() + 1) % 7 in self.days_of_week
        return True

    def next_after(self, after: datetime, inclusive: bool = False) -> datetime:
        """Return the first matching minute strictly after ``after``.

        The result is timezone-aware: arithmetic is performed by shifting the
        tzinfo-aware datetime minute by minute so DST transitions are handled
        by the underlying timezone database.
        """
        if after.tzinfo is None:
            raise CronError("next_after requires a timezone-aware datetime")
        candidate = after.replace(second=0, microsecond=0)
        if inclusive and self.matches(candidate):
            return candidate
        candidate = candidate + timedelta(minutes=1)
        for _ in range(_MAX_MINUTES):
            if self.matches(candidate):
                return candidate
            candidate += timedelta(minutes=1)
        raise CronError(
            f"no matching time within {_MAX_MINUTES} minutes for {self.expression!r}"
        )

    def __eq__(self, other: object) -> bool:
        return isinstance(other, CronExpression) and other.expression == self.expression

    def __repr__(self) -> str:
        return f"CronExpression({self.expression!r})"
