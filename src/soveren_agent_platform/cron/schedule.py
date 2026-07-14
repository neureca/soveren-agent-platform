"""Validation and recurrence calculations for cron schedules."""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dateutil.rrule import rrulestr


def validate_schedule(*, run_at: int, rrule_body: str | None, timezone: str) -> None:
    if not isinstance(run_at, int) or isinstance(run_at, bool):
        raise TypeError("run_at must be an integer Unix timestamp")
    try:
        tz = ZoneInfo(timezone)
        anchor = datetime.fromtimestamp(run_at, tz)
    except (OSError, OverflowError, TypeError, ValueError, ZoneInfoNotFoundError) as exc:
        raise ValueError(f"invalid timezone or run_at: {timezone!r}") from exc
    if rrule_body is None:
        return
    if not rrule_body.strip():
        raise ValueError("rrule must be non-empty when provided")
    try:
        rrulestr(rrule_body, dtstart=anchor).after(anchor - timedelta(seconds=1))
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid rrule") from exc


def next_run_at(
    schedule_anchor_at: int,
    rrule_body: str | None,
    timezone: str,
    fired_at: int,
) -> int | None:
    if not rrule_body:
        return None
    tz = ZoneInfo(timezone)
    anchor = datetime.fromtimestamp(schedule_anchor_at, tz)
    after = datetime.fromtimestamp(fired_at, tz)
    next_dt = rrulestr(rrule_body, dtstart=anchor).after(after)
    return int(next_dt.timestamp()) if next_dt is not None else None
