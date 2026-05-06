"""Schedule ordering invariants.

Bug repro context (2026-05-06):
  morning_briefing was at 07:00 reading war-room/portfolio-health.ir.json,
  but portfolio_health was scheduled an hour LATER at 08:00. So the 7am
  briefing always rendered yesterday's portfolio-health IR.

Fix: portfolio_health must run STRICTLY BEFORE morning_briefing every
day. This test enforces that invariant against the schedule.yaml file
so the regression can't return silently.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from operator_core.recipes.schedule import load_schedule

SCHEDULE_PATH = Path(__file__).resolve().parent.parent / "schedules" / "schedule.yaml"


def _cron_to_minute_of_day(expr: str) -> int:
    """Convert daily cron `M H * * *` to minutes since midnight.

    Returns -1 for non-daily schedules (we only enforce ordering on
    daily ones — interval recipes like */15 are unrelated).
    """
    parts = expr.split()
    if len(parts) != 5:
        return -1
    m, h, dom, mo, dow = parts
    # Skip if anything but daily-everywhere
    if dom != "*" or mo != "*" or dow != "*":
        return -1
    if "/" in m or "/" in h or "," in m or "," in h or "-" in m or "-" in h:
        return -1
    try:
        return int(h) * 60 + int(m)
    except ValueError:
        return -1


@pytest.fixture
def schedule():
    """Use operator-core's own loader so we exercise the parser the
    production scheduler uses (custom line-by-line, not PyYAML)."""
    return load_schedule(SCHEDULE_PATH)


def test_portfolio_health_runs_before_morning_briefing(schedule) -> None:
    mb = schedule.find("morning_briefing")
    ph = schedule.find("portfolio_health")
    assert mb is not None, "morning_briefing missing from schedule"
    assert ph is not None, "portfolio_health missing from schedule"

    mb_minute = _cron_to_minute_of_day(mb.cron)
    ph_minute = _cron_to_minute_of_day(ph.cron)

    assert mb_minute >= 0, f"morning_briefing cron {mb.cron!r} is not a simple daily schedule"
    assert ph_minute >= 0, f"portfolio_health cron {ph.cron!r} is not a simple daily schedule"

    # Strict less-than: portfolio_health must finish before briefing fires.
    assert ph_minute < mb_minute, (
        f"portfolio_health (cron={ph.cron}, minute_of_day={ph_minute}) "
        f"MUST run before morning_briefing (cron={mb.cron}, "
        f"minute_of_day={mb_minute}). The briefing reads "
        f"war-room/portfolio-health.ir.json -- if portfolio_health hasn't "
        f"run yet today, the briefing renders yesterday's IR."
    )

    # Sanity: at least 5 minutes of headroom so portfolio_health has time
    # to actually finish before the briefing reads its output.
    headroom = mb_minute - ph_minute
    assert headroom >= 5, (
        f"portfolio_health finishes only {headroom}m before morning_briefing; "
        f"need >= 5m headroom for the IR file to be written"
    )
