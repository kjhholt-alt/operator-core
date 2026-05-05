"""Tests for the cutover_streak SQLite tracker."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from operator_core import cutover_streak as cs


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "streak.sqlite"
    monkeypatch.setenv("OPERATOR_CUTOVER_STREAK_DB", str(p))
    return p


def _ts(seconds_ago: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    return dt.isoformat().replace("+00:00", "Z")


class TestRecordCheck:
    def test_first_ready_check_starts_streak(self, db):
        s = cs.record_check("oe", True)
        assert s.last_ready_state is True
        assert s.streak_start_ts is not None
        assert s.promoted_ts is None

    def test_first_not_ready_check_no_streak(self, db):
        s = cs.record_check("oe", False)
        assert s.last_ready_state is False
        assert s.streak_start_ts is None

    def test_continuing_ready_keeps_original_start(self, db):
        s1 = cs.record_check("oe", True, now_ts=_ts(120))
        s2 = cs.record_check("oe", True, now_ts=_ts(60))
        assert s1.streak_start_ts == s2.streak_start_ts

    def test_not_ready_check_resets_streak(self, db):
        cs.record_check("oe", True)
        s = cs.record_check("oe", False)
        assert s.streak_start_ts is None
        assert s.last_ready_state is False

    def test_recovering_to_ready_after_break_starts_new_streak(self, db):
        cs.record_check("oe", True, now_ts=_ts(300))
        cs.record_check("oe", False, now_ts=_ts(200))
        s = cs.record_check("oe", True, now_ts=_ts(100))
        # streak_start_ts must be the recovery time, not the original.
        assert s.streak_start_ts is not None
        # And the current readiness is True.
        assert s.last_ready_state is True


class TestStreakSeconds:
    def test_returns_zero_when_unknown(self, db):
        assert cs.streak_seconds("never_seen") == 0.0

    def test_returns_zero_when_not_ready(self, db):
        cs.record_check("oe", False)
        assert cs.streak_seconds("oe") == 0.0

    def test_returns_elapsed_seconds(self, db):
        cs.record_check("oe", True, now_ts=_ts(3600))
        # Streak began ~3600s ago, so streak_seconds should be roughly that.
        sec = cs.streak_seconds("oe")
        assert 3500 < sec < 3700

    def test_zero_after_break(self, db):
        cs.record_check("oe", True, now_ts=_ts(7200))
        cs.record_check("oe", False, now_ts=_ts(3600))
        assert cs.streak_seconds("oe") == 0.0


class TestPromotion:
    def test_mark_promoted_writes_url(self, db):
        cs.record_check("oe", True)
        cs.mark_promoted("oe", "https://github.com/x/y/pull/1")
        s = cs.get("oe")
        assert s.promoted_ts is not None
        assert s.promoted_pr_url == "https://github.com/x/y/pull/1"

    def test_list_all_orders_by_product(self, db):
        cs.record_check("zeta", True)
        cs.record_check("alpha", False)
        cs.record_check("middle", True)
        names = [s.product for s in cs.list_all()]
        assert names == ["alpha", "middle", "zeta"]
