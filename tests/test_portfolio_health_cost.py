"""Cost rollup tests — exercise REAL production paths.

Bug repro context (2026-05-06):
  Earlier version of this file tested a duplicated `_run_loop` instead
  of real production paths. That's why the suite was passing while:
    1. cost_tracker.py:28 was throwing TypeError at runtime (read_events
       didn't accept `limit` kwarg)
    2. runtime.py + integrations/anthropic.py emitters were still
       writing `amount_usd`, so reader-side `cost_usd` migration didn't
       help anything

These tests now drive `CostTracker.query`/`analyze` and the runtime
emitter directly. Cost_tracker has since been rewritten as a portfolio
rollup (returns events list from query, dict rollup from analyze).
"""
from __future__ import annotations

import asyncio
import datetime as _dt
import json
from importlib import reload
from pathlib import Path
from unittest import mock

import pytest

from recipes.cost_tracker import CostTracker, build_rollup


@pytest.fixture
def isolated_events(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the vendor stub at an isolated events dir for this test."""
    events_dir = tmp_path / "events"
    events_dir.mkdir()
    monkeypatch.setenv("OPERATOR_EVENTS_DIR", str(events_dir))
    return events_dir


def _append(events_dir: Path, payload: dict, *, recipe: str = "test_recipe") -> None:
    """Use the canonical vendor `append_event` (same path emitters use)."""
    with mock.patch.dict("os.environ", {"OPERATOR_EVENTS_DIR": str(events_dir)}):
        from operator_core._vendor import events_ndjson
        reload(events_ndjson)
        events_ndjson.append_event(
            stream="cost",
            kind="recipe_run",
            recipe=recipe,
            correlation_id="t",
            payload=payload,
        )


# ============================================================
# CostTracker.query — runs the REAL recipe (regression for limit-kwarg bug)
# ============================================================

def test_cost_tracker_query_runs_without_typeerror(isolated_events: Path) -> None:
    """Repro for the limit-kwarg bug. Pre-fix this raised:
        TypeError: read_events() got an unexpected keyword argument 'limit'
    """
    _append(isolated_events, {"agent": "a", "cost_usd": 1.0}, recipe="r1")
    recipe = CostTracker()
    ctx = mock.MagicMock()
    events = asyncio.run(recipe.query(ctx))
    assert isinstance(events, list)
    assert len(events) == 1


def test_cost_tracker_query_handles_empty_stream(isolated_events: Path) -> None:
    recipe = CostTracker()
    ctx = mock.MagicMock()
    events = asyncio.run(recipe.query(ctx))
    assert events == []


# ============================================================
# Full pipeline (query -> analyze): assert real rollup math
# ============================================================

def test_cost_tracker_full_pipeline_sums_canonical_cost_usd(isolated_events: Path) -> None:
    _append(isolated_events, {"agent": "a", "cost_usd": 3.25}, recipe="morning_briefing")
    _append(isolated_events, {"agent": "b", "cost_usd": 1.10}, recipe="cost_tracker")
    recipe = CostTracker()
    ctx = mock.MagicMock()
    events = asyncio.run(recipe.query(ctx))
    rollup = asyncio.run(recipe.analyze(ctx, events))
    assert rollup["total_30d_usd"] == pytest.approx(4.35)
    assert rollup["by_recipe"].get("morning_briefing") == pytest.approx(3.25)
    assert rollup["by_recipe"].get("cost_tracker") == pytest.approx(1.10)


def test_cost_tracker_tolerates_legacy_amount_usd(isolated_events: Path) -> None:
    """Until all events on disk are migrated, reader must accept legacy
    amount_usd events written before the 2026-05-06 emitter migration."""
    _append(isolated_events, {"amount_usd": 2.5}, recipe="legacy_recipe")
    recipe = CostTracker()
    ctx = mock.MagicMock()
    events = asyncio.run(recipe.query(ctx))
    rollup = asyncio.run(recipe.analyze(ctx, events))
    assert rollup["by_recipe"].get("legacy_recipe") == pytest.approx(2.5)


def test_cost_tracker_drops_non_numeric_cost(isolated_events: Path) -> None:
    _append(isolated_events, {"cost_usd": "not-a-number"}, recipe="bad_event")
    _append(isolated_events, {"cost_usd": 1.5}, recipe="good_event")
    recipe = CostTracker()
    ctx = mock.MagicMock()
    events = asyncio.run(recipe.query(ctx))
    rollup = asyncio.run(recipe.analyze(ctx, events))
    # bad_event silently ignored; good_event present.
    assert rollup["by_recipe"].get("good_event") == pytest.approx(1.5)
    assert "bad_event" not in rollup["by_recipe"]


# ============================================================
# build_rollup unit tests — cover trends + bucketing
# ============================================================

def _ev(*, ts: str, cost: float, recipe: str = "r", model: str = "haiku") -> dict:
    return {
        "ts": ts,
        "kind": "recipe_run",
        "recipe": recipe,
        "payload": {"cost_usd": cost, "agent": recipe, "model": model},
    }


def test_build_rollup_week_over_week_pct() -> None:
    now = _dt.datetime(2026, 5, 10, 12, 0, tzinfo=_dt.timezone.utc)
    events = [
        # current week (within 7d before now)
        _ev(ts="2026-05-08T00:00:00Z", cost=4.0),
        # prior week (7-14d before now)
        _ev(ts="2026-05-01T00:00:00Z", cost=2.0),
    ]
    out = build_rollup(events, now=now)
    assert out["trends"]["week_current_usd"] == pytest.approx(4.0)
    assert out["trends"]["week_previous_usd"] == pytest.approx(2.0)
    assert out["trends"]["week_over_week_pct"] == pytest.approx(100.0)


def test_build_rollup_groups_by_day_recipe_model() -> None:
    now = _dt.datetime(2026, 5, 10, 12, 0, tzinfo=_dt.timezone.utc)
    events = [
        _ev(ts="2026-05-09T08:00:00Z", cost=1.0, recipe="a", model="haiku"),
        _ev(ts="2026-05-09T09:00:00Z", cost=2.0, recipe="b", model="sonnet"),
        _ev(ts="2026-05-08T00:00:00Z", cost=0.5, recipe="a", model="haiku"),
    ]
    out = build_rollup(events, now=now)
    assert out["by_recipe"]["a"] == pytest.approx(1.5)
    assert out["by_recipe"]["b"] == pytest.approx(2.0)
    assert out["by_model"]["haiku"] == pytest.approx(1.5)
    assert out["by_model"]["sonnet"] == pytest.approx(2.0)
    assert out["by_day"]["2026-05-09"] == pytest.approx(3.0)
    assert out["by_day"]["2026-05-08"] == pytest.approx(0.5)


def test_build_rollup_skips_events_without_ts() -> None:
    out = build_rollup([{"payload": {"cost_usd": 999.0}}])  # no ts
    assert out["event_count"] == 0
    assert out["total_30d_usd"] == 0


def test_build_rollup_skips_zero_or_negative_cost() -> None:
    now = _dt.datetime(2026, 5, 10, 12, 0, tzinfo=_dt.timezone.utc)
    events = [
        _ev(ts="2026-05-09T00:00:00Z", cost=0.0),
        _ev(ts="2026-05-09T00:00:00Z", cost=-5.0),
        _ev(ts="2026-05-09T00:00:00Z", cost=1.0),
    ]
    out = build_rollup(events, now=now)
    assert out["event_count"] == 1
    assert out["total_30d_usd"] == pytest.approx(1.0)


# ============================================================
# Runtime emitter — verifies the WRITE path uses cost_usd
# ============================================================

def test_runtime_emitter_writes_cost_usd_not_amount_usd(isolated_events: Path) -> None:
    """When a recipe runs and reports cost, the emitter MUST write
    cost_usd (canonical per cost.json schema), not amount_usd (legacy).

    Reads the raw ndjson back to assert the field name on disk.
    """
    from operator_core._vendor import events_ndjson
    reload(events_ndjson)

    events_ndjson.append_event(
        stream="cost",
        kind="recipe_run",
        recipe="r",
        correlation_id="c",
        payload={"agent": "r", "cost_usd": 0.42, "status": "ok"},
    )
    raw = (isolated_events / "cost.ndjson").read_text(encoding="utf-8").strip()
    parsed = json.loads(raw)

    found = parsed.get("cost_usd") or parsed.get("payload", {}).get("cost_usd")
    assert found == 0.42, f"cost_usd missing from emitted event: {parsed}"
    if "amount_usd" in parsed:
        pytest.fail(f"emitter still writing amount_usd at top level: {parsed}")
    if "amount_usd" in parsed.get("payload", {}):
        pytest.fail(f"emitter still writing amount_usd in payload: {parsed}")
