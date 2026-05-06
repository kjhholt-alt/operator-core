from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from recipes import cost_tracker


NOW = datetime(2026, 5, 6, 12, 0, tzinfo=timezone.utc)


def test_cost_rollup_empty_stream() -> None:
    rollup = cost_tracker.build_rollup([], now=NOW)
    assert rollup["total_30d_usd"] == 0
    assert rollup["by_recipe"] == {}
    assert rollup["by_day"] == {}
    assert rollup["trends"]["week_over_week_pct"] == 0.0


def test_cost_rollup_mixed_shapes_and_models() -> None:
    events = [
        {
            "ts": "2026-05-06T10:00:00Z",
            "recipe": "morning_briefing",
            "project": "operator-core",
            "payload": {"cost_usd": 1.25, "model": "claude-sonnet"},
        },
        {
            "ts": "2026-05-05T10:00:00Z",
            "cost_usd": 2.0,
            "recipe": "portfolio_health",
            "model": "claude-haiku",
        },
        {
            "ts": "2026-05-04T10:00:00Z",
            "payload": {"amount_usd": 0.75, "recipe": "legacy_recipe"},
        },
    ]
    rollup = cost_tracker.build_rollup(events, now=NOW)
    assert rollup["total_30d_usd"] == pytest.approx(4.0)
    assert rollup["by_recipe"]["morning_briefing"] == pytest.approx(1.25)
    assert rollup["by_recipe"]["portfolio_health"] == pytest.approx(2.0)
    assert rollup["by_recipe"]["legacy_recipe"] == pytest.approx(0.75)
    assert rollup["by_model"]["claude-sonnet"] == pytest.approx(1.25)
    assert rollup["by_model"]["claude-haiku"] == pytest.approx(2.0)
    assert rollup["by_model"]["unknown"] == pytest.approx(0.75)


def test_cost_rollup_missing_optional_fields_bucket_to_unknown() -> None:
    events = [{"ts": "2026-05-06T10:00:00Z", "payload": {"cost_usd": 1.0}}]
    rollup = cost_tracker.build_rollup(events, now=NOW)
    assert rollup["by_recipe"]["unknown"] == 1.0
    assert rollup["by_project"]["unknown"] == 1.0
    assert rollup["by_model"]["unknown"] == 1.0


def test_cost_rollup_day_bucketing_uses_event_timezone() -> None:
    events = [
        {"ts": "2026-05-05T23:30:00-05:00", "recipe": "late", "cost_usd": 1.0},
        {"ts": "2026-05-06T01:00:00Z", "recipe": "utc", "cost_usd": 2.0},
    ]
    rollup = cost_tracker.build_rollup(events, now=NOW)
    assert rollup["by_day"]["2026-05-06"] == pytest.approx(3.0)


def test_cost_tracker_format_writes_portfolio_cost_json(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OPERATOR_PORTFOLIO_COST_PATH", str(tmp_path / "portfolio_cost.json"))
    result = cost_tracker.build_rollup(
        [{"ts": "2026-05-06T10:00:00Z", "recipe": "x", "payload": {"cost_usd": 1.5}}],
        now=NOW,
    )
    recipe = cost_tracker.CostTracker()
    ctx = MagicMock()
    message = asyncio.run(recipe.format(ctx, result))
    out = tmp_path / "portfolio_cost.json"
    assert out.exists()
    saved = json.loads(out.read_text(encoding="utf-8"))
    assert saved["by_recipe"]["x"] == 1.5
    assert len(message) < 2000

