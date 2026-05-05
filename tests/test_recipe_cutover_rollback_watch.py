"""Tests for the cutover_rollback_watch recipe."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from operator_core import cutover_streak as cs
from operator_core import outreach_audit
from operator_core.recipes import RecipeContext


def _load_module():
    repo_root = Path(__file__).resolve().parent.parent
    src = repo_root / "recipes" / "cutover_rollback_watch.py"
    spec = importlib.util.spec_from_file_location("operator_recipes_cutover_rollback_watch", src)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _ctx(**ov):
    base = dict(
        recipe_name="cutover_rollback_watch",
        correlation_id="t",
        env={}, clients={}, cost_so_far=0.0, cost_budget_usd=0.0, dry_run=False,
    )
    base.update(ov)
    return RecipeContext(**base)


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_CUTOVER_STREAK_DB", str(tmp_path / "streak.sqlite"))
    return tmp_path


def _ts(seconds_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat().replace("+00:00", "Z")


def _ready_summary(product: str):
    return outreach_audit.ProductSummary(product=product, total=100, match=99, would_block_new=1)


def _regressed_summary(product: str):
    return outreach_audit.ProductSummary(
        product=product, total=100, match=80, would_block_new=10, would_allow_new=10,
    )


def test_metadata():
    mod = _load_module()
    r = mod.CutoverRollbackWatch()
    assert r.name == "cutover_rollback_watch"
    assert r.schedule == "*/30 * * * *"


def test_no_promoted_products_means_quiet(isolated, monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [_ready_summary("oe")])
    r = mod.CutoverRollbackWatch()
    result = asyncio.run(r.query(_ctx()))
    assert result["watched"] == []
    assert result["alerts"] == []
    assert asyncio.run(r.format(_ctx(), result)) == ""


def test_promoted_product_outside_window_is_skipped(isolated, monkeypatch):
    cs.record_check("oe", True)
    cs.mark_promoted("oe", "https://x/pr/1", now_ts=_ts(60 * 60 * 60))  # 60h ago
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [_regressed_summary("oe")])
    mod = _load_module()
    r = mod.CutoverRollbackWatch()
    result = asyncio.run(r.query(_ctx()))
    assert result["watched"] == []  # filtered out
    assert result["alerts"] == []


def test_promoted_in_window_with_healthy_audit_is_quiet(isolated, monkeypatch):
    cs.record_check("oe", True)
    cs.mark_promoted("oe", "https://x/pr/1", now_ts=_ts(60 * 60 * 6))  # 6h ago
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [_ready_summary("oe")])
    mod = _load_module()
    r = mod.CutoverRollbackWatch()
    result = asyncio.run(r.query(_ctx()))
    assert len(result["watched"]) == 1
    assert result["watched"][0]["product"] == "oe"
    assert result["alerts"] == []
    assert asyncio.run(r.format(_ctx(), result)) == ""


def test_regression_fires_alert(isolated, monkeypatch):
    cs.record_check("oe", True)
    cs.mark_promoted("oe", "https://x/pr/1", now_ts=_ts(60 * 60 * 6))
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [_regressed_summary("oe")])
    mod = _load_module()
    r = mod.CutoverRollbackWatch()
    result = asyncio.run(r.query(_ctx(dry_run=True)))  # dry_run skips the events emit only
    assert len(result["alerts"]) == 1
    a = result["alerts"][0]
    assert a["product"] == "oe"
    assert any("match_pct" in reason for reason in a["reasons"])
    assert any("would_allow_new" in reason for reason in a["reasons"])
    msg = asyncio.run(r.format(_ctx(), result))
    assert ":rotating_light:" in msg
    assert "oe" in msg


def test_no_audit_data_after_promotion_alerts(isolated, monkeypatch):
    cs.record_check("oe", True)
    cs.mark_promoted("oe", "https://x/pr/1", now_ts=_ts(60 * 60 * 6))
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [])  # zero summaries
    mod = _load_module()
    r = mod.CutoverRollbackWatch()
    result = asyncio.run(r.query(_ctx(dry_run=True)))
    assert len(result["alerts"]) == 1
    assert "no_audit_data_post_promotion" in result["alerts"][0]["reasons"]


def test_emit_swallows_exception(isolated, monkeypatch):
    """Even if events_ndjson append blows up, the alert dict still surfaces."""
    cs.record_check("oe", True)
    cs.mark_promoted("oe", "https://x/pr/1", now_ts=_ts(60 * 60 * 6))
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [_regressed_summary("oe")])
    mod = _load_module()
    from operator_core._vendor import events_ndjson
    monkeypatch.setattr(events_ndjson, "append_event",
                         lambda **kw: (_ for _ in ()).throw(RuntimeError("boom")))
    r = mod.CutoverRollbackWatch()
    result = asyncio.run(r.query(_ctx()))  # not dry_run -> tries emit
    assert len(result["alerts"]) == 1  # alert still recorded despite emit failure
