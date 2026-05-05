"""Unit tests for the cut_over_promoter recipe."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from operator_core import cutover_streak as cs
from operator_core import outreach_audit
from operator_core.recipes import RecipeContext


def _load_recipe_module():
    repo_root = Path(__file__).resolve().parent.parent
    src = repo_root / "recipes" / "cut_over_promoter.py"
    spec = importlib.util.spec_from_file_location("operator_recipes_cut_over_promoter", src)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _ctx(**overrides):
    base = dict(
        recipe_name="cut_over_promoter",
        correlation_id="test-corr",
        env={}, clients={}, cost_so_far=0.0,
        cost_budget_usd=0.0, dry_run=False,
    )
    base.update(overrides)
    return RecipeContext(**base)


@pytest.fixture
def isolated_state(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_CUTOVER_STREAK_DB", str(tmp_path / "streak.sqlite"))
    cfg_path = tmp_path / "targets.json"
    monkeypatch.setenv("OPERATOR_CUTOVER_PROMOTER_CONFIG", str(cfg_path))
    return tmp_path, cfg_path


def _write_targets(cfg_path: Path, mapping: dict) -> None:
    cfg_path.write_text(json.dumps(mapping), encoding="utf-8")


def _ready_summary(product: str, total: int = 100, match: int = 100):
    s = outreach_audit.ProductSummary(product=product, total=total, match=match)
    return s


def _not_ready_summary(product: str, total: int = 100, match: int = 80):
    return outreach_audit.ProductSummary(
        product=product, total=total, match=match, would_block_new=10, would_allow_new=10,
    )


# ------- metadata + disabled paths --------------------------------------------

def test_metadata():
    mod = _load_recipe_module()
    r = mod.CutOverPromoter()
    assert r.name == "cut_over_promoter"
    assert r.schedule == "0 * * * *"


def test_quiet_when_disabled(isolated_state, monkeypatch):
    mod = _load_recipe_module()
    monkeypatch.delenv("OPERATOR_CUTOVER_PROMOTER", raising=False)
    r = mod.CutOverPromoter()
    result = asyncio.run(r.query(_ctx()))
    assert result["enabled"] is False


def test_no_targets_returns_error_row(isolated_state, monkeypatch):
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_CUTOVER_PROMOTER", "1")
    r = mod.CutOverPromoter()
    result = asyncio.run(r.query(_ctx()))
    assert result["enabled"] is True
    assert result["promoted"] == []
    assert any("no targets" in e for e in result["errors"])


# ------- streak math through the recipe --------------------------------------

def test_not_ready_does_not_promote(isolated_state, monkeypatch):
    _, cfg_path = isolated_state
    _write_targets(cfg_path, {
        "oe": {
            "repo": "kjhholt-alt/outreach-engine",
            "config_path": ".env.production",
            "audit_only_pattern": "AUDIT_ONLY=true",
            "audit_only_replacement": "AUDIT_ONLY=false",
            "route_pattern": "ROUTE=false",
            "route_replacement": "ROUTE=true",
        },
    })
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_CUTOVER_PROMOTER", "1")
    monkeypatch.setattr(outreach_audit, "_iter_events", lambda paths: [])
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [_not_ready_summary("oe")])

    r = mod.CutOverPromoter()
    result = asyncio.run(r.query(_ctx()))
    assert result["promoted"] == []
    assert result["checked"][0]["ready_now"] is False


def test_ready_but_short_streak_does_not_promote(isolated_state, monkeypatch):
    _, cfg_path = isolated_state
    _write_targets(cfg_path, {
        "oe": {
            "repo": "kjhholt-alt/outreach-engine",
            "config_path": ".env.production",
            "audit_only_pattern": "AUDIT_ONLY=true",
            "audit_only_replacement": "AUDIT_ONLY=false",
            "route_pattern": "ROUTE=false",
            "route_replacement": "ROUTE=true",
        },
    })
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_CUTOVER_PROMOTER", "1")
    monkeypatch.setenv("OPERATOR_CUTOVER_STREAK_SECONDS", "86400")  # 24h
    monkeypatch.setattr(outreach_audit, "_iter_events", lambda paths: [])
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [_ready_summary("oe")])

    r = mod.CutOverPromoter()
    result = asyncio.run(r.query(_ctx()))
    assert result["promoted"] == []
    assert result["checked"][0]["ready_now"] is True
    assert result["checked"][0]["streak_seconds"] < 86400


def test_long_streak_dry_run_records_intent_without_pr(isolated_state, monkeypatch):
    _, cfg_path = isolated_state
    _write_targets(cfg_path, {
        "oe": {
            "repo": "kjhholt-alt/outreach-engine",
            "config_path": ".env.production",
            "audit_only_pattern": "AUDIT_ONLY=true",
            "audit_only_replacement": "AUDIT_ONLY=false",
            "route_pattern": "ROUTE=false",
            "route_replacement": "ROUTE=true",
        },
    })
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_CUTOVER_PROMOTER", "1")
    monkeypatch.setenv("OPERATOR_CUTOVER_STREAK_SECONDS", "10")  # 10s for testability

    # Seed a streak that's already older than the threshold by setting
    # the streak DB row directly via record_check + manual override.
    long_ago = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat().replace("+00:00", "Z")
    cs.record_check("oe", True, now_ts=long_ago)

    monkeypatch.setattr(outreach_audit, "_iter_events", lambda paths: [])
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [_ready_summary("oe")])

    r = mod.CutOverPromoter()
    result = asyncio.run(r.query(_ctx(dry_run=True)))
    assert len(result["promoted"]) == 1
    assert result["promoted"][0]["dry_run"] is True
    # Not actually marked promoted in the DB during dry-run.
    assert cs.get("oe").promoted_ts is None


def test_long_streak_real_run_calls_open_pr_and_marks_promoted(isolated_state, monkeypatch):
    _, cfg_path = isolated_state
    _write_targets(cfg_path, {
        "oe": {
            "repo": "kjhholt-alt/outreach-engine",
            "config_path": ".env.production",
            "audit_only_pattern": "AUDIT_ONLY=true",
            "audit_only_replacement": "AUDIT_ONLY=false",
            "route_pattern": "ROUTE=false",
            "route_replacement": "ROUTE=true",
        },
    })
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_CUTOVER_PROMOTER", "1")
    monkeypatch.setenv("OPERATOR_CUTOVER_STREAK_SECONDS", "10")
    monkeypatch.setenv("GITHUB_TOKEN", "fake-token")

    long_ago = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat().replace("+00:00", "Z")
    cs.record_check("oe", True, now_ts=long_ago)

    monkeypatch.setattr(outreach_audit, "_iter_events", lambda paths: [])
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [_ready_summary("oe")])

    captured = {}
    def fake_open_pr(token, product, cfg):
        captured["token"] = token
        captured["product"] = product
        captured["cfg"] = cfg
        return "https://github.com/x/y/pull/42"
    monkeypatch.setattr(mod, "_open_flip_pr", fake_open_pr)

    r = mod.CutOverPromoter()
    result = asyncio.run(r.query(_ctx()))
    assert len(result["promoted"]) == 1
    assert result["promoted"][0]["pr_url"] == "https://github.com/x/y/pull/42"
    assert captured["product"] == "oe"
    assert cs.get("oe").promoted_ts is not None
    assert cs.get("oe").promoted_pr_url == "https://github.com/x/y/pull/42"


def test_already_promoted_is_skipped(isolated_state, monkeypatch):
    _, cfg_path = isolated_state
    _write_targets(cfg_path, {
        "oe": {
            "repo": "kjhholt-alt/outreach-engine",
            "config_path": ".env.production",
            "audit_only_pattern": "AUDIT_ONLY=true",
            "audit_only_replacement": "AUDIT_ONLY=false",
            "route_pattern": "ROUTE=false",
            "route_replacement": "ROUTE=true",
        },
    })
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_CUTOVER_PROMOTER", "1")
    monkeypatch.setenv("OPERATOR_CUTOVER_STREAK_SECONDS", "10")
    monkeypatch.setenv("GITHUB_TOKEN", "fake-token")

    long_ago = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat().replace("+00:00", "Z")
    cs.record_check("oe", True, now_ts=long_ago)
    cs.mark_promoted("oe", "https://github.com/x/y/pull/1")  # already done

    monkeypatch.setattr(outreach_audit, "_iter_events", lambda paths: [])
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [_ready_summary("oe")])
    monkeypatch.setattr(mod, "_open_flip_pr",
                         lambda *a, **kw: pytest.fail("must not re-open PR for promoted product"))

    r = mod.CutOverPromoter()
    result = asyncio.run(r.query(_ctx()))
    assert result["promoted"] == []
    assert result["checked"][0]["already_promoted"] is True


def test_promoter_error_surfaced_no_db_write(isolated_state, monkeypatch):
    _, cfg_path = isolated_state
    _write_targets(cfg_path, {
        "oe": {
            "repo": "kjhholt-alt/outreach-engine",
            "config_path": ".env.production",
            "audit_only_pattern": "AUDIT_ONLY=true",
            "audit_only_replacement": "AUDIT_ONLY=false",
            "route_pattern": "ROUTE=false",
            "route_replacement": "ROUTE=true",
        },
    })
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_CUTOVER_PROMOTER", "1")
    monkeypatch.setenv("OPERATOR_CUTOVER_STREAK_SECONDS", "10")
    monkeypatch.setenv("GITHUB_TOKEN", "fake-token")

    long_ago = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat().replace("+00:00", "Z")
    cs.record_check("oe", True, now_ts=long_ago)

    monkeypatch.setattr(outreach_audit, "_iter_events", lambda paths: [])
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [_ready_summary("oe")])

    def boom(*a, **kw):
        raise mod._PromoterError("HTTP 422: validation failed")
    monkeypatch.setattr(mod, "_open_flip_pr", boom)

    r = mod.CutOverPromoter()
    result = asyncio.run(r.query(_ctx()))
    assert result["promoted"] == []
    assert any("HTTP 422" in e for e in result["errors"])
    # Importantly, the streak row was NOT marked promoted -- a future tick can retry.
    assert cs.get("oe").promoted_ts is None


def test_format_emits_when_promoted(isolated_state):
    mod = _load_recipe_module()
    r = mod.CutOverPromoter()
    msg = asyncio.run(r.format(_ctx(), {
        "enabled": True,
        "checked": [], "errors": [],
        "promoted": [{"product": "oe", "streak_seconds": 86400, "pr_url": "https://x/pr/1"}],
    }))
    assert "cut_over_promoter" in msg
    assert "oe" in msg
    assert "https://x/pr/1" in msg


def test_format_quiet_on_steady_state(isolated_state):
    mod = _load_recipe_module()
    r = mod.CutOverPromoter()
    msg = asyncio.run(r.format(_ctx(), {
        "enabled": True, "checked": [], "promoted": [], "errors": [],
    }))
    assert msg == ""
