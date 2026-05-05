"""Unit tests for the gate_audit_ingest recipe."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest

from operator_core.recipes import RecipeContext


def _load_recipe_module():
    # Recipes live under <repo>/recipes/, not under the package; load it directly.
    repo_root = Path(__file__).resolve().parent.parent
    src = repo_root / "recipes" / "gate_audit_ingest.py"
    spec = importlib.util.spec_from_file_location("operator_recipes_gate_audit_ingest", src)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _ctx(**overrides):
    base = dict(
        recipe_name="gate_audit_ingest",
        correlation_id="test-corr",
        env={},
        clients={},
        cost_so_far=0.0,
        cost_budget_usd=0.0,
        dry_run=False,
    )
    base.update(overrides)
    return RecipeContext(**base)


def test_recipe_metadata():
    mod = _load_recipe_module()
    recipe = mod.GateAuditIngest()
    assert recipe.name == "gate_audit_ingest"
    assert recipe.schedule == "*/10 * * * *"
    assert "outreach" in recipe.tags
    assert recipe.cost_budget_usd == 0.0


def test_verify_passes_when_modules_importable():
    mod = _load_recipe_module()
    recipe = mod.GateAuditIngest()
    assert asyncio.run(recipe.verify(_ctx())) is True


def test_format_quiet_on_empty_ingest():
    mod = _load_recipe_module()
    recipe = mod.GateAuditIngest()
    msg = asyncio.run(recipe.format(_ctx(), {"new": 0, "updated": 0, "pending_total": 0, "dry_run": False}))
    assert msg == ""


def test_format_quiet_on_dry_run():
    mod = _load_recipe_module()
    recipe = mod.GateAuditIngest()
    msg = asyncio.run(recipe.format(_ctx(dry_run=True), {"new": 5, "updated": 0, "pending_total": 5, "dry_run": True}))
    assert msg == ""


def test_format_emits_when_ingest_landed():
    mod = _load_recipe_module()
    recipe = mod.GateAuditIngest()
    msg = asyncio.run(recipe.format(_ctx(), {
        "new": 3,
        "updated": 1,
        "pending_total": 4,
        "pending_by_product": {"oe": 2, "pp": 2},
        "dry_run": False,
    }))
    assert "Sender Gate audit ingest" in msg
    assert "3 new" in msg
    assert "pending total: 4" in msg
    assert "oe: 2" in msg
    assert "/op gate-review" in msg
