"""Tests for the portfolio_health recipe.

Covers:
  - Recipe instantiates with required class metadata.
  - verify() passes when fixtures + libs present.
  - format() produces a valid IR (validates against the
    templated-dashboards JSON Schema) and writes HTML + MD output.
  - HTML renders without raising.
  - Stale alert fires when a fixture status doc is >48h old.
"""

from __future__ import annotations

import asyncio
import importlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from operator_core.recipes import RecipeContext

# The recipe consumes templated-dashboards at format/render time. Skip the
# whole module if it's not available in this environment (e.g. plain
# `pip install -e .` without the dev extras).
pytest.importorskip("dashboards", reason="templated-dashboards not installed; install with .[dev]")


@pytest.fixture
def tmp_env(tmp_path, monkeypatch):
    """Isolate every disk side-effect this recipe makes."""
    status_dir = tmp_path / "status"
    events_dir = tmp_path / "events"
    war_room = tmp_path / "war-room"
    projects = tmp_path / "Projects"
    status_dir.mkdir()
    events_dir.mkdir()
    war_room.mkdir()
    projects.mkdir()

    monkeypatch.setenv("OPERATOR_STATUS_DIR", str(status_dir))
    monkeypatch.setenv("OPERATOR_EVENTS_DIR", str(events_dir))
    monkeypatch.setenv("OPERATOR_WAR_ROOM_DIR", str(war_room))
    monkeypatch.setenv("OPERATOR_PROJECTS_DIR", str(projects))

    # Reload the recipe module so module-level path captures (if any) refresh.
    if "_operator_recipe_portfolio_health" in importlib.sys.modules:
        del importlib.sys.modules["_operator_recipe_portfolio_health"]

    return {
        "status": status_dir,
        "events": events_dir,
        "war_room": war_room,
        "projects": projects,
    }


_RECIPE_CACHE: dict[str, object] = {}


def _load_recipe_module():
    """Import the on-disk recipe file like the CLI does.

    Cached after the first load so we don't re-trigger ``register_recipe``
    inside a single pytest session (the duplicate registration emits a
    log warning that conflicts with stdlib LogRecord's reserved ``name``
    attribute, which is a pre-existing operator-core issue not in this
    recipe's scope).
    """
    if "mod" in _RECIPE_CACHE:
        return _RECIPE_CACHE["mod"]
    from operator_core.recipes.registry import clear_registry

    clear_registry()
    here = Path(__file__).resolve().parents[1] / "recipes" / "portfolio_health.py"
    spec = importlib.util.spec_from_file_location(
        "_operator_recipe_portfolio_health_test", here
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _RECIPE_CACHE["mod"] = mod
    return mod


def test_recipe_instantiates(tmp_env):
    mod = _load_recipe_module()
    recipe = mod.PortfolioHealth()
    assert recipe.name == "portfolio_health"
    assert recipe.version == "1.0.0"
    assert recipe.cost_budget_usd == 0.0
    assert recipe.schedule == "0 8-22 * * *"
    assert "dashboard" in recipe.tags


def test_verify_passes_when_libs_present(tmp_env):
    mod = _load_recipe_module()
    recipe = mod.PortfolioHealth()
    ctx = RecipeContext(recipe_name=recipe.name, correlation_id="test")
    assert asyncio.run(recipe.verify(ctx)) is True


def test_aggregate_returns_full_state(tmp_env):
    mod = _load_recipe_module()
    state = mod._aggregate(tmp_env["projects"], gh_available=False)
    assert "counts" in state
    assert "stale" in state
    assert "projects" in state
    assert "runs_window" in state
    assert "cost_window" in state
    # Tracked-project list is fixed; should match TRACKED_PROJECTS length.
    assert len(state["projects"]) == len(mod.TRACKED_PROJECTS)
    # Without any project dirs present every project should show "exists" False.
    assert all(p["exists"] is False for p in state["projects"])
    # Without a status emitter and no fallback recipes, tone should be neutral.
    neutral = [p for p in state["projects"] if p["tone"] == "neutral"]
    assert len(neutral) >= 1


def test_format_writes_html_and_md(tmp_env):
    mod = _load_recipe_module()
    recipe = mod.PortfolioHealth()
    ctx = RecipeContext(recipe_name=recipe.name, correlation_id="test")
    state = mod._aggregate(tmp_env["projects"], gh_available=False)
    summary = asyncio.run(recipe.format(ctx, state))

    html_path = tmp_env["war_room"] / "portfolio-health.html"
    md_path = tmp_env["war_room"] / "portfolio-health.md"
    ir_path = tmp_env["war_room"] / "portfolio-health.ir.json"
    assert html_path.exists() and html_path.stat().st_size > 0
    assert md_path.exists() and md_path.stat().st_size > 0
    assert ir_path.exists() and ir_path.stat().st_size > 0
    assert "Portfolio Health" in summary or "portfolio_health" in summary.lower() \
           or "green" in summary


def test_ir_validates_against_dashboards_schema(tmp_env):
    """The IR built by the recipe must round-trip through dashboards.load()."""
    mod = _load_recipe_module()
    state = mod._aggregate(tmp_env["projects"], gh_available=False)
    dashboard = mod._build_dashboard(state)

    # Pydantic + JSON Schema both via dashboards.load().
    from dashboards import load, dump
    raw = json.loads(dump(dashboard))
    loaded = load(raw)
    assert loaded.title == "Portfolio Health"
    # palantir is the project's design language per memory.
    assert loaded.theme == "palantir"
    # Banner section must be first.
    assert loaded.sections[0].title == "Overview"


def test_html_renders_without_error(tmp_env):
    mod = _load_recipe_module()
    state = mod._aggregate(tmp_env["projects"], gh_available=False)
    dashboard = mod._build_dashboard(state)
    from dashboards import render
    html = render(dashboard, "html")
    assert isinstance(html, str)
    assert "<html" in html.lower()
    assert "Portfolio Health" in html
    # Must mention at least one project.
    assert mod.TRACKED_PROJECTS[0]["name"] in html


def test_stale_alert_triggers_when_status_doc_older_than_threshold(tmp_env):
    """Write a v1 status-spec doc with ts > 48h old; expect a stale entry."""
    mod = _load_recipe_module()

    old_iso = (
        datetime.now(timezone.utc) - timedelta(hours=72)
    ).isoformat().replace("+00:00", "Z")

    # Pick a real tracked project name to seed the status file under.
    target = mod.TRACKED_PROJECTS[0]["name"]
    status_doc = {
        "schema_version": "status-spec/v1",
        "project": target,
        "ts": old_iso,
        "health": "green",
        "summary": "stale fixture",
    }
    (tmp_env["status"] / f"{target}.json").write_text(
        json.dumps(status_doc), encoding="utf-8"
    )

    state = mod._aggregate(tmp_env["projects"], gh_available=False)
    stale_names = [s["project"] for s in state["stale"]]
    assert target in stale_names


def test_no_stale_alert_when_status_doc_fresh(tmp_env):
    mod = _load_recipe_module()
    fresh_iso = (
        datetime.now(timezone.utc) - timedelta(minutes=30)
    ).isoformat().replace("+00:00", "Z")
    target = mod.TRACKED_PROJECTS[1]["name"]
    status_doc = {
        "schema_version": "status-spec/v1",
        "project": target,
        "ts": fresh_iso,
        "health": "green",
        "summary": "fresh fixture",
    }
    (tmp_env["status"] / f"{target}.json").write_text(
        json.dumps(status_doc), encoding="utf-8"
    )
    state = mod._aggregate(tmp_env["projects"], gh_available=False)
    stale_names = [s["project"] for s in state["stale"]]
    assert target not in stale_names


def test_classify_health_uses_emitted_status_first(tmp_env):
    mod = _load_recipe_module()
    project = mod.TRACKED_PROJECTS[0]
    doc = {"health": "red", "summary": "down"}
    tone, reason = mod._classify_health(project, doc, [], None)
    assert tone == "bad"
    assert "down" in reason


def test_classify_health_falls_back_to_recipes(tmp_env):
    mod = _load_recipe_module()
    project = mod.TRACKED_PROJECTS[0]
    components = [{"name": "deploy_checker", "status": "error"}]
    tone, reason = mod._classify_health(project, None, components, None)
    assert tone == "bad"
    assert "deploy_checker" in reason


def test_classify_health_neutral_when_no_signals(tmp_env):
    mod = _load_recipe_module()
    # Use a custom project def without any status_recipes.
    project = {"name": "x", "dir": "x", "repo": "x/x", "category": "Other",
               "status_recipes": []}
    tone, reason = mod._classify_health(project, None, [], None)
    assert tone == "neutral"
    assert "no status emitter" in reason
