"""Tests for morning_briefing v2 recipe.

Covers:
- Aggregators degrade gracefully when data sources missing
- Ranker prioritizes hot leads, fills to 3, caps at 3
- format() produces valid IR + writes both HTML and MD
- verify() always passes if war-room dir writable
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from recipes import morning_briefing as mb
from recipes.morning_briefing import (
    SectionResult,
    _build_dashboard_ir,
    _gather_audit_pipeline,
    _gather_cost_rollup,
    _gather_open_prs,
    _gather_pool_prospector,
    _gather_portfolio_health,
    _gather_stale_alerts,
    _rank_today,
    MorningBriefing,
)


@pytest.fixture
def tmp_war_room(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    war = tmp_path / "war-room"
    war.mkdir()
    monkeypatch.setattr(mb, "WAR_ROOM_DIR", war)
    monkeypatch.setattr(mb, "PORTFOLIO_HEALTH_IR", war / "portfolio-health.ir.json")
    monkeypatch.setattr(mb, "MORNING_HTML", war / "morning.html")
    monkeypatch.setattr(mb, "MORNING_MD", war / "morning.md")
    return war


@pytest.fixture
def tmp_status(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    status = tmp_path / "status"
    status.mkdir()
    monkeypatch.setattr(mb, "STATUS_DIR", status)
    return status


# ---------------------------------------------------------------------
# Aggregator graceful degradation
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aggregator_handles_missing_portfolio_health(tmp_war_room: Path) -> None:
    result = await _gather_portfolio_health()
    assert result.title == "Portfolio health"
    assert result.error is not None
    assert "missing" in result.error.lower()


@pytest.mark.asyncio
async def test_aggregator_reads_portfolio_health_kpis(tmp_war_room: Path) -> None:
    ir = {
        "title": "Portfolio Health",
        "subtitle": "test",
        "sections": [
            {
                "title": "Overview",
                "components": [
                    {"type": "kpi_tile", "label": "Tracked", "value": 25},
                    {"type": "kpi_tile", "label": "Green", "value": 6},
                    {"type": "kpi_tile", "label": "Red", "value": 2},
                ],
            }
        ],
    }
    (tmp_war_room / "portfolio-health.ir.json").write_text(json.dumps(ir), encoding="utf-8")
    result = await _gather_portfolio_health()
    assert result.error is None
    assert result.payload["kpis"]["Tracked"] == 25
    assert result.payload["kpis"]["Red"] == 2
    assert result.score == 70  # red>0 triggers score


@pytest.mark.asyncio
async def test_aggregator_handles_no_status_dir(tmp_status: Path) -> None:
    # status dir empty, no audit
    result = await _gather_audit_pipeline()
    assert result.error is None
    assert result.payload["recent_audit"] is None


@pytest.mark.asyncio
async def test_aggregator_audit_picks_up_recent_completion(tmp_status: Path) -> None:
    doc = {
        "schema_version": "status-spec/v1",
        "project": "ai-ops-consulting",
        "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        "health": "green",
        "last_event": {
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "type": "audit_completed",
            "summary": "Linden Crest HVAC",
        },
    }
    (tmp_status / "ai-ops-consulting.json").write_text(json.dumps(doc), encoding="utf-8")
    result = await _gather_audit_pipeline()
    assert result.error is None
    assert result.payload["recent_audit"]["type"] == "audit_completed"
    assert result.score == 40


@pytest.mark.asyncio
async def test_aggregator_cost_handles_missing_stream(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("OPERATOR_COST_NDJSON_PATH", str(tmp_path / "nonexistent.ndjson"))
    result = await _gather_cost_rollup()
    assert result.error is None
    assert result.payload["total_usd"] == 0.0


@pytest.mark.asyncio
async def test_aggregator_stale_alerts_finds_old_status(tmp_status: Path) -> None:
    # stale: 72 hours old
    old_ts = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=72)).isoformat()
    doc = {"schema_version": "status-spec/v1", "project": "demo", "ts": old_ts, "health": "yellow"}
    (tmp_status / "demo.json").write_text(json.dumps(doc), encoding="utf-8")
    # fresh: 1 hour old
    fresh_ts = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).isoformat()
    fresh = {"schema_version": "status-spec/v1", "project": "fresh", "ts": fresh_ts, "health": "green"}
    (tmp_status / "fresh.json").write_text(json.dumps(fresh), encoding="utf-8")
    result = await _gather_stale_alerts()
    assert result.error is None
    stale = result.payload["stale_projects"]
    assert len(stale) == 1
    assert stale[0]["project"] == "demo"
    assert result.score == 50


@pytest.mark.asyncio
async def test_aggregator_pool_prospector_finds_uncle_demo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pp = tmp_path / "pool-prospector"
    pp.mkdir()
    demo_dir = pp / "examples" / "uncle_demo" / "2026-05-05"
    demo_dir.mkdir(parents=True)
    monkeypatch.setattr(mb, "PROJECTS_DIR", tmp_path)
    result = await _gather_pool_prospector()
    assert result.error is None
    assert result.payload["latest_uncle_demo"] == "2026-05-05"


# ---------------------------------------------------------------------
# Ranker
# ---------------------------------------------------------------------


def test_ranker_picks_hot_lead_first() -> None:
    sections = {
        "Overnight replies": SectionResult(
            title="Overnight replies",
            payload={"hot": 2, "total": 5, "by_category": {"interested": [{}, {}]}},
            score=100,
        ),
    }
    top3 = _rank_today(sections)
    assert top3[0]["kicker"] == "HOT LEAD"
    assert "interested" in top3[0]["title"] or "2 " in top3[0]["title"]


def test_ranker_fills_to_3_even_with_few_items() -> None:
    sections = {
        "Overnight replies": SectionResult(title="Overnight replies", payload={"hot": 0, "total": 0, "by_category": {}}),
    }
    top3 = _rank_today(sections)
    assert len(top3) == 3
    # last items should be CLEAR placeholders since nothing scored
    assert any(item["kicker"] == "CLEAR" for item in top3)


def test_ranker_caps_at_3() -> None:
    sections = {
        "Overnight replies": SectionResult(
            title="Overnight replies",
            payload={"hot": 5, "total": 10, "by_category": {"interested": [{}]}},
            score=100,
        ),
        "Open PRs": SectionResult(
            title="Open PRs",
            payload={"prs": [{"repo": "foo", "number": 1, "title": "x", "age_hours": 30, "state": "CLEAN"}]},
            score=80,
        ),
        "Portfolio health": SectionResult(
            title="Portfolio health",
            payload={"kpis": {"Red": 3}},
            score=70,
        ),
        "Stale alerts": SectionResult(
            title="Stale alerts",
            payload={"stale_projects": [{"project": "demo", "ts": ""}], "blockers": []},
            score=50,
        ),
        "AI Ops audit": SectionResult(
            title="AI Ops audit",
            payload={"recent_audit": {"ts": "", "summary": "x"}},
            score=40,
        ),
    }
    top3 = _rank_today(sections)
    assert len(top3) == 3
    # Should be sorted by score descending
    assert top3[0]["kicker"] == "HOT LEAD"
    assert top3[1]["kicker"] == "READY TO MERGE"
    assert top3[2]["kicker"] == "RED"


def test_ranker_sorts_by_score() -> None:
    sections = {
        "Stale alerts": SectionResult(
            title="Stale alerts",
            payload={"stale_projects": [{"project": "x", "ts": ""}], "blockers": []},
            score=50,
        ),
        "Portfolio health": SectionResult(
            title="Portfolio health",
            payload={"kpis": {"Red": 1}},
            score=70,
        ),
    }
    top3 = _rank_today(sections)
    assert top3[0]["kicker"] == "RED"
    assert top3[1]["kicker"] == "STALE"


# ---------------------------------------------------------------------
# Format
# ---------------------------------------------------------------------


def test_format_produces_valid_ir() -> None:
    pytest.importorskip("dashboards", reason="templated-dashboards not installed; install with .[dev]")
    sections = {
        "Overnight replies": SectionResult(title="Overnight replies", payload={"hot": 0, "total": 0, "by_category": {}}),
        "Portfolio health": SectionResult(title="Portfolio health", payload={"kpis": {"Tracked": 25, "Green": 6, "Yellow": 4, "Red": 0, "Unknown": 15}, "ir_subtitle": "test"}),
        "Open PRs": SectionResult(title="Open PRs", payload={"prs": []}),
        "Waitlist signups": SectionResult(title="Waitlist signups", payload={"recent_24h": {}, "totals": {}}),
        "AI Ops audit": SectionResult(title="AI Ops audit", payload={"recent_audit": None}),
        "Pool Prospector": SectionResult(title="Pool Prospector", payload={"latest_uncle_demo": "2026-05-05", "leads_count": None}),
        "Cost (24h)": SectionResult(title="Cost (24h)", payload={"total_usd": 0.0, "by_recipe": {}, "spike": False, "seven_avg": 0.0}),
        "Recent commits (24h)": SectionResult(title="Recent commits (24h)", payload={"commits": []}),
        "Stale alerts": SectionResult(title="Stale alerts", payload={"stale_projects": [], "blockers": []}),
    }
    top3 = _rank_today(sections)
    d = _build_dashboard_ir(sections, top3)
    built = d.build()  # validates internally
    assert built.title.startswith("Morning briefing")
    assert len(built.sections) == 10  # today's 3 + 9 data sections


def test_format_writes_html_and_md(tmp_war_room: Path) -> None:
    """Recipe.format() writes both files."""
    pytest.importorskip("dashboards", reason="templated-dashboards not installed; install with .[dev]")
    sections = {
        "Overnight replies": SectionResult(title="Overnight replies", payload={"hot": 0, "total": 0, "by_category": {}}),
        "Portfolio health": SectionResult(title="Portfolio health", error="missing"),
        "Open PRs": SectionResult(title="Open PRs", payload={"prs": []}),
        "Waitlist signups": SectionResult(title="Waitlist signups", error="no supabase"),
        "AI Ops audit": SectionResult(title="AI Ops audit", payload={"recent_audit": None}),
        "Pool Prospector": SectionResult(title="Pool Prospector", payload={"latest_uncle_demo": None, "leads_count": None}),
        "Cost (24h)": SectionResult(title="Cost (24h)", payload={"total_usd": 0.0, "by_recipe": {}, "spike": False, "seven_avg": 0.0}),
        "Recent commits (24h)": SectionResult(title="Recent commits (24h)", payload={"commits": []}),
        "Stale alerts": SectionResult(title="Stale alerts", payload={"stale_projects": [], "blockers": []}),
    }
    top3 = _rank_today(sections)
    recipe = MorningBriefing()
    ctx = MagicMock()
    ctx.logger = MagicMock()
    asyncio.run(recipe.format(ctx, {"sections": sections, "today": top3}))
    assert mb.MORNING_HTML.exists()
    assert mb.MORNING_MD.exists()
    assert mb.MORNING_HTML.stat().st_size > 0
    assert mb.MORNING_MD.stat().st_size > 0


# ---------------------------------------------------------------------
# Recipe lifecycle
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recipe_verify_creates_war_room_dir(tmp_war_room: Path) -> None:
    recipe = MorningBriefing()
    ctx = MagicMock()
    ctx.clients = {}
    ctx.logger = MagicMock()
    ok = await recipe.verify(ctx)
    assert ok is True


@pytest.mark.asyncio
async def test_recipe_query_returns_sections_dict(tmp_war_room: Path, tmp_status: Path) -> None:
    """query() runs all aggregators in parallel; each returns a SectionResult."""
    recipe = MorningBriefing()
    ctx = MagicMock()
    ctx.clients = {}
    ctx.logger = MagicMock()
    out = await recipe.query(ctx)
    assert "sections" in out
    sections = out["sections"]
    # 9 aggregators should each return a SectionResult
    assert len(sections) == 9
    for title, res in sections.items():
        assert isinstance(res, SectionResult)


@pytest.mark.asyncio
async def test_recipe_post_skips_when_no_discord(tmp_war_room: Path) -> None:
    recipe = MorningBriefing()
    ctx = MagicMock()
    ctx.clients = {}  # no discord adapter
    ctx.logger = MagicMock()
    # post() should not raise
    await recipe.post(ctx, "test message")
    ctx.logger.info.assert_called()


def test_recipe_metadata() -> None:
    """v2 metadata sanity."""
    assert MorningBriefing.name == "morning_briefing"
    assert MorningBriefing.version == "2.0.0"
    assert MorningBriefing.schedule == "0 7 * * *"
    assert MorningBriefing.cost_budget_usd == 0.05
