"""Tests for the native daemon modules: adapters, portfolio, analysis, recipes, autonomy, briefing."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

# ── adapters ───────────────────────────────────────────────────────────────────

from operator_core.adapters import (
    ADAPTER_REGISTRY,
    HealthCheck,
    ProjectAdapter,
    ProjectType,
    Signal,
    Urgency,
    Workflow,
    _build_adapter,
    _type_from_str,
    get_adapter,
    list_adapters,
    load_adapters,
)
from operator_core.config import DeployConfig, HealthConfig, ProjectConfig


def _make_project_config(slug: str = "test-project", **overrides) -> ProjectConfig:
    defaults = dict(
        slug=slug,
        path=Path(tempfile.gettempdir()) / slug,
        repo=f"kjhholt-alt/{slug}",
        type="saas",
        deploy=DeployConfig(provider="vercel", url=f"https://{slug}.vercel.app"),
        health=HealthConfig(path="/"),
        checks=["npm run lint", "npm run build", "npm run test"],
        autonomy_tier="tiered_auto_deploy",
        protected_patterns=[".env*"],
        auto_merge=False,
    )
    defaults.update(overrides)
    return ProjectConfig(**defaults)


class TestAdapters:
    def test_type_from_str_valid(self):
        assert _type_from_str("saas") == ProjectType.SAAS
        assert _type_from_str("internal") == ProjectType.INTERNAL

    def test_type_from_str_unknown(self):
        assert _type_from_str("bogus") == ProjectType.INTERNAL

    def test_build_adapter_basic(self):
        cfg = _make_project_config()
        adapter = _build_adapter(cfg)
        assert adapter.slug == "test-project"
        assert adapter.project_type == ProjectType.SAAS
        assert adapter.revenue_proximity == "near"
        assert len(adapter.health_checks) >= 1
        assert len(adapter.workflows) >= 1

    def test_build_adapter_non_saas(self):
        cfg = _make_project_config(type="internal")
        adapter = _build_adapter(cfg)
        assert adapter.revenue_proximity == "none"

    def test_build_adapter_local_deploy_no_health_url(self):
        cfg = _make_project_config(
            deploy=DeployConfig(provider="local", url=""),
            health=HealthConfig(path="/"),
        )
        adapter = _build_adapter(cfg)
        deploy_checks = [h for h in adapter.health_checks if h.name == "deploy"]
        assert len(deploy_checks) == 0

    def test_load_adapters_from_configs(self):
        configs = [_make_project_config("alpha"), _make_project_config("beta")]
        registry = load_adapters(configs)
        assert "alpha" in registry
        assert "beta" in registry
        assert len(registry) == 2

    def test_get_adapter_exists(self):
        load_adapters([_make_project_config("gamma")])
        adapter = get_adapter("gamma")
        assert adapter is not None
        assert adapter.slug == "gamma"

    def test_get_adapter_missing(self):
        load_adapters([_make_project_config("delta")])
        assert get_adapter("nonexistent") is None

    def test_list_adapters(self):
        load_adapters([_make_project_config("a"), _make_project_config("b")])
        adapters = list_adapters()
        assert len(adapters) == 2

    def test_urgency_enum(self):
        assert Urgency.CRITICAL.value == "critical"
        assert Urgency.NONE.value == "none"


# ── portfolio ──────────────────────────────────────────────────────────────────

from operator_core.portfolio import (
    NextAction,
    PortfolioSnapshot,
    ProjectState,
    _compute_priorities,
    collect_project_state,
    load_snapshot,
    save_snapshot,
    snapshot_to_dict,
)


class TestPortfolio:
    def test_project_state_defaults(self):
        state = ProjectState(slug="test", project_type="saas")
        assert state.health == "unknown"
        assert state.urgency == "none"
        assert state.risk_level == "low"
        assert state.blockers == []

    def test_next_action_model(self):
        action = NextAction(
            action="fix_spf",
            description="Add SPF record",
            urgency="critical",
            estimated_minutes=5,
            autonomous_ok=True,
            requires_human=False,
        )
        assert action.autonomous_ok is True
        assert action.estimated_minutes == 5

    def test_compute_priorities_empty(self):
        snap = PortfolioSnapshot()
        _compute_priorities(snap)
        assert snap.top_priority == ""
        assert snap.best_use_of_time == []

    def test_compute_priorities_with_projects(self):
        snap = PortfolioSnapshot(
            projects={
                "alpha": ProjectState(slug="alpha", project_type="saas", urgency="high", urgency_reason="deploy down", revenue_proximity="near"),
                "beta": ProjectState(slug="beta", project_type="internal", urgency="low", revenue_proximity="none"),
                "gamma": ProjectState(slug="gamma", project_type="saas", urgency="critical", urgency_reason="SPF missing", revenue_proximity="near"),
            }
        )
        _compute_priorities(snap)
        assert snap.top_priority == "gamma"
        assert "gamma" in snap.best_use_of_time
        assert "alpha" in snap.best_use_of_time

    def test_snapshot_to_dict_and_save_load(self):
        snap = PortfolioSnapshot(
            generated_at="2026-04-12T00:00:00Z",
            projects={
                "test": ProjectState(slug="test", project_type="saas", health="green"),
            },
            top_priority="test",
            top_priority_reason="testing",
            revenue_closest="test",
        )
        d = snapshot_to_dict(snap)
        assert d["top_priority"] == "test"
        assert "test" in d["projects"]
        assert d["projects"]["test"]["health"] == "green"

    def test_save_and_load_snapshot(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as f:
            path = Path(f.name)
        try:
            snap = PortfolioSnapshot(
                generated_at="2026-04-12T00:00:00Z",
                top_priority="proj",
                top_priority_reason="test",
                projects={
                    "proj": ProjectState(slug="proj", project_type="saas", health="yellow"),
                },
            )
            save_snapshot(snap, path)
            loaded = load_snapshot(path)
            assert loaded is not None
            assert loaded.top_priority == "proj"
            assert "proj" in loaded.projects
            assert loaded.projects["proj"].health == "yellow"
        finally:
            path.unlink(missing_ok=True)

    def test_load_snapshot_missing(self):
        result = load_snapshot(Path("/nonexistent/path.json"))
        assert result is None

    def test_collect_project_state_missing_dir(self):
        adapter = ProjectAdapter(
            slug="ghost",
            path=Path("/nonexistent/ghost"),
            project_type=ProjectType.SAAS,
        )
        state = collect_project_state(adapter)
        assert state.health == "red"
        assert "directory missing" in state.risk_factors[0]


# ── analysis ───────────────────────────────────────────────────────────────────

from operator_core.analysis import (
    AnalysisResponse,
    EvidencePacket,
    Finding,
    RecommendedStep,
    Verdict,
    analyze_portfolio_local,
    analyze_project_local,
    build_portfolio_evidence,
    build_project_evidence,
    log_analysis,
)


class TestAnalysis:
    def test_evidence_packet_to_dict(self):
        pkt = EvidencePacket(timestamp="now", scope="single_project")
        d = pkt.to_dict()
        assert d["scope"] == "single_project"

    def test_analyze_project_healthy(self):
        state = ProjectState(
            slug="healthy",
            project_type="saas",
            health="green",
            health_details={"deploy": "healthy"},
            urgency="none",
            git_dirty=False,
        )
        resp = analyze_project_local(state)
        assert resp.verdict.safe_to_use is True
        assert resp.verdict.trust_level == "high"
        assert len(resp.findings) == 0

    def test_analyze_project_unhealthy(self):
        state = ProjectState(
            slug="broken",
            project_type="saas",
            health="red",
            health_details={"deploy": "unhealthy"},
            urgency="high",
            urgency_reason="deploy is unhealthy",
            git_dirty=True,
            blockers=["missing API key"],
        )
        resp = analyze_project_local(state)
        assert resp.verdict.safe_to_use is False
        assert resp.human_input_required is True
        assert len(resp.findings) >= 2

    def test_analyze_project_dirty_git(self):
        state = ProjectState(
            slug="messy",
            project_type="internal",
            health="yellow",
            health_details={"deploy": "healthy"},
            git_dirty=True,
            commits_ahead=10,
        )
        resp = analyze_project_local(state)
        assert any(f.category == "git_hygiene" for f in resp.findings)
        assert any("ahead" in f.title for f in resp.findings)

    def test_analyze_portfolio_local(self):
        snap = PortfolioSnapshot(
            projects={
                "a": ProjectState(slug="a", project_type="saas", health="green", urgency="low", revenue_proximity="near"),
                "b": ProjectState(slug="b", project_type="saas", health="red", urgency="critical", urgency_reason="down", revenue_proximity="near"),
            },
            critical_issues=["b: down"],
            blocked_on_human=["b: needs restart"],
            best_use_of_time=["b", "a"],
            best_agent_work=["a"],
        )
        resp = analyze_portfolio_local(snap)
        assert "Portfolio" in resp.summary
        assert resp.human_input_required is True
        assert any(f.severity == "critical" for f in resp.findings)

    def test_analysis_response_to_dict(self):
        resp = AnalysisResponse(
            timestamp="now",
            findings=[Finding(severity="high", title="test")],
            verdict=Verdict(safe_to_use=True, reason="ok"),
            summary="test summary",
        )
        d = resp.to_dict()
        assert d["summary"] == "test summary"
        assert len(d["findings"]) == 1

    def test_log_analysis(self):
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as f:
            log_path = Path(f.name)
        try:
            from operator_core import analysis as analysis_mod
            original = analysis_mod.ANALYSIS_LOG_PATH
            analysis_mod.ANALYSIS_LOG_PATH = log_path

            pkt = EvidencePacket(scope="test")
            resp = AnalysisResponse(summary="test", findings=[Finding(severity="info", title="t")])
            log_analysis(pkt, resp)

            lines = log_path.read_text().splitlines()
            assert len(lines) == 1
            entry = json.loads(lines[0])
            assert entry["scope"] == "test"
            assert entry["findings_count"] == 1

            analysis_mod.ANALYSIS_LOG_PATH = original
        finally:
            log_path.unlink(missing_ok=True)


# ── recipes ────────────────────────────────────────────────────────────────────

from operator_core.recipes import (
    ALL_RECIPES,
    AgentRecipe,
    format_recipe_list,
    get_recipe,
    list_recipes,
    select_recipe,
)


class TestRecipes:
    def test_all_recipes_count(self):
        assert len(ALL_RECIPES) == 10

    def test_get_recipe_exists(self):
        recipe = get_recipe("code-reviewer")
        assert recipe is not None
        assert recipe.autonomous_ok is True

    def test_get_recipe_missing(self):
        assert get_recipe("nonexistent") is None

    def test_list_recipes(self):
        recipes = list_recipes()
        assert len(recipes) == 10

    def test_select_recipe_exact_match(self):
        recipe = select_recipe("review")
        assert recipe is not None
        assert recipe.name == "code-reviewer"

    def test_select_recipe_feature(self):
        recipe = select_recipe("feature")
        assert recipe is not None
        assert recipe.name == "planner-coder-reviewer"

    def test_select_recipe_high_risk_filters_autonomous(self):
        recipe = select_recipe("feature", risk_level="high")
        assert recipe is not None
        assert recipe.autonomous_ok is False

    def test_select_recipe_with_history(self):
        history = [
            {"recipe": "code-reviewer", "status": "complete"},
            {"recipe": "code-reviewer", "status": "complete"},
            {"recipe": "sprint-planner", "status": "failed"},
        ]
        recipe = select_recipe("review", history=history)
        assert recipe is not None
        assert recipe.name == "code-reviewer"

    def test_select_recipe_unknown_falls_to_sprint_planner(self):
        recipe = select_recipe("xyz_completely_unknown_999")
        assert recipe is not None
        assert recipe.name == "sprint-planner"

    def test_format_recipe_list(self):
        text = format_recipe_list()
        assert "10 built-in" in text
        assert "planner-coder-reviewer" in text


# ── autonomy ───────────────────────────────────────────────────────────────────

from operator_core.autonomy import (
    TIER_ANALYZE,
    TIER_DEPLOY,
    TIER_DESTRUCTIVE,
    TIER_GUARDED_EXECUTE,
    TIER_OBSERVE,
    TIER_SAFE_EXECUTE,
    EscalationResult,
    PolicyDecision,
    check_action,
    check_escalation,
    classify_action_tier,
    format_escalation,
    format_policy_summary,
    get_autonomy,
)


class TestAutonomy:
    def test_get_autonomy_known(self):
        a = get_autonomy("pl-engine")
        assert a.max_tier == TIER_SAFE_EXECUTE
        assert a.analysis_auto is True

    def test_get_autonomy_unknown(self):
        a = get_autonomy("nonexistent")
        assert a.max_tier == TIER_OBSERVE

    def test_classify_action_tier(self):
        assert classify_action_tier("health_check") == TIER_OBSERVE
        assert classify_action_tier("analyze") == TIER_ANALYZE
        assert classify_action_tier("run_tests") == TIER_SAFE_EXECUTE
        assert classify_action_tier("create_branch") == TIER_GUARDED_EXECUTE
        assert classify_action_tier("deploy") == TIER_DEPLOY
        assert classify_action_tier("force_push") == TIER_DESTRUCTIVE

    def test_check_action_allowed(self):
        decision = check_action("pl-engine", "run_tests")
        assert decision.allowed is True

    def test_check_action_denied(self):
        decision = check_action("outdoor-crm", "run_tests")
        assert decision.allowed is False
        assert "Tier 2" in decision.reason

    def test_check_action_deploy_denied(self):
        decision = check_action("prospector-pro", "deploy")
        assert decision.allowed is False

    def test_check_escalation_no_triggers(self):
        resp = AnalysisResponse(summary="all good")
        esc = check_escalation(resp)
        assert esc.should_escalate is False
        assert esc.reasons == []

    def test_check_escalation_human_required(self):
        resp = AnalysisResponse(human_input_required=True)
        esc = check_escalation(resp)
        assert esc.should_escalate is True
        assert any("human input" in r for r in esc.reasons)

    def test_check_escalation_low_confidence(self):
        resp = AnalysisResponse(
            findings=[Finding(severity="high", title="problem", confidence=0.4)]
        )
        esc = check_escalation(resp)
        assert esc.should_escalate is True

    def test_check_escalation_too_many_steps(self):
        resp = AnalysisResponse()
        esc = check_escalation(resp, autonomous_steps_taken=4)
        assert esc.should_escalate is True

    def test_check_escalation_cost_ceiling(self):
        resp = AnalysisResponse()
        with patch.dict(os.environ, {"OPERATOR_COST_CEILING_USD": "3.0"}):
            esc = check_escalation(resp, cost_today_usd=5.0)
        assert esc.should_escalate is True

    def test_format_policy_summary(self):
        text = format_policy_summary("pl-engine")
        assert "pl-engine" in text
        assert "Tier 2" in text

    def test_format_escalation(self):
        resp = AnalysisResponse(
            summary="deploy failed",
            findings=[Finding(severity="high", title="unhealthy")],
            recommended_next_step=RecommendedStep(action="check_logs", reason="see error"),
        )
        esc = EscalationResult(should_escalate=True, reasons=["human input needed"])
        text = format_escalation("test-project", resp, esc)
        assert "ESCALATION" in text
        assert "test-project" in text


# ── briefing ───────────────────────────────────────────────────────────────────

from operator_core.briefing import (
    briefing_compact,
    briefing_html_section,
    briefing_markdown,
    priorities_json,
)


class TestBriefing:
    @pytest.fixture
    def sample_snapshot(self):
        return PortfolioSnapshot(
            generated_at="2026-04-12T00:00:00Z",
            projects={
                "alpha": ProjectState(slug="alpha", project_type="saas", health="green", urgency="low", revenue_proximity="near"),
                "beta": ProjectState(slug="beta", project_type="saas", health="red", urgency="high", urgency_reason="down", revenue_proximity="near", blockers=["API key missing"]),
            },
            top_priority="beta",
            top_priority_reason="deploy is down",
            best_use_of_time=["beta", "alpha"],
            best_agent_work=["alpha"],
            blocked_on_human=["beta: API key missing"],
            critical_issues=[],
            revenue_closest="alpha",
        )

    def test_briefing_markdown(self, sample_snapshot):
        text = briefing_markdown(sample_snapshot)
        assert "Portfolio Briefing" in text
        assert "beta" in text
        assert "alpha" in text
        assert "| Project |" in text

    def test_briefing_compact(self, sample_snapshot):
        text = briefing_compact(sample_snapshot)
        assert "Portfolio Briefing" in text
        assert len(text) < 2000

    def test_briefing_html_section(self, sample_snapshot):
        html = briefing_html_section(sample_snapshot)
        assert "<table>" in html
        assert "alpha" in html

    def test_priorities_json(self, sample_snapshot):
        data = priorities_json(sample_snapshot)
        assert "for_human" in data
        assert "for_agents" in data
        assert "blocked" in data
        assert len(data["blocked"]) == 1


# ── commands (new portfolio commands) ──────────────────────────────────────────

from operator_core.commands import CommandParseError, ParsedCommand, parse_operator_command


class TestPortfolioCommands:
    def test_portfolio_command(self):
        cmd = parse_operator_command("!op portfolio")
        assert cmd.action == "portfolio"

    def test_priorities_command(self):
        cmd = parse_operator_command("!op priorities")
        assert cmd.action == "priorities"

    def test_blocked_command(self):
        cmd = parse_operator_command("!op blocked")
        assert cmd.action == "blocked"

    def test_blockers_alias(self):
        cmd = parse_operator_command("!op blockers")
        assert cmd.action == "blocked"

    def test_brief_command(self):
        cmd = parse_operator_command("!op brief")
        assert cmd.action == "portfolio_brief"

    def test_briefing_alias(self):
        cmd = parse_operator_command("!op briefing")
        assert cmd.action == "portfolio_brief"

    def test_analyze_no_project(self):
        cmd = parse_operator_command("!op analyze")
        assert cmd.action == "analyze"
        assert cmd.project is None

    def test_analyze_with_project(self):
        cmd = parse_operator_command("!op analyze pl-engine")
        assert cmd.action == "analyze"
        assert cmd.project == "pl-engine"

    def test_recipes_command(self):
        cmd = parse_operator_command("!op recipes")
        assert cmd.action == "recipes"

    def test_sprint_command(self):
        cmd = parse_operator_command("!op sprint")
        assert cmd.action == "sprint_recommend"


# ── portfolio_routes ───────────────────────────────────────────────────────────

from operator_core.portfolio_routes import register_portfolio_routes
from operator_core.http_server import EXTRA_ROUTES


class TestPortfolioRoutes:
    def test_register_creates_routes(self):
        keys_before = set(EXTRA_ROUTES.keys())
        register_portfolio_routes()
        expected = {
            ("GET", "/portfolio"),
            ("GET", "/priorities"),
            ("GET", "/blocked"),
            ("POST", "/analyze"),
            ("POST", "/brief"),
        }
        assert expected.issubset(set(EXTRA_ROUTES.keys()))
