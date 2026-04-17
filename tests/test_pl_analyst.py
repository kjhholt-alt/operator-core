"""Tests for PL Engine Always-On Analyst Loop."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from operator_core.commands import parse_operator_command
from operator_core.pl_analyst import (
    CANONICAL_PATHS,
    KNOWN_ISSUES,
    MAX_AUTONOMOUS_STEPS,
    MAX_LOOP_ITERATIONS,
    SAFE_AUTONOMOUS_ACTIONS,
    AnalysisRequest,
    AnalysisResponse,
    AutonomousAction,
    EscalationReason,
    EvidencePacket,
    Finding,
    LoopResult,
    TrustVerdict,
    build_escalation,
    check_canonical_paths,
    execute_autonomous_step,
    run_analyst_loop,
    _build_analysis_request,
    _check_escalation,
    _dry_run_analysis,
)
from operator_core.pl_engine import CheckOutcome, PlResult


# ── Fixtures ────────────────────────────────────────────────────────────────

def _make_pl_result(
    action: str = "validate",
    factory: str = "AX02",
    ok: bool = True,
    exit_code: int = 0,
    checks: list[CheckOutcome] | None = None,
    artifacts: list[str] | None = None,
    raw_output: str = "",
) -> PlResult:
    if checks is None:
        checks = [
            CheckOutcome(name="Schema Check", passed=True, severity="info", detail="All columns present"),
            CheckOutcome(name="CPOH Range", passed=True, severity="info", detail="Within $100-$1500"),
            CheckOutcome(name="Crossfoot", passed=True, severity="info", detail="Sums match"),
        ]
    return PlResult(
        action=action,
        factory=factory,
        ok=ok,
        exit_code=exit_code,
        checks=checks,
        summary=f"{len(checks)} checks passed",
        artifacts=artifacts or [],
        raw_output=raw_output,
        verdict=f"{factory}: {'PASS' if ok else 'FAIL'}",
        next_action="" if ok else "Fix errors",
    )


def _make_failing_result(factory: str = "AX02") -> PlResult:
    return _make_pl_result(
        factory=factory,
        ok=False,
        exit_code=1,
        checks=[
            CheckOutcome(name="Schema Check", passed=True, severity="info"),
            CheckOutcome(name="Baseline", passed=False, severity="error", detail="PL27 deviates >35% from SL26"),
            CheckOutcome(name="Freshness", passed=False, severity="warning", detail="Budget CSV is 3 days old"),
            CheckOutcome(name="CPOH Range", passed=True, severity="info"),
        ],
    )


# ── EvidencePacket Tests ────────────────────────────────────────────────────

class TestEvidencePacket:
    def test_from_pl_result_passing(self):
        result = _make_pl_result()
        packet = EvidencePacket.from_pl_result(result)

        assert packet.workflow_type == "validate"
        assert packet.factory == "AX02"
        assert packet.pass_count == 3
        assert packet.fail_count == 0
        assert packet.warn_count == 0
        assert packet.confidence == 1.0
        assert len(packet.confidence_flags) == 0
        assert packet.canonical_path_used == "run.py"

    def test_from_pl_result_failing(self):
        result = _make_failing_result()
        packet = EvidencePacket.from_pl_result(result)

        assert packet.fail_count == 1
        assert packet.warn_count == 1
        assert packet.confidence < 1.0
        assert any("validation error" in f for f in packet.confidence_flags)

    def test_from_pl_result_no_checks(self):
        result = _make_pl_result(checks=[])
        packet = EvidencePacket.from_pl_result(result)

        assert packet.confidence == 0.5
        assert any("No validation checks" in f for f in packet.confidence_flags)

    def test_from_pl_result_crash(self):
        result = _make_pl_result(ok=False, exit_code=1, checks=[])
        packet = EvidencePacket.from_pl_result(result)

        assert any("possible crash" in f for f in packet.confidence_flags)

    def test_known_issues_included(self):
        result = _make_pl_result(factory="AX02")
        packet = EvidencePacket.from_pl_result(result)

        assert "P0_DEPRECIATION" in packet.active_known_issues
        assert "P0_T801_BENEFITS" not in packet.active_known_issues

    def test_known_issues_t801(self):
        result = _make_pl_result(factory="T801")
        packet = EvidencePacket.from_pl_result(result)

        assert "P0_T801_BENEFITS" in packet.active_known_issues

    def test_to_dict_omits_empty(self):
        packet = EvidencePacket(workflow_type="validate", factory="AX02")
        d = packet.to_dict()

        assert "workflow_type" in d
        assert "factory" in d
        assert "drift_signals" not in d
        assert "raw_output_tail" not in d

    def test_timestamp_populated(self):
        result = _make_pl_result()
        packet = EvidencePacket.from_pl_result(result)

        assert packet.timestamp
        assert "T" in packet.timestamp


# ── AnalysisResponse Tests ──────────────────────────────────────────────────

class TestAnalysisResponse:
    def test_from_claude_json_full(self):
        data = {
            "findings": [
                {
                    "description": "Baseline check failed",
                    "severity": "high",
                    "root_cause": "PL27 budget deviates >35%",
                    "confidence": 0.9,
                    "evidence_ref": "Baseline",
                }
            ],
            "verdict": "not_safe_to_use",
            "verdict_explanation": "Critical validation failure",
            "recommended_next_step": "Run carry-forward script",
            "next_step_action": "validate",
            "next_step_factory": "AX02",
            "autonomous_ok": True,
            "human_input_required": False,
            "escalation_reason": "",
            "summary": "AX02 has a baseline deviation.",
        }
        resp = AnalysisResponse.from_claude_json(data)

        assert len(resp.findings) == 1
        assert resp.findings[0].severity == "high"
        assert resp.verdict == TrustVerdict.UNSAFE
        assert resp.autonomous_ok is True
        assert resp.next_step_action == "validate"

    def test_from_claude_json_minimal(self):
        resp = AnalysisResponse.from_claude_json({})

        assert resp.verdict == TrustVerdict.UNKNOWN
        assert resp.findings == []
        assert resp.autonomous_ok is False

    def test_from_claude_json_invalid_verdict(self):
        resp = AnalysisResponse.from_claude_json({"verdict": "maybe"})

        assert resp.verdict == TrustVerdict.UNKNOWN

    def test_verdict_enum_values(self):
        assert TrustVerdict.SAFE.value == "safe_to_use"
        assert TrustVerdict.CAVEATS.value == "use_with_caveats"
        assert TrustVerdict.UNSAFE.value == "not_safe_to_use"


# ── AnalysisRequest Tests ───────────────────────────────────────────────────

class TestAnalysisRequest:
    def test_to_dict_serializable(self):
        evidence = EvidencePacket.from_pl_result(_make_pl_result())
        request = _build_analysis_request(
            evidence=evidence,
            iteration=1,
            history=[],
            factory="AX02",
        )
        d = request.to_dict()

        json_str = json.dumps(d, default=str)
        assert "AX02" in json_str
        assert "evidence" in d
        assert "candidate_next_steps" in d

    def test_candidates_include_explain_on_failure(self):
        evidence = EvidencePacket.from_pl_result(_make_failing_result())
        request = _build_analysis_request(
            evidence=evidence,
            iteration=1,
            history=[],
            factory="AX02",
        )

        candidates = request.candidate_next_steps
        assert any("explain_cpoh" in c for c in candidates)

    def test_risk_policy_includes_safe_actions(self):
        evidence = EvidencePacket.from_pl_result(_make_pl_result())
        request = _build_analysis_request(evidence=evidence, iteration=1, history=[], factory="AX02")

        safe = request.risk_policy["safe_actions"]
        assert "validate" in safe
        assert "explain_cpoh" in safe
        never = request.risk_policy["never_autonomous"]
        assert "pipeline" in never


# ── Escalation Tests ────────────────────────────────────────────────────────

class TestEscalation:
    def test_check_escalation_human_required(self):
        analysis = AnalysisResponse(human_input_required=True, escalation_reason="ambiguous_result")
        reason = _check_escalation(analysis, iteration=1, autonomous_steps=0)

        assert reason == EscalationReason.AMBIGUOUS

    def test_check_escalation_loop_limit(self):
        analysis = AnalysisResponse()
        reason = _check_escalation(analysis, iteration=MAX_LOOP_ITERATIONS, autonomous_steps=0)

        assert reason == EscalationReason.LOOP_LIMIT

    def test_check_escalation_max_autonomous(self):
        analysis = AnalysisResponse()
        reason = _check_escalation(analysis, iteration=2, autonomous_steps=MAX_AUTONOMOUS_STEPS)

        assert reason == EscalationReason.LOOP_LIMIT

    def test_check_escalation_mutation_required(self):
        analysis = AnalysisResponse(
            next_step_action="modify_code",
            autonomous_ok=True,
        )
        reason = _check_escalation(analysis, iteration=1, autonomous_steps=0)

        assert reason == EscalationReason.MUTATION_REQUIRED

    def test_check_escalation_none_when_safe(self):
        analysis = AnalysisResponse(
            next_step_action="validate",
            autonomous_ok=True,
        )
        reason = _check_escalation(analysis, iteration=1, autonomous_steps=0)

        assert reason is None

    def test_build_escalation_format(self):
        history = [{"action": "validate", "verdict": "not_safe_to_use"}]
        analysis = AnalysisResponse(
            findings=[Finding(description="Baseline failed", severity="high", root_cause="PL27 deviation")],
            escalation_reason="high_risk_action",
        )
        escalation = build_escalation(EscalationReason.HIGH_RISK, history, analysis, "AX02")

        text = escalation.format()
        assert "Escalation Required" in text
        assert "AX02" in text
        assert "Valley City" in text
        assert "validate: not_safe_to_use" in text


# ── Canonical Path Registry Tests ───────────────────────────────────────────

class TestCanonicalPaths:
    def test_registry_has_expected_entries(self):
        assert "run.py" in CANONICAL_PATHS
        assert "cli.py" in CANONICAL_PATHS
        assert "whatif.py" in CANONICAL_PATHS

    def test_run_py_is_canonical(self):
        assert CANONICAL_PATHS["run.py"].status == "canonical"

    def test_cli_py_is_legacy(self):
        assert CANONICAL_PATHS["cli.py"].status == "legacy"

    def test_whatif_py_is_broken(self):
        assert CANONICAL_PATHS["whatif.py"].status == "broken"

    def test_check_canonical_paths_returns_warnings(self):
        warnings = check_canonical_paths()
        assert isinstance(warnings, list)
        paths_warned = [w["path"] for w in warnings]
        assert "whatif.py" in paths_warned


# ── Known Issues Registry Tests ─────────────────────────────────────────────

class TestKnownIssues:
    def test_p0_issues_present(self):
        ids = [i["id"] for i in KNOWN_ISSUES]
        assert "P0_DEPRECIATION" in ids
        assert "P0_T801_BENEFITS" in ids

    def test_p1_issues_present(self):
        ids = [i["id"] for i in KNOWN_ISSUES]
        assert "P1_GOLDEN_SNAPSHOTS" in ids
        assert "P1_PPTX_VOLUME" in ids

    def test_all_issues_have_detection(self):
        for issue in KNOWN_ISSUES:
            assert issue.get("detection"), f"Issue {issue['id']} missing detection hint"


# ── Trust Verdict Tests ─────────────────────────────────────────────────────

class TestTrustVerdicts:
    def test_dry_run_passing(self):
        result = _make_pl_result(ok=True)
        evidence = EvidencePacket.from_pl_result(result)
        analysis = _dry_run_analysis(result, evidence)

        assert analysis.verdict in (TrustVerdict.SAFE, TrustVerdict.CAVEATS)

    def test_dry_run_failing(self):
        result = _make_failing_result()
        evidence = EvidencePacket.from_pl_result(result)
        analysis = _dry_run_analysis(result, evidence)

        assert analysis.verdict == TrustVerdict.UNSAFE
        assert analysis.human_input_required is True

    def test_dry_run_warnings_only(self):
        checks = [
            CheckOutcome(name="Schema", passed=True),
            CheckOutcome(name="W1", passed=False, severity="warning"),
            CheckOutcome(name="W2", passed=False, severity="warning"),
            CheckOutcome(name="W3", passed=False, severity="warning"),
            CheckOutcome(name="W4", passed=False, severity="warning"),
        ]
        result = _make_pl_result(ok=True, checks=checks)
        evidence = EvidencePacket.from_pl_result(result)
        analysis = _dry_run_analysis(result, evidence)

        assert analysis.verdict == TrustVerdict.CAVEATS


# ── Autonomous Step Tests ───────────────────────────────────────────────────

class TestAutonomousSteps:
    def test_safe_actions_set(self):
        assert AutonomousAction.VALIDATE in SAFE_AUTONOMOUS_ACTIONS
        assert AutonomousAction.EXPLAIN_CPOH in SAFE_AUTONOMOUS_ACTIONS

    @patch("operator_core.pl_analyst.validate_factory")
    def test_execute_validate(self, mock_validate):
        mock_validate.return_value = _make_pl_result()
        result = execute_autonomous_step("validate", "AX02")

        assert result is not None
        mock_validate.assert_called_once_with("AX02")

    @patch("operator_core.pl_analyst.validate_all")
    def test_execute_validate_all(self, mock_validate_all):
        mock_validate_all.return_value = _make_pl_result(factory=None)
        result = execute_autonomous_step("validate_all", None)

        assert result is not None
        mock_validate_all.assert_called_once()

    @patch("operator_core.pl_analyst.explain_cpoh")
    def test_execute_explain(self, mock_explain):
        mock_explain.return_value = _make_pl_result(action="explain")
        result = execute_autonomous_step("explain_cpoh", "AX02")

        assert result is not None
        mock_explain.assert_called_once_with("AX02")

    def test_execute_unknown_action(self):
        result = execute_autonomous_step("modify_code", "AX02")
        assert result is None

    def test_execute_validate_no_factory(self):
        result = execute_autonomous_step("validate", None)
        assert result is None


# ── Loop Controller Tests ───────────────────────────────────────────────────

class TestAnalystLoop:
    @patch("operator_core.pl_analyst.validate_factory")
    @patch("operator_core.pl_analyst.call_claude_analysis")
    def test_single_iteration_pass(self, mock_claude, mock_validate):
        mock_validate.return_value = _make_pl_result()
        mock_claude.return_value = AnalysisResponse(
            verdict=TrustVerdict.SAFE,
            summary="AX02 validation clean.",
            autonomous_ok=False,
        )

        result = run_analyst_loop(factory="AX02", initial_action="validate")

        assert result.iterations == 1
        assert result.final_verdict == TrustVerdict.SAFE
        assert not result.escalated
        assert result.autonomous_steps_taken == 0

    @patch("operator_core.pl_analyst.validate_factory")
    @patch("operator_core.pl_analyst.explain_cpoh")
    @patch("operator_core.pl_analyst.call_claude_analysis")
    def test_two_iteration_chain(self, mock_claude, mock_explain, mock_validate):
        mock_validate.return_value = _make_failing_result()
        mock_explain.return_value = _make_pl_result(action="explain", ok=True)

        mock_claude.side_effect = [
            AnalysisResponse(
                verdict=TrustVerdict.UNSAFE,
                summary="Baseline failed — checking CPOH.",
                recommended_next_step="Run CPOH explainability",
                next_step_action="explain_cpoh",
                next_step_factory="AX02",
                autonomous_ok=True,
            ),
            AnalysisResponse(
                verdict=TrustVerdict.CAVEATS,
                summary="CPOH explains the deviation. Use with caveats.",
                autonomous_ok=False,
            ),
        ]

        result = run_analyst_loop(factory="AX02", initial_action="validate")

        assert result.iterations == 2
        assert result.autonomous_steps_taken == 1
        assert result.final_verdict == TrustVerdict.CAVEATS

    @patch("operator_core.pl_analyst.validate_factory")
    def test_dry_run_no_claude_call(self, mock_validate):
        mock_validate.return_value = _make_pl_result()

        result = run_analyst_loop(factory="AX02", initial_action="validate", dry_run=True)

        assert result.iterations == 1
        assert "[DRY RUN]" in result.final_summary

    @patch("operator_core.pl_analyst.validate_factory")
    @patch("operator_core.pl_analyst.call_claude_analysis")
    def test_escalation_on_human_required(self, mock_claude, mock_validate):
        mock_validate.return_value = _make_failing_result()
        mock_claude.return_value = AnalysisResponse(
            verdict=TrustVerdict.UNSAFE,
            summary="Cannot diagnose autonomously.",
            human_input_required=True,
            escalation_reason="ambiguous_result",
        )

        result = run_analyst_loop(factory="AX02")

        assert result.escalated is True
        assert result.escalation is not None
        assert "Escalation Required" in result.escalation.format()

    @patch("operator_core.pl_analyst._execute_step")
    @patch("operator_core.pl_analyst.call_claude_analysis")
    def test_max_iterations_escalation(self, mock_claude, mock_execute):
        mock_execute.return_value = _make_pl_result()
        mock_claude.return_value = AnalysisResponse(
            verdict=TrustVerdict.CAVEATS,
            summary="Still checking...",
            recommended_next_step="validate again",
            next_step_action="validate",
            next_step_factory="AX02",
            autonomous_ok=True,
        )

        result = run_analyst_loop(factory="AX02")

        assert result.iterations == MAX_AUTONOMOUS_STEPS + 1
        assert result.escalated is True

    @patch("operator_core.pl_analyst.validate_factory")
    @patch("operator_core.pl_analyst.call_claude_analysis")
    def test_unsafe_action_escalates(self, mock_claude, mock_validate):
        mock_validate.return_value = _make_pl_result()
        mock_claude.return_value = AnalysisResponse(
            verdict=TrustVerdict.CAVEATS,
            summary="Need to rebuild dashboard.",
            recommended_next_step="Rebuild the dashboard",
            next_step_action="pipeline",
            autonomous_ok=True,
        )

        result = run_analyst_loop(factory="AX02")

        assert result.iterations == 1
        assert result.escalated is True
        assert result.escalation is not None
        assert result.escalation.reason == EscalationReason.MUTATION_REQUIRED


# ── LoopResult Formatting Tests ─────────────────────────────────────────────

class TestLoopResultFormat:
    def test_format_discord_passing(self):
        result = LoopResult(
            factory="AX02",
            iterations=1,
            history=[{"action": "validate", "verdict": "safe_to_use", "autonomous": False}],
            final_verdict=TrustVerdict.SAFE,
            final_summary="AX02 clean.",
            escalated=False,
            total_duration_sec=5.2,
        )
        text = result.format_discord()

        assert "PASS" in text
        assert "AX02" in text
        assert "Valley City" in text
        assert "safe to use" in text

    def test_format_discord_escalated(self):
        escalation = build_escalation(
            EscalationReason.AMBIGUOUS,
            [{"action": "validate", "verdict": "unknown"}],
            None, "AX02",
        )
        result = LoopResult(
            factory="AX02",
            iterations=1,
            history=[{"action": "validate", "verdict": "unknown", "autonomous": False}],
            final_verdict=TrustVerdict.UNKNOWN,
            final_summary="Ambiguous.",
            escalated=True,
            escalation=escalation,
            total_duration_sec=3.1,
        )
        text = result.format_discord()

        assert "FAIL" in text
        assert "Escalation" in text

    def test_to_metadata(self):
        result = LoopResult(
            factory="AX02",
            iterations=2,
            history=[
                {"action": "validate", "verdict": "use_with_caveats", "autonomous": False},
                {"action": "explain_cpoh", "verdict": "safe_to_use", "autonomous": True},
            ],
            final_verdict=TrustVerdict.SAFE,
            final_summary="OK",
            escalated=False,
            autonomous_steps_taken=1,
            total_duration_sec=8.5,
        )
        meta = result.to_metadata()

        assert meta["analyst_factory"] == "AX02"
        assert meta["analyst_iterations"] == 2
        assert meta["analyst_verdict"] == "safe_to_use"
        assert meta["analyst_autonomous_steps"] == 1
        assert len(meta["analyst_history"]) == 2


# ── Command Parsing Tests ──────────────────────────────────────────────────

class TestCommandParsing:
    def test_pl_analyst_no_factory(self):
        cmd = parse_operator_command("!op pl analyst")
        assert cmd.action == "pl_analyst"
        assert cmd.project is None

    def test_pl_analyst_with_factory(self):
        cmd = parse_operator_command("!op pl analyst AX02")
        assert cmd.action == "pl_analyst"
        assert cmd.project == "AX02"

    def test_pl_analyst_pipeline(self):
        cmd = parse_operator_command("!op pl analyst pipeline AX02")
        assert cmd.action == "pl_analyst"
        assert cmd.project == "AX02"
        assert cmd.args.get("initial_action") == "pipeline"

    def test_pl_analyst_validate(self):
        cmd = parse_operator_command("!op pl analyst validate JL01")
        assert cmd.action == "pl_analyst"
        assert cmd.project == "JL01"
        assert cmd.args.get("initial_action") == "validate"

    def test_pl_brief_no_factory(self):
        cmd = parse_operator_command("!op pl brief")
        assert cmd.action == "pl_brief"
        assert cmd.project is None

    def test_pl_brief_with_factory(self):
        cmd = parse_operator_command("!op pl brief HX01")
        assert cmd.action == "pl_brief"
        assert cmd.project == "HX01"

    def test_existing_pl_commands_still_work(self):
        assert parse_operator_command("!op pl status").action == "pl_status"
        assert parse_operator_command("!op pl validate AX02").action == "pl_validate"
        assert parse_operator_command("!op pl pptx AX02").action == "pl_pptx"
        assert parse_operator_command("!op pl explain AX02").action == "pl_explain"
        assert parse_operator_command("!op pl adjustments AX02").action == "pl_adjustments"
        assert parse_operator_command("!op pl morning").action == "pl_morning"
