"""PL Engine Always-On Analyst Loop.

Continuous reasoning-and-action loop for pl-engine work tasks.

Architecture:
    1. Execute a pl-engine operation (validate, pipeline, audit, explain)
    2. Collect structured evidence into an EvidencePacket
    3. Call Claude API for interpretation → AnalysisResponse
    4. Decide: auto-run next safe action, schedule follow-up, or escalate
    5. Repeat until complete, blocked, or risk boundary crossed

Design rule: deterministic execution first, Claude interpretation second.
Claude interprets structured evidence — it does not freestyle diagnoses.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

try:
    import anthropic
except ImportError:
    anthropic = None  # Optional; module remains importable for tests/static use.

from .pl_engine import (
    ACTIVE_FACTORIES,
    ALL_FACTORIES,
    FACTORY_NAMES,
    CheckOutcome,
    PlResult,
    PL_ENGINE_DIR,
    adjustment_status,
    build_pptx,
    explain_cpoh,
    pl_status,
    run_pipeline,
    validate_all,
    validate_factory,
)

logger = logging.getLogger("operator_v3.pl_analyst")

# ── Configuration ───────────────────────────────────────────────────────────

MAX_LOOP_ITERATIONS = 6
MAX_AUTONOMOUS_STEPS = 4
ANALYSIS_MODEL = os.environ.get("PL_ANALYST_MODEL", "claude-sonnet-4-20250514")
ANALYSIS_MAX_TOKENS = 2048
ANALYSIS_TEMPERATURE = 0.2


# ── Enums ───────────────────────────────────────────────────────────────────

class TrustVerdict(str, Enum):
    SAFE = "safe_to_use"
    CAVEATS = "use_with_caveats"
    UNSAFE = "not_safe_to_use"
    UNKNOWN = "unknown"


class EscalationReason(str, Enum):
    HIGH_RISK = "high_risk_action"
    AMBIGUOUS = "ambiguous_result"
    BLOCKED = "blocked_on_external"
    DIVERGENT = "multiple_divergent_paths"
    LOOP_LIMIT = "max_iterations_reached"
    CONFIDENCE_LOW = "confidence_below_threshold"
    MUTATION_REQUIRED = "requires_code_mutation"


class AutonomousAction(str, Enum):
    """Actions the loop can take without human approval."""
    VALIDATE = "validate"
    VALIDATE_ALL = "validate_all"
    AUDIT_PPTX = "audit_pptx"
    EXPLAIN_CPOH = "explain_cpoh"
    ADJUSTMENT_STATUS = "adjustment_status"
    STATUS_CHECK = "status_check"
    COMPARE_BASELINE = "compare_baseline"
    NARROW_VALIDATE = "narrow_validate"


# Actions that are read-only and safe to run autonomously
SAFE_AUTONOMOUS_ACTIONS = {
    AutonomousAction.VALIDATE,
    AutonomousAction.VALIDATE_ALL,
    AutonomousAction.AUDIT_PPTX,
    AutonomousAction.EXPLAIN_CPOH,
    AutonomousAction.ADJUSTMENT_STATUS,
    AutonomousAction.STATUS_CHECK,
    AutonomousAction.COMPARE_BASELINE,
    AutonomousAction.NARROW_VALIDATE,
}


# ── Canonical Path Registry ─────────────────────────────────────────────────

@dataclass(frozen=True)
class PathEntry:
    """Registry entry for a pl-engine entrypoint or module."""
    path: str
    status: str          # "canonical" | "legacy" | "broken" | "reference"
    description: str
    replacement: str = ""  # What to use instead if not canonical


CANONICAL_PATHS: dict[str, PathEntry] = {
    "run.py": PathEntry(
        path="run.py",
        status="canonical",
        description="Main entry point. Supports --validate, --factory, --all, --force, --strict.",
    ),
    "cli.py": PathEntry(
        path="cli.py",
        status="legacy",
        description="Interactive menu. References missing whatif.py. Shells to run_full_pipeline.py directly.",
        replacement="Use run.py instead.",
    ),
    "whatif.py": PathEntry(
        path="whatif.py",
        status="broken",
        description="Does not exist. Referenced by cli.py but never created.",
        replacement="Use run.py --scenario (if implemented) or manual adjustment CSVs.",
    ),
    "scripts/run_full_pipeline.py": PathEntry(
        path="scripts/run_full_pipeline.py",
        status="reference",
        description="Pipeline orchestrator called BY run.py. Do not invoke directly.",
        replacement="Use run.py --factory {CODE}.",
    ),
    "src/pptx_builder.py": PathEntry(
        path="src/pptx_builder.py",
        status="canonical",
        description="PPTX slide builder (2818 LOC). Known issue: volume slide pulls wrong data source.",
    ),
    "src/pptx_output.py": PathEntry(
        path="src/pptx_output.py",
        status="reference",
        description="PPTX output utilities. Used by pptx_builder, not standalone.",
    ),
    "src/pptx_slide_map.py": PathEntry(
        path="src/pptx_slide_map.py",
        status="canonical",
        description="Slide type routing. Maps slide index to data handler.",
    ),
}


def check_canonical_paths() -> list[dict[str, str]]:
    """Check for path issues in the pl-engine directory."""
    warnings: list[dict[str, str]] = []
    for name, entry in CANONICAL_PATHS.items():
        full_path = PL_ENGINE_DIR / entry.path
        if entry.status == "broken":
            if full_path.exists():
                warnings.append({
                    "path": name,
                    "issue": f"Previously broken path now exists — verify if functional",
                    "status": entry.status,
                })
            else:
                warnings.append({
                    "path": name,
                    "issue": f"Missing: {entry.description}",
                    "status": entry.status,
                    "fix": entry.replacement,
                })
        elif entry.status == "legacy":
            if full_path.exists():
                warnings.append({
                    "path": name,
                    "issue": f"Legacy path still present: {entry.description}",
                    "status": entry.status,
                    "fix": entry.replacement,
                })
        elif entry.status == "canonical":
            if not full_path.exists():
                warnings.append({
                    "path": name,
                    "issue": f"Canonical path MISSING: {entry.description}",
                    "status": "critical",
                })
    return warnings


# ── Known Issues Registry ───────────────────────────────────────────────────

KNOWN_ISSUES: list[dict[str, str]] = [
    {
        "id": "P0_DEPRECIATION",
        "severity": "P0",
        "summary": "Depreciation silently $0 in PL27",
        "affected": "src/budget_engine.py, src/labor_engine.py",
        "impact": "Depreciation forecast produces $0 even when template has values",
        "detection": "Check if any depreciation account has $0 across all scenarios",
    },
    {
        "id": "P0_T801_BENEFITS",
        "severity": "P0",
        "summary": "T801 benefits double-counted",
        "affected": "3-CuratedTables/T801_PL2027_Budget_clean.csv",
        "impact": "Benefits rows in real CCs + synthetic CCs = double-count",
        "detection": "T801 only: check for duplicate benefits account rows",
    },
    {
        "id": "P1_GOLDEN_SNAPSHOTS",
        "severity": "P1",
        "summary": "Golden snapshot regression suite broken (11 failures)",
        "affected": "tests/test_golden_snapshots.py",
        "impact": "Suite reads sample_data/ instead of canonical 3-CuratedTables/",
        "detection": "Run pytest tests/test_golden_snapshots.py -q",
    },
    {
        "id": "P1_PPTX_VOLUME",
        "severity": "P1",
        "summary": "PPTX volume slide uses wrong data source",
        "affected": "src/pptx_builder.py:1980-2005",
        "impact": "Slides 3-4 pull normal_output_hours from config instead of product volumes",
        "detection": "Inspect PPTX slides 3-4 for volume data accuracy",
    },
]


# ── Evidence Packet ─────────────────────────────────────────────────────────

@dataclass
class EvidencePacket:
    """Structured evidence emitted by every meaningful pl-engine job.

    This is the contract between deterministic execution and Claude analysis.
    Claude receives this — not raw logs.
    """
    # Identity
    workflow_type: str          # "validate", "pipeline", "pptx", "explain", "audit", "brief"
    factory: str | None = None
    timestamp: str = ""

    # Execution
    commands_run: list[str] = field(default_factory=list)
    exit_codes: list[int] = field(default_factory=list)

    # Structured results
    checks: list[dict[str, Any]] = field(default_factory=list)  # CheckOutcome as dicts
    pass_count: int = 0
    fail_count: int = 0
    warn_count: int = 0

    # Key metrics
    metrics: dict[str, Any] = field(default_factory=dict)

    # Artifacts
    artifacts: list[str] = field(default_factory=list)

    # Drift / anomaly signals
    drift_signals: list[str] = field(default_factory=list)
    known_anomalies: list[str] = field(default_factory=list)

    # Confidence
    confidence: float = 0.0        # 0.0 to 1.0
    confidence_flags: list[str] = field(default_factory=list)

    # Path enforcement
    canonical_path_used: str = ""
    path_warnings: list[dict[str, str]] = field(default_factory=list)

    # Known issues that apply
    active_known_issues: list[str] = field(default_factory=list)

    # Raw tail (last resort for Claude)
    raw_output_tail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v}

    @staticmethod
    def from_pl_result(result: PlResult, commands: list[str] | None = None) -> "EvidencePacket":
        """Build an EvidencePacket from an existing PlResult."""
        checks_data = [
            {"name": c.name, "passed": c.passed, "severity": c.severity, "detail": c.detail}
            for c in result.checks
        ]
        pass_count = sum(1 for c in result.checks if c.passed)
        fail_count = len(result.errors)
        warn_count = len(result.warnings)

        # Compute confidence
        total_checks = len(result.checks)
        if total_checks > 0:
            confidence = pass_count / total_checks
        else:
            confidence = 0.5  # No checks = uncertain

        confidence_flags = []
        if fail_count > 0:
            confidence_flags.append(f"{fail_count} validation error(s)")
        if warn_count > 3:
            confidence_flags.append(f"High warning count ({warn_count})")
        if total_checks == 0:
            confidence_flags.append("No validation checks found in output")
        if result.exit_code != 0 and fail_count == 0:
            confidence_flags.append("Non-zero exit but no parsed validation errors — possible crash")

        # Check for known issues
        active_issues = []
        factory = result.factory or ""
        for issue in KNOWN_ISSUES:
            if issue["id"] == "P0_T801_BENEFITS" and factory != "T801":
                continue
            active_issues.append(issue["id"])

        # Path warnings
        path_warnings = check_canonical_paths()

        return EvidencePacket(
            workflow_type=result.action,
            factory=result.factory,
            timestamp=datetime.now(timezone.utc).isoformat(),
            commands_run=commands or [f"run.py --{result.action} --factory {result.factory}"],
            exit_codes=[result.exit_code],
            checks=checks_data,
            pass_count=pass_count,
            fail_count=fail_count,
            warn_count=warn_count,
            metrics={
                "total_checks": total_checks,
                "error_count": fail_count,
                "warning_count": warn_count,
            },
            artifacts=result.artifacts,
            confidence=round(confidence, 3),
            confidence_flags=confidence_flags,
            canonical_path_used="run.py" if result.action in ("validate", "pipeline", "pptx") else "",
            path_warnings=path_warnings,
            active_known_issues=active_issues,
            raw_output_tail=result.raw_output[-3000:] if result.raw_output else "",
        )


# ── Analysis Contract ───────────────────────────────────────────────────────

@dataclass
class AnalysisRequest:
    """Stable payload from daemon → Claude."""
    job_context: dict[str, Any]    # workflow, factory, iteration, history
    evidence: dict[str, Any]       # EvidencePacket.to_dict()
    candidate_next_steps: list[str]
    risk_policy: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Finding:
    """A single finding from Claude analysis."""
    description: str
    severity: str       # "critical", "high", "medium", "low", "info"
    root_cause: str = ""
    confidence: float = 0.0
    evidence_ref: str = ""  # Which check/metric this relates to


@dataclass
class AnalysisResponse:
    """Stable response schema from Claude → daemon."""
    findings: list[Finding] = field(default_factory=list)
    verdict: TrustVerdict = TrustVerdict.UNKNOWN
    verdict_explanation: str = ""
    recommended_next_step: str = ""
    next_step_action: str = ""          # Maps to AutonomousAction value
    next_step_factory: str = ""         # Factory code for next step
    autonomous_ok: bool = False
    human_input_required: bool = False
    escalation_reason: str = ""
    summary: str = ""                   # One-paragraph analyst summary

    @staticmethod
    def from_claude_json(data: dict[str, Any]) -> "AnalysisResponse":
        """Parse Claude's JSON response into a typed AnalysisResponse."""
        findings = []
        for f in data.get("findings", []):
            findings.append(Finding(
                description=f.get("description", ""),
                severity=f.get("severity", "info"),
                root_cause=f.get("root_cause", ""),
                confidence=f.get("confidence", 0.0),
                evidence_ref=f.get("evidence_ref", ""),
            ))

        verdict_raw = data.get("verdict", "unknown")
        try:
            verdict = TrustVerdict(verdict_raw)
        except ValueError:
            verdict = TrustVerdict.UNKNOWN

        return AnalysisResponse(
            findings=findings,
            verdict=verdict,
            verdict_explanation=data.get("verdict_explanation", ""),
            recommended_next_step=data.get("recommended_next_step", ""),
            next_step_action=data.get("next_step_action", ""),
            next_step_factory=data.get("next_step_factory", ""),
            autonomous_ok=bool(data.get("autonomous_ok", False)),
            human_input_required=bool(data.get("human_input_required", False)),
            escalation_reason=data.get("escalation_reason", ""),
            summary=data.get("summary", ""),
        )


# ── Claude API Caller ──────────────────────────────────────────────────────

ANALYST_SYSTEM_PROMPT = """You are the PL Engine Analyst — an always-on analysis layer for John Deere's overhead budget pipeline (PL2027).

You receive structured evidence packets from deterministic pipeline runs and return structured analysis.

Your job:
1. Interpret the evidence (validation checks, exit codes, metrics, artifacts, drift signals)
2. Identify the most important findings by severity
3. Issue a trust verdict: "safe_to_use", "use_with_caveats", or "not_safe_to_use"
4. Recommend the single best next step
5. Decide if that step is safe to run autonomously (read-only checks only)

Rules:
- Base every finding on evidence in the packet. Do not guess or speculate.
- If checks passed and no anomalies exist, say so clearly. Don't manufacture problems.
- Autonomous next steps are limited to: validate, validate_all, audit_pptx, explain_cpoh, adjustment_status, status_check, compare_baseline, narrow_validate
- Any action that writes files, modifies code, or changes business logic is NOT autonomous — set autonomous_ok=false
- If the evidence is ambiguous or confidence is low, escalate to the human
- Be concise and operational. You are an analyst, not a chatbot.

Known P0 issues (always flag if relevant):
- Depreciation silently $0 in PL27 (all factories)
- T801 benefits double-counted (T801 only)

Known P1 issues:
- Golden snapshot regression suite broken (11 failures)
- PPTX volume slide uses wrong data source (slides 3-4)

Respond with ONLY a JSON object matching this schema:
{
  "findings": [{"description": str, "severity": str, "root_cause": str, "confidence": float, "evidence_ref": str}],
  "verdict": "safe_to_use" | "use_with_caveats" | "not_safe_to_use",
  "verdict_explanation": str,
  "recommended_next_step": str,
  "next_step_action": str,
  "next_step_factory": str,
  "autonomous_ok": bool,
  "human_input_required": bool,
  "escalation_reason": str,
  "summary": str
}"""


def call_claude_analysis(request: AnalysisRequest) -> AnalysisResponse:
    """Call Claude API with structured evidence, return structured analysis.

    Uses the Anthropic Python SDK with the Messages API.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set — cannot run Claude analysis")
        return AnalysisResponse(
            findings=[Finding(
                description="Claude analysis skipped — ANTHROPIC_API_KEY not configured",
                severity="critical",
            )],
            verdict=TrustVerdict.UNKNOWN,
            human_input_required=True,
            escalation_reason="API key not configured",
            summary="Analysis could not run. Set ANTHROPIC_API_KEY.",
        )

    user_message = json.dumps(request.to_dict(), indent=2, default=str)

    if anthropic is None:
        return AnalysisResponse(
            findings=[Finding(
                category="environment",
                detail="anthropic SDK not installed; install operator-core with the analyst extra.",
                severity="critical",
            )],
            verdict=TrustVerdict.UNKNOWN,
            human_input_required=True,
            escalation_reason="anthropic SDK not installed",
            summary="Analysis could not run. Install the `anthropic` package.",
        )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=ANALYSIS_MODEL,
            max_tokens=ANALYSIS_MAX_TOKENS,
            temperature=ANALYSIS_TEMPERATURE,
            system=ANALYST_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )

        # Extract text content
        text = ""
        for block in response.content:
            if hasattr(block, "text"):
                text += block.text

        # Parse JSON from response
        # Handle possible markdown code fences
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            # Remove first and last lines (fences)
            text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
            text = text.strip()

        data = json.loads(text)
        result = AnalysisResponse.from_claude_json(data)

        logger.info(
            "Claude analysis complete: verdict=%s, autonomous_ok=%s, findings=%d",
            result.verdict.value, result.autonomous_ok, len(result.findings),
        )
        return result

    except json.JSONDecodeError as exc:
        logger.warning("Claude returned non-JSON response: %s", exc)
        return AnalysisResponse(
            findings=[Finding(
                description=f"Claude response was not valid JSON: {exc}",
                severity="high",
            )],
            verdict=TrustVerdict.UNKNOWN,
            human_input_required=True,
            escalation_reason="Claude response parse failure",
            summary=f"Analysis returned but could not be parsed. Raw: {text[:500]}",
        )
    except Exception as exc:
        logger.error("Claude API call failed: %s", exc)
        return AnalysisResponse(
            findings=[Finding(
                description=f"Claude API call failed: {exc}",
                severity="critical",
            )],
            verdict=TrustVerdict.UNKNOWN,
            human_input_required=True,
            escalation_reason=f"API error: {type(exc).__name__}",
            summary=f"Analysis could not complete: {exc}",
        )


# ── Escalation Formatting ──────────────────────────────────────────────────

@dataclass
class Escalation:
    """High-quality escalation for the human."""
    what_happened: str
    what_matters: str
    what_was_tried: list[str]
    likely_issue: str
    recommended_actions: list[str]
    reason: EscalationReason

    def format(self) -> str:
        lines = [
            "**PL Engine Analyst — Escalation Required**",
            "",
            f"**What happened:** {self.what_happened}",
            f"**What matters:** {self.what_matters}",
            "",
            "**What was already tried:**",
        ]
        for step in self.what_was_tried:
            lines.append(f"  - {step}")
        lines.extend([
            "",
            f"**Likely issue:** {self.likely_issue}",
            "",
            "**Recommended next actions:**",
        ])
        for i, action in enumerate(self.recommended_actions, 1):
            lines.append(f"  {i}. {action}")
        lines.append(f"\nReason for escalation: {self.reason.value}")
        return "\n".join(lines)


def build_escalation(
    reason: EscalationReason,
    history: list[dict[str, Any]],
    last_analysis: AnalysisResponse | None,
    factory: str | None,
) -> Escalation:
    """Build a high-quality escalation from loop history."""
    steps_tried = []
    for step in history:
        action = step.get("action", "unknown")
        verdict = step.get("verdict", "")
        steps_tried.append(f"{action}: {verdict}")

    what_happened = f"Analyst loop ran {len(history)} iteration(s)"
    if factory:
        what_happened += f" for {factory} ({FACTORY_NAMES.get(factory, '')})"

    what_matters = "Could not reach a definitive conclusion autonomously."
    likely_issue = "Unknown"
    recommended_actions = ["Review the loop history below", "Run manually: `python run.py --validate --factory {}`".format(factory or "AX02")]

    if last_analysis:
        if last_analysis.findings:
            top = last_analysis.findings[0]
            what_matters = top.description
            likely_issue = top.root_cause or "See findings"
        if last_analysis.escalation_reason:
            likely_issue = last_analysis.escalation_reason
        recommended_actions = []
        if last_analysis.recommended_next_step:
            recommended_actions.append(last_analysis.recommended_next_step)
        recommended_actions.append("Check raw validation output in the daemon logs")
        recommended_actions.append("Run `!op pl status` for current state")

    if reason == EscalationReason.LOOP_LIMIT:
        what_matters = f"Loop hit {MAX_LOOP_ITERATIONS} iterations without converging."
    elif reason == EscalationReason.CONFIDENCE_LOW:
        what_matters = "Analysis confidence is too low to proceed autonomously."
    elif reason == EscalationReason.MUTATION_REQUIRED:
        what_matters = "Next step requires modifying files or business logic — human approval needed."

    return Escalation(
        what_happened=what_happened,
        what_matters=what_matters,
        what_was_tried=steps_tried or ["No steps completed"],
        likely_issue=likely_issue,
        recommended_actions=recommended_actions or ["Check daemon logs"],
        reason=reason,
    )


# ── Autonomous Step Executor ────────────────────────────────────────────────

def execute_autonomous_step(action: str, factory: str | None) -> PlResult | None:
    """Execute a safe autonomous action. Returns None if action is not allowed."""
    try:
        action_enum = AutonomousAction(action)
    except ValueError:
        logger.warning("Unknown autonomous action: %s — skipping", action)
        return None

    if action_enum not in SAFE_AUTONOMOUS_ACTIONS:
        logger.warning("Action %s is not in safe set — skipping", action)
        return None

    logger.info("Executing autonomous step: %s factory=%s", action, factory)

    if action_enum == AutonomousAction.VALIDATE:
        if not factory:
            return None
        return validate_factory(factory)

    if action_enum == AutonomousAction.VALIDATE_ALL:
        return validate_all()

    if action_enum == AutonomousAction.EXPLAIN_CPOH:
        if not factory:
            return None
        return explain_cpoh(factory)

    if action_enum == AutonomousAction.ADJUSTMENT_STATUS:
        if not factory:
            return None
        return adjustment_status(factory)

    if action_enum == AutonomousAction.STATUS_CHECK:
        return pl_status()

    if action_enum == AutonomousAction.AUDIT_PPTX:
        # Run the PPTX audit script if it exists
        from .pl_engine import _python, _run
        script = PL_ENGINE_DIR / "scripts" / "audit_pptx_output.py"
        if not script.exists():
            return PlResult(
                action="audit_pptx",
                factory=factory,
                ok=False,
                exit_code=127,
                summary="audit_pptx_output.py not found",
                verdict="Audit script not available",
            )
        args = [_python(), str(script)]
        if factory:
            args.extend(["--factory", factory])
        exit_code, raw = _run(args, timeout=120)
        return PlResult(
            action="audit_pptx",
            factory=factory,
            ok=exit_code == 0,
            exit_code=exit_code,
            summary=raw.strip()[:2000] if exit_code == 0 else f"Audit failed (exit {exit_code})",
            raw_output=raw[-6000:],
            verdict=f"PPTX audit {'passed' if exit_code == 0 else 'FAILED'}",
        )

    if action_enum in (AutonomousAction.COMPARE_BASELINE, AutonomousAction.NARROW_VALIDATE):
        # These use validate with strict mode as a proxy
        if not factory:
            return None
        return validate_factory(factory, strict=True)

    return None


# ── Analyst Loop Controller ─────────────────────────────────────────────────

@dataclass
class LoopResult:
    """Final result of an analyst loop run."""
    factory: str | None
    iterations: int
    history: list[dict[str, Any]]
    final_verdict: TrustVerdict
    final_summary: str
    escalated: bool
    escalation: Escalation | None = None
    autonomous_steps_taken: int = 0
    total_duration_sec: float = 0.0

    def to_metadata(self) -> dict[str, Any]:
        """Convert to job metadata for storage."""
        result: dict[str, Any] = {
            "analyst_factory": self.factory,
            "analyst_iterations": self.iterations,
            "analyst_verdict": self.final_verdict.value,
            "analyst_summary": self.final_summary,
            "analyst_escalated": self.escalated,
            "analyst_autonomous_steps": self.autonomous_steps_taken,
            "analyst_duration_sec": round(self.total_duration_sec, 2),
        }
        if self.escalation:
            result["analyst_escalation"] = self.escalation.format()
        # Compact history — just action + verdict per step
        result["analyst_history"] = [
            {"action": h.get("action"), "verdict": h.get("verdict"), "autonomous": h.get("autonomous", False)}
            for h in self.history
        ]
        return result

    def format_discord(self) -> str:
        """Format for Discord output."""
        lines = []
        marker = "PASS" if self.final_verdict == TrustVerdict.SAFE else (
            "WARN" if self.final_verdict == TrustVerdict.CAVEATS else "FAIL"
        )
        factory_label = f" {self.factory} ({FACTORY_NAMES.get(self.factory or '', '')})" if self.factory else ""

        lines.append(f"**PL Engine Analyst{factory_label}** [{marker}]")
        lines.append("")
        lines.append(self.final_summary)
        lines.append("")
        lines.append(f"Verdict: **{self.final_verdict.value.replace('_', ' ')}**")
        lines.append(f"Iterations: {self.iterations} | Autonomous steps: {self.autonomous_steps_taken} | Duration: {self.total_duration_sec:.1f}s")

        if self.escalated and self.escalation:
            lines.append("")
            lines.append(self.escalation.format())

        if self.history:
            lines.append("")
            lines.append("Loop trace:")
            for i, step in enumerate(self.history, 1):
                auto = " (auto)" if step.get("autonomous") else ""
                lines.append(f"  {i}. {step.get('action', '?')}: {step.get('verdict', '?')}{auto}")

        return "\n".join(lines)


def run_analyst_loop(
    factory: str | None = None,
    initial_action: str = "validate",
    dry_run: bool = False,
) -> LoopResult:
    """Run the always-on analyst loop for a pl-engine workflow.

    Args:
        factory: Factory code (e.g. "AX02"). None for all-factory operations.
        initial_action: Starting action — usually "validate" or "status_check".
        dry_run: If True, skip Claude API calls and autonomous steps.

    Returns:
        LoopResult with full history, verdict, and optional escalation.
    """
    start_time = time.monotonic()
    history: list[dict[str, Any]] = []
    autonomous_steps = 0
    current_action = initial_action
    current_factory = factory

    logger.info("Analyst loop starting: factory=%s, action=%s, dry_run=%s", factory, initial_action, dry_run)

    for iteration in range(1, MAX_LOOP_ITERATIONS + 1):
        logger.info("Analyst loop iteration %d: action=%s, factory=%s", iteration, current_action, current_factory)

        # ── Step 1: Execute ──────────────────────────────────────────────
        pl_result = _execute_step(current_action, current_factory)
        if pl_result is None:
            history.append({
                "action": current_action,
                "iteration": iteration,
                "verdict": "skipped — action not executable",
                "autonomous": iteration > 1,
            })
            break

        # ── Step 2: Collect evidence ─────────────────────────────────────
        evidence = EvidencePacket.from_pl_result(pl_result)

        # ── Step 3: Call Claude for analysis ─────────────────────────────
        if dry_run:
            # Dry run: produce a synthetic analysis
            analysis = _dry_run_analysis(pl_result, evidence)
        else:
            request = _build_analysis_request(
                evidence=evidence,
                iteration=iteration,
                history=history,
                factory=current_factory,
            )
            analysis = call_claude_analysis(request)

        # Record this iteration
        step_record = {
            "action": current_action,
            "factory": current_factory,
            "iteration": iteration,
            "verdict": analysis.verdict.value,
            "summary": analysis.summary[:500],
            "autonomous": iteration > 1,
            "findings_count": len(analysis.findings),
            "autonomous_ok": analysis.autonomous_ok,
            "human_input_required": analysis.human_input_required,
        }
        history.append(step_record)

        # ── Step 4: Decide next step ─────────────────────────────────────

        # Check for escalation triggers
        escalation_reason = _check_escalation(analysis, iteration, autonomous_steps)
        if escalation_reason:
            escalation = build_escalation(escalation_reason, history, analysis, current_factory)
            duration = time.monotonic() - start_time
            return LoopResult(
                factory=factory,
                iterations=iteration,
                history=history,
                final_verdict=analysis.verdict,
                final_summary=analysis.summary or "Escalated to human.",
                escalated=True,
                escalation=escalation,
                autonomous_steps_taken=autonomous_steps,
                total_duration_sec=duration,
            )

        # If no more steps needed, we're done
        if not analysis.recommended_next_step or not analysis.autonomous_ok:
            duration = time.monotonic() - start_time
            return LoopResult(
                factory=factory,
                iterations=iteration,
                history=history,
                final_verdict=analysis.verdict,
                final_summary=analysis.summary or f"Analysis complete after {iteration} iteration(s).",
                escalated=False,
                autonomous_steps_taken=autonomous_steps,
                total_duration_sec=duration,
            )

        # ── Step 5: Execute autonomous follow-up ─────────────────────────
        next_action = analysis.next_step_action
        next_factory = analysis.next_step_factory or current_factory

        # Validate that it's a safe action
        try:
            action_enum = AutonomousAction(next_action)
            if action_enum not in SAFE_AUTONOMOUS_ACTIONS:
                raise ValueError(f"Not in safe set: {next_action}")
        except ValueError:
            # Claude recommended something outside our safe set — escalate
            logger.warning("Claude recommended non-safe action: %s — stopping", next_action)
            duration = time.monotonic() - start_time
            return LoopResult(
                factory=factory,
                iterations=iteration,
                history=history,
                final_verdict=analysis.verdict,
                final_summary=analysis.summary + f"\n\nRecommended non-autonomous action: {analysis.recommended_next_step}",
                escalated=False,
                autonomous_steps_taken=autonomous_steps,
                total_duration_sec=duration,
            )

        current_action = next_action
        current_factory = next_factory
        autonomous_steps += 1
        logger.info("Chaining autonomous step %d: %s factory=%s", autonomous_steps, next_action, next_factory)

    # Hit max iterations
    duration = time.monotonic() - start_time
    escalation = build_escalation(EscalationReason.LOOP_LIMIT, history, None, factory)
    last_verdict = TrustVerdict.UNKNOWN
    last_summary = f"Loop hit max iterations ({MAX_LOOP_ITERATIONS})"
    if history:
        try:
            last_verdict = TrustVerdict(history[-1].get("verdict", "unknown"))
        except ValueError:
            pass

    return LoopResult(
        factory=factory,
        iterations=MAX_LOOP_ITERATIONS,
        history=history,
        final_verdict=last_verdict,
        final_summary=last_summary,
        escalated=True,
        escalation=escalation,
        autonomous_steps_taken=autonomous_steps,
        total_duration_sec=duration,
    )


# ── Briefing Mode ───────────────────────────────────────────────────────────

def run_analyst_brief(factory: str | None = None) -> LoopResult:
    """Quick analyst briefing — validate + analyze, no chaining.

    Answers: "What's the current state of {factory}?"
    """
    start_time = time.monotonic()

    if factory:
        pl_result = validate_factory(factory)
    else:
        pl_result = validate_all()

    evidence = EvidencePacket.from_pl_result(pl_result)

    # For briefing, ask Claude for a status summary oriented to the analyst
    request = AnalysisRequest(
        job_context={
            "mode": "briefing",
            "factory": factory,
            "factories": ACTIVE_FACTORIES if not factory else [factory],
            "question": f"What is the current state of {'factory ' + factory if factory else 'all active factories'}? What is safe to use, what is blocked, what are the top risks?",
        },
        evidence=evidence.to_dict(),
        candidate_next_steps=[],
        risk_policy={"mode": "briefing", "autonomous_ok": False},
    )

    analysis = call_claude_analysis(request)

    duration = time.monotonic() - start_time
    return LoopResult(
        factory=factory,
        iterations=1,
        history=[{
            "action": "brief",
            "factory": factory,
            "iteration": 1,
            "verdict": analysis.verdict.value,
            "summary": analysis.summary[:500],
            "autonomous": False,
        }],
        final_verdict=analysis.verdict,
        final_summary=analysis.summary or "Briefing complete.",
        escalated=False,
        autonomous_steps_taken=0,
        total_duration_sec=duration,
    )


# ── Internal Helpers ────────────────────────────────────────────────────────

def _execute_step(action: str, factory: str | None) -> PlResult | None:
    """Execute a pl-engine step by action name."""
    try:
        action_enum = AutonomousAction(action)
    except ValueError:
        # Direct action names from pl_engine module
        pass

    if action in ("validate", "narrow_validate"):
        return validate_factory(factory) if factory else validate_all()
    if action == "validate_all":
        return validate_all()
    if action == "status_check":
        return pl_status()
    if action == "explain_cpoh":
        return explain_cpoh(factory) if factory else None
    if action == "adjustment_status":
        return adjustment_status(factory) if factory else None
    if action == "audit_pptx":
        return execute_autonomous_step("audit_pptx", factory)
    if action == "compare_baseline":
        return validate_factory(factory, strict=True) if factory else None
    if action == "pipeline":
        return run_pipeline(factory) if factory else None
    if action == "pptx":
        return build_pptx(factory) if factory else None

    logger.warning("Unknown action: %s", action)
    return None


def _build_analysis_request(
    evidence: EvidencePacket,
    iteration: int,
    history: list[dict[str, Any]],
    factory: str | None,
) -> AnalysisRequest:
    """Build the analysis request payload for Claude."""
    # Determine candidate next steps based on current state
    candidates = []
    if evidence.fail_count > 0:
        candidates.append("validate — rerun validation to confirm failures")
        candidates.append("explain_cpoh — get CPOH explainability if relevant")
    if evidence.workflow_type in ("pipeline", "pptx") and evidence.artifacts:
        candidates.append("audit_pptx — audit generated PPTX slides")
    if evidence.warn_count > 0:
        candidates.append("narrow_validate — run strict validation to surface warnings as errors")
    candidates.append("status_check — get overall factory status")
    if factory:
        candidates.append("adjustment_status — check manual adjustments")

    return AnalysisRequest(
        job_context={
            "factory": factory,
            "factory_name": FACTORY_NAMES.get(factory or "", ""),
            "iteration": iteration,
            "max_iterations": MAX_LOOP_ITERATIONS,
            "history": [
                {"action": h.get("action"), "verdict": h.get("verdict")}
                for h in history
            ],
            "active_factories": ACTIVE_FACTORIES,
            "known_issues": KNOWN_ISSUES,
        },
        evidence=evidence.to_dict(),
        candidate_next_steps=candidates,
        risk_policy={
            "max_autonomous_steps": MAX_AUTONOMOUS_STEPS,
            "safe_actions": [a.value for a in SAFE_AUTONOMOUS_ACTIONS],
            "never_autonomous": ["pipeline", "pptx", "modify_code", "delete_files", "write_config"],
            "confidence_threshold": 0.7,
        },
    )


def _check_escalation(
    analysis: AnalysisResponse,
    iteration: int,
    autonomous_steps: int,
) -> EscalationReason | None:
    """Check if we should escalate instead of continuing."""
    if analysis.human_input_required:
        if analysis.escalation_reason:
            # Try to map to our enum
            for reason in EscalationReason:
                if reason.value in analysis.escalation_reason.lower():
                    return reason
        return EscalationReason.AMBIGUOUS

    if iteration >= MAX_LOOP_ITERATIONS:
        return EscalationReason.LOOP_LIMIT

    if autonomous_steps >= MAX_AUTONOMOUS_STEPS:
        return EscalationReason.LOOP_LIMIT

    # If Claude says next step requires mutation
    next_action = analysis.next_step_action
    if next_action and next_action not in {a.value for a in SAFE_AUTONOMOUS_ACTIONS}:
        return EscalationReason.MUTATION_REQUIRED

    return None


def _dry_run_analysis(pl_result: PlResult, evidence: EvidencePacket) -> AnalysisResponse:
    """Produce a synthetic analysis without calling Claude. For testing/dry-run."""
    findings = []
    if evidence.fail_count > 0:
        findings.append(Finding(
            description=f"{evidence.fail_count} validation error(s) detected",
            severity="high",
            confidence=0.9,
        ))
    if evidence.warn_count > 0:
        findings.append(Finding(
            description=f"{evidence.warn_count} warning(s) detected",
            severity="medium",
            confidence=0.8,
        ))
    for issue_id in evidence.active_known_issues:
        issue = next((i for i in KNOWN_ISSUES if i["id"] == issue_id), None)
        if issue:
            findings.append(Finding(
                description=f"Known issue active: {issue['summary']}",
                severity=issue["severity"].lower(),
                confidence=1.0,
            ))

    if evidence.fail_count > 0:
        verdict = TrustVerdict.UNSAFE
    elif evidence.warn_count > 3 or evidence.active_known_issues:
        verdict = TrustVerdict.CAVEATS
    elif pl_result.ok:
        verdict = TrustVerdict.SAFE
    else:
        verdict = TrustVerdict.UNKNOWN

    return AnalysisResponse(
        findings=findings,
        verdict=verdict,
        verdict_explanation=f"Dry-run analysis: {evidence.pass_count} passed, {evidence.fail_count} failed, {evidence.warn_count} warnings",
        recommended_next_step="",
        autonomous_ok=False,
        human_input_required=evidence.fail_count > 0,
        summary=f"[DRY RUN] {pl_result.verdict or 'Analysis complete'}. {len(findings)} finding(s).",
    )
