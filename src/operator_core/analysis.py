"""Claude analysis engine — structured evidence in, findings + next steps out.

The analysis engine interprets structured evidence from the daemon and projects,
producing findings, verdicts, and next-step recommendations. It does NOT guess
from vibes — it reasons over deterministic facts.

Two entry points:
  - ``analyze_project(slug)`` — single-project analysis
  - ``analyze_portfolio(snapshot)`` — cross-project prioritization
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .paths import DATA_DIR
from .portfolio import (
    NextAction,
    PortfolioSnapshot,
    ProjectState,
    collect_project_state,
    snapshot_to_dict,
)
from .adapters import get_adapter


ANALYSIS_LOG_PATH = DATA_DIR / "logs" / "analysis.jsonl"

# Cost ceiling: refuse to run analysis if today's spend exceeds this
COST_CEILING_USD = 5.0


# ── Evidence + response models ─────────────────────────────────────────────────

@dataclass
class EvidencePacket:
    """Structured evidence the daemon provides to Claude for analysis."""
    timestamp: str = ""
    scope: str = "single_project"  # or "portfolio"

    job_context: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    candidate_next_steps: list[dict[str, Any]] = field(default_factory=list)
    risk_policy: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Finding:
    severity: str = "info"       # "critical", "high", "medium", "low", "info"
    category: str = ""           # "test_regression", "deploy_failure", etc.
    title: str = ""
    detail: str = ""
    confidence: float = 0.0
    evidence_refs: list[str] = field(default_factory=list)


@dataclass
class Verdict:
    safe_to_use: bool = True
    reason: str = ""
    trust_level: str = "unknown"
    caveats: list[str] = field(default_factory=list)


@dataclass
class RecommendedStep:
    action: str = ""
    command: str | None = None
    reason: str = ""
    autonomous_ok: bool = False
    risk: str = "low"
    priority: int = 1


@dataclass
class AnalysisResponse:
    """The structured output from Claude analysis."""
    timestamp: str = ""
    findings: list[Finding] = field(default_factory=list)
    verdict: Verdict = field(default_factory=Verdict)
    recommended_next_step: RecommendedStep | None = None
    additional_steps: list[RecommendedStep] = field(default_factory=list)
    escalation: str | None = None
    human_input_required: bool = False
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "findings": [asdict(f) for f in self.findings],
            "verdict": asdict(self.verdict),
            "recommended_next_step": asdict(self.recommended_next_step) if self.recommended_next_step else None,
            "additional_steps": [asdict(s) for s in self.additional_steps],
            "escalation": self.escalation,
            "human_input_required": self.human_input_required,
            "summary": self.summary,
        }


# ── Evidence builders ──────────────────────────────────────────────────────────

def build_project_evidence(state: ProjectState) -> EvidencePacket:
    """Build an evidence packet from a project state."""
    now = datetime.now(timezone.utc).isoformat()
    return EvidencePacket(
        timestamp=now,
        scope="single_project",
        job_context={"project": state.slug, "action": "analyze"},
        evidence={
            "health": state.health,
            "health_details": state.health_details,
            "urgency": state.urgency,
            "urgency_reason": state.urgency_reason,
            "risk_level": state.risk_level,
            "risk_factors": state.risk_factors,
            "trust_level": state.trust_level,
            "blockers": state.blockers,
            "git_dirty": state.git_dirty,
            "commits_ahead": state.commits_ahead,
            "last_commit_age_hours": round(state.last_commit_age_hours, 1),
            "revenue_proximity": state.revenue_proximity,
            "runnable_workflows": state.runnable_workflows,
        },
        risk_policy={
            "max_autonomous_actions": 3,
            "autonomy_tier": get_adapter(state.slug).autonomy_tier if get_adapter(state.slug) else "manual",
        },
    )


def build_portfolio_evidence(snapshot: PortfolioSnapshot) -> EvidencePacket:
    """Build an evidence packet from a full portfolio snapshot."""
    now = datetime.now(timezone.utc).isoformat()
    return EvidencePacket(
        timestamp=now,
        scope="portfolio",
        job_context={"action": "portfolio_analyze"},
        evidence=snapshot_to_dict(snapshot),
        risk_policy={"max_autonomous_actions": 0},  # portfolio analysis is read-only
    )


# ── Local (non-Claude) analysis ────────────────────────────────────────────────
#
# These produce AnalysisResponse objects using deterministic rules —
# no Claude API call needed. Claude-powered analysis can be layered on top
# when ANTHROPIC_API_KEY is available and cost ceiling is not exceeded.

def analyze_project_local(state: ProjectState) -> AnalysisResponse:
    """Produce a deterministic analysis from project state signals."""
    now = datetime.now(timezone.utc).isoformat()
    findings: list[Finding] = []
    steps: list[RecommendedStep] = []

    # Deploy health
    deploy = state.health_details.get("deploy", "unknown")
    if deploy == "unhealthy":
        findings.append(Finding(
            severity="high",
            category="deploy_failure",
            title=f"{state.slug} deploy is unhealthy",
            detail="HTTP health check failed. Service may be down.",
            confidence=0.95,
            evidence_refs=["health_details.deploy"],
        ))
        steps.append(RecommendedStep(
            action="check_deploy_logs",
            reason="Deploy is unhealthy — check logs for errors",
            autonomous_ok=False,
            risk="low",
        ))

    # Git state
    if state.git_dirty:
        findings.append(Finding(
            severity="low",
            category="git_hygiene",
            title=f"{state.slug} has uncommitted changes",
            detail="Working tree is dirty. Consider committing or stashing.",
            confidence=1.0,
            evidence_refs=["git_dirty"],
        ))

    if state.commits_ahead > 5:
        findings.append(Finding(
            severity="medium",
            category="git_hygiene",
            title=f"{state.slug} is {state.commits_ahead} commits ahead",
            detail="Significant divergence from remote. Push or reconcile.",
            confidence=1.0,
            evidence_refs=["commits_ahead"],
        ))

    # Blockers
    for blocker in state.blockers:
        findings.append(Finding(
            severity="high",
            category="blocker",
            title=f"{state.slug} blocked: {blocker}",
            detail=blocker,
            confidence=1.0,
        ))

    # Risk factors
    for risk in state.risk_factors:
        if risk not in [b for b in state.blockers] and "uncommitted" not in risk:
            findings.append(Finding(
                severity="medium",
                category="risk",
                title=f"{state.slug}: {risk}",
                detail=risk,
                confidence=0.8,
            ))

    # Verdict
    if state.health == "red":
        verdict = Verdict(safe_to_use=False, reason="Project health is red", trust_level="low")
    elif state.health == "yellow":
        verdict = Verdict(safe_to_use=True, reason="Minor issues present", trust_level="medium", caveats=state.risk_factors[:3])
    else:
        verdict = Verdict(safe_to_use=True, reason="Project is healthy", trust_level="high")

    # Next step
    recommended = steps[0] if steps else None
    if not recommended and state.runnable_workflows:
        recommended = RecommendedStep(
            action=state.runnable_workflows[0],
            reason="Routine workflow available",
            autonomous_ok=True,
            risk="low",
        )

    # Summary
    n_findings = len(findings)
    high_sev = sum(1 for f in findings if f.severity in ("critical", "high"))
    summary = f"{state.slug}: {n_findings} finding(s), {high_sev} high-severity. Health: {state.health}."

    return AnalysisResponse(
        timestamp=now,
        findings=findings,
        verdict=verdict,
        recommended_next_step=recommended,
        additional_steps=steps[1:],
        human_input_required=bool(state.blockers),
        summary=summary,
    )


def analyze_portfolio_local(snapshot: PortfolioSnapshot) -> AnalysisResponse:
    """Produce cross-project prioritization from portfolio snapshot."""
    now = datetime.now(timezone.utc).isoformat()
    findings: list[Finding] = []
    steps: list[RecommendedStep] = []

    # Critical issues
    for issue in snapshot.critical_issues:
        findings.append(Finding(
            severity="critical",
            category="cross_project",
            title=issue,
            confidence=1.0,
        ))

    # Red health projects
    for slug, state in snapshot.projects.items():
        if state.health == "red":
            findings.append(Finding(
                severity="high",
                category="health",
                title=f"{slug} is red",
                detail=state.urgency_reason or "deploy unhealthy",
                confidence=0.95,
            ))

    # Blocked items
    for blocked in snapshot.blocked_on_human:
        findings.append(Finding(
            severity="medium",
            category="blocked",
            title=f"Blocked: {blocked}",
            confidence=1.0,
        ))

    # Priority recommendations
    for slug in snapshot.best_use_of_time[:3]:
        state = snapshot.projects.get(slug)
        if state:
            steps.append(RecommendedStep(
                action=f"work_on_{slug}",
                reason=state.urgency_reason or f"Urgency: {state.urgency}, Revenue: {state.revenue_proximity}",
                autonomous_ok=False,
                risk="low",
                priority=snapshot.best_use_of_time.index(slug) + 1,
            ))

    # Agent work
    for slug in snapshot.best_agent_work[:3]:
        state = snapshot.projects.get(slug)
        if state and state.runnable_workflows:
            steps.append(RecommendedStep(
                action=f"agent_{state.runnable_workflows[0]}_{slug}",
                reason=f"Autonomous {state.runnable_workflows[0]} available",
                autonomous_ok=True,
                risk="low",
            ))

    verdict = Verdict(
        safe_to_use=True,
        reason=f"Portfolio: {len(snapshot.projects)} projects, {len(snapshot.critical_issues)} critical",
        trust_level="high" if not snapshot.critical_issues else "medium",
    )

    summary_lines = [
        f"**Portfolio Analysis** ({len(snapshot.projects)} projects)",
        f"Top priority: {snapshot.top_priority} — {snapshot.top_priority_reason}",
        f"Revenue closest: {snapshot.revenue_closest}",
        f"Critical issues: {len(snapshot.critical_issues)}",
        f"Blocked on human: {len(snapshot.blocked_on_human)}",
    ]

    return AnalysisResponse(
        timestamp=now,
        findings=findings,
        verdict=verdict,
        recommended_next_step=steps[0] if steps else None,
        additional_steps=steps[1:],
        human_input_required=bool(snapshot.blocked_on_human),
        summary="\n".join(summary_lines),
    )


# ── Logging ────────────────────────────────────────────────────────────────────

def log_analysis(evidence: EvidencePacket, response: AnalysisResponse) -> None:
    """Append analysis record to JSONL log."""
    ANALYSIS_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "scope": evidence.scope,
        "project": evidence.job_context.get("project"),
        "findings_count": len(response.findings),
        "high_severity": sum(1 for f in response.findings if f.severity in ("critical", "high")),
        "verdict_safe": response.verdict.safe_to_use,
        "human_required": response.human_input_required,
        "summary": response.summary[:500],
    }
    with ANALYSIS_LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, default=str) + "\n")
