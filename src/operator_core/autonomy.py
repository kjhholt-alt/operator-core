"""Autonomy policy engine for Operator V3.

Defines what can run automatically vs. what requires human approval,
and the escalation triggers that stop autonomous execution.

The tier model:
  Tier 0: Observe — read-only signal collection
  Tier 1: Analyze — Claude interpretation, briefings
  Tier 2: Safe Execute — tests, lints, builds, validations, audits
  Tier 3: Guarded Execute — worktree branches, PRs (dry-run)
  Tier 4: Deploy — push, deploy, merge
  Tier 5: Destructive — delete, force-push, drop data
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Any

from .analysis import AnalysisResponse
from .portfolio import ProjectState


# ── Tier definitions ───────────────────────────────────────────────────────────

TIER_OBSERVE = 0
TIER_ANALYZE = 1
TIER_SAFE_EXECUTE = 2
TIER_GUARDED_EXECUTE = 3
TIER_DEPLOY = 4
TIER_DESTRUCTIVE = 5

TIER_NAMES = {
    0: "Observe",
    1: "Analyze",
    2: "Safe Execute",
    3: "Guarded Execute",
    4: "Deploy",
    5: "Destructive",
}


# ── Per-project autonomy map ──────────────────────────────────────────────────

@dataclass(frozen=True)
class ProjectAutonomy:
    max_tier: int = TIER_OBSERVE
    analysis_auto: bool = False
    deploy_auto: bool = False


DEFAULT_AUTONOMY = ProjectAutonomy(max_tier=TIER_OBSERVE)

AUTONOMY_MAP: dict[str, ProjectAutonomy] = {
    "prospector-pro": ProjectAutonomy(max_tier=TIER_SAFE_EXECUTE, analysis_auto=True),
    "ai-ops-consulting": ProjectAutonomy(max_tier=TIER_SAFE_EXECUTE, analysis_auto=True),
    "ai-voice-receptionist": ProjectAutonomy(max_tier=TIER_SAFE_EXECUTE, analysis_auto=True),
    "deal-brain": ProjectAutonomy(max_tier=TIER_SAFE_EXECUTE, analysis_auto=True),
    "pool-prospector": ProjectAutonomy(max_tier=TIER_SAFE_EXECUTE, analysis_auto=True),
    "pl-engine": ProjectAutonomy(max_tier=TIER_SAFE_EXECUTE, analysis_auto=True),
    "operator-scripts": ProjectAutonomy(max_tier=TIER_GUARDED_EXECUTE, analysis_auto=True),
    "ag-market-pulse": ProjectAutonomy(max_tier=TIER_SAFE_EXECUTE, analysis_auto=True),
    "outdoor-crm": ProjectAutonomy(max_tier=TIER_OBSERVE),
    "municipal-crm": ProjectAutonomy(max_tier=TIER_OBSERVE),
}


def get_autonomy(slug: str) -> ProjectAutonomy:
    """Get autonomy config for a project."""
    return AUTONOMY_MAP.get(slug, DEFAULT_AUTONOMY)


# ── Action classification ──────────────────────────────────────────────────────

# Map action types to their minimum required tier
ACTION_TIERS: dict[str, int] = {
    # Tier 0: Observe
    "health_check": TIER_OBSERVE,
    "git_status": TIER_OBSERVE,
    "read_file": TIER_OBSERVE,
    "collect_signals": TIER_OBSERVE,

    # Tier 1: Analyze
    "analyze": TIER_ANALYZE,
    "brief": TIER_ANALYZE,
    "prioritize": TIER_ANALYZE,
    "portfolio_analyze": TIER_ANALYZE,

    # Tier 2: Safe Execute
    "run_tests": TIER_SAFE_EXECUTE,
    "run_lint": TIER_SAFE_EXECUTE,
    "run_build": TIER_SAFE_EXECUTE,
    "validate": TIER_SAFE_EXECUTE,
    "audit": TIER_SAFE_EXECUTE,
    "explain": TIER_SAFE_EXECUTE,
    "pl_validate": TIER_SAFE_EXECUTE,
    "pl_explain": TIER_SAFE_EXECUTE,
    "pl_pptx": TIER_SAFE_EXECUTE,

    # Tier 3: Guarded Execute
    "create_branch": TIER_GUARDED_EXECUTE,
    "write_file": TIER_GUARDED_EXECUTE,
    "open_pr": TIER_GUARDED_EXECUTE,
    "build": TIER_GUARDED_EXECUTE,

    # Tier 4: Deploy
    "push": TIER_DEPLOY,
    "merge_pr": TIER_DEPLOY,
    "deploy": TIER_DEPLOY,

    # Tier 5: Destructive
    "delete_branch": TIER_DESTRUCTIVE,
    "force_push": TIER_DESTRUCTIVE,
    "drop_table": TIER_DESTRUCTIVE,
}


def classify_action_tier(action: str) -> int:
    """Classify an action into its minimum required tier."""
    return ACTION_TIERS.get(action.lower(), TIER_GUARDED_EXECUTE)


# ── Policy checks ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    reason: str
    tier_required: int
    tier_allowed: int


def check_action(slug: str, action: str) -> PolicyDecision:
    """Check if an action is allowed for a project under the autonomy policy."""
    autonomy = get_autonomy(slug)
    tier_required = classify_action_tier(action)
    tier_allowed = autonomy.max_tier

    if tier_required <= tier_allowed:
        return PolicyDecision(
            allowed=True,
            reason=f"{action} is Tier {tier_required} ({TIER_NAMES.get(tier_required, '?')}), "
                   f"project allows up to Tier {tier_allowed}",
            tier_required=tier_required,
            tier_allowed=tier_allowed,
        )

    return PolicyDecision(
        allowed=False,
        reason=f"{action} requires Tier {tier_required} ({TIER_NAMES.get(tier_required, '?')}), "
               f"but {slug} only allows Tier {tier_allowed} ({TIER_NAMES.get(tier_allowed, '?')})",
        tier_required=tier_required,
        tier_allowed=tier_allowed,
    )


# ── Escalation checks ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class EscalationResult:
    should_escalate: bool
    reasons: list[str]


def check_escalation(
    analysis: AnalysisResponse,
    autonomous_steps_taken: int = 0,
    cost_today_usd: float = 0.0,
) -> EscalationResult:
    """Determine whether the daemon should stop and escalate to a human.

    Triggers:
    1. Analysis requires human input
    2. Confidence below threshold on any high-severity finding
    3. Too many autonomous steps without improvement
    4. Cost ceiling exceeded
    5. Escalation field set by analysis
    """
    reasons: list[str] = []
    cost_ceiling = float(os.environ.get("OPERATOR_COST_CEILING_USD", str(5.0)))

    # 1. Human input required
    if analysis.human_input_required:
        reasons.append("Analysis indicates human input is required")

    # 2. Low confidence on high-severity findings
    for finding in analysis.findings:
        if finding.severity in ("critical", "high") and finding.confidence < 0.6:
            reasons.append(
                f"Low confidence ({finding.confidence:.0%}) on high-severity finding: {finding.title}"
            )
            break

    # 3. Loop convergence failure
    if autonomous_steps_taken >= 3:
        reasons.append(f"3+ autonomous steps taken ({autonomous_steps_taken}) without convergence")

    # 4. Cost ceiling
    if cost_today_usd >= cost_ceiling:
        reasons.append(f"Daily cost ceiling exceeded (${cost_today_usd:.2f} >= ${cost_ceiling:.2f})")

    # 5. Explicit escalation
    if analysis.escalation:
        reasons.append(f"Analysis flagged escalation: {analysis.escalation}")

    return EscalationResult(
        should_escalate=bool(reasons),
        reasons=reasons,
    )


# ── Formatting ─────────────────────────────────────────────────────────────────

def format_policy_summary(slug: str) -> str:
    """Human-readable autonomy summary for a project."""
    autonomy = get_autonomy(slug)
    tier_name = TIER_NAMES.get(autonomy.max_tier, "Unknown")
    auto_analysis = "yes" if autonomy.analysis_auto else "no"
    auto_deploy = "yes" if autonomy.deploy_auto else "no"
    return (
        f"**{slug}** autonomy: Tier {autonomy.max_tier} ({tier_name})\n"
        f"  Auto-analysis: {auto_analysis} | Auto-deploy: {auto_deploy}"
    )


def format_escalation(
    project: str,
    analysis: AnalysisResponse,
    escalation: EscalationResult,
) -> str:
    """Format an escalation message for Discord or IDE."""
    lines = [f"🔶 **ESCALATION** — {project}"]

    if analysis.summary:
        lines.append(f"\n**What happened:**\n{analysis.summary}")

    if analysis.findings:
        lines.append("\n**Findings:**")
        for f in analysis.findings[:5]:
            marker = "🔴" if f.severity in ("critical", "high") else "🟡"
            lines.append(f"  {marker} [{f.severity}] {f.title}")

    if escalation.reasons:
        lines.append("\n**Why escalating:**")
        for reason in escalation.reasons:
            lines.append(f"  - {reason}")

    if analysis.recommended_next_step:
        step = analysis.recommended_next_step
        lines.append(f"\n**Recommended next action:** {step.action}")
        if step.reason:
            lines.append(f"  Reason: {step.reason}")
        if step.command:
            lines.append(f"  Command: `{step.command}`")

    return "\n".join(lines)
