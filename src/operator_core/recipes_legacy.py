"""Agent recipe registry and routing for Operator V3.

A recipe is a reusable multi-agent pattern the daemon can spawn for a task.
The router selects the best recipe based on task type, project context,
risk level, and historical effectiveness.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ── Models ─────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class AgentDef:
    role: str               # "planner", "coder", "reviewer"
    model: str              # "opus", "sonnet", "haiku"
    tools: list[str]
    system_prompt: str
    parallel: bool = False


@dataclass(frozen=True)
class AgentRecipe:
    name: str
    description: str
    agents: list[AgentDef]
    trigger_conditions: list[str]   # task types this recipe handles
    risk_tier: str = "low"
    autonomous_ok: bool = False
    max_runtime_minutes: int = 30
    fallback_recipe: str | None = None


# ── Built-in recipes ───────────────────────────────────────────────────────────

PLANNER_CODER_REVIEWER = AgentRecipe(
    name="planner-coder-reviewer",
    description="Full feature implementation: plan → code → review",
    agents=[
        AgentDef(
            role="planner",
            model="opus",
            tools=["Read", "Glob", "Grep"],
            system_prompt="You are a senior architect. Plan the implementation, identify files to change, outline the approach. Do NOT write code.",
        ),
        AgentDef(
            role="coder",
            model="sonnet",
            tools=["Read", "Edit", "Write", "Bash", "Glob", "Grep"],
            system_prompt="You are a senior developer. Implement exactly what the planner specified. Write clean, tested code.",
        ),
        AgentDef(
            role="reviewer",
            model="sonnet",
            tools=["Read", "Glob", "Grep"],
            system_prompt="You are a code reviewer. Check for bugs, security issues, and style violations. Be specific and actionable.",
        ),
    ],
    trigger_conditions=["feature", "build", "implement"],
    risk_tier="medium",
    autonomous_ok=False,
    max_runtime_minutes=30,
)

ANALYST_FIXER_VERIFIER = AgentRecipe(
    name="analyst-fixer-verifier",
    description="Bug diagnosis: analyze → fix → verify",
    agents=[
        AgentDef(
            role="analyst",
            model="sonnet",
            tools=["Read", "Glob", "Grep", "Bash"],
            system_prompt="You are a bug analyst. Diagnose the root cause. Read logs, traces, and code. Report findings clearly.",
        ),
        AgentDef(
            role="fixer",
            model="sonnet",
            tools=["Read", "Edit", "Write", "Bash", "Glob", "Grep"],
            system_prompt="You are a developer fixing a diagnosed bug. Apply the minimal correct fix.",
        ),
        AgentDef(
            role="verifier",
            model="haiku",
            tools=["Read", "Glob", "Grep", "Bash"],
            system_prompt="You are a test verifier. Run tests and confirm the fix works without regressions.",
        ),
    ],
    trigger_conditions=["bug", "fix", "debug", "diagnose"],
    risk_tier="medium",
    autonomous_ok=False,
    max_runtime_minutes=20,
)

CODE_REVIEWER = AgentRecipe(
    name="code-reviewer",
    description="PR or code quality review",
    agents=[
        AgentDef(
            role="reviewer",
            model="opus",
            tools=["Read", "Glob", "Grep"],
            system_prompt="You are a senior code reviewer. Check for bugs, security issues, performance problems, and style violations. Be thorough and specific.",
        ),
    ],
    trigger_conditions=["review", "audit", "pr_review"],
    risk_tier="low",
    autonomous_ok=True,
    max_runtime_minutes=15,
)

SPRINT_PLANNER = AgentRecipe(
    name="sprint-planner",
    description="Cross-project sprint recommendation",
    agents=[
        AgentDef(
            role="planner",
            model="opus",
            tools=["Read", "Glob", "Grep"],
            system_prompt="You are a sprint planner. Analyze project states, priorities, and blockers. Recommend the next best sprint with reasoning.",
        ),
    ],
    trigger_conditions=["sprint", "plan", "prioritize", "recommend"],
    risk_tier="low",
    autonomous_ok=True,
    max_runtime_minutes=10,
)

PORTFOLIO_BRIEFER = AgentRecipe(
    name="portfolio-briefer",
    description="Morning briefing across all projects",
    agents=[
        AgentDef(
            role="briefer",
            model="opus",
            tools=["Read", "Glob", "Grep"],
            system_prompt="You are an operations briefer. Summarize the state of all projects. Highlight what matters, what's blocked, and what to work on next.",
        ),
    ],
    trigger_conditions=["brief", "morning", "status_all"],
    risk_tier="low",
    autonomous_ok=True,
    max_runtime_minutes=10,
)

INFRA_DIAGNOSTICIAN = AgentRecipe(
    name="infra-diagnostician",
    description="Deploy or service health diagnosis",
    agents=[
        AgentDef(
            role="diagnostician",
            model="sonnet",
            tools=["Read", "Glob", "Grep", "Bash"],
            system_prompt="You are an infrastructure diagnostician. Check deploy health, logs, and config. Report findings and recommend fixes.",
        ),
    ],
    trigger_conditions=["deploy", "health", "infra", "diagnose_deploy"],
    risk_tier="low",
    autonomous_ok=True,
    max_runtime_minutes=10,
)

PL_ENGINE_ANALYST = AgentRecipe(
    name="pl-engine-analyst",
    description="PL Engine validation and trust checking",
    agents=[
        AgentDef(
            role="analyst",
            model="opus",
            tools=["Read", "Glob", "Grep", "Bash"],
            system_prompt="You are a PL Engine analyst. Run validation checks, analyze results, and produce trust verdicts for factory artifacts.",
        ),
    ],
    trigger_conditions=["pl_validate", "pl_analyze", "pl_trust"],
    risk_tier="low",
    autonomous_ok=True,
    max_runtime_minutes=15,
)

PL_ENGINE_EXPLAINER = AgentRecipe(
    name="pl-engine-explainer",
    description="PL Engine metric explainability",
    agents=[
        AgentDef(
            role="explainer",
            model="sonnet",
            tools=["Read", "Glob", "Grep"],
            system_prompt="You are a PL Engine explainer. Trace metric lineage (CPOH, volume, labor rates) and explain how numbers flow through the pipeline.",
        ),
    ],
    trigger_conditions=["pl_explain", "cpoh", "explain_metric"],
    risk_tier="low",
    autonomous_ok=True,
    max_runtime_minutes=10,
)

OUTREACH_AUDITOR = AgentRecipe(
    name="outreach-auditor",
    description="Email compliance and deliverability audit",
    agents=[
        AgentDef(
            role="auditor",
            model="sonnet",
            tools=["Read", "Glob", "Grep"],
            system_prompt="You are an email compliance auditor. Check for SPF/DKIM, unsubscribe links, CAN-SPAM compliance, webhook security, and deliverability issues.",
        ),
    ],
    trigger_conditions=["outreach", "email_audit", "compliance"],
    risk_tier="low",
    autonomous_ok=True,
    max_runtime_minutes=10,
)

BROWSER_TEST = AgentRecipe(
    name="browser-test",
    description="UI validation via Playwright",
    agents=[
        AgentDef(
            role="tester",
            model="sonnet",
            tools=["Read", "Glob", "Grep", "Bash"],
            system_prompt="You are a UI tester. Use Playwright to navigate deployed pages, check for rendering issues, broken links, and user flow completeness.",
        ),
    ],
    trigger_conditions=["browser_test", "ui_test", "e2e"],
    risk_tier="low",
    autonomous_ok=True,
    max_runtime_minutes=15,
    fallback_recipe="code-reviewer",
)


# ── Registry ───────────────────────────────────────────────────────────────────

ALL_RECIPES: dict[str, AgentRecipe] = {
    r.name: r for r in [
        PLANNER_CODER_REVIEWER,
        ANALYST_FIXER_VERIFIER,
        CODE_REVIEWER,
        SPRINT_PLANNER,
        PORTFOLIO_BRIEFER,
        INFRA_DIAGNOSTICIAN,
        PL_ENGINE_ANALYST,
        PL_ENGINE_EXPLAINER,
        OUTREACH_AUDITOR,
        BROWSER_TEST,
    ]
}

# Map task types to candidate recipes
RECIPE_TASK_MAP: dict[str, list[str]] = {}
for recipe in ALL_RECIPES.values():
    for condition in recipe.trigger_conditions:
        RECIPE_TASK_MAP.setdefault(condition, []).append(recipe.name)


def get_recipe(name: str) -> AgentRecipe | None:
    """Get a recipe by name."""
    return ALL_RECIPES.get(name)


def list_recipes() -> list[AgentRecipe]:
    """List all registered recipes."""
    return list(ALL_RECIPES.values())


def select_recipe(
    task_type: str,
    risk_level: str = "low",
    history: list[dict[str, Any]] | None = None,
) -> AgentRecipe | None:
    """Select the best recipe for a task type and risk level.

    Uses task type matching and historical success rates to rank candidates.
    Returns None if no recipe matches.
    """
    # Find candidates by task type
    candidate_names = RECIPE_TASK_MAP.get(task_type.lower(), [])
    if not candidate_names:
        # Fuzzy match: check if task_type is a substring of any condition
        for condition, names in RECIPE_TASK_MAP.items():
            if task_type.lower() in condition or condition in task_type.lower():
                candidate_names.extend(names)
        candidate_names = list(dict.fromkeys(candidate_names))

    if not candidate_names:
        return SPRINT_PLANNER  # fallback

    candidates = [ALL_RECIPES[n] for n in candidate_names if n in ALL_RECIPES]

    # Filter by risk: high-risk tasks should not use autonomous recipes
    if risk_level == "high":
        non_auto = [r for r in candidates if not r.autonomous_ok]
        if non_auto:
            candidates = non_auto

    # Rank by historical success rate if history provided
    if history:
        scored: list[tuple[AgentRecipe, float]] = []
        for recipe in candidates:
            runs = [h for h in history if h.get("recipe") == recipe.name]
            if runs:
                successes = sum(1 for r in runs if r.get("status") == "complete")
                rate = successes / len(runs)
            else:
                rate = 0.5  # neutral for untested
            scored.append((recipe, rate))
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[0][0]

    return candidates[0]


def format_recipe_list() -> str:
    """Human-readable recipe listing."""
    lines = ["**Agent Recipes** (10 built-in)"]
    for recipe in ALL_RECIPES.values():
        auto = "auto" if recipe.autonomous_ok else "manual"
        agents = ", ".join(a.role for a in recipe.agents)
        lines.append(f"- `{recipe.name}` ({auto}, {recipe.risk_tier}) — {recipe.description} [{agents}]")
    return "\n".join(lines)
