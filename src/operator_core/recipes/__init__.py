"""operator-core recipe framework.

Two layers live behind this name:

1. The new declarative ``Recipe`` framework: lifecycle hooks, cost budget,
   verify, status writes, Discord routing. See ``base``, ``runtime``,
   ``registry``, ``verify``, ``schedule``.

2. The legacy ``AgentRecipe`` registry that the V3 daemon and tests use to
   pick subagent loadouts (planner-coder-reviewer, etc.). Kept here so the
   single ``operator_core.recipes`` import surface stays stable.
"""

from __future__ import annotations

# --- new declarative framework ------------------------------------------------
from .base import Recipe, RecipeContext, RecipeResult, RecipeStatus
from .lifecycle import RecipeError, BudgetExceeded, RecipeTimeout
from .registry import (
    discover_recipes,
    get_registered_recipe,
    list_registered_recipes,
    register_recipe,
)
from .runtime import RecipeRunner, run_recipe

# --- legacy AgentRecipe registry (V3 daemon) ---------------------------------
# Imported lazily/wildcard so existing callers (tests, daemon) don't break.
from ..recipes_legacy import (  # noqa: F401  (re-export)
    ALL_RECIPES,
    AgentDef,
    AgentRecipe,
    BROWSER_TEST,
    CODE_REVIEWER,
    INFRA_DIAGNOSTICIAN,
    OUTREACH_AUDITOR,
    PL_ENGINE_ANALYST,
    PL_ENGINE_EXPLAINER,
    PLANNER_CODER_REVIEWER,
    PORTFOLIO_BRIEFER,
    RECIPE_TASK_MAP,
    SPRINT_PLANNER,
    ANALYST_FIXER_VERIFIER,
    format_recipe_list,
    get_recipe,
    list_recipes,
    select_recipe,
)


__all__ = [
    # framework
    "Recipe",
    "RecipeContext",
    "RecipeResult",
    "RecipeStatus",
    "RecipeError",
    "BudgetExceeded",
    "RecipeTimeout",
    "RecipeRunner",
    "run_recipe",
    "register_recipe",
    "get_registered_recipe",
    "list_registered_recipes",
    "discover_recipes",
    # legacy AgentRecipe surface
    "ALL_RECIPES",
    "AgentDef",
    "AgentRecipe",
    "BROWSER_TEST",
    "CODE_REVIEWER",
    "INFRA_DIAGNOSTICIAN",
    "OUTREACH_AUDITOR",
    "PL_ENGINE_ANALYST",
    "PL_ENGINE_EXPLAINER",
    "PLANNER_CODER_REVIEWER",
    "PORTFOLIO_BRIEFER",
    "RECIPE_TASK_MAP",
    "SPRINT_PLANNER",
    "ANALYST_FIXER_VERIFIER",
    "format_recipe_list",
    "get_recipe",
    "list_recipes",
    "select_recipe",
]
