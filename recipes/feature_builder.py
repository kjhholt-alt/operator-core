"""feature_builder -- on-demand recipe to dispatch a feature build job.

Mostly a thin wrapper; the heavy lifting still lives in V3 worktree jobs.
"""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class FeatureBuilder(Recipe):
    name = "feature_builder"
    version = "1.0.0"
    description = "Dispatch a feature build job (planner -> coder -> reviewer)"
    cost_budget_usd = 5.00
    schedule = None  # on-demand
    timeout_sec = 1800
    discord_channel = "code_review"
    requires_clients = ("discord",)
    tags = ("on-demand",)

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        return {"note": "feature_builder is dispatched manually; see operator-v3 worktree jobs"}

    async def format(self, ctx: RecipeContext, result: Any) -> str:
        return ""
