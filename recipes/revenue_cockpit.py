"""revenue_cockpit -- daily ranked revenue actions across portfolio."""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class RevenueCockpit(Recipe):
    name = "revenue_cockpit"
    version = "1.0.0"
    description = "Daily ranked revenue actions across all projects"
    cost_budget_usd = 2.00
    schedule = "30 6 * * *"
    timeout_sec = 600
    discord_channel = "projects"
    requires_clients = ("anthropic", "supabase", "discord")
    tags = ("daily", "revenue")

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        # The full implementation lives in operator_core.revenue; this recipe
        # currently delegates only when fully wired. For now the query returns
        # an empty placeholder so verify+lifecycle still flow.
        return {"actions": []}

    async def format(self, ctx: RecipeContext, result: Any) -> str:
        actions = result.get("actions") or []
        if not actions:
            return ""
        return f"**Revenue cockpit** -- {len(actions)} ranked actions"
