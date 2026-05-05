"""briefing_eod -- evening wrap-up."""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class BriefingEod(Recipe):
    name = "briefing_eod"
    version = "1.0.0"
    description = "End-of-day wrap"
    cost_budget_usd = 0.50
    schedule = "0 22 * * *"
    timeout_sec = 180
    discord_channel = "projects"
    requires_clients = ("discord",)
    tags = ("daily", "briefing")

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        return {"slot": "eod"}

    async def format(self, ctx: RecipeContext, result: Any) -> str:
        return "**End of day** -- wrapping the loop."
