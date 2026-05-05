"""strategic_advisor -- weekly Claude-driven priorities recommendation."""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class StrategicAdvisor(Recipe):
    name = "strategic_advisor"
    version = "1.0.0"
    description = "Sunday strategic-priorities recap"
    cost_budget_usd = 5.00
    schedule = "0 19 * * 0"
    timeout_sec = 600
    discord_channel = "projects"
    requires_clients = ("anthropic", "discord")
    tags = ("weekly", "strategy")

    async def verify(self, ctx: RecipeContext) -> bool:
        a = ctx.clients.get("anthropic")
        return bool(a and a.configured and ctx.clients.get("discord"))

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        # Real implementation would call Claude with portfolio context.
        # Stubbed: returns no-op so verify+lifecycle still tested.
        return {"prompt_tokens_estimate": 4000}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        return {"recommendation": "(strategic advisor stub -- AI call disabled)"}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        return f"**Strategic advisor**\n{result['recommendation']}"
