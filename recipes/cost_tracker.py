"""cost_tracker -- weekly summary of recipe spend from events stream."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe
from operator_core._vendor.events_ndjson import read_events


@register_recipe
class CostTracker(Recipe):
    name = "cost_tracker"
    version = "1.0.0"
    description = "Weekly cost roll-up across all recipes"
    cost_budget_usd = 0.0
    schedule = "0 9 * * 0"  # Sunday 9am
    timeout_sec = 60
    discord_channel = "automations"
    requires_clients = ("discord",)
    tags = ("weekly",)

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, float]:
        events = read_events("cost", limit=10_000)
        by_recipe: dict[str, float] = defaultdict(float)
        for evt in events:
            recipe = evt.get("recipe") or "unknown"
            amount = float(evt.get("amount_usd") or 0.0)
            by_recipe[recipe] += amount
        return dict(by_recipe)

    async def analyze(self, ctx: RecipeContext, data: dict[str, float]) -> dict[str, Any]:
        ranked = sorted(data.items(), key=lambda kv: -kv[1])
        total = sum(data.values())
        return {"ranked": ranked, "total": total}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        lines = [f"**Cost report** -- ${result['total']:.4f} total"]
        for recipe, amt in result["ranked"][:15]:
            lines.append(f"- {recipe}: ${amt:.4f}")
        return "\n".join(lines)
