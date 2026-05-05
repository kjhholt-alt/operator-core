"""marketing_pulse -- daily SaaS pipeline + outreach metrics."""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class MarketingPulse(Recipe):
    name = "marketing_pulse"
    version = "1.0.0"
    description = "SaaS pipeline + outreach metrics digest"
    cost_budget_usd = 3.00
    schedule = "30 6 * * *"
    timeout_sec = 300
    discord_channel = "projects"
    requires_clients = ("supabase", "discord")
    tags = ("daily", "marketing")

    async def verify(self, ctx: RecipeContext) -> bool:
        sb = ctx.clients.get("supabase")
        return bool(sb and sb.configured and ctx.clients.get("discord"))

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        sb = ctx.clients["supabase"]
        out: dict[str, Any] = {}
        for table in ("ao_leads", "pp_leads", "dealbrain_reports"):
            try:
                rows = sb.select(table, limit=1000)
                out[table] = len(rows)
            except Exception:  # noqa: BLE001
                out[table] = None
        return out

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        return {"counts": data}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        lines = ["**Marketing pulse**"]
        for k, v in result["counts"].items():
            lines.append(f"- {k}: {v if v is not None else 'unavailable'}")
        return "\n".join(lines)
