"""outreach_weekly -- Sunday recap of outreach activity."""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class OutreachWeekly(Recipe):
    name = "outreach_weekly"
    version = "1.0.0"
    description = "Sunday recap of outreach activity (replies, sends, suppressions)"
    cost_budget_usd = 1.50
    schedule = "0 18 * * 0"
    timeout_sec = 300
    discord_channel = "projects"
    requires_clients = ("supabase", "discord")
    tags = ("weekly", "outreach")

    async def verify(self, ctx: RecipeContext) -> bool:
        sb = ctx.clients.get("supabase")
        return bool(sb and sb.configured and ctx.clients.get("discord"))

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        sb = ctx.clients["supabase"]
        out: dict[str, Any] = {}
        for table in ("outreach_sends", "outreach_replies", "outreach_suppressions"):
            try:
                out[table] = len(sb.select(table, limit=1000))
            except Exception:  # noqa: BLE001
                out[table] = None
        return out

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        return {"counts": data}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        lines = ["**Outreach weekly**"]
        for k, v in result["counts"].items():
            lines.append(f"- {k}: {v if v is not None else 'unavailable'}")
        return "\n".join(lines)
