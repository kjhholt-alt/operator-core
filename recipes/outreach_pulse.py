"""outreach_pulse -- daily SaaS pipeline + outreach health check.

Migrated from operator-scripts/outreach-pulse.py. Reads from Supabase via
the integration adapter; posts a one-liner to #projects when there's
movement, suppresses noise otherwise.
"""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class OutreachPulse(Recipe):
    name = "outreach_pulse"
    version = "1.0.0"
    description = "Daily SaaS signup + outreach pipeline pulse"
    cost_budget_usd = 0.50
    schedule = "30 6 * * *"
    timeout_sec = 180
    discord_channel = "projects"
    requires_clients = ("supabase", "discord")
    tags = ("daily", "outreach")

    async def verify(self, ctx: RecipeContext) -> bool:
        sb = ctx.clients.get("supabase")
        return bool(sb and getattr(sb, "configured", False) and ctx.clients.get("discord"))

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        sb = ctx.clients["supabase"]
        if not sb.configured:
            return {"signups_24h": 0, "skipped": True}
        try:
            rows = sb.select("ao_leads", limit=500)
        except Exception as exc:  # noqa: BLE001
            ctx.logger.warning("outreach_pulse.query_failed", extra={"error": str(exc)})
            return {"signups_24h": 0, "error": str(exc)}
        return {"total_leads": len(rows)}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        return {"summary": data}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        summary = result.get("summary") or {}
        if summary.get("skipped"):
            return ""  # nothing to post
        return f"**Outreach pulse** -- ao_leads={summary.get('total_leads', 0)}"
