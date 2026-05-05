"""audit_intake -- monitor AI Ops Tier-1 audit intake form."""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class AuditIntake(Recipe):
    name = "audit_intake"
    version = "1.0.0"
    description = "Watch AI Ops audit intake submissions"
    cost_budget_usd = 0.50
    schedule = "*/30 * * * *"
    timeout_sec = 120
    discord_channel = "projects"
    requires_clients = ("supabase", "discord")
    tags = ("intake",)

    async def verify(self, ctx: RecipeContext) -> bool:
        sb = ctx.clients.get("supabase")
        return bool(sb and sb.configured and ctx.clients.get("discord"))

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        sb = ctx.clients["supabase"]
        try:
            rows = sb.select("ai_ops_intake_submissions", limit=50)
        except Exception as exc:  # noqa: BLE001
            ctx.logger.debug("audit_intake.query_failed", extra={"error": str(exc)})
            return {"submissions": []}
        return {"submissions": rows}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        return {"new_count": len(data.get("submissions", []))}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        if not result["new_count"]:
            return ""
        return f"**Audit intake** -- {result['new_count']} pending submissions"
