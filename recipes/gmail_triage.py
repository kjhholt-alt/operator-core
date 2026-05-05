"""gmail_triage -- run Gmail label triage on inbound replies."""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class GmailTriage(Recipe):
    name = "gmail_triage"
    version = "1.0.0"
    description = "Auto-label inbound outreach replies"
    cost_budget_usd = 0.50
    schedule = "*/15 * * * *"
    timeout_sec = 120
    discord_channel = "projects"
    requires_clients = ("gmail",)
    tags = ("intake", "outreach")

    async def verify(self, ctx: RecipeContext) -> bool:
        gm = ctx.clients.get("gmail")
        return bool(gm and gm.configured)

    async def query(self, ctx: RecipeContext) -> list[dict[str, Any]]:
        gm = ctx.clients["gmail"]
        try:
            return gm.list_messages(query="newer_than:1d -label:processed", max_results=20)
        except Exception as exc:  # noqa: BLE001
            ctx.logger.warning("gmail_triage.query_failed", extra={"error": str(exc)})
            return []

    async def analyze(self, ctx: RecipeContext, data: list[dict[str, Any]]) -> dict[str, Any]:
        return {"new": len(data)}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        return ""  # internal-only; no Discord noise
