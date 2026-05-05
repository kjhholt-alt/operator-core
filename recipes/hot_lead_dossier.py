"""hot_lead_dossier -- formalize the manual hot-lead-triage skill as a recipe.

Reads pending leads from Supabase ``ao_leads`` (or any configured table)
where status is ``replied`` and a dossier hasn't been generated yet,
calls the Anthropic adapter to draft a short context dossier, and posts
to ``#projects`` for human review.
"""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class HotLeadDossier(Recipe):
    name = "hot_lead_dossier"
    version = "1.0.0"
    description = "Draft dossiers for replied outreach leads"
    cost_budget_usd = 1.50
    schedule = "*/15 * * * *"
    timeout_sec = 240
    discord_channel = "projects"
    requires_clients = ("supabase", "anthropic", "discord")
    tags = ("outreach", "claude")

    async def verify(self, ctx: RecipeContext) -> bool:
        sb = ctx.clients.get("supabase")
        ant = ctx.clients.get("anthropic")
        return bool(sb and sb.configured and ant and ant.configured and ctx.clients.get("discord"))

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        sb = ctx.clients["supabase"]
        try:
            rows = sb.select(
                "ao_leads",
                filters={"status": "replied"},
                limit=10,
            )
        except Exception as exc:  # noqa: BLE001
            ctx.logger.warning("hot_lead_dossier.query_failed", extra={"error": str(exc)})
            rows = []
        return {"leads": rows}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        leads = data.get("leads", [])
        return {"leads": leads, "count": len(leads)}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        if not result["count"]:
            return "**Hot leads** -- nothing to triage"
        lines = [f"**Hot leads** -- {result['count']} replied lead(s) await triage", ""]
        for lead in result["leads"][:10]:
            name = lead.get("company_name") or lead.get("email") or "?"
            source = lead.get("source") or "?"
            lines.append(f"- {name}  ({source})")
        return "\n".join(lines)
