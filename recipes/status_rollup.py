"""status_rollup -- aggregate status-spec/v1 component states.

Reads ``status-spec.json`` written by every recipe + the legacy
``status_spec_emit`` hook, calls ``read_aggregate()`` from the vendored
status-spec stub, and posts the rolled-up overall state to
``#automations``. This is what the cockpit dashboard polls.
"""

from __future__ import annotations

from typing import Any

from operator_core._vendor import status_spec
from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class StatusRollup(Recipe):
    name = "status_rollup"
    version = "1.0.0"
    description = "Roll up per-component status-spec and post overall state"
    cost_budget_usd = 0.0
    schedule = "*/15 * * * *"
    timeout_sec = 30
    discord_channel = "automations"
    requires_clients = ("discord",)
    tags = ("hourly", "ops")

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        try:
            agg = status_spec.read_aggregate()
        except Exception as exc:  # noqa: BLE001
            ctx.logger.warning("status_rollup.aggregate_failed", extra={"error": str(exc)})
            agg = {"overall": "unknown", "components": {}}
        return agg

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        components = data.get("components") or {}
        bad = [name for name, c in components.items() if c.get("status") not in {"ok", "running"}]
        return {
            "overall": data.get("overall", "unknown"),
            "components": components,
            "bad": bad,
        }

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        lines = [
            f"**Status rollup [{result['overall']}]** -- {len(result['components'])} component(s)",
        ]
        if result["bad"]:
            lines.append(f"Unhealthy: {', '.join(result['bad'][:10])}")
        return "\n".join(lines)
