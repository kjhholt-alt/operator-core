"""watchdog -- liveness check for the operator-core stack itself.

Migrated from operator-scripts/watchdog.py. Reads the aggregate
``status.json`` and alerts when overall is non-OK.
"""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe
from operator_core._vendor.status_spec import read_aggregate


@register_recipe
class Watchdog(Recipe):
    name = "watchdog"
    version = "1.0.0"
    description = "Reads operator status aggregate; alerts when overall != ok"
    cost_budget_usd = 0.0
    schedule = "*/30 * * * *"  # every 30 minutes
    timeout_sec = 60
    discord_channel = "automations"
    requires_clients = ("discord",)
    tags = ("watchdog",)

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        return read_aggregate()

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        overall = data.get("overall", "unknown")
        bad = []
        for name, comp in (data.get("components") or {}).items():
            if comp.get("status") in {"error", "warn"}:
                bad.append((name, comp.get("status"), comp.get("error")))
        return {"overall": overall, "bad": bad}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        if result.get("overall") in {"ok", "unknown"} and not result.get("bad"):
            return ""
        lines = [f"**Watchdog** -- overall={result['overall']}"]
        for name, status, err in result.get("bad", []):
            err_str = f" -- {err}" if err else ""
            lines.append(f"- {status.upper()} {name}{err_str}")
        return "\n".join(lines)
