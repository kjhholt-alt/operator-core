"""discord_health -- verify every configured Discord webhook responds.

A 0-cost ops recipe that probes each ``DISCORD_*_WEBHOOK_URL`` env var
with a HEAD-style noop and reports any that return non-2xx.
"""

from __future__ import annotations

import json
import os
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from operator_core.recipes import Recipe, RecipeContext, register_recipe

WEBHOOK_VARS = (
    "DISCORD_PROJECTS_WEBHOOK_URL",
    "DISCORD_CODE_REVIEW_WEBHOOK_URL",
    "DISCORD_DEPLOYS_WEBHOOK_URL",
    "DISCORD_AUTOMATIONS_WEBHOOK_URL",
    "DISCORD_WEBHOOK_URL",
)


@register_recipe
class DiscordHealth(Recipe):
    name = "discord_health"
    version = "1.0.0"
    description = "Probe every configured Discord webhook for reachability"
    cost_budget_usd = 0.0
    schedule = "0 7 * * 1"  # Monday 7am
    timeout_sec = 60
    discord_channel = "automations"
    requires_clients = ("discord",)
    tags = ("weekly", "ops")

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        for var in WEBHOOK_VARS:
            url = os.environ.get(var)
            if not url:
                results.append({"var": var, "status": "unset"})
                continue
            try:
                req = Request(url, method="GET", headers={"User-Agent": "OperatorRecipe/1.0"})
                with urlopen(req, timeout=10) as resp:
                    results.append({"var": var, "status": resp.status})
            except URLError as exc:
                results.append({"var": var, "status": f"error: {exc}"})
        return {"results": results}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        results = data["results"]
        bad = [r for r in results if not (isinstance(r["status"], int) and 200 <= r["status"] < 300)]
        return {"results": results, "bad_count": len(bad)}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        lines = [f"**Discord health** -- {result['bad_count']} unhealthy webhook(s)"]
        for r in result["results"]:
            lines.append(f"- {r['var']}: {r['status']}")
        return "\n".join(lines)
