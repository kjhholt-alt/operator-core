"""commitment_tracker -- nightly check on outstanding commitments file."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe

COMMITMENTS_PATH = Path("C:/Users/Kruz/Desktop/Projects/operator-scripts/commitments")


@register_recipe
class CommitmentTracker(Recipe):
    name = "commitment_tracker"
    version = "1.0.0"
    description = "Nightly check on outstanding commitments"
    cost_budget_usd = 0.0
    schedule = "0 21 * * *"
    timeout_sec = 60
    discord_channel = "projects"
    requires_clients = ("discord",)
    tags = ("daily",)

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        if not COMMITMENTS_PATH.exists():
            return {"open": 0}
        files = list(COMMITMENTS_PATH.glob("*.json"))
        return {"open": len(files)}

    async def format(self, ctx: RecipeContext, result: Any) -> str:
        return f"**Commitments** -- {result.get('open', 0)} open" if result.get("open") else ""
