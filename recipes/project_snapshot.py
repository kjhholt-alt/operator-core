"""project_snapshot -- broad portfolio snapshot for the dashboard."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe

PROJECTS_DIR = Path("C:/Users/Kruz/Desktop/Projects")


@register_recipe
class ProjectSnapshot(Recipe):
    name = "project_snapshot"
    version = "1.0.0"
    description = "Half-hourly project listing snapshot"
    cost_budget_usd = 0.0
    schedule = "*/30 * * * *"
    timeout_sec = 60
    discord_channel = None  # writes to status only, no Discord noise
    requires_clients = ()
    tags = ("snapshot",)

    async def verify(self, ctx: RecipeContext) -> bool:
        return PROJECTS_DIR.exists()

    async def query(self, ctx: RecipeContext) -> list[str]:
        return [c.name for c in PROJECTS_DIR.iterdir() if c.is_dir() and not c.name.startswith(("_", "."))]

    async def analyze(self, ctx: RecipeContext, data: list[str]) -> dict[str, Any]:
        return {"slugs": data, "count": len(data)}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        return ""

    async def post(self, ctx: RecipeContext, message: str) -> bool:
        return False
