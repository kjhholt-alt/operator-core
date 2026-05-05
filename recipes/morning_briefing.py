"""morning_briefing -- daily portfolio briefing posted to #projects.

Migrated from operator-scripts/morning-briefing.py. The raw script crammed
env load, project discovery, todo scan, and Discord post into one file. As
a Recipe it splits cleanly across query/analyze/format/post.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe

PROJECTS_DIR = Path("C:/Users/Kruz/Desktop/Projects")


@register_recipe
class MorningBriefing(Recipe):
    name = "morning_briefing"
    version = "1.0.0"
    description = "Daily status across all active projects"
    cost_budget_usd = 1.00
    schedule = "0 6 * * *"
    timeout_sec = 300
    discord_channel = "projects"
    requires_clients = ("discord",)
    tags = ("daily", "briefing")

    async def verify(self, ctx: RecipeContext) -> bool:
        # Need the projects directory and a Discord adapter wired up.
        if not PROJECTS_DIR.exists():
            ctx.logger.warning("morning_briefing.no_projects_dir", extra={"path": str(PROJECTS_DIR)})
            return False
        discord = ctx.clients.get("discord")
        return discord is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        active = []
        if PROJECTS_DIR.exists():
            for child in sorted(PROJECTS_DIR.iterdir()):
                if not child.is_dir() or child.name.startswith(("_", ".")):
                    continue
                git_dir = child / ".git"
                if git_dir.exists():
                    active.append(child.name)
        return {"projects": active[:30]}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        projects = data.get("projects", [])
        return {
            "summary": f"{len(projects)} active project folders",
            "projects": projects,
        }

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        lines = [f"**Morning briefing** -- {result['summary']}", ""]
        for slug in result["projects"][:20]:
            lines.append(f"- {slug}")
        return "\n".join(lines)
