"""sprint_report -- daily sprint progress digest from STATUS.md files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe

PROJECTS_DIR = Path("C:/Users/Kruz/Desktop/Projects")
STATUS_FILE = "STATUS.md"
MAX_BYTES = 4000


@register_recipe
class SprintReport(Recipe):
    name = "sprint_report"
    version = "1.0.0"
    description = "Roll up STATUS.md heads across active projects"
    cost_budget_usd = 0.0
    schedule = "0 17 * * *"  # 5pm daily
    timeout_sec = 120
    discord_channel = "projects"
    requires_clients = ("discord",)
    tags = ("daily", "briefing")

    async def verify(self, ctx: RecipeContext) -> bool:
        return PROJECTS_DIR.exists() and ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        rows: list[dict[str, Any]] = []
        if not PROJECTS_DIR.exists():
            return {"rows": rows}
        for proj in sorted(PROJECTS_DIR.iterdir()):
            if not proj.is_dir():
                continue
            status = proj / STATUS_FILE
            if not status.exists():
                continue
            try:
                head = status.read_text(encoding="utf-8", errors="replace")[:MAX_BYTES]
            except OSError:
                continue
            first_line = next((ln for ln in head.splitlines() if ln.strip()), "")
            rows.append({"project": proj.name, "first_line": first_line})
        return {"rows": rows}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        return {"rows": data["rows"], "count": len(data["rows"])}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        lines = [f"**Sprint report** -- {result['count']} STATUS.md files", ""]
        for row in result["rows"][:25]:
            lines.append(f"- **{row['project']}**: {row['first_line'][:120]}")
        return "\n".join(lines)
