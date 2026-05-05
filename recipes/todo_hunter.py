"""todo_hunter -- scan project folders for TODO markers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe

PROJECTS_DIR = Path("C:/Users/Kruz/Desktop/Projects")


@register_recipe
class TodoHunter(Recipe):
    name = "todo_hunter"
    version = "1.0.0"
    description = "Count TODO markers across project folders"
    cost_budget_usd = 0.01
    schedule = "0 7 * * *"
    timeout_sec = 120
    discord_channel = "projects"
    requires_clients = ("discord",)
    tags = ("daily", "tooling")

    async def verify(self, ctx: RecipeContext) -> bool:
        return PROJECTS_DIR.exists() and ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, int]:
        counts: dict[str, int] = {}
        for project in sorted(PROJECTS_DIR.iterdir()):
            if not project.is_dir() or project.name.startswith(("_", ".")):
                continue
            n = 0
            for root, dirs, files in os.walk(project):
                # skip noisy dirs
                dirs[:] = [d for d in dirs if d not in {"node_modules", ".git", ".venv", "__pycache__", "dist", "build"}]
                for fname in files:
                    if not fname.endswith((".py", ".ts", ".tsx", ".js", ".jsx", ".md")):
                        continue
                    try:
                        with open(Path(root) / fname, "r", encoding="utf-8", errors="ignore") as fh:
                            for line in fh:
                                if "TODO" in line or "FIXME" in line:
                                    n += 1
                    except OSError:
                        continue
            if n:
                counts[project.name] = n
        return counts

    async def analyze(self, ctx: RecipeContext, data: dict[str, int]) -> dict[str, Any]:
        ranked = sorted(data.items(), key=lambda kv: -kv[1])[:10]
        return {"top": ranked, "total": sum(data.values())}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        if not result["top"]:
            return ""
        lines = [f"**TODO hunter** -- {result['total']} markers across {len(result['top'])} projects"]
        for name, n in result["top"]:
            lines.append(f"- {name}: {n}")
        return "\n".join(lines)
