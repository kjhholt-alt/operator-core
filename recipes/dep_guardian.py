"""dep_guardian -- scan project package manifests for outdated deps.

Stub recipe; the real heavy lifting lives in scripts/dep-guardian.py and
will be ported into ``query`` over a follow-up sprint.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe

from recipes._paths import projects_dir

PROJECTS_DIR = projects_dir()


@register_recipe
class DepGuardian(Recipe):
    name = "dep_guardian"
    version = "1.0.0"
    description = "Scan package manifests for outdated dependencies"
    cost_budget_usd = 0.50
    schedule = "0 8 * * 1"  # Mondays 8am
    timeout_sec = 600
    discord_channel = "code_review"
    requires_clients = ("discord",)
    tags = ("weekly", "deps")

    async def verify(self, ctx: RecipeContext) -> bool:
        return PROJECTS_DIR.exists() and ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        manifests: list[Path] = []
        if PROJECTS_DIR.exists():
            for child in PROJECTS_DIR.iterdir():
                if not child.is_dir():
                    continue
                for name in ("package.json", "requirements.txt", "pyproject.toml"):
                    p = child / name
                    if p.exists():
                        manifests.append(p)
        return {"manifests": [str(p) for p in manifests]}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        return {"manifest_count": len(data.get("manifests", []))}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        return f"**Dep guardian** -- {result['manifest_count']} manifests scanned"
