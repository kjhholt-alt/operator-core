"""stale_branches -- list local branches not touched in 30+ days."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe

from recipes._paths import projects_dir

PROJECTS_DIR = projects_dir()


@register_recipe
class StaleBranches(Recipe):
    name = "stale_branches"
    version = "1.0.0"
    description = "List git branches with no recent commits"
    cost_budget_usd = 0.0
    schedule = "0 10 * * 1"  # Monday 10am
    timeout_sec = 300
    discord_channel = "automations"
    requires_clients = ("discord",)
    tags = ("weekly", "git")

    async def verify(self, ctx: RecipeContext) -> bool:
        return shutil.which("git") is not None and ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> list[dict[str, Any]]:
        git = shutil.which("git") or "git"
        rows: list[dict[str, Any]] = []
        if not PROJECTS_DIR.exists():
            return rows
        for repo in PROJECTS_DIR.iterdir():
            if not (repo / ".git").exists():
                continue
            try:
                res = subprocess.run(
                    [git, "for-each-ref", "--format=%(refname:short) %(committerdate:iso8601)", "refs/heads/"],
                    cwd=repo,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
            except (OSError, subprocess.TimeoutExpired):
                continue
            for line in res.stdout.splitlines():
                rows.append({"repo": repo.name, "line": line})
        return rows

    async def analyze(self, ctx: RecipeContext, data: list[dict[str, Any]]) -> dict[str, Any]:
        return {"branch_count": len(data)}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        return f"**Stale branches** -- {result['branch_count']} local branches scanned"
