"""git_drift_audit -- list projects with uncommitted changes.

Surfaces work that's stranded in the working tree across the entire
projects root. Useful as a nightly safety net before sleep.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe

PROJECTS_DIR = Path("C:/Users/Kruz/Desktop/Projects")


@register_recipe
class GitDriftAudit(Recipe):
    name = "git_drift_audit"
    version = "1.0.0"
    description = "List projects with uncommitted git working trees"
    cost_budget_usd = 0.0
    schedule = "0 22 * * *"  # 10pm nightly
    timeout_sec = 180
    discord_channel = "automations"
    requires_clients = ("discord",)
    tags = ("nightly", "git", "safety")

    async def verify(self, ctx: RecipeContext) -> bool:
        return shutil.which("git") is not None and ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        git = shutil.which("git") or "git"
        dirty: list[dict[str, Any]] = []
        if not PROJECTS_DIR.exists():
            return {"dirty": dirty}
        for repo in sorted(PROJECTS_DIR.iterdir()):
            if not (repo / ".git").exists():
                continue
            try:
                res = subprocess.run(
                    [git, "status", "--porcelain"],
                    cwd=repo,
                    capture_output=True,
                    text=True,
                    timeout=15,
                )
            except (OSError, subprocess.TimeoutExpired):
                continue
            lines = [ln for ln in res.stdout.splitlines() if ln.strip()]
            if lines:
                dirty.append({"repo": repo.name, "changes": len(lines)})
        return {"dirty": dirty}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        return {"dirty": data["dirty"], "count": len(data["dirty"])}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        if not result["count"]:
            return "**Git drift** -- all repos clean"
        lines = [f"**Git drift** -- {result['count']} repos with uncommitted work", ""]
        for row in result["dirty"][:20]:
            lines.append(f"- {row['repo']}: {row['changes']} changed file(s)")
        return "\n".join(lines)
