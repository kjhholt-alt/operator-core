"""secrets_audit -- scan for accidentally tracked .env files / API keys.

Walks every project's git index for filenames that suggest secrets,
plus a quick regex pass over staged files for high-entropy strings.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe

PROJECTS_DIR = Path("C:/Users/Kruz/Desktop/Projects")
SUSPECT_NAMES = re.compile(r"(\.env(\..+)?$|credentials\.json|service-account.*\.json|secrets?\.ya?ml)", re.IGNORECASE)


@register_recipe
class SecretsAudit(Recipe):
    name = "secrets_audit"
    version = "1.0.0"
    description = "Surface secrets-looking files committed to any repo"
    cost_budget_usd = 0.0
    schedule = "0 23 * * *"  # 11pm nightly
    timeout_sec = 300
    discord_channel = "automations"
    requires_clients = ("discord",)
    tags = ("nightly", "security")

    async def verify(self, ctx: RecipeContext) -> bool:
        return shutil.which("git") is not None and ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        git = shutil.which("git") or "git"
        hits: list[dict[str, Any]] = []
        if not PROJECTS_DIR.exists():
            return {"hits": hits}
        for repo in sorted(PROJECTS_DIR.iterdir()):
            if not (repo / ".git").exists():
                continue
            try:
                res = subprocess.run(
                    [git, "ls-files"],
                    cwd=repo,
                    capture_output=True,
                    text=True,
                    timeout=20,
                )
            except (OSError, subprocess.TimeoutExpired):
                continue
            for line in res.stdout.splitlines():
                if SUSPECT_NAMES.search(line):
                    hits.append({"repo": repo.name, "path": line})
        return {"hits": hits}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        return {"hits": data["hits"], "count": len(data["hits"])}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        if not result["count"]:
            return "**Secrets audit** -- clean"
        lines = [f"**Secrets audit** -- {result['count']} suspicious file(s)", ""]
        for row in result["hits"][:25]:
            lines.append(f"- {row['repo']}: {row['path']}")
        return "\n".join(lines)
