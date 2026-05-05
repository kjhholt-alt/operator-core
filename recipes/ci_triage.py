"""ci_triage -- scan CI failures across known repos."""

from __future__ import annotations

import shutil
import subprocess
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class CiTriage(Recipe):
    name = "ci_triage"
    version = "1.0.0"
    description = "Find recently failed CI runs across configured repos"
    cost_budget_usd = 0.00
    schedule = "30 7 * * *"
    timeout_sec = 180
    discord_channel = "automations"
    requires_clients = ("discord",)
    tags = ("daily", "ci")

    async def verify(self, ctx: RecipeContext) -> bool:
        return shutil.which("gh") is not None and ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> list[dict[str, Any]]:
        gh = shutil.which("gh") or "gh"
        try:
            res = subprocess.run(
                [gh, "run", "list", "--status", "failure", "--limit", "10", "--json", "name,workflowName,conclusion,headBranch,url"],
                capture_output=True,
                text=True,
                check=False,
                timeout=60,
            )
        except (OSError, subprocess.TimeoutExpired):
            return []
        if res.returncode != 0:
            return []
        import json as _json

        try:
            return _json.loads(res.stdout or "[]")
        except _json.JSONDecodeError:
            return []

    async def analyze(self, ctx: RecipeContext, data: list[dict[str, Any]]) -> dict[str, Any]:
        return {"failures": data}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        failures = result.get("failures", [])
        if not failures:
            return ""
        lines = [f"**CI triage** -- {len(failures)} recent failures"]
        for f in failures[:10]:
            lines.append(f"- [{f.get('workflowName', '?')}] {f.get('headBranch', '?')} -- {f.get('url', '')}")
        return "\n".join(lines)
