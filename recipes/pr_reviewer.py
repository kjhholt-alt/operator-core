"""pr_reviewer -- review open PRs across configured repos.

Migrated from operator-scripts/pr-reviewer.py. Pulls open PR list via gh CLI
and posts a digest to #code-review.
"""

from __future__ import annotations

import shutil
import subprocess
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class PrReviewer(Recipe):
    name = "pr_reviewer"
    version = "1.0.0"
    description = "Review open PRs and post a digest"
    cost_budget_usd = 3.00
    schedule = "10 6 * * *"
    timeout_sec = 600
    discord_channel = "code_review"
    requires_clients = ("discord",)
    tags = ("daily", "code-review")

    async def verify(self, ctx: RecipeContext) -> bool:
        gh = shutil.which("gh")
        if gh is None:
            ctx.logger.warning("pr_reviewer.no_gh_cli")
            return False
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> list[dict[str, Any]]:
        gh = shutil.which("gh") or "gh"
        try:
            res = subprocess.run(
                [gh, "search", "prs", "--state", "open", "--author", "@me", "--json", "url,title,repository", "--limit", "25"],
                capture_output=True,
                text=True,
                check=False,
                timeout=60,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            ctx.logger.error("pr_reviewer.gh_failed", extra={"error": str(exc)})
            return []
        if res.returncode != 0:
            return []
        import json as _json

        try:
            return _json.loads(res.stdout or "[]")
        except _json.JSONDecodeError:
            return []

    async def analyze(self, ctx: RecipeContext, data: list[dict[str, Any]]) -> dict[str, Any]:
        return {"open_pr_count": len(data), "prs": data}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        prs = result.get("prs", [])
        if not prs:
            return "**PR review** -- no open PRs."
        lines = [f"**PR review** -- {len(prs)} open PR(s)", ""]
        for pr in prs[:25]:
            repo = (pr.get("repository") or {}).get("nameWithOwner", "?")
            lines.append(f"- [{repo}] {pr.get('title', '?')} -- {pr.get('url', '')}")
        return "\n".join(lines)
