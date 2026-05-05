"""deploy_checker -- service health probe.

Migrated from operator-scripts/deploy-checker.py. Pings each configured
SaaS URL and reports any that returned a non-2xx.
"""

from __future__ import annotations

from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from operator_core.recipes import Recipe, RecipeContext, register_recipe

DEFAULT_URLS = [
    "https://deal-brain.vercel.app",
    "https://prospector-pro.vercel.app",
    "https://ai-ops-consulting.vercel.app",
    "https://ai-voice-receptionist.vercel.app",
    "https://pool-prospector.vercel.app",
]


@register_recipe
class DeployChecker(Recipe):
    name = "deploy_checker"
    version = "1.0.0"
    description = "Probe SaaS deploys and alert on red status"
    cost_budget_usd = 0.0  # purely network probes
    schedule = "20 6 * * *"
    timeout_sec = 120
    discord_channel = "deploys"
    requires_clients = ("discord",)
    tags = ("daily", "infra")

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for url in DEFAULT_URLS:
            results.append(_probe(url))
        return results

    async def analyze(self, ctx: RecipeContext, data: list[dict[str, Any]]) -> dict[str, Any]:
        red = [r for r in data if r["status"] == "red"]
        yellow = [r for r in data if r["status"] == "yellow"]
        return {"red": red, "yellow": yellow, "all": data}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        red = result.get("red", [])
        yellow = result.get("yellow", [])
        if not red and not yellow:
            return ""  # silent on green
        lines = ["**Deploy health**", ""]
        for r in red:
            lines.append(f"- RED {r['url']} ({r.get('detail', 'no response')})")
        for r in yellow:
            lines.append(f"- YELLOW {r['url']} ({r.get('detail', '')})")
        return "\n".join(lines)


def _probe(url: str) -> dict[str, Any]:
    req = Request(url, method="HEAD", headers={"User-Agent": "OperatorRecipe/1.0"})
    try:
        with urlopen(req, timeout=10) as resp:
            code = resp.status
            if 200 <= code < 400:
                return {"url": url, "status": "green", "code": code}
            if 400 <= code < 500:
                return {"url": url, "status": "yellow", "code": code, "detail": f"HTTP {code}"}
            return {"url": url, "status": "red", "code": code, "detail": f"HTTP {code}"}
    except URLError as exc:
        return {"url": url, "status": "red", "code": None, "detail": str(exc)}
