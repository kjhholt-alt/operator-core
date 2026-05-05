"""client_health -- weekly client site monitoring."""

from __future__ import annotations

from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from operator_core.recipes import Recipe, RecipeContext, register_recipe

CLIENT_URLS = [
    "https://outdoor-crm.vercel.app",
    "https://n16soccer.com",
]


@register_recipe
class ClientHealth(Recipe):
    name = "client_health"
    version = "1.0.0"
    description = "Weekly probe of client-deployed sites"
    cost_budget_usd = 0.50
    schedule = "0 9 * * 1"  # Mondays 9am
    timeout_sec = 120
    discord_channel = "deploys"
    requires_clients = ("discord",)
    tags = ("weekly", "clients")

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for url in CLIENT_URLS:
            req = Request(url, method="HEAD", headers={"User-Agent": "OperatorRecipe/1.0"})
            try:
                with urlopen(req, timeout=10) as resp:
                    out.append({"url": url, "status": resp.status})
            except URLError as exc:
                out.append({"url": url, "status": None, "error": str(exc)})
        return out

    async def analyze(self, ctx: RecipeContext, data: list[dict[str, Any]]) -> dict[str, Any]:
        bad = [r for r in data if r.get("status") is None or r["status"] >= 500]
        return {"all": data, "bad": bad}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        if not result.get("bad"):
            return ""
        lines = ["**Client health -- attention needed**"]
        for r in result["bad"]:
            lines.append(f"- {r['url']}: {r.get('error') or r.get('status')}")
        return "\n".join(lines)
