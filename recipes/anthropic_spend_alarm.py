"""anthropic_spend_alarm -- alert when day's Claude API spend crosses a threshold.

Reads ``costs.csv`` (the legacy operator-scripts cost ledger) and the
new ``cost.ndjson`` stream, sums today's totals, and posts a red embed
to ``#automations`` if either crosses the threshold.
"""

from __future__ import annotations

import csv
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe

DEFAULT_THRESHOLD = 50.0  # USD/day


@register_recipe
class AnthropicSpendAlarm(Recipe):
    name = "anthropic_spend_alarm"
    version = "1.0.0"
    description = "Alarm when Claude API spend crosses a daily threshold"
    cost_budget_usd = 0.0
    schedule = "*/30 * * * *"
    timeout_sec = 60
    discord_channel = "automations"
    requires_clients = ("discord",)
    tags = ("hourly", "ops", "cost")

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        today = date.today().isoformat()
        csv_total = self._sum_csv(today)
        ndjson_total = self._sum_ndjson(today)
        threshold = float(os.environ.get("OPERATOR_SPEND_THRESHOLD_USD", DEFAULT_THRESHOLD))
        return {
            "date": today,
            "csv_usd": csv_total,
            "ndjson_usd": ndjson_total,
            "threshold_usd": threshold,
        }

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        breach = max(data["csv_usd"], data["ndjson_usd"]) >= data["threshold_usd"]
        return {**data, "breach": breach}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        flag = "ALARM" if result["breach"] else "ok"
        return (
            f"**Anthropic spend [{flag}]** -- {result['date']}\n"
            f"- costs.csv: ${result['csv_usd']:.2f}\n"
            f"- cost.ndjson: ${result['ndjson_usd']:.2f}\n"
            f"- threshold: ${result['threshold_usd']:.2f}"
        )

    # --- helpers --------------------------------------------------------------

    def _sum_csv(self, today: str) -> float:
        path_str = os.environ.get("OPERATOR_COSTS_CSV") or str(
            Path("C:/Users/Kruz/Desktop/Projects/operator-scripts/costs.csv")
        )
        path = Path(path_str)
        if not path.exists():
            return 0.0
        total = 0.0
        try:
            with open(path, "r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    if (row.get("date") or row.get("ts") or "").startswith(today):
                        try:
                            total += float(row.get("cost_usd") or row.get("usd") or 0)
                        except (TypeError, ValueError):
                            continue
        except OSError:
            return 0.0
        return total

    def _sum_ndjson(self, today: str) -> float:
        path_str = os.environ.get("OPERATOR_COST_NDJSON_PATH")
        if not path_str:
            return 0.0
        path = Path(path_str)
        if not path.exists():
            return 0.0
        total = 0.0
        try:
            with open(path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        evt = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    ts = evt.get("ts", "")
                    if not ts.startswith(today):
                        continue
                    payload = evt.get("payload") or {}
                    try:
                        total += float(payload.get("cost_usd") or 0)
                    except (TypeError, ValueError):
                        continue
        except OSError:
            return 0.0
        return total
