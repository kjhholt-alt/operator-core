"""disk_pressure -- watch C:/ free space and alarm when it crosses a threshold.

Per Kruz memory: 2026-04-22 the projects archive (17 GB) had to be moved
off C: when free space dropped to 5%. This recipe is the early-warning.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe

DEFAULT_DRIVE = "C:\\"
DEFAULT_THRESHOLD_PCT = 15.0  # alarm when <=15% free


@register_recipe
class DiskPressure(Recipe):
    name = "disk_pressure"
    version = "1.0.0"
    description = "Alarm when C: drive free space drops below threshold"
    cost_budget_usd = 0.0
    schedule = "0 */4 * * *"
    timeout_sec = 30
    discord_channel = "automations"
    requires_clients = ("discord",)
    tags = ("ops", "system")

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None and Path(DEFAULT_DRIVE).exists()

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        drive = os.environ.get("OPERATOR_DISK_DRIVE", DEFAULT_DRIVE)
        threshold = float(os.environ.get("OPERATOR_DISK_THRESHOLD_PCT", DEFAULT_THRESHOLD_PCT))
        try:
            usage = shutil.disk_usage(drive)
        except OSError as exc:
            ctx.logger.warning("disk_pressure.usage_failed", extra={"error": str(exc)})
            return {"drive": drive, "free_pct": None, "threshold_pct": threshold}
        free_pct = (usage.free / usage.total) * 100 if usage.total else 0
        return {
            "drive": drive,
            "free_pct": free_pct,
            "free_gb": usage.free / 1024 ** 3,
            "total_gb": usage.total / 1024 ** 3,
            "threshold_pct": threshold,
        }

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        free = data.get("free_pct")
        breach = bool(free is not None and free <= data["threshold_pct"])
        return {**data, "breach": breach}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        flag = "ALARM" if result.get("breach") else "ok"
        if result.get("free_pct") is None:
            return f"**Disk pressure [{flag}]** -- could not read drive {result['drive']}"
        return (
            f"**Disk pressure [{flag}]** -- {result['drive']}\n"
            f"- {result['free_pct']:.1f}% free ({result['free_gb']:.1f} / {result['total_gb']:.1f} GB)\n"
            f"- threshold: {result['threshold_pct']:.1f}%"
        )
