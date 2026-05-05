"""Bulk verify: run every registered recipe in dry-run mode and report."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .base import RecipeStatus
from .registry import list_registered_recipes
from .runtime import RecipeRunner

logger = logging.getLogger("operator.recipe.verify")


@dataclass
class VerifyReport:
    total: int
    passed: int
    failed: int
    failures: list[tuple[str, str]]  # (recipe_name, error)

    @property
    def green(self) -> bool:
        return self.failed == 0


async def verify_all() -> VerifyReport:
    classes = list_registered_recipes()
    failures: list[tuple[str, str]] = []
    passed = 0

    for cls in classes:
        runner = RecipeRunner(cls, dry_run=True)
        try:
            result = await runner.run()
        except Exception as exc:  # noqa: BLE001
            failures.append((cls.name, f"{type(exc).__name__}: {exc}"))
            continue
        if result.status == RecipeStatus.OK:
            passed += 1
        else:
            failures.append((cls.name, result.error or result.status))

    return VerifyReport(
        total=len(classes),
        passed=passed,
        failed=len(failures),
        failures=failures,
    )


def verify_all_sync() -> VerifyReport:
    return asyncio.run(verify_all())
