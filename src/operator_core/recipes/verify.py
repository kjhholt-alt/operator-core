"""Bulk verify: run every registered recipe in dry-run mode and report.

In strict mode (default), every recipe must verify(). In CI / lenient
mode (``OPERATOR_VERIFY_DRY=1``), a recipe whose required clients aren't
configured is treated as SKIPPED rather than FAILED -- this lets the
verify gate pass on a fresh CI runner that doesn't have prod secrets.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass

from .base import RecipeStatus
from .registry import list_registered_recipes
from .runtime import RecipeRunner

logger = logging.getLogger("operator.recipe.verify")


# Map required-client name -> env var that proves it's configured. If
# ANY of these env vars is set we consider the integration "available"
# enough that verify failure should be a real failure, not a skip.
_INTEGRATION_ENV_VARS = {
    "supabase": ("SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_SERVICE_ROLE_KEY"),
    "discord": (
        "DISCORD_WEBHOOK_URL",
        "DISCORD_PROJECTS_WEBHOOK_URL",
        "DISCORD_DEPLOYS_WEBHOOK_URL",
        "DISCORD_AUTOMATIONS_WEBHOOK_URL",
        "DISCORD_CODE_REVIEW_WEBHOOK_URL",
    ),
    "anthropic": ("ANTHROPIC_API_KEY",),
    "gmail": ("GMAIL_TOKEN_PATH", "GOOGLE_CREDENTIALS_PATH"),
}


def _missing_integrations(required: tuple[str, ...]) -> list[str]:
    """Return integrations whose env vars are entirely unset."""
    missing: list[str] = []
    for name in required:
        env_vars = _INTEGRATION_ENV_VARS.get(name, ())
        if not env_vars:
            continue
        if not any(os.environ.get(v) for v in env_vars):
            missing.append(name)
    return missing


@dataclass
class VerifyReport:
    total: int
    passed: int
    failed: int
    failures: list[tuple[str, str]]  # (recipe_name, error)
    skipped: list[tuple[str, str]] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.skipped is None:
            self.skipped = []

    @property
    def green(self) -> bool:
        return self.failed == 0


async def verify_all(*, lenient: bool | None = None) -> VerifyReport:
    """Run every registered recipe's verify() in dry mode.

    ``lenient=True`` (or env ``OPERATOR_VERIFY_DRY=1``) treats unconfigured-
    integration failures as SKIPPED rather than FAILED.
    """
    if lenient is None:
        lenient = os.environ.get("OPERATOR_VERIFY_DRY", "").lower() in {"1", "true", "yes"}

    classes = list_registered_recipes()
    failures: list[tuple[str, str]] = []
    skipped: list[tuple[str, str]] = []
    passed = 0

    for cls in classes:
        # In lenient mode, skip the run entirely if obvious deps are missing.
        if lenient:
            missing = _missing_integrations(getattr(cls, "requires_clients", ()))
            if missing:
                skipped.append((cls.name, f"missing integrations: {','.join(missing)}"))
                continue

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
        skipped=skipped,
    )


def verify_all_sync(*, lenient: bool | None = None) -> VerifyReport:
    return asyncio.run(verify_all(lenient=lenient))
