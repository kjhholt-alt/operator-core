"""Recipe lifecycle exceptions + retry/timeout helpers."""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, TypeVar

T = TypeVar("T")


class RecipeError(Exception):
    """Base class for recipe lifecycle errors."""


class BudgetExceeded(RecipeError):
    """Raised mid-run when ``ctx.cost_so_far`` crosses ``cost_budget_usd``."""


class RecipeTimeout(RecipeError):
    """Raised when a recipe exceeds ``timeout_sec``."""


class VerifyFailed(RecipeError):
    """Raised when ``verify()`` returns False or throws."""


async def with_timeout(coro: Awaitable[T], timeout_sec: int, *, label: str = "") -> T:
    """Run ``coro`` under ``timeout_sec``; raises RecipeTimeout on timeout."""
    try:
        return await asyncio.wait_for(coro, timeout=timeout_sec)
    except asyncio.TimeoutError as exc:
        raise RecipeTimeout(f"{label} exceeded {timeout_sec}s") from exc


async def with_retries(
    fn: Callable[[], Awaitable[T]],
    *,
    retries: int,
    label: str = "",
) -> T:
    """Run ``fn()`` up to ``retries+1`` times. Last exception is re-raised."""
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return await fn()
        except (BudgetExceeded, RecipeTimeout):
            # Hard stops -- never retry.
            raise
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= retries:
                break
            await asyncio.sleep(min(2 ** attempt, 10))
    assert last_exc is not None
    raise last_exc
