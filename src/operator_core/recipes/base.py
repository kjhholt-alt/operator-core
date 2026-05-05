"""Recipe abstract base class + result/context types.

A ``Recipe`` is a declarative description of an automation. Subclasses fill in
lifecycle hooks; the runner (see ``runtime.py``) handles cost tracking,
status writes, Discord routing, retries, timeouts, and verify.

Lifecycle order:
    verify -> query -> analyze -> format -> post -> log_cost

Hard rules enforced elsewhere:
- No raw ``requests`` / ``httpx`` in recipes; use integration adapters.
- No ``print``; use ``ctx.logger``.
- Every recipe must implement ``verify``.
- Cost budget is enforced; recipe halts if mid-run cost exceeds budget.
"""

from __future__ import annotations

import abc
import logging
from dataclasses import dataclass, field
from typing import Any


# --- status enum (string) -----------------------------------------------------

class RecipeStatus:
    OK = "ok"
    WARN = "warn"
    ERROR = "error"
    SKIPPED = "skipped"
    BUDGET_EXCEEDED = "budget_exceeded"
    TIMEOUT = "timeout"
    VERIFY_FAILED = "verify_failed"


# --- context + result ---------------------------------------------------------

@dataclass
class RecipeContext:
    """Per-run context passed into every lifecycle hook.

    ``clients`` holds shared integration adapters (Supabase, Discord, Gmail,
    Anthropic). Recipes read what they need; nothing is auto-instantiated
    that isn't requested. ``cost_so_far`` accumulates as adapters log spend.
    """

    recipe_name: str
    correlation_id: str
    env: dict[str, str] = field(default_factory=dict)
    clients: dict[str, Any] = field(default_factory=dict)
    cost_so_far: float = 0.0
    cost_budget_usd: float = 0.0
    dry_run: bool = False
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger("operator.recipe"))

    def add_cost(self, amount: float, *, source: str = "unknown") -> None:
        """Track an additional cost; raises BudgetExceeded if over budget."""
        from .lifecycle import BudgetExceeded

        self.cost_so_far += float(amount)
        self.logger.debug(
            "recipe.cost", extra={"recipe": self.recipe_name, "source": source, "delta": amount, "total": self.cost_so_far}
        )
        if self.cost_budget_usd > 0 and self.cost_so_far > self.cost_budget_usd:
            raise BudgetExceeded(
                f"recipe={self.recipe_name} cost={self.cost_so_far:.4f} budget={self.cost_budget_usd:.4f}"
            )


@dataclass
class RecipeResult:
    """What ``Recipe`` lifecycle returns. Status is one of RecipeStatus."""

    status: str = RecipeStatus.OK
    cost_usd: float = 0.0
    duration_sec: float = 0.0
    payload: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    discord_posted: bool = False
    correlation_id: str | None = None


# --- abstract base recipe -----------------------------------------------------

class Recipe(abc.ABC):
    """Declarative recipe base.

    Subclasses set class-level metadata and override the lifecycle hooks.
    ``verify`` is mandatory; everything else has a no-op default so simple
    recipes can override only what they need.
    """

    name: str = ""
    version: str = "0.1.0"
    description: str = ""
    cost_budget_usd: float = 0.0          # 0 means "no budget enforcement"
    schedule: str | None = None            # cron string (UTC). None = on-demand
    timeout_sec: int = 300
    retries: int = 0
    discord_channel: str | None = None     # e.g., "projects", "code_review"
    requires_clients: tuple[str, ...] = ()  # e.g., ("supabase", "discord")
    tags: tuple[str, ...] = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Soft validation: name must be set on concrete subclasses.
        if not getattr(cls, "name", "") and not getattr(cls, "_abstract", False):
            # Abstract intermediate classes can opt out by setting _abstract = True.
            if not abc_has_unimplemented(cls):
                raise TypeError(f"Recipe subclass {cls.__name__} must set `name`")

    # --- lifecycle hooks ------------------------------------------------------

    async def query(self, ctx: RecipeContext) -> Any:
        """Pull the data this recipe needs (Supabase, Gmail, gh, etc.)."""
        return None

    async def analyze(self, ctx: RecipeContext, data: Any) -> Any:
        """Apply Claude / heuristics / aggregation. Returns a structured result."""
        return data

    async def format(self, ctx: RecipeContext, result: Any) -> str:
        """Render a Discord-ready message. Return an empty string to skip post."""
        return ""

    async def post(self, ctx: RecipeContext, message: str) -> bool:
        """Post the rendered message. Default routes to ``discord_channel``."""
        if not message:
            return False
        if ctx.dry_run:
            ctx.logger.info("recipe.post.dry_run", extra={"recipe": self.name, "len": len(message)})
            return False
        discord = ctx.clients.get("discord")
        if discord is None or not self.discord_channel:
            ctx.logger.debug("recipe.post.skipped", extra={"recipe": self.name})
            return False
        return discord.notify(
            channel=self.discord_channel,
            title=self.name.replace("_", " ").title(),
            body=message,
            footer=f"{self.name} | v{self.version} | cost ${ctx.cost_so_far:.4f}",
        )

    async def log_cost(self, ctx: RecipeContext) -> None:
        """Final cost write. Runner records to events stream; override for extras."""
        return None

    @abc.abstractmethod
    async def verify(self, ctx: RecipeContext) -> bool:
        """Dry-run sanity check. Must return True if recipe is wired up.

        Should NOT post to Discord, should NOT write to Supabase. Pure read
        + connectivity sniff. Used by ``operator verify`` as a CI gate.
        """
        raise NotImplementedError


def abc_has_unimplemented(cls: type) -> bool:
    """Return True if ``cls`` still has unimplemented abstract methods."""
    return bool(getattr(cls, "__abstractmethods__", set()))
