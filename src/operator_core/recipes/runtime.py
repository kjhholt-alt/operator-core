"""Recipe runtime: lifecycle orchestration, cost + status emission, retry/timeout."""

from __future__ import annotations

import logging
import os
import time
import traceback
import uuid
from dataclasses import asdict
from typing import Any

from .base import Recipe, RecipeContext, RecipeResult, RecipeStatus
from .lifecycle import (
    BudgetExceeded,
    RecipeTimeout,
    VerifyFailed,
    with_retries,
    with_timeout,
)
from .registry import get_registered_recipe

# vendored stubs -- swap to real packages once siblings publish.
from .._vendor import events_ndjson, status_spec  # type: ignore

logger = logging.getLogger("operator.recipe.runtime")


def _make_correlation_id() -> str:
    return uuid.uuid4().hex[:12]


def _build_clients(recipe: Recipe) -> dict[str, Any]:
    """Lazily import + instantiate integration adapters the recipe asks for."""
    clients: dict[str, Any] = {}
    needed = set(recipe.requires_clients)
    if "discord" in needed:
        from ..integrations.discord import DiscordAdapter

        clients["discord"] = DiscordAdapter()
    if "supabase" in needed:
        from ..integrations.supabase import SupabaseAdapter

        clients["supabase"] = SupabaseAdapter()
    if "gmail" in needed:
        from ..integrations.gmail import GmailAdapter

        clients["gmail"] = GmailAdapter()
    if "anthropic" in needed:
        from ..integrations.anthropic import AnthropicAdapter

        clients["anthropic"] = AnthropicAdapter()
    return clients


class RecipeRunner:
    """Orchestrates a single recipe execution end-to-end."""

    def __init__(
        self,
        recipe: Recipe | type[Recipe],
        *,
        dry_run: bool = False,
        env: dict[str, str] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        self.recipe = recipe() if isinstance(recipe, type) else recipe
        self.dry_run = dry_run
        self.env = dict(env or os.environ)
        self.correlation_id = correlation_id or _make_correlation_id()

    async def run(self) -> RecipeResult:
        recipe = self.recipe
        start = time.time()
        ctx = RecipeContext(
            recipe_name=recipe.name,
            correlation_id=self.correlation_id,
            env=self.env,
            clients=_build_clients(recipe),
            cost_budget_usd=float(recipe.cost_budget_usd or 0.0),
            dry_run=self.dry_run,
            logger=logger.getChild(recipe.name),
        )

        # emit run.started
        events_ndjson.append_event(
            stream="runs",
            kind="started",
            recipe=recipe.name,
            correlation_id=self.correlation_id,
            payload={"version": recipe.version, "dry_run": self.dry_run},
        )
        status_spec.write_component_status(
            recipe.name,
            "running",
            version=recipe.version,
        )

        result = RecipeResult(correlation_id=self.correlation_id)

        try:
            ok = await with_timeout(recipe.verify(ctx), recipe.timeout_sec, label=f"{recipe.name}.verify")
            if not ok:
                raise VerifyFailed(f"{recipe.name}.verify returned False")

            if self.dry_run:
                result.status = RecipeStatus.OK
                result.payload = {"dry_run": True, "verify": True}
            else:
                # query -> analyze -> format -> post -> log_cost, with retry
                async def _pipeline() -> dict[str, Any]:
                    data = await with_timeout(recipe.query(ctx), recipe.timeout_sec, label=f"{recipe.name}.query")
                    analyzed = await with_timeout(recipe.analyze(ctx, data), recipe.timeout_sec, label=f"{recipe.name}.analyze")
                    rendered = await with_timeout(recipe.format(ctx, analyzed), recipe.timeout_sec, label=f"{recipe.name}.format")
                    posted = await with_timeout(recipe.post(ctx, rendered), recipe.timeout_sec, label=f"{recipe.name}.post")
                    await recipe.log_cost(ctx)
                    return {"data": data, "analyzed": analyzed, "rendered_len": len(rendered or ""), "posted": posted}

                pipeline_payload = await with_retries(_pipeline, retries=recipe.retries, label=recipe.name)
                result.payload = pipeline_payload
                result.discord_posted = bool(pipeline_payload.get("posted"))
                result.status = RecipeStatus.OK

        except BudgetExceeded as exc:
            result.status = RecipeStatus.BUDGET_EXCEEDED
            result.error = str(exc)
            ctx.logger.warning("recipe.budget_exceeded", extra={"recipe": recipe.name})
        except RecipeTimeout as exc:
            result.status = RecipeStatus.TIMEOUT
            result.error = str(exc)
            ctx.logger.error("recipe.timeout", extra={"recipe": recipe.name})
        except VerifyFailed as exc:
            result.status = RecipeStatus.VERIFY_FAILED
            result.error = str(exc)
            ctx.logger.error("recipe.verify_failed", extra={"recipe": recipe.name})
        except Exception as exc:  # noqa: BLE001
            result.status = RecipeStatus.ERROR
            result.error = f"{type(exc).__name__}: {exc}"
            ctx.logger.error(
                "recipe.error",
                extra={"recipe": recipe.name, "trace": traceback.format_exc()},
            )

        result.duration_sec = time.time() - start
        result.cost_usd = ctx.cost_so_far

        # emit run.finished
        events_ndjson.append_event(
            stream="runs",
            kind="finished",
            recipe=recipe.name,
            correlation_id=self.correlation_id,
            payload={
                "status": result.status,
                "duration_sec": result.duration_sec,
                "cost_usd": result.cost_usd,
                "error": result.error,
            },
        )
        # cost roll-up event -- canonical field is `cost_usd` per
        # events-ndjson/spec/schema/v1/streams/cost.json.
        if result.cost_usd > 0:
            events_ndjson.append_event(
                stream="cost",
                kind="recipe_run",
                recipe=recipe.name,
                correlation_id=self.correlation_id,
                payload={
                    "cost_usd": result.cost_usd,
                    "agent": recipe.name,
                    "status": result.status,
                },
            )

        # status-spec write
        status_value = _result_to_status(result.status)
        status_spec.write_component_status(
            recipe.name,
            status_value,
            duration_sec=result.duration_sec,
            cost_usd=result.cost_usd,
            error=result.error,
            version=recipe.version,
            extra={"correlation_id": self.correlation_id, "discord_posted": result.discord_posted},
        )

        return result


def _result_to_status(status: str) -> str:
    """Map RecipeStatus -> status-spec component status."""
    if status == RecipeStatus.OK:
        return "ok"
    if status in (RecipeStatus.SKIPPED,):
        return "skipped"
    if status in (RecipeStatus.BUDGET_EXCEEDED, RecipeStatus.TIMEOUT, RecipeStatus.VERIFY_FAILED, RecipeStatus.WARN):
        return "warn"
    return "error"


async def run_recipe(name_or_class: str | type[Recipe] | Recipe, *, dry_run: bool = False) -> RecipeResult:
    """Convenience: resolve a recipe by name and run it."""
    if isinstance(name_or_class, str):
        cls = get_registered_recipe(name_or_class)
        if cls is None:
            raise KeyError(f"recipe not registered: {name_or_class}")
        target: Recipe | type[Recipe] = cls
    else:
        target = name_or_class
    runner = RecipeRunner(target, dry_run=dry_run)
    return await runner.run()
