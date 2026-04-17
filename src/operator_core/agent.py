"""Thin wrapper over claude-agent-sdk for Operator recipes.

Every recipe that calls Claude goes through `run_agent()`. The wrapper:

- Imports `claude_agent_sdk` lazily so environments without the SDK still
  boot (recipes degrade with `error="claude-agent-sdk not installed"`).
- Enforces an optional per-call `max_cost_usd` cap via the SDK's own
  `max_budget_usd` option and a defensive post-check.
- Writes a row to the existing sqlite jobs ledger (`JobStore`) under
  `action="agent.run"` so /metrics and the cost ledger can see every
  agent call.
- Returns a plain `AgentResult` dataclass with the text, token counts,
  cost, duration, and error (None on success).

The wrapper does not interpret tool calls or orchestrate multi-turn
conversations; recipes that need that build their own loop on top.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from .store import JobStore


@dataclass
class AgentResult:
    text: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    duration_ms: int = 0
    error: str | None = None
    model: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


def run_agent(
    prompt: str,
    *,
    system: str | None = None,
    model: str = "claude-sonnet-4-6",
    max_turns: int = 1,
    tools: list[str] | None = None,
    max_cost_usd: float | None = None,
    store: JobStore | None = None,
    project: str | None = None,
) -> AgentResult:
    """Run a single Claude query and return AgentResult.

    Never raises for SDK / network / cost issues — surfaces them via
    `result.error`. Always writes a ledger row (with or without error).
    """
    started = time.monotonic()
    try:
        from claude_agent_sdk import (
            AssistantMessage,
            ClaudeAgentOptions,
            ResultMessage,
            TextBlock,
            query,
        )
    except ImportError:
        result = AgentResult(
            error="claude-agent-sdk not installed",
            duration_ms=int((time.monotonic() - started) * 1000),
            model=model,
        )
        _write_ledger(store, prompt, result, project=project)
        return result

    options_kwargs: dict[str, Any] = {
        "model": model,
        "max_turns": max_turns,
        "allowed_tools": tools or [],
    }
    if system is not None:
        options_kwargs["system_prompt"] = system
    if max_cost_usd is not None:
        options_kwargs["max_budget_usd"] = float(max_cost_usd)

    options = ClaudeAgentOptions(**options_kwargs)

    text_parts: list[str] = []
    input_tokens = 0
    output_tokens = 0
    cost_usd = 0.0
    resolved_model: str | None = model
    error: str | None = None

    async def _drain() -> None:
        nonlocal input_tokens, output_tokens, cost_usd, resolved_model, error
        try:
            async for message in query(prompt=prompt, options=options):
                if isinstance(message, AssistantMessage):
                    resolved_model = getattr(message, "model", resolved_model)
                    for block in message.content or []:
                        if isinstance(block, TextBlock):
                            text_parts.append(block.text)
                elif isinstance(message, ResultMessage):
                    cost_usd = float(getattr(message, "total_cost_usd", 0) or 0)
                    usage = getattr(message, "usage", None) or {}
                    input_tokens = int(
                        usage.get("input_tokens", 0)
                        + usage.get("cache_read_input_tokens", 0)
                    )
                    output_tokens = int(usage.get("output_tokens", 0) or 0)
                    if getattr(message, "is_error", False):
                        error = (
                            message.result
                            or getattr(message, "stop_reason", None)
                            or "agent error"
                        )
        except Exception as exc:  # noqa: BLE001 - wrapper owns all failures
            error = f"{type(exc).__name__}: {exc}"

    try:
        asyncio.run(_drain())
    except RuntimeError as exc:
        # Nested event loop — fall back to a new loop in a thread.
        if "running event loop" in str(exc):
            import concurrent.futures

            def _run() -> None:
                asyncio.run(_drain())

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(_run).result()
        else:
            error = f"RuntimeError: {exc}"

    duration_ms = int((time.monotonic() - started) * 1000)

    if (
        error is None
        and max_cost_usd is not None
        and cost_usd > float(max_cost_usd)
    ):
        error = f"cost cap exceeded: ${cost_usd:.4f} > ${max_cost_usd:.4f}"

    result = AgentResult(
        text="".join(text_parts).strip(),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cost_usd=cost_usd,
        duration_ms=duration_ms,
        error=error,
        model=resolved_model,
    )
    _write_ledger(store, prompt, result, project=project)
    return result


def _write_ledger(
    store: JobStore | None,
    prompt: str,
    result: AgentResult,
    *,
    project: str | None,
) -> None:
    """Best-effort ledger write; never raises back to the caller."""
    try:
        ledger = store or JobStore()
    except Exception:  # noqa: BLE001
        return
    try:
        metadata = {
            "model": result.model,
            "input_tokens": result.input_tokens,
            "output_tokens": result.output_tokens,
            "duration_ms": result.duration_ms,
        }
        if result.error:
            metadata["error"] = result.error
        job = ledger.create_job(
            action="agent.run",
            prompt=prompt[:2000],
            project=project,
            metadata=metadata,
        )
        ledger.update_job(
            job.id,
            status="failed" if result.error else "complete",
            cost_usd=result.cost_usd,
            metadata={**metadata, "text_len": len(result.text)},
        )
    except Exception:  # noqa: BLE001
        return
