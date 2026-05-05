"""Anthropic adapter -- wraps anthropic SDK + records token cost into events.

Recipe code calls ``adapter.create(...)`` and the adapter:
1. Routes through the official ``anthropic`` SDK if installed.
2. Records a cost event (events-ndjson stream=cost, kind=anthropic_call).
3. Returns the parsed response.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from .._vendor import events_ndjson  # type: ignore

logger = logging.getLogger("operator.integration.anthropic")

# Approximate USD-per-token rates (2026 model pricing, tweak as needed).
DEFAULT_PRICING = {
    # input_per_mtok, output_per_mtok
    "claude-opus-4-5": (15.0, 75.0),
    "claude-opus-4-6": (15.0, 75.0),
    "claude-opus-4-7": (15.0, 75.0),
    "claude-sonnet-4-5": (3.0, 15.0),
    "claude-haiku-4-5": (0.80, 4.0),
}


class AnthropicAdapter:
    def __init__(self, env: dict[str, str] | None = None) -> None:
        self.env = env if env is not None else dict(os.environ)
        self._client: Any = None

    @property
    def configured(self) -> bool:
        return bool(self.env.get("ANTHROPIC_API_KEY"))

    def client(self) -> Any:
        if self._client is not None:
            return self._client
        if not self.configured:
            raise RuntimeError("anthropic: ANTHROPIC_API_KEY missing")
        try:
            import anthropic  # type: ignore
        except ImportError as exc:
            raise RuntimeError("anthropic package not installed; pip install anthropic") from exc
        self._client = anthropic.Anthropic(api_key=self.env["ANTHROPIC_API_KEY"])
        return self._client

    def estimate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        in_rate, out_rate = DEFAULT_PRICING.get(model, (3.0, 15.0))
        return (input_tokens / 1_000_000.0) * in_rate + (output_tokens / 1_000_000.0) * out_rate

    def create(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        max_tokens: int = 1024,
        recipe: str | None = None,
        correlation_id: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Issue a Messages API call and emit a cost event."""
        client = self.client()
        resp = client.messages.create(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            **kwargs,
        )

        usage = getattr(resp, "usage", None)
        in_tok = int(getattr(usage, "input_tokens", 0) or 0)
        out_tok = int(getattr(usage, "output_tokens", 0) or 0)
        cost = self.estimate_cost(model, in_tok, out_tok)

        events_ndjson.append_event(
            stream="cost",
            kind="anthropic_call",
            recipe=recipe,
            correlation_id=correlation_id,
            payload={
                "model": model,
                "input_tokens": in_tok,
                "output_tokens": out_tok,
                "amount_usd": cost,
            },
        )

        return {
            "response": resp,
            "input_tokens": in_tok,
            "output_tokens": out_tok,
            "cost_usd": cost,
            "model": model,
        }

    def ping(self) -> bool:
        return self.configured  # cheap: just check key presence
