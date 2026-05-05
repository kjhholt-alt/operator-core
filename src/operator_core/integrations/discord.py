"""Discord adapter -- single client recipes share via ``ctx.clients['discord']``.

Wraps the ``operator-scripts/utils/discord.py`` notify() flow but with
structured logging and dependency-free fallbacks. No raw requests / httpx.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

logger = logging.getLogger("operator.integration.discord")

COLORS = {"green": 0x2ECC71, "yellow": 0xF39C12, "red": 0xE74C3C, "blue": 0x3498DB}

ENV_MAP = {
    "projects": "DISCORD_PROJECTS_WEBHOOK_URL",
    "code_review": "DISCORD_CODE_REVIEW_WEBHOOK_URL",
    "deploys": "DISCORD_DEPLOYS_WEBHOOK_URL",
    "automations": "DISCORD_AUTOMATIONS_WEBHOOK_URL",
    "claude_chat": "DISCORD_WEBHOOK_URL",
}

FALLBACK_CHAIN = {
    "code_review": ["projects", "claude_chat"],
    "automations": ["projects", "claude_chat"],
    "deploys": ["claude_chat"],
    "projects": ["claude_chat"],
}


class DiscordAdapter:
    """Posts color-coded embeds via Discord webhooks. Rate-limited 1 msg/sec."""

    def __init__(self, env: dict[str, str] | None = None) -> None:
        self.env = env if env is not None else os.environ
        self._last_send = 0.0

    def webhook_url(self, channel: str) -> str | None:
        for key in [channel, *FALLBACK_CHAIN.get(channel, [])]:
            env_var = ENV_MAP.get(key)
            if not env_var:
                continue
            url = self.env.get(env_var)
            if url:
                if key != channel:
                    logger.debug("discord.fallback", extra={"channel": channel, "to": key})
                return url
        return None

    def notify(
        self,
        *,
        channel: str,
        title: str,
        body: str = "",
        color: str = "blue",
        footer: str = "",
        fields: list[dict[str, Any]] | None = None,
        content: str = "",
    ) -> bool:
        url = self.webhook_url(channel)
        if not url:
            logger.warning("discord.no_webhook", extra={"channel": channel})
            return False

        embed: dict[str, Any] = {
            "title": title[:256],
            "description": _truncate(body),
            "color": COLORS.get(color, COLORS["blue"]),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if footer:
            embed["footer"] = {"text": footer[:2048]}
        if fields:
            embed["fields"] = [
                {"name": f["name"][:256], "value": f["value"][:1024], "inline": f.get("inline", False)}
                for f in fields[:25]
            ]
        payload: dict[str, Any] = {"embeds": [embed]}
        if content:
            payload["content"] = content[:2000]

        return self._send(url, payload)

    # --- internals ------------------------------------------------------------

    def _send(self, url: str, payload: dict[str, Any]) -> bool:
        elapsed = time.time() - self._last_send
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        data = json.dumps(payload).encode("utf-8")
        req = Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "OperatorRecipe/1.0",
            },
        )
        try:
            with urlopen(req, timeout=10) as resp:
                self._last_send = time.time()
                ok = resp.status in (200, 204)
                if not ok:
                    logger.warning("discord.http_status", extra={"status": resp.status})
                return ok
        except URLError as exc:
            logger.error("discord.url_error", extra={"error": str(exc)})
            return False


def _truncate(text: str, max_len: int = 4000) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 20] + "\n\n... (truncated)"
