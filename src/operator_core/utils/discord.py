"""
Discord webhook notification utility.
Reads webhook URLs from DISCORD_WEBHOOK_REGISTRY.json and posts
color-coded embeds to the correct channel.
"""
import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError
from dotenv import load_dotenv

try:
    from ..config import webhook_url_for
    from ..paths import WEBHOOK_REGISTRY as _REGISTRY_PATH_LAZY
except ImportError:  # pragma: no cover - keeps legacy direct execution resilient
    webhook_url_for = None
    _REGISTRY_PATH_LAZY = None

# Historic external-registry file. Resolves via the configured data_dir
# (~/.operator/data/discord-webhooks.json) unless OPERATOR_WEBHOOK_REGISTRY
# env var overrides.
_ENV_REG = os.environ.get("OPERATOR_WEBHOOK_REGISTRY")
if _ENV_REG:
    REGISTRY_PATH = Path(_ENV_REG)
elif _REGISTRY_PATH_LAZY is not None:
    REGISTRY_PATH = _REGISTRY_PATH_LAZY  # LazyPath resolves at first use
else:
    REGISTRY_PATH = Path.home() / ".operator" / "data" / "discord-webhooks.json"

load_dotenv()

# Color codes for Discord embeds
COLORS = {
    "green": 0x2ECC71,
    "yellow": 0xF39C12,
    "red": 0xE74C3C,
    "blue": 0x3498DB,
}

# Rate limit: track last send time
_last_send = 0.0

# Cache the registry
_registry_cache = None


def _load_registry() -> dict:
    global _registry_cache
    if _registry_cache is not None:
        return _registry_cache
    if REGISTRY_PATH.exists():
        with open(REGISTRY_PATH, "r") as f:
            _registry_cache = json.load(f)
        return _registry_cache
    return {"webhooks": {}}


def get_webhook_url(channel: str) -> str | None:
    """Get webhook URL for a channel key (e.g., 'projects', 'code_review', 'deploys', 'automations')."""
    if webhook_url_for is not None:
        resolved = webhook_url_for(channel, REGISTRY_PATH)
        if resolved:
            return resolved

    registry = _load_registry()
    entry = registry.get("webhooks", {}).get(channel)
    if entry and entry.get("env_var") and os.environ.get(entry["env_var"]):
        return os.environ[entry["env_var"]]
    # Also check env vars as fallback
    env_map = {
        "projects": "DISCORD_PROJECTS_WEBHOOK_URL",
        "code_review": "DISCORD_CODE_REVIEW_WEBHOOK_URL",
        "deploys": "DISCORD_DEPLOYS_WEBHOOK_URL",
        "automations": "DISCORD_AUTOMATIONS_WEBHOOK_URL",
        "claude_chat": "DISCORD_WEBHOOK_URL",
    }
    env_key = env_map.get(channel)
    if env_key:
        return os.environ.get(env_key)
    if entry and os.environ.get("OPERATOR_ALLOW_REGISTRY_WEBHOOK_URLS") == "1":
        return entry.get("url")
    return None


def _truncate(text: str, max_len: int = 4000) -> str:
    """Truncate text to fit Discord embed limits."""
    if len(text) <= max_len:
        return text
    return text[: max_len - 20] + "\n\n... (truncated)"


def _send_webhook(url: str, payload: dict) -> bool:
    """Send a webhook payload. Returns True on success."""
    global _last_send
    # Rate limit: 1 msg/sec
    elapsed = time.time() - _last_send
    if elapsed < 1.0:
        time.sleep(1.0 - elapsed)

    data = json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, headers={
        "Content-Type": "application/json",
        "User-Agent": "OperatorCore (https://github.com/kjhholt-alt/operator-core, 0.1)",
    })
    try:
        with urlopen(req, timeout=10) as resp:
            _last_send = time.time()
            return resp.status in (200, 204)
    except URLError as e:
        print(f"[discord] Webhook error: {e}")
        return False


def notify(
    channel: str,
    title: str,
    body: str = "",
    color: str = "blue",
    footer: str = "",
    fields: list[dict] | None = None,
    content: str = "",
) -> bool:
    """
    Post a message to a Discord channel via webhook.

    Args:
        channel: Registry key — 'projects', 'code_review', 'deploys', 'automations'
        title: Embed title
        body: Embed description (markdown supported)
        color: 'green', 'yellow', 'red', or 'blue'
        footer: Footer text (e.g., "morning-briefing | Cost: $0.42 | 2026-04-09 07:03")
        fields: Optional list of {"name": "...", "value": "...", "inline": True/False}
        content: Optional text outside the embed (for @here mentions etc.)
    """
    url = get_webhook_url(channel)
    if not url:
        print(f"[discord] No webhook URL found for channel '{channel}'")
        return False

    embed = {
        "title": title[:256],
        "description": _truncate(body),
        "color": COLORS.get(color, COLORS["blue"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if footer:
        embed["footer"] = {"text": footer[:2048]}

    if fields:
        embed["fields"] = [
            {
                "name": f["name"][:256],
                "value": f["value"][:1024],
                "inline": f.get("inline", False),
            }
            for f in fields[:25]  # Discord max 25 fields
        ]

    payload = {"embeds": [embed]}
    if content:
        payload["content"] = content[:2000]

    return _send_webhook(url, payload)


def notify_sync(channel: str, **kwargs) -> bool:
    """Synchronous wrapper for notify()."""
    return notify(channel, **kwargs)


def make_footer(script_name: str, cost: float | None = None, duration: float | None = None) -> str:
    """Build a standard footer string."""
    parts = [script_name]
    if cost is not None:
        parts.append(f"Cost: ${cost:.2f}")
    if duration is not None:
        parts.append(f"{duration:.0f}s")
    parts.append(datetime.now().strftime("%Y-%m-%d %H:%M"))
    return " | ".join(parts)
