"""Compatibility shim — legacy API over settings.py.

Every import site historically pulls `load_projects`, `find_project`,
`DeployConfig`, `HealthConfig`, `ProjectConfig`, `ConfigError`, plus the
`webhook_env_for` / `webhook_url_for` helpers. Keep them working while
the actual loading/validation lives in settings.py.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .settings import (
    ConfigError,
    DeployConfig,
    HealthConfig,
    ProjectConfig,
    load_settings,
)

__all__ = [
    "ConfigError",
    "DeployConfig",
    "HealthConfig",
    "ProjectConfig",
    "load_projects",
    "find_project",
    "load_webhook_registry",
    "webhook_env_for",
    "webhook_url_for",
]


def load_projects(path: Path | None = None) -> list[ProjectConfig]:
    """Load the project list from the active config (or a specific file)."""
    settings = load_settings(path)
    return list(settings.projects)


def find_project(
    slug_or_path: str, projects: list[ProjectConfig] | None = None
) -> ProjectConfig:
    """Resolve a slug or directory name to a ProjectConfig."""
    if projects is not None:
        needle = slug_or_path.strip().lower()
        for project in projects:
            if needle in {project.slug.lower(), project.path.name.lower()}:
                return project
        raise ConfigError(f"Unknown project: {slug_or_path}")
    return load_settings().find_project(slug_or_path)


def load_webhook_registry(path: Path | None = None) -> dict[str, Any]:
    """Return the Discord channel → env-var mapping.

    In the legacy format this was `{"webhooks": {channel: {env_var: ...}}}`.
    We preserve that shape for consumers that haven't migrated.
    """
    channels = load_settings().discord_channels
    return {
        "webhooks": {
            channel: {"env_var": env_var} for channel, env_var in channels.items()
        }
    }


def webhook_env_for(channel: str, registry_path: Path | None = None) -> str | None:
    """Env var name that holds the webhook URL for `channel`."""
    return load_settings().discord_channels.get(channel)


def webhook_url_for(channel: str, registry_path: Path | None = None) -> str | None:
    """Resolve `channel` → webhook URL via the env var, or legacy fallbacks.

    Webhook URLs are never stored in config — only the env var name is. This
    keeps secrets out of version control.
    """
    env_name = webhook_env_for(channel, registry_path)
    if env_name and os.environ.get(env_name):
        return os.environ[env_name]

    legacy_env = {
        "projects": "DISCORD_PROJECTS_WEBHOOK_URL",
        "code_review": "DISCORD_CODE_REVIEW_WEBHOOK_URL",
        "deploys": "DISCORD_DEPLOYS_WEBHOOK_URL",
        "automations": "DISCORD_AUTOMATIONS_WEBHOOK_URL",
        "claude_chat": "DISCORD_WEBHOOK_URL",
    }.get(channel)
    if legacy_env and os.environ.get(legacy_env):
        return os.environ[legacy_env]

    return None
