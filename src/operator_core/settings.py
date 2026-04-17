"""Runtime settings for Operator Core.

Single source of truth for WHERE things live on disk. All other modules
import from here. This replaces the legacy `paths.py` + `config.py` split
in the monolith — that one was env-var driven with package-relative
defaults that only work for the original author. This one is TOML-driven
from `~/.operator/config.toml` (override with `OPERATOR_CONFIG`).

Structure:
    Settings
      ├─ config_path   — the TOML file
      ├─ data_dir      — ~/.operator/data by default
      ├─ projects_root — user-configured (REQUIRED in config)
      ├─ worktrees_dir — ~/.operator/worktrees by default (outside projects_root
      │                  so Node toolchains don't walk up from a worktree and
      │                  discover the monorepo's package-lock.json)
      ├─ daemon        — bind/port
      ├─ discord       — channel name → env var for webhook URL
      └─ projects      — list[ProjectConfig]

Load once per process with `load_settings()`. For tests, pass an explicit
config path or use `Settings.from_dict()`.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


DEFAULT_CONFIG_PATH = Path.home() / ".operator" / "config.toml"
DEFAULT_DATA_DIR = Path.home() / ".operator" / "data"
DEFAULT_WORKTREES_DIR = Path.home() / ".operator" / "worktrees"


class ConfigError(RuntimeError):
    """Raised when the config file is missing, invalid, or incomplete."""


@dataclass(frozen=True)
class DeployConfig:
    provider: str
    url: str


@dataclass(frozen=True)
class HealthConfig:
    path: str
    expected_status: int = 200


@dataclass(frozen=True)
class RevenueConfig:
    """Optional per-project revenue lookup for the /kruz revenue heartbeat.

    provider: "stripe" | "supabase" | "none" (default: "none" — zeros).
    signups_table / subscriptions_table: Supabase tables to COUNT rows
    from (when provider is "supabase"). Either can be omitted to skip.
    """

    provider: str = "none"
    signups_table: str | None = None
    subscriptions_table: str | None = None
    mrr_field: str | None = None  # column name holding per-row $/mo


@dataclass(frozen=True)
class ProjectConfig:
    slug: str
    path: Path
    repo: str
    type: str
    deploy: DeployConfig
    health: HealthConfig
    checks: list[str]
    autonomy_tier: str
    protected_patterns: list[str]
    auto_merge: bool = False
    revenue: RevenueConfig | None = None

    @property
    def deploy_health_url(self) -> str:
        base = self.deploy.url.rstrip("/")
        path = self.health.path
        if not path.startswith("/"):
            path = "/" + path
        return base + path


@dataclass(frozen=True)
class DaemonConfig:
    bind: str = "127.0.0.1"
    port: int = 8765


@dataclass
class Settings:
    """Resolved runtime settings. Load with `load_settings()`."""

    config_path: Path
    data_dir: Path
    projects_root: Path
    worktrees_dir: Path
    github_handle: str
    daemon: DaemonConfig
    discord_channels: dict[str, str]
    projects: list[ProjectConfig] = field(default_factory=list)

    # Derived paths (computed once for convenience)
    @property
    def db_path(self) -> Path:
        return self.data_dir / "operator.sqlite3"

    @property
    def status_path(self) -> Path:
        return self.data_dir / "status.json"

    @property
    def scheduler_state_path(self) -> Path:
        return self.data_dir / "scheduler-state.json"

    @property
    def webhook_registry_path(self) -> Path:
        return self.data_dir / "discord-webhooks.json"

    def ensure_dirs(self) -> None:
        """Create every directory the daemon writes to."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.worktrees_dir.mkdir(parents=True, exist_ok=True)

    def find_project(self, slug_or_name: str) -> ProjectConfig:
        needle = slug_or_name.strip().lower()
        for project in self.projects:
            names = {project.slug.lower(), project.path.name.lower()}
            if needle in names:
                return project
        raise ConfigError(f"Unknown project: {slug_or_name}")

    @classmethod
    def from_dict(
        cls,
        raw: dict[str, Any],
        *,
        config_path: Path = DEFAULT_CONFIG_PATH,
    ) -> "Settings":
        """Build a Settings object from a parsed-TOML dict. Validates as it goes."""
        user = _require_table(raw, "user")
        data = raw.get("data", {}) or {}
        daemon = raw.get("daemon", {}) or {}
        discord = raw.get("discord", {}) or {}
        discord_channels = discord.get("channels", {}) or {}

        # REQUIRED — no sensible default.
        projects_root_raw = user.get("projects_root")
        if not projects_root_raw:
            raise ConfigError("config: [user].projects_root is required")
        projects_root = _expand(projects_root_raw)
        if not projects_root.is_absolute():
            raise ConfigError(
                f"config: [user].projects_root must be an absolute path "
                f"(got {projects_root_raw})"
            )

        github_handle = user.get("github") or user.get("github_handle") or ""
        if not github_handle:
            raise ConfigError("config: [user].github is required")

        data_dir = _expand(data.get("dir") or DEFAULT_DATA_DIR)
        worktrees_dir = _expand(data.get("worktrees_dir") or DEFAULT_WORKTREES_DIR)

        daemon_cfg = DaemonConfig(
            bind=str(daemon.get("bind", "127.0.0.1")),
            port=int(daemon.get("port", 8765)),
        )

        if not isinstance(discord_channels, dict):
            raise ConfigError("config: [discord.channels] must be a table")
        discord_channels = {
            str(k): str(v) for k, v in discord_channels.items() if v is not None
        }

        projects_raw = raw.get("projects") or []
        if projects_raw and not isinstance(projects_raw, list):
            raise ConfigError("config: [[projects]] must be an array of tables")
        projects = [
            _project_from_dict(item, projects_root=projects_root)
            for item in projects_raw
        ]

        slugs = [p.slug for p in projects]
        duplicates = sorted({s for s in slugs if slugs.count(s) > 1})
        if duplicates:
            raise ConfigError(f"Duplicate project slugs: {', '.join(duplicates)}")

        return cls(
            config_path=config_path,
            data_dir=data_dir,
            projects_root=projects_root,
            worktrees_dir=worktrees_dir,
            github_handle=str(github_handle),
            daemon=daemon_cfg,
            discord_channels=discord_channels,
            projects=projects,
        )


def _expand(path_like: Any) -> Path:
    """Expand ~/ and env vars, return an absolute Path."""
    return Path(os.path.expandvars(os.path.expanduser(str(path_like)))).resolve()


def _require_table(raw: dict[str, Any], key: str) -> dict[str, Any]:
    val = raw.get(key)
    if val is None:
        raise ConfigError(f"config: missing required section [{key}]")
    if not isinstance(val, dict):
        raise ConfigError(f"config: [{key}] must be a table")
    return val


def _project_from_dict(
    raw: dict[str, Any], *, projects_root: Path
) -> ProjectConfig:
    required = [
        "slug",
        "path",
        "repo",
        "type",
        "autonomy_tier",
    ]
    missing = [k for k in required if k not in raw]
    if missing:
        raise ConfigError(
            f"project {raw.get('slug', '<unknown>')!r} missing keys: "
            f"{', '.join(missing)}"
        )

    deploy = raw.get("deploy") or {}
    if not isinstance(deploy, dict) or not deploy.get("provider") or not deploy.get("url"):
        raise ConfigError(
            f"project {raw['slug']!r}: [projects.deploy] must set provider + url"
        )

    health = raw.get("health") or {}
    if not isinstance(health, dict) or not health.get("path"):
        raise ConfigError(
            f"project {raw['slug']!r}: [projects.health] must set path"
        )

    project_path = Path(str(raw["path"]))
    if not project_path.is_absolute():
        project_path = projects_root / project_path

    revenue_raw = raw.get("revenue")
    revenue_cfg: RevenueConfig | None = None
    if isinstance(revenue_raw, dict):
        revenue_cfg = RevenueConfig(
            provider=str(revenue_raw.get("provider") or "none"),
            signups_table=(
                str(revenue_raw["signups_table"])
                if revenue_raw.get("signups_table")
                else None
            ),
            subscriptions_table=(
                str(revenue_raw["subscriptions_table"])
                if revenue_raw.get("subscriptions_table")
                else None
            ),
            mrr_field=(
                str(revenue_raw["mrr_field"])
                if revenue_raw.get("mrr_field")
                else None
            ),
        )

    return ProjectConfig(
        slug=str(raw["slug"]),
        path=project_path,
        repo=str(raw["repo"]),
        type=str(raw["type"]),
        deploy=DeployConfig(
            provider=str(deploy["provider"]),
            url=str(deploy["url"]),
        ),
        health=HealthConfig(
            path=str(health["path"]),
            expected_status=int(health.get("expected_status", 200)),
        ),
        checks=[str(c) for c in raw.get("checks", [])],
        autonomy_tier=str(raw["autonomy_tier"]),
        protected_patterns=[str(p) for p in raw.get("protected_patterns", [])],
        auto_merge=bool(raw.get("auto_merge", False)),
        revenue=revenue_cfg,
    )


def config_path() -> Path:
    """Resolve the active config path. `OPERATOR_CONFIG` env var overrides."""
    override = os.environ.get("OPERATOR_CONFIG")
    if override:
        return _expand(override)
    return DEFAULT_CONFIG_PATH


_cached_settings: Settings | None = None


def load_settings(path: Path | None = None, *, reload: bool = False) -> Settings:
    """Load and validate settings from TOML. Cached per process.

    Raises `ConfigError` if the file is missing or invalid. Callers that
    want to bootstrap (e.g. `operator init`) should catch and handle.
    """
    global _cached_settings
    if _cached_settings is not None and not reload and path is None:
        return _cached_settings

    target = path or config_path()
    if not target.exists():
        raise ConfigError(
            f"config not found at {target}. "
            f"Run `operator init` to create it."
        )
    try:
        with open(target, "rb") as fh:
            raw = tomllib.load(fh)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"config: {target} is not valid TOML: {exc}") from exc

    settings = Settings.from_dict(raw, config_path=target)
    if path is None:
        _cached_settings = settings
    return settings


def clear_cache() -> None:
    """Forget the cached settings — useful after writing the config."""
    global _cached_settings
    _cached_settings = None
