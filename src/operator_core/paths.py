"""Compatibility shim over settings.py.

Historic code imports `PROJECTS_ROOT`, `DATA_DIR`, etc. as module-level
constants from `operator_v3.paths`. Every one of those is now derived from
the user's config at load time. This module preserves those names so the
bulk-copied modules keep working without a mechanical rewrite.

Lazy-resolve so `operator init` (which runs BEFORE a valid config exists)
doesn't crash on import.
"""

from __future__ import annotations

from pathlib import Path

from .settings import DEFAULT_DATA_DIR, DEFAULT_WORKTREES_DIR, load_settings


# Compatibility constant — the python package dir. Legacy modules used
# this to resolve bundled templates; still works for that.
PACKAGE_ROOT: Path = Path(__file__).resolve().parent


def _safe_settings():
    try:
        return load_settings()
    except Exception:
        return None


class _LazyPath:
    """Proxy that resolves to the real Path the first time it's used."""

    def __init__(self, resolver):
        self._resolver = resolver
        self._cache: Path | None = None

    def _resolve(self) -> Path:
        if self._cache is None:
            self._cache = self._resolver()
        return self._cache

    def __fspath__(self) -> str:
        return str(self._resolve())

    def __str__(self) -> str:
        return str(self._resolve())

    def __repr__(self) -> str:
        return f"<LazyPath {self._resolve()!r}>"

    def __truediv__(self, other):
        return self._resolve() / other

    def __rtruediv__(self, other):
        return other / self._resolve()

    def __eq__(self, other):
        return self._resolve() == other

    def __hash__(self):
        return hash(self._resolve())

    def exists(self):
        return self._resolve().exists()

    def mkdir(self, *args, **kwargs):
        return self._resolve().mkdir(*args, **kwargs)

    @property
    def parent(self):
        return self._resolve().parent

    @property
    def name(self):
        return self._resolve().name


def _projects_root() -> Path:
    s = _safe_settings()
    if s:
        return s.projects_root
    raise RuntimeError("projects_root unavailable — run `operator init` first")


def _data_dir() -> Path:
    s = _safe_settings()
    return s.data_dir if s else DEFAULT_DATA_DIR


def _worktrees_dir() -> Path:
    s = _safe_settings()
    return s.worktrees_dir if s else DEFAULT_WORKTREES_DIR


def _db_path() -> Path:
    s = _safe_settings()
    return s.db_path if s else DEFAULT_DATA_DIR / "operator.sqlite3"


def _status_path() -> Path:
    s = _safe_settings()
    return s.status_path if s else DEFAULT_DATA_DIR / "status.json"


def _scheduler_state_path() -> Path:
    s = _safe_settings()
    return s.scheduler_state_path if s else DEFAULT_DATA_DIR / "scheduler-state.json"


def _webhook_registry_path() -> Path:
    s = _safe_settings()
    return s.webhook_registry_path if s else DEFAULT_DATA_DIR / "discord-webhooks.json"


# Lazy module-level constants — same names the legacy codebase expects.
PROJECTS_ROOT = _LazyPath(_projects_root)
DATA_DIR = _LazyPath(_data_dir)
WORKTREES_DIR = _LazyPath(_worktrees_dir)
DB_PATH = _LazyPath(_db_path)
STATUS_PATH = _LazyPath(_status_path)
SCHEDULER_STATE_PATH = _LazyPath(_scheduler_state_path)
WEBHOOK_REGISTRY = _LazyPath(_webhook_registry_path)

# Aliases for code that reached for the config dir directly.
CONFIG_DIR = _LazyPath(_data_dir)
PROJECTS_CONFIG = _LazyPath(
    lambda: (
        _safe_settings().config_path
        if _safe_settings()
        else Path("~/.operator/config.toml").expanduser()
    )
)


def ensure_data_dirs() -> None:
    """Create the directories the daemon writes to. Safe before config exists."""
    s = _safe_settings()
    if s:
        s.ensure_dirs()
    else:
        DEFAULT_DATA_DIR.mkdir(parents=True, exist_ok=True)
        DEFAULT_WORKTREES_DIR.mkdir(parents=True, exist_ok=True)
