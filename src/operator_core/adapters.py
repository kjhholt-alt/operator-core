"""Project adapter registry for the native daemon.

Each project in the portfolio plugs into the daemon through an adapter that
declares what the daemon can observe, check, and do for that project.
Adapters are loaded from ``config/projects.json`` and enriched with per-project
``.operator-adapter.json`` overrides when present.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from .config import ProjectConfig, load_projects
from .paths import PROJECTS_ROOT


# ── Enums ──────────────────────────────────────────────────────────────────────

class ProjectType(Enum):
    SAAS = "saas"
    INTERNAL = "internal"
    INTERNAL_REPORT = "internal_report"
    CLIENT = "client"
    GAME = "game"
    OPERATOR = "operator"
    MARKETING = "marketing"


class Urgency(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


# ── Adapter components ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class HealthCheck:
    name: str
    command: str | None = None
    url: str | None = None
    expected_status: int = 200
    timeout_seconds: int = 30


@dataclass(frozen=True)
class Signal:
    """A fact the adapter can observe about the project."""
    name: str
    collector: str  # "git", "shell", "http", "file", "supabase"
    command: str | None = None
    url: str | None = None


@dataclass(frozen=True)
class Workflow:
    """A runnable action the daemon can execute for this project."""
    name: str
    command: str
    risk_tier: str = "low"
    autonomous_ok: bool = True
    produces_artifact: bool = False
    artifact_pattern: str | None = None


# ── Main adapter ───────────────────────────────────────────────────────────────

@dataclass
class ProjectAdapter:
    slug: str
    path: Path
    project_type: ProjectType
    repo: str | None = None

    health_checks: list[HealthCheck] = field(default_factory=list)
    signals: list[Signal] = field(default_factory=list)
    workflows: list[Workflow] = field(default_factory=list)

    protected_patterns: list[str] = field(default_factory=list)
    autonomy_tier: str = "manual"

    sprint_doc: str | None = None
    blockers: list[str] = field(default_factory=list)
    revenue_proximity: str = "none"  # "live", "near", "far", "none"

    domain_commands: dict[str, str] = field(default_factory=dict)

    @property
    def display_name(self) -> str:
        return self.slug.replace("-", " ").title()


# ── Registry ───────────────────────────────────────────────────────────────────

ADAPTER_REGISTRY: dict[str, ProjectAdapter] = {}


def _type_from_str(raw: str) -> ProjectType:
    try:
        return ProjectType(raw)
    except ValueError:
        return ProjectType.INTERNAL


def _build_adapter(cfg: ProjectConfig) -> ProjectAdapter:
    """Build an adapter from a ProjectConfig (projects.json entry)."""
    health_checks: list[HealthCheck] = []
    if cfg.deploy.url and cfg.deploy.provider != "local":
        health_checks.append(HealthCheck(
            name="deploy",
            url=cfg.deploy_health_url,
            expected_status=cfg.health.expected_status,
        ))
    for cmd in cfg.checks:
        name = cmd.split()[1] if len(cmd.split()) > 1 else cmd.split()[0]
        name = name.lower().replace("run", "").strip()
        health_checks.append(HealthCheck(name=name or "check", command=cmd))

    signals = [
        Signal(name="git_dirty", collector="git"),
        Signal(name="deploy_status", collector="http", url=cfg.deploy_health_url if cfg.deploy.url else None),
    ]

    workflows = [
        Workflow(name="run_tests", command=next((c for c in cfg.checks if "test" in c.lower()), "echo no tests"), risk_tier="low"),
        Workflow(name="build", command=next((c for c in cfg.checks if "build" in c.lower()), "echo no build"), risk_tier="low"),
    ]

    proximity = "none"
    if cfg.type == "saas":
        proximity = "near"

    adapter = ProjectAdapter(
        slug=cfg.slug,
        path=cfg.path,
        project_type=_type_from_str(cfg.type),
        repo=cfg.repo,
        health_checks=health_checks,
        signals=signals,
        workflows=workflows,
        protected_patterns=cfg.protected_patterns,
        autonomy_tier=cfg.autonomy_tier,
        revenue_proximity=proximity,
    )
    return adapter


def _apply_override(adapter: ProjectAdapter, overrides: dict[str, Any]) -> None:
    """Apply per-project .operator-adapter.json overrides."""
    if "blockers" in overrides:
        adapter.blockers = list(overrides["blockers"])
    if "revenue_proximity" in overrides:
        adapter.revenue_proximity = str(overrides["revenue_proximity"])
    if "sprint_doc" in overrides:
        adapter.sprint_doc = str(overrides["sprint_doc"])
    if "domain_commands" in overrides:
        adapter.domain_commands.update(overrides["domain_commands"])
    if "autonomy_tier" in overrides:
        adapter.autonomy_tier = str(overrides["autonomy_tier"])


def load_adapters(projects: list[ProjectConfig] | None = None) -> dict[str, ProjectAdapter]:
    """Build adapter registry from projects.json + per-project overrides."""
    global ADAPTER_REGISTRY
    configs = projects or load_projects()
    registry: dict[str, ProjectAdapter] = {}

    for cfg in configs:
        adapter = _build_adapter(cfg)
        override_path = cfg.path / ".operator-adapter.json"
        if override_path.exists():
            try:
                data = json.loads(override_path.read_text(encoding="utf-8"))
                _apply_override(adapter, data)
            except (json.JSONDecodeError, OSError):
                pass  # skip bad overrides
        registry[adapter.slug] = adapter

    ADAPTER_REGISTRY = registry
    return registry


def get_adapter(slug: str) -> ProjectAdapter | None:
    """Get an adapter by project slug."""
    if not ADAPTER_REGISTRY:
        load_adapters()
    return ADAPTER_REGISTRY.get(slug)


def list_adapters() -> list[ProjectAdapter]:
    """List all registered adapters."""
    if not ADAPTER_REGISTRY:
        load_adapters()
    return list(ADAPTER_REGISTRY.values())
