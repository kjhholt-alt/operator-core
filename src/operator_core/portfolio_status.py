"""Portfolio status — read status-spec/v1 docs across sibling repos.

Walks ``projects_root`` looking for either a sibling repo's
``~/.operator/data/status-spec.json`` *or* a per-repo
``status-spec.json`` / ``status.json`` checked into the repo root /
docs / .status / data dirs. Renders a single rolled-up health view.

This is the demo of why the new ``[specs]`` extra is worth it: every
sibling project that emits status-spec/v1 docs (operator-core itself
once you install ``[specs]`` and the other projects as they adopt the
spec) shows up in one CLI call.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

# Health ordering (worst first) for roll-up.
_HEALTH_RANK = {"red": 3, "yellow": 2, "green": 1, None: 0}


@dataclass
class ProjectStatus:
    project: str
    source_path: Path
    health: Optional[str]
    summary: Optional[str]
    subsystem_count: int
    error_count: int
    raw: dict


def _candidate_paths(repo_dir: Path) -> Iterable[Path]:
    """Where a repo might keep its status-spec/v1 doc."""
    yield repo_dir / "status-spec.json"
    yield repo_dir / ".status" / "status-spec.json"
    yield repo_dir / "data" / "status-spec.json"
    yield repo_dir / "docs" / "status-spec.json"


def _load_status_doc(path: Path) -> Optional[dict]:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None
    try:
        doc = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(doc, dict):
        return None
    if doc.get("schema_version") != "status-spec/v1":
        return None
    return doc


_CONTAINER_DIRS = {".status", "data", "docs"}


def _fallback_project_name(source: Path) -> str:
    """Use the repo dir name, walking past container dirs like .status/data/docs."""
    parent = source.parent
    if parent.name in _CONTAINER_DIRS and parent.parent != parent:
        return parent.parent.name
    return parent.name


def _summarize(doc: dict, source: Path) -> ProjectStatus:
    subsystems = doc.get("subsystems") or []
    error_count = sum(
        1 for s in subsystems if isinstance(s, dict) and s.get("health") == "red"
    )
    return ProjectStatus(
        project=str(doc.get("project") or _fallback_project_name(source)),
        source_path=source,
        health=doc.get("health"),
        summary=doc.get("summary"),
        subsystem_count=len(subsystems),
        error_count=error_count,
        raw=doc,
    )


def collect(projects_root: Optional[Path] = None,
            extra_path: Optional[Path] = None) -> List[ProjectStatus]:
    """Walk projects_root for status-spec/v1 docs. Optionally include
    ``extra_path`` (e.g. operator-core's own ~/.operator/data/status-spec.json).
    """
    out: List[ProjectStatus] = []

    if extra_path is not None:
        doc = _load_status_doc(extra_path)
        if doc:
            out.append(_summarize(doc, extra_path))

    if projects_root is None:
        try:
            from .paths import _projects_root
            projects_root = Path(str(_projects_root()))
        except Exception:
            projects_root = Path.cwd()

    if not projects_root.exists() or not projects_root.is_dir():
        return out

    seen: set[Path] = {p.source_path for p in out}
    for repo in sorted(p for p in projects_root.iterdir() if p.is_dir()):
        if repo.name.startswith((".", "_")):
            continue
        for cand in _candidate_paths(repo):
            if cand in seen or not cand.is_file():
                continue
            doc = _load_status_doc(cand)
            if doc:
                seen.add(cand)
                out.append(_summarize(doc, cand))
                break  # one doc per repo

    return out


def overall_health(statuses: List[ProjectStatus]) -> str:
    """Worst-of-N roll-up. Returns 'unknown' if nothing reported."""
    if not statuses:
        return "unknown"
    worst = max(_HEALTH_RANK.get(s.health, 0) for s in statuses)
    for h, rank in _HEALTH_RANK.items():
        if h is not None and rank == worst:
            return h
    return "unknown"


def render_table(statuses: List[ProjectStatus]) -> str:
    if not statuses:
        return "No status-spec/v1 documents found.\n"
    name_w = max(8, max(len(s.project) for s in statuses) + 2)
    header = f"{'PROJECT':<{name_w}} {'HEALTH':<8} {'SUBS':>4} {'ERR':>4}  SUMMARY"
    lines = [header, "-" * len(header)]
    for s in sorted(statuses, key=lambda x: (-_HEALTH_RANK.get(x.health, 0), x.project)):
        marker = {"red": "x", "yellow": "!", "green": "."}.get(s.health, "?")
        summary_lines = (s.summary or "").splitlines()
        summary = summary_lines[0][:60] if summary_lines else ""
        lines.append(
            f"{s.project:<{name_w}} "
            f"{marker} {s.health or 'unknown':<6} "
            f"{s.subsystem_count:>4} "
            f"{s.error_count:>4}  {summary}"
        )
    lines.append("")
    lines.append(f"OVERALL: {overall_health(statuses)} ({len(statuses)} project(s))")
    return "\n".join(lines) + "\n"


def render_json(statuses: List[ProjectStatus]) -> str:
    payload = {
        "overall": overall_health(statuses),
        "count": len(statuses),
        "projects": [
            {
                "project": s.project,
                "source": str(s.source_path),
                "health": s.health,
                "summary": s.summary,
                "subsystems": s.subsystem_count,
                "errors": s.error_count,
                "raw": s.raw,
            }
            for s in statuses
        ],
    }
    return json.dumps(payload, indent=2) + "\n"
