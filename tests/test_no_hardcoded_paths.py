"""Guardrail: no recipe should hardcode a host-specific user directory.

Recipes ran fine on the original developer's machine and silently
verify-failed everywhere else (project_snapshot + 10 others). Catching
the pattern at PR time avoids the next round.

If a recipe genuinely needs a fixed path (e.g. a network share), set it
via env override and resolve through `recipes._paths` so the failure
mode stays loud.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
RECIPES_DIR = REPO_ROOT / "recipes"
SRC_DIR = REPO_ROOT / "src"

# Patterns that should not appear in tracked source. Tests are exempt
# (they may need fixture data referencing concrete paths).
FORBIDDEN = re.compile(
    r"(C:[/\\]Users[/\\](Kruz|GQETCUM)[/\\]"        # Windows user dirs
    r"|/Users/[a-zA-Z0-9_-]+/"                       # macOS user dirs
    r")"
)

ALLOWLIST = {
    # The helper module documents the historical hardcoded path in its
    # docstring as motivation. Exempt it explicitly.
    "recipes\\_paths.py",
    "recipes/_paths.py",
}


def _collect_python_files() -> list[Path]:
    out: list[Path] = []
    for base in (RECIPES_DIR, SRC_DIR):
        for p in base.rglob("*.py"):
            if "_vendor" in p.parts:
                continue
            if any(part.startswith(".") for part in p.parts):
                continue
            out.append(p)
    return out


@pytest.mark.parametrize("path", _collect_python_files(), ids=lambda p: str(p.relative_to(REPO_ROOT)))
def test_no_hardcoded_user_paths(path: Path) -> None:
    rel = path.relative_to(REPO_ROOT)
    if str(rel) in ALLOWLIST:
        pytest.skip(f"explicitly allow-listed: {rel}")
    text = path.read_text(encoding="utf-8")
    matches = FORBIDDEN.findall(text)
    assert not matches, (
        f"{rel} contains a host-specific user-dir path: {matches!r}. "
        f"Resolve via `recipes._paths.projects_dir()` or an env override "
        f"instead."
    )
