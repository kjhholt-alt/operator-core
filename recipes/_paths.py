"""Path helpers for recipes.

Recipes were originally extracted from one founder's monolith and inherited
hardcoded `C:/Users/Kruz/Desktop/Projects` paths. Anyone else running
operator-core (CI, a second machine, a downstream user) hits a silent
verify() failure because those paths don't exist on their host.

This module gives recipes one place to ask "where are my projects?" and
"where do I read static state from?" with the right precedence:

    1. Explicit env override (e.g. OPERATOR_PROJECTS_DIR)
    2. operator-core settings (`operator init`-derived projects_root)
    3. Common host conventions ($HOME/Desktop/Projects, $HOME/Projects)
    4. cwd as a last resort

Recipes should call ``projects_dir()`` at module load and treat the
return as immutable for the lifetime of the process.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def projects_dir(env_var: str = "OPERATOR_PROJECTS_DIR") -> Path:
    """Return the user's projects root, resolved with sensible fallbacks."""
    override = os.environ.get(env_var)
    if override:
        return Path(override)
    try:
        from operator_core.paths import _projects_root
        root = Path(str(_projects_root()))
        if root.exists():
            return root
    except Exception:
        pass
    home = Path.home()
    for candidate in (home / "Desktop" / "Projects", home / "Projects"):
        if candidate.exists():
            return candidate
    return Path.cwd()


def project_subpath(*parts: str, env_var: Optional[str] = None) -> Path:
    """Return projects_dir / parts, with optional explicit env override."""
    if env_var:
        override = os.environ.get(env_var)
        if override:
            return Path(override)
    return projects_dir().joinpath(*parts)
