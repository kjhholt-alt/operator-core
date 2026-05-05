"""Generate run-<name>.bat redirects for every registered recipe.

Run from the operator-core repo root:

    py scripts/redirects/generate.py

The generated .bat files write logs to ``%~dp0..\\logs\\<name>.log``
(i.e. ``operator-core/logs/<name>.log``) so the scheduler doesn't
litter random paths.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SRC_DIR = REPO_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from operator_core.recipes import discover_recipes  # noqa: E402

TEMPLATE = """\
@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist ..\\..\\logs mkdir ..\\..\\logs
py -m operator_core.cli recipe run {name} >> ..\\..\\logs\\{name}.log 2>&1
exit /b %errorlevel%
"""


def main() -> int:
    recipes_dir = REPO_ROOT / "recipes"
    found = discover_recipes(recipes_dir)
    out_dir = Path(__file__).resolve().parent
    written = 0
    for cls in found:
        target = out_dir / f"run-{cls.name.replace('_', '-')}.bat"
        target.write_text(TEMPLATE.format(name=cls.name), encoding="utf-8")
        written += 1
    print(f"wrote {written} redirect .bat files to {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
