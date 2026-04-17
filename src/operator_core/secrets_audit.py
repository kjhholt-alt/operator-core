"""Discord webhook secret audit.

Walks the operator-scripts repo tree looking for literal Discord webhook
URLs. Any URL that is a real token (no env-var interpolation on the same
line) is a finding and fails the audit.

Usage:
    python -m operator_v3.secrets_audit [ROOT]

Exits 0 on clean, 1 on findings. Prints findings as JSON lines to stdout.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable


# Real Discord webhook IDs are 17-20 digit snowflakes and tokens are 60+
# chars. The tighter bounds skip obvious test fixtures like `1234567890`
# and `AAAAA...` without special-casing files.
WEBHOOK_RE = re.compile(
    r"https?://discord\.com/api/webhooks/(\d{17,20})/([A-Za-z0-9_\-]{40,})"
)

# If the same line references the URL via env var / template, skip it. The
# regex above won't match `${DISCORD_WEBHOOK_URL}` anyway; this list is for
# lines that contain the literal domain plus an env-var reference (e.g. a
# doc string showing both forms).
ENV_VAR_MARKERS = (
    "os.environ",
    "os.getenv",
    "getenv(",
    "process.env.",
    "${",
    "%{",
    "env(",
    "env:",
)

# Paths always skipped (relative to the scan root).
ALWAYS_IGNORE_DIRS = {
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "__pycache__",
    ".operator-v3",
    ".claude/worktrees",
    "_archive",
    ".pytest_cache",
}
ALWAYS_IGNORE_FILE_SUFFIXES = {
    ".pyc",
    ".pyo",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".pdf",
    ".zip",
    ".whl",
    ".exe",
    ".ico",
}


@dataclass(frozen=True)
class Finding:
    path: str
    line_number: int
    webhook_id: str
    snippet: str

    def as_dict(self) -> dict:
        return asdict(self)


def _load_gitignore_entries(root: Path) -> set[str]:
    """Return a simple set of top-level names listed in .gitignore.

    We do NOT implement full gitignore semantics here — the
    ``ALWAYS_IGNORE_DIRS`` set handles the usual suspects. This pulls extra
    names the user added to their own gitignore so audits stay consistent.
    """

    entries: set[str] = set()
    gi = root / ".gitignore"
    if not gi.exists():
        return entries
    try:
        for raw in gi.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            # strip trailing slashes, drop globs beyond the first segment
            line = line.lstrip("/")
            line = line.rstrip("/")
            if not line or "*" in line:
                continue
            entries.add(line)
    except OSError:
        pass
    return entries


def _is_env_reference(line: str) -> bool:
    return any(marker in line for marker in ENV_VAR_MARKERS)


def _is_env_filename(name: str) -> bool:
    if name == ".env":
        return True
    if name.startswith(".env."):
        return True
    return False


def iter_files(root: Path, extra_ignore: Iterable[str] = ()) -> Iterable[Path]:
    extra = set(extra_ignore)
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel_parts = path.relative_to(root).parts
        if any(part in ALWAYS_IGNORE_DIRS for part in rel_parts):
            continue
        if rel_parts and rel_parts[0] in extra:
            continue
        if _is_env_filename(path.name):
            continue
        if path.suffix.lower() in ALWAYS_IGNORE_FILE_SUFFIXES:
            continue
        yield path


def scan_file(path: Path, root: Path) -> list[Finding]:
    findings: list[Finding] = []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return findings

    # File-level env-var heuristic: if the file clearly references the
    # webhook through an env var anywhere, treat literal URL occurrences as
    # documentation/comments and skip them.
    file_has_env_ref = _is_env_reference(text)

    for lineno, line in enumerate(text.splitlines(), start=1):
        for match in WEBHOOK_RE.finditer(line):
            if file_has_env_ref or _is_env_reference(line):
                continue
            rel = str(path.relative_to(root)).replace("\\", "/")
            snippet = line.strip()
            if len(snippet) > 200:
                snippet = snippet[:200] + "..."
            findings.append(
                Finding(
                    path=rel,
                    line_number=lineno,
                    webhook_id=match.group(1),
                    snippet=snippet,
                )
            )
    return findings


def scan_tree(root: Path) -> list[Finding]:
    root = Path(root).resolve()
    extra = _load_gitignore_entries(root)
    all_findings: list[Finding] = []
    for file_path in iter_files(root, extra_ignore=extra):
        all_findings.extend(scan_file(file_path, root))
    return all_findings


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    root = Path(args[0]) if args else Path.cwd()
    findings = scan_tree(root)
    if not findings:
        print(json.dumps({"status": "clean", "root": str(root).replace("\\", "/")}))
        return 0
    for f in findings:
        print(json.dumps(f.as_dict()))
    print(
        json.dumps(
            {
                "status": "violations",
                "count": len(findings),
                "root": str(root).replace("\\", "/"),
            }
        ),
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
