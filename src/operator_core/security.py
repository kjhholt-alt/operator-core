"""Guardrails, redaction, secret scanning, and autonomy decisions."""

from __future__ import annotations

import fnmatch
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .config import ProjectConfig


# C4 — risk-tier learning loop.
RISK_LEARNING_ENV = "OPERATOR_RISK_LEARNING_ENABLED"
RISK_LEARNING_MIN_SUCCESSES = 5
# Memory keys used by the learning loop. Kept here so callers (runner etc.)
# can `remember`/`increment` against the same namespace.
AUTO_MERGE_SUCCESS_KEY = "auto_merge_successes"
AUTO_MERGE_ROLLBACK_KEY = "auto_merge_rollbacks"

# Single-file config patterns that learning relaxation may promote to low-risk.
CONFIG_PATH_PATTERNS = (
    "*.json",
    "*.yaml",
    "*.yml",
    "*.toml",
    "*.ini",
    "*.cfg",
    "*.env.example",
    "config/*",
    "*.config.js",
    "*.config.ts",
)

# High-risk paths that are never promoted by the learning loop, regardless of
# success history. Hard cap — protects auth, billing, secrets, migrations.
HIGH_RISK_PATH_PATTERNS = (
    "*.env",
    "*.env.*",
    "**/.env",
    "**/.env.*",
    "**/auth/*",
    "**/billing/*",
    "**/payments/*",
    "**/stripe/*",
    "**/migrations/*",
    "**/*secret*",
    "**/*password*",
    "**/*credential*",
)


def _risk_learning_enabled() -> bool:
    return os.environ.get(RISK_LEARNING_ENV, "0") == "1"


def _path_matches_any(path: str, patterns: tuple[str, ...]) -> bool:
    normalized = path.replace("\\", "/")
    for pattern in patterns:
        if fnmatch.fnmatch(normalized, pattern):
            return True
        # Also match basename for bare glob like "*.json"
        if "/" not in pattern and fnmatch.fnmatch(normalized.rsplit("/", 1)[-1], pattern):
            return True
    return False


def _is_single_file_config(changed_files: list[str]) -> bool:
    if len(changed_files) != 1:
        return False
    only = changed_files[0]
    if _path_matches_any(only, HIGH_RISK_PATH_PATTERNS):
        return False
    return _path_matches_any(only, CONFIG_PATH_PATTERNS)


def _project_has_clean_history(project_slug: str) -> bool:
    """Check memory store for ≥N successes and 0 rollbacks for this project."""
    try:
        from .memory import recall  # local import to avoid cycles at module load
    except ImportError:
        return False
    try:
        successes_raw = recall(project_slug, AUTO_MERGE_SUCCESS_KEY)
        rollbacks_raw = recall(project_slug, AUTO_MERGE_ROLLBACK_KEY)
    except Exception:  # noqa: BLE001 — memory store unavailable, degrade safely
        return False
    try:
        successes = int(successes_raw) if successes_raw is not None else 0
        rollbacks = int(rollbacks_raw) if rollbacks_raw is not None else 0
    except (TypeError, ValueError):
        return False
    return successes >= RISK_LEARNING_MIN_SUCCESSES and rollbacks == 0


def record_auto_merge_success(project_slug: str, job_id: str | None = None) -> int:
    """Bump auto_merge_successes counter. Returns the new count."""
    from .memory import increment  # local import to avoid cycles
    return increment(project_slug, AUTO_MERGE_SUCCESS_KEY, delta=1, source_job_id=job_id)


def record_auto_merge_rollback(project_slug: str, job_id: str | None = None) -> int:
    """Bump auto_merge_rollbacks counter. Returns the new count."""
    from .memory import increment  # local import to avoid cycles
    return increment(project_slug, AUTO_MERGE_ROLLBACK_KEY, delta=1, source_job_id=job_id)


SECRET_PATTERNS = [
    re.compile(r"https://discord(?:app)?\.com/api/webhooks/[0-9]+/[A-Za-z0-9_\-]+"),
    re.compile(r"sk-ant-[A-Za-z0-9_\-]{20,}"),
    re.compile(r"ghp_[A-Za-z0-9_]{20,}"),
    re.compile(r"github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"(?i)(api[_-]?key|secret|token|password)\s*=\s*['\"]?[A-Za-z0-9_\-./+=]{16,}"),
]

DESTRUCTIVE_COMMANDS = [
    re.compile(r"\brm\s+-rf\b", re.IGNORECASE),
    re.compile(r"\bgit\s+reset\s+--hard\b", re.IGNORECASE),
    re.compile(r"\bgit\s+checkout\s+--\b", re.IGNORECASE),
    re.compile(r"\bgit\s+push\s+.*(--force\b|-f\b)", re.IGNORECASE),
    re.compile(r"\bgit\s+commit\b.*--no-verify\b", re.IGNORECASE),
    re.compile(r"\bRemove-Item\b.*\s-Recurse\b", re.IGNORECASE),
    re.compile(r"\bdel\s+/[sq]\b", re.IGNORECASE),
    re.compile(r"\bDROP\s+TABLE\b", re.IGNORECASE),
    re.compile(r"\bDROP\s+DATABASE\b", re.IGNORECASE),
    re.compile(r"\btruncate\s+table\b", re.IGNORECASE),
    re.compile(r"\btaskkill\b.*?/f\b.*?/pid\s+0\b", re.IGNORECASE),
    re.compile(r"\btaskkill\b.*?/pid\s+0\b.*?/f\b", re.IGNORECASE),
    re.compile(r":\(\)\s*\{\s*:\|:&\s*\};:"),
]

SECRET_KEY_HINTS = ("env", "token", "key", "password", "secret", "authorization", "auth", "credential")

HIGH_RISK_WORDS = {
    "auth",
    "billing",
    "payment",
    "stripe",
    "delete",
    "migration",
    "migrate",
    "secret",
    "token",
    "password",
    "outreach",
    "email",
    "send",
    "customer",
    "client",
    "external",
}

LOW_RISK_PATH_PATTERNS = [
    "*.md",
    "docs/*",
    "test/*",
    "tests/*",
    "*.test.ts",
    "*.test.tsx",
    "*.spec.ts",
    "*.spec.tsx",
    "*.test.py",
    "README*",
]


@dataclass(frozen=True)
class SecretFinding:
    path: str
    line: int
    pattern: str


@dataclass(frozen=True)
class AutonomyDecision:
    allowed: bool
    reason: str
    requires_manual: bool = False


@dataclass(frozen=True)
class CheckResults:
    tests_passed: bool = False
    secret_scan_passed: bool = False
    reviewer_verdict: str = "PENDING"
    ci_green: bool = False
    deploy_green: bool = False
    unresolved_comments: bool = False
    approvals: int = 0
    global_auto_merge_enabled: bool = False


@dataclass(frozen=True)
class AutoMergeDecision:
    allowed: bool
    risk: str
    reason: str
    reasoning: tuple[str, ...] = ()
    requires_manual: bool = False


def redact_secrets(text: str) -> str:
    """Redact common secret shapes from logs before storing or posting."""
    redacted = text
    for pattern in SECRET_PATTERNS:
        redacted = pattern.sub("[REDACTED_SECRET]", redacted)
    return redacted


def _key_looks_sensitive(key: str) -> bool:
    lowered = key.lower()
    return any(hint in lowered for hint in SECRET_KEY_HINTS)


def redact_mapping(value: Any) -> Any:
    """Recursively redact sensitive values from a dict/list payload.

    Drops any key whose name looks like a secret (env, token, key, password,
    secret, authorization, credential) and runs `redact_secrets` on string
    leaves so inline shapes like Discord webhooks are scrubbed too.
    """
    if isinstance(value, Mapping):
        scrubbed: dict[str, Any] = {}
        for k, v in value.items():
            key = str(k)
            if _key_looks_sensitive(key):
                scrubbed[key] = "[REDACTED]"
            else:
                scrubbed[key] = redact_mapping(v)
        return scrubbed
    if isinstance(value, list):
        return [redact_mapping(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_mapping(item) for item in value)
    if isinstance(value, str):
        return redact_secrets(value)
    return value


def scan_text_for_secrets(text: str, path: str = "<memory>") -> list[SecretFinding]:
    findings: list[SecretFinding] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        for pattern in SECRET_PATTERNS:
            if pattern.search(line):
                findings.append(SecretFinding(path=path, line=line_number, pattern=pattern.pattern))
    return findings


def scan_files_for_secrets(paths: list[Path]) -> list[SecretFinding]:
    """Scan changed text files for secret-like values."""
    findings: list[SecretFinding] = []
    for path in paths:
        if not path.exists() or not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        findings.extend(scan_text_for_secrets(text, str(path)))
    return findings


def command_is_blocked(command: str) -> str | None:
    """Return a denial reason for commands hooks should block."""
    for pattern in DESTRUCTIVE_COMMANDS:
        if pattern.search(command):
            return f"Blocked destructive command: {pattern.pattern}"
    if scan_text_for_secrets(command):
        return "Blocked command because it appears to contain a secret"
    return None


def classify_risk(prompt: str, changed_files: list[str], project: ProjectConfig | None = None) -> str:
    """Classify a job as low, medium, or high risk.

    When ``OPERATOR_RISK_LEARNING_ENABLED=1`` and the project has a clean
    auto-merge history (≥5 successes, 0 rollbacks), a single-file config
    change may be relaxed from medium to low. High-risk paths are never
    relaxed — the ``HIGH_RISK_PATH_PATTERNS`` hard cap applies first.
    """
    haystack = f"{prompt}\n" + "\n".join(changed_files)
    tokens = set(re.findall(r"[A-Za-z0-9_-]+", haystack.lower()))
    if tokens & HIGH_RISK_WORDS:
        return "high"

    if project:
        for file_name in changed_files:
            for protected in project.protected_patterns:
                if fnmatch.fnmatch(file_name, protected):
                    return "high"

    # Hard cap: any high-risk path pattern forces high regardless of learning.
    for file_name in changed_files:
        if _path_matches_any(file_name, HIGH_RISK_PATH_PATTERNS):
            return "high"

    if changed_files and all(_is_low_risk_path(path) for path in changed_files):
        return "low"

    # C4 relaxation: learned low-risk single-file config change.
    if (
        project is not None
        and _risk_learning_enabled()
        and _is_single_file_config(changed_files)
        and _project_has_clean_history(project.slug)
    ):
        return "low"

    return "medium"


def _is_low_risk_path(path: str) -> bool:
    normalized = path.replace("\\", "/")
    return any(fnmatch.fnmatch(normalized, pattern) for pattern in LOW_RISK_PATH_PATTERNS)


def can_auto_merge(
    project: ProjectConfig,
    risk: str,
    tests_passed: bool,
    secret_scan_passed: bool,
    reviewer_verdict: str,
    ci_green: bool,
    deploy_green: bool,
    unresolved_comments: bool = False,
    approvals: int = 0,
    global_auto_merge_enabled: bool = False,
) -> AutonomyDecision:
    """Apply the tiered autonomy policy from the Operator V3 spec."""
    if not global_auto_merge_enabled:
        return AutonomyDecision(False, "Global auto-merge is disabled", requires_manual=True)
    if not project.auto_merge:
        return AutonomyDecision(False, f"{project.slug} is not auto-merge allowlisted", True)
    if risk == "high":
        return AutonomyDecision(False, "High-risk changes require manual approval", True)
    if not secret_scan_passed:
        return AutonomyDecision(False, "Secret scan failed", True)
    if not tests_passed:
        return AutonomyDecision(False, "Tests did not pass", True)
    if reviewer_verdict.upper() == "REQUEST_CHANGES":
        return AutonomyDecision(False, "Reviewer requested changes", True)
    if unresolved_comments:
        return AutonomyDecision(False, "PR has unresolved comments", True)

    if risk == "low":
        return AutonomyDecision(True, "Low-risk change passed tests and review")

    if risk == "medium":
        if approvals < 2:
            return AutonomyDecision(False, "Medium-risk product code needs two agent approvals", True)
        if not ci_green:
            return AutonomyDecision(False, "Medium-risk product code needs green CI", True)
        if not deploy_green:
            return AutonomyDecision(False, "Deploy health did not verify green", True)
        return AutonomyDecision(True, "Medium-risk change passed two approvals, CI, and deploy health")

    return AutonomyDecision(False, f"Unknown risk tier: {risk}", True)


def classify_and_decide(
    changed_files: list[str],
    project_cfg: ProjectConfig,
    check_results: CheckResults,
    prompt: str = "",
) -> AutoMergeDecision:
    """Pure tiered gate: classify risk then apply the auto-merge policy.

    No side effects, no I/O. Returns a single object describing the final
    decision plus the ordered reasoning trail that led there.
    """
    trail: list[str] = []
    risk = classify_risk(prompt, changed_files, project_cfg)
    trail.append(f"classified risk={risk} for {len(changed_files)} changed file(s)")

    if not check_results.global_auto_merge_enabled:
        trail.append("global auto-merge disabled (OPERATOR_AUTO_MERGE_ENABLED=0)")
        return AutoMergeDecision(False, risk, "Global auto-merge is disabled", tuple(trail), True)

    if not project_cfg.auto_merge:
        trail.append(f"project {project_cfg.slug} is not allowlisted")
        return AutoMergeDecision(
            False, risk, f"{project_cfg.slug} is not auto-merge allowlisted", tuple(trail), True
        )

    if risk == "high":
        trail.append("high-risk tier always requires manual approval")
        return AutoMergeDecision(
            False, risk, "High-risk changes require manual approval", tuple(trail), True
        )

    if not check_results.secret_scan_passed:
        trail.append("secret scan failed")
        return AutoMergeDecision(False, risk, "Secret scan failed", tuple(trail), True)

    if not check_results.tests_passed:
        trail.append("tests did not pass")
        return AutoMergeDecision(False, risk, "Tests did not pass", tuple(trail), True)

    if check_results.reviewer_verdict.upper() == "REQUEST_CHANGES":
        trail.append("reviewer requested changes")
        return AutoMergeDecision(False, risk, "Reviewer requested changes", tuple(trail), True)

    if check_results.unresolved_comments:
        trail.append("PR has unresolved comments")
        return AutoMergeDecision(False, risk, "PR has unresolved comments", tuple(trail), True)

    if risk == "low":
        trail.append("low-risk: tests + reviewer pass sufficient")
        return AutoMergeDecision(
            True, risk, "Low-risk change passed tests and review", tuple(trail), False
        )

    if risk == "medium":
        if check_results.approvals < 2:
            trail.append(f"medium-risk needs 2 approvals (have {check_results.approvals})")
            return AutoMergeDecision(
                False, risk, "Medium-risk product code needs two agent approvals", tuple(trail), True
            )
        if not check_results.ci_green:
            trail.append("medium-risk needs green GitHub CI")
            return AutoMergeDecision(
                False, risk, "Medium-risk product code needs green CI", tuple(trail), True
            )
        if not check_results.deploy_green:
            trail.append("deploy health did not recover post-merge")
            return AutoMergeDecision(
                False, risk, "Deploy health did not verify green", tuple(trail), True
            )
        trail.append("medium-risk: two approvals + CI + deploy health OK")
        return AutoMergeDecision(
            True,
            risk,
            "Medium-risk change passed two approvals, CI, and deploy health",
            tuple(trail),
            False,
        )

    trail.append(f"unknown risk tier: {risk}")
    return AutoMergeDecision(False, risk, f"Unknown risk tier: {risk}", tuple(trail), True)
