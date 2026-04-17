"""PL Engine adapter — first-class domain support for pl-engine in Operator V3.

This module provides structured execution, validation, explainability, and
result formatting for the pl-engine overhead budget pipeline.  It shells out
to the real ``pl-engine`` entrypoints (``run.py``, helper scripts) and parses
their output into daemon-friendly result objects.

Canonical paths
---------------
- **Validate only**: ``python run.py --validate --factory {CODE}``
- **Full pipeline**: ``python run.py --factory {CODE}``
- **All factories**: ``python run.py --validate --all``

Dead / legacy paths (DO NOT USE):
- ``cli.py`` → interactive menu, calls missing ``whatif.py``
- ``scripts/run_full_pipeline.py`` directly → use ``run.py`` instead
- ``whatif.py`` → does not exist, replaced by ``run.py --scenario``
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .paths import PROJECTS_ROOT


# ── Constants ────────────────────────────────────────────────────────────────

PL_ENGINE_DIR = Path(os.environ.get("PL_ENGINE_DIR", PROJECTS_ROOT / "pl-engine"))

# Authoritative factory list — must match pl-engine/src/factory_registry.py.
ACTIVE_FACTORIES = ["AX02", "JL01", "HX01", "EX01"]
MAINTAINED_FACTORIES = ["AX01", "T801"]
ALL_FACTORIES = ["AX01", "AX02", "JL01", "HX01", "T801", "EX01"]

FACTORY_NAMES = {
    "AX01": "Moline",
    "AX02": "Valley City",
    "JL01": "Paton",
    "HX01": "Harvester",
    "T801": "Thibodaux",
    "EX01": "Ottumwa",
}

# Subprocess timeout defaults (seconds)
VALIDATE_TIMEOUT = 120
PIPELINE_TIMEOUT = 600
EXPLAIN_TIMEOUT = 60


# ── Result objects ───────────────────────────────────────────────────────────

@dataclass
class CheckOutcome:
    name: str
    passed: bool
    severity: str = "info"     # "error", "warning", "info"
    detail: str = ""


@dataclass
class PlResult:
    """Unified result for any pl-engine daemon task."""
    action: str
    factory: str | None = None
    ok: bool = False
    exit_code: int = -1
    checks: list[CheckOutcome] = field(default_factory=list)
    summary: str = ""
    artifacts: list[str] = field(default_factory=list)
    raw_output: str = ""
    verdict: str = ""          # human-readable one-liner
    next_action: str = ""      # what the human should do if not ok

    @property
    def errors(self) -> list[CheckOutcome]:
        return [c for c in self.checks if not c.passed and c.severity == "error"]

    @property
    def warnings(self) -> list[CheckOutcome]:
        return [c for c in self.checks if not c.passed and c.severity == "warning"]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _python() -> str:
    """Return the Python executable, preferring 'py' on Windows."""
    if sys.platform == "win32":
        return "py"
    return sys.executable


def _run(args: list[str], timeout: int = VALIDATE_TIMEOUT) -> tuple[int, str]:
    """Run a command in the pl-engine directory and capture output."""
    try:
        result = subprocess.run(
            args,
            cwd=str(PL_ENGINE_DIR),
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )
        combined = result.stdout + result.stderr
        return result.returncode, combined
    except subprocess.TimeoutExpired:
        return 124, f"Command timed out after {timeout}s"
    except FileNotFoundError as exc:
        return 127, f"Command not found: {exc}"
    except Exception as exc:
        return 1, f"Execution error: {exc}"


def _validate_factory_code(code: str) -> str:
    """Normalize and validate a factory code. Raises ValueError if unknown."""
    upper = code.strip().upper()
    if upper not in ALL_FACTORIES:
        raise ValueError(
            f"Unknown factory code: {code!r}. "
            f"Valid codes: {', '.join(ALL_FACTORIES)}"
        )
    return upper


def _parse_validation_output(raw: str) -> list[CheckOutcome]:
    """Parse validation gate output into structured check results.

    The validation gate prints a table like:
      | Schema Check           |  OK PASS   | All required columns...
      | T/N/P Ordering         |   ! WARN   | 4 CCs have unexpected...
      | CPOH Reasonableness    |   X FAIL   | CPOH too high...
      | Completeness           |   i INFO   | All 41 CCs...
    """
    checks: list[CheckOutcome] = []
    for line in raw.splitlines():
        stripped = line.strip()

        # Table format: | Check Name | Status | Detail
        m = re.match(
            r"\|\s*(.+?)\s*\|\s*(?:OK|i|!|X|x|\u2713|\u2717)?\s*(PASS|WARN|FAIL|INFO|ERROR|SKIP)\s*\|\s*(.*)",
            stripped,
        )
        if m:
            name = m.group(1).strip()
            status = m.group(2).upper()
            detail = m.group(3).strip().rstrip("|").strip()
            passed = status in ("PASS", "INFO", "SKIP")
            severity = "error" if status in ("FAIL", "ERROR") else (
                "warning" if status == "WARN" else "info"
            )
            checks.append(CheckOutcome(name=name, passed=passed, severity=severity, detail=detail))
            continue

        # Fallback: bracket format [PASS] check_name — detail
        m2 = re.match(
            r"\[(PASS|FAIL|WARN|ERROR|INFO|SKIP)\]\s+(\S+)\s*(?:[-\u2014]\s*(.*))?",
            stripped,
        )
        if m2:
            status, name, detail = m2.group(1), m2.group(2), (m2.group(3) or "").strip()
            passed = status in ("PASS", "INFO", "SKIP")
            severity = "error" if status in ("FAIL", "ERROR") else (
                "warning" if status == "WARN" else "info"
            )
            checks.append(CheckOutcome(name=name, passed=passed, severity=severity, detail=detail))
    return checks


def _find_artifacts(raw: str) -> list[str]:
    """Extract artifact file paths from pipeline output."""
    artifacts: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        # Look for lines like "Output: path/to/file.pptx" or "Wrote: ..."
        m = re.match(r"(?:Output|Wrote|Generated|Saved|Created):\s*(.+)", stripped, re.IGNORECASE)
        if m:
            path = m.group(1).strip().strip("'\"")
            if path:
                artifacts.append(path)
        # Also catch PPTX file paths
        if stripped.endswith(".pptx") and ("/" in stripped or "\\" in stripped):
            artifacts.append(stripped)
    return list(dict.fromkeys(artifacts))  # deduplicate, preserve order


# ── Core operations ──────────────────────────────────────────────────────────

def validate_factory(factory: str, strict: bool = False) -> PlResult:
    """Run validation checks for a single factory via run.py."""
    code = _validate_factory_code(factory)
    args = [_python(), "run.py", "--validate", "--factory", code]
    if strict:
        args.append("--strict")

    exit_code, raw = _run(args, timeout=VALIDATE_TIMEOUT)
    checks = _parse_validation_output(raw)
    errors = [c for c in checks if not c.passed and c.severity == "error"]
    warnings = [c for c in checks if not c.passed and c.severity == "warning"]

    ok = exit_code == 0
    if ok:
        verdict = f"{code} ({FACTORY_NAMES.get(code, code)}): PASS — {len(checks)} checks, {len(warnings)} warnings"
    else:
        verdict = f"{code} ({FACTORY_NAMES.get(code, code)}): FAIL — {len(errors)} errors, {len(warnings)} warnings"

    next_action = ""
    if not ok:
        if errors:
            next_action = f"Fix {len(errors)} validation error(s) before running pipeline. First error: {errors[0].name} — {errors[0].detail}"
        else:
            next_action = "Check raw output for non-validation failures (import error, missing data, etc.)"

    return PlResult(
        action="validate",
        factory=code,
        ok=ok,
        exit_code=exit_code,
        checks=checks,
        summary=f"{len(checks)} checks: {sum(1 for c in checks if c.passed)} passed, {len(errors)} errors, {len(warnings)} warnings",
        raw_output=raw[-6000:],
        verdict=verdict,
        next_action=next_action,
    )


def validate_all(strict: bool = False) -> PlResult:
    """Run validation for all factories. Returns aggregate result."""
    results: list[PlResult] = []
    for code in ACTIVE_FACTORIES:
        results.append(validate_factory(code, strict=strict))

    all_ok = all(r.ok for r in results)
    total_errors = sum(len(r.errors) for r in results)
    total_warnings = sum(len(r.warnings) for r in results)

    lines = []
    for r in results:
        marker = "PASS" if r.ok else "FAIL"
        lines.append(f"[{marker}] {r.factory} ({FACTORY_NAMES.get(r.factory or '', '')}): {r.summary}")

    verdict = f"All {len(results)} factories: {'PASS' if all_ok else 'FAIL'} — {total_errors} errors, {total_warnings} warnings"

    return PlResult(
        action="validate_all",
        factory=None,
        ok=all_ok,
        exit_code=0 if all_ok else 1,
        checks=[c for r in results for c in r.checks],
        summary="\n".join(lines),
        raw_output="\n---\n".join(r.raw_output for r in results)[-6000:],
        verdict=verdict,
        next_action="" if all_ok else f"Fix {total_errors} error(s) across factories before proceeding.",
    )


def run_pipeline(factory: str, force: bool = False) -> PlResult:
    """Run the full pipeline for a factory via run.py (the canonical path)."""
    code = _validate_factory_code(factory)
    args = [_python(), "run.py", "--factory", code]
    if force:
        args.append("--force")

    exit_code, raw = _run(args, timeout=PIPELINE_TIMEOUT)
    checks = _parse_validation_output(raw)
    artifacts = _find_artifacts(raw)

    ok = exit_code == 0
    if ok:
        verdict = f"{code} pipeline complete — {len(artifacts)} artifact(s) generated"
        safe_msg = "safe to use"
    else:
        verdict = f"{code} pipeline FAILED (exit {exit_code})"
        safe_msg = "NOT safe to use"

    return PlResult(
        action="pipeline",
        factory=code,
        ok=ok,
        exit_code=exit_code,
        checks=checks,
        summary=f"Pipeline {code}: {safe_msg}",
        artifacts=artifacts,
        raw_output=raw[-6000:],
        verdict=verdict,
        next_action="" if ok else "Check raw output for failure cause. Run `!op pl validate " + code + "` first.",
    )


def build_pptx(factory: str) -> PlResult:
    """Build the overhead review PPTX for a factory.

    This runs the full pipeline (which includes PPTX generation as step 4).
    The PPTX is an artifact of the pipeline, not a standalone operation.
    """
    result = run_pipeline(factory)
    result.action = "pptx"
    # Filter artifacts to PPTX only
    pptx_artifacts = [a for a in result.artifacts if a.endswith(".pptx")]
    if pptx_artifacts:
        result.artifacts = pptx_artifacts
        if result.ok:
            result.verdict = f"{result.factory} PPTX generated: {pptx_artifacts[0]}"
    elif result.ok:
        result.verdict = f"{result.factory} pipeline complete but no PPTX found in output — may need network access"
        result.next_action = "Check if PPTX template is accessible on network share"
    return result


def explain_cpoh(factory: str) -> PlResult:
    """Run CPOH explainability for a factory via helper script."""
    code = _validate_factory_code(factory)
    script = PL_ENGINE_DIR / "scripts" / "explain_cpoh.py"

    if not script.exists():
        return PlResult(
            action="explain",
            factory=code,
            ok=False,
            exit_code=127,
            summary="explain_cpoh.py not found",
            verdict=f"Explain script not available at {script}",
            next_action="Run the pl-engine setup or check scripts/ directory",
        )

    exit_code, raw = _run([_python(), str(script), "--factory", code], timeout=EXPLAIN_TIMEOUT)

    return PlResult(
        action="explain",
        factory=code,
        ok=exit_code == 0,
        exit_code=exit_code,
        summary=raw.strip()[:2000] if exit_code == 0 else f"Explain failed (exit {exit_code})",
        raw_output=raw[-6000:],
        verdict=f"{code} CPOH explanation {'generated' if exit_code == 0 else 'FAILED'}",
        next_action="" if exit_code == 0 else "Check script output for errors",
    )


def adjustment_status(factory: str) -> PlResult:
    """Show manual adjustment status for a factory."""
    code = _validate_factory_code(factory)
    script = PL_ENGINE_DIR / "scripts" / "adjustment_status.py"

    if not script.exists():
        return PlResult(
            action="adjustments",
            factory=code,
            ok=False,
            exit_code=127,
            summary="adjustment_status.py not found",
            verdict=f"Adjustment script not available at {script}",
            next_action="Run the pl-engine setup or check scripts/ directory",
        )

    exit_code, raw = _run([_python(), str(script), "--factory", code], timeout=EXPLAIN_TIMEOUT)

    return PlResult(
        action="adjustments",
        factory=code,
        ok=exit_code == 0,
        exit_code=exit_code,
        summary=raw.strip()[:2000] if exit_code == 0 else f"Adjustment status failed (exit {exit_code})",
        raw_output=raw[-6000:],
        verdict=f"{code} adjustments {'loaded' if exit_code == 0 else 'FAILED'}",
        next_action="" if exit_code == 0 else "Check adjustment CSV files in seed_data/",
    )


def pl_status() -> PlResult:
    """Quick status: validate all active factories, summarize readiness."""
    result = validate_all()
    result.action = "status"

    # Enrich with factory-level readiness
    factory_lines = []
    for code in ACTIVE_FACTORIES:
        name = FACTORY_NAMES.get(code, code)
        factory_lines.append(f"  {code} ({name}): active")
    for code in MAINTAINED_FACTORIES:
        name = FACTORY_NAMES.get(code, code)
        factory_lines.append(f"  {code} ({name}): maintained (opted out PL27)")

    result.summary = (
        f"**PL Engine Status**\n"
        f"Active factories: {', '.join(ACTIVE_FACTORIES)}\n"
        f"Maintained: {', '.join(MAINTAINED_FACTORIES)}\n\n"
        f"Validation:\n{result.summary}\n\n"
        f"Verdict: {result.verdict}"
    )
    return result


def pl_morning() -> PlResult:
    """Morning readiness check: validation + data freshness for active factories."""
    result = validate_all()
    result.action = "pl_morning"

    # Add canonical-path warnings
    warnings: list[str] = []
    cli_py = PL_ENGINE_DIR / "cli.py"
    whatif_py = PL_ENGINE_DIR / "whatif.py"
    if cli_py.exists() and not whatif_py.exists():
        warnings.append("cli.py references missing whatif.py — use run.py instead")

    warning_text = ""
    if warnings:
        warning_text = "\nPath warnings:\n" + "\n".join(f"  - {w}" for w in warnings)

    result.summary = (
        f"**PL Engine Morning Readiness**\n\n"
        f"{result.summary}\n"
        f"{warning_text}\n\n"
        f"Canonical entry: `python run.py --validate --all`\n"
        f"Verdict: {result.verdict}"
    )
    return result


# ── Result formatting ────────────────────────────────────────────────────────

def format_pl_result(result: PlResult) -> str:
    """Format a PlResult into operator-style Discord output."""
    lines: list[str] = []

    # Header
    marker = "PASS" if result.ok else "FAIL"
    factory_label = f" {result.factory}" if result.factory else ""
    lines.append(f"**PL Engine {result.action}{factory_label}** [{marker}]")

    # Summary
    if result.summary:
        lines.append(result.summary)

    # Artifacts
    if result.artifacts:
        lines.append("Artifacts:")
        for artifact in result.artifacts[:5]:
            lines.append(f"  `{artifact}`")

    # Verdict
    if result.verdict:
        lines.append(f"\nVerdict: {result.verdict}")

    # Next action (only on failure)
    if not result.ok and result.next_action:
        lines.append(f"Next: {result.next_action}")

    return "\n".join(lines)


def format_brief_result(result: PlResult) -> str:
    """One-line result for job history."""
    marker = "ok" if result.ok else "FAIL"
    factory_label = f" {result.factory}" if result.factory else ""
    return f"pl {result.action}{factory_label}: {marker} — {result.verdict}"
