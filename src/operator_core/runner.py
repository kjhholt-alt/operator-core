"""Job execution for Operator V3."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import smtplib
import sqlite3
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Callable, Iterator

from .config import ProjectConfig, find_project, load_projects
from .fleet import collect_fleet_report, render_fleet_report, write_fleet_report
from .paths import DATA_DIR, PACKAGE_ROOT, PROJECTS_ROOT, WORKTREES_DIR, ensure_data_dirs
from .security import (
    AutonomyDecision,
    SecretFinding,
    can_auto_merge,
    classify_risk,
    redact_secrets,
    scan_files_for_secrets,
)
from .store import JobRecord, JobStore
from .store_migrations import apply_migrations


REFLECTIONS_LOG_PATH = DATA_DIR / "logs" / "reflections.jsonl"
SUCCESS_STATUSES = {"complete", "needs_manual", "needs_fix"}


def _files_touched_for_job(job: JobRecord) -> list[str]:
    """Return changed files for a git-backed job, or empty list.

    Prefers metadata already captured by `_run_build` (`changed_files`), falls
    back to `git diff --name-only` against the worktree if present.
    """
    meta_files = job.metadata.get("changed_files") if isinstance(job.metadata, dict) else None
    if isinstance(meta_files, list):
        return [str(f) for f in meta_files]
    if not job.worktree:
        return []
    worktree_path = Path(job.worktree)
    if not worktree_path.exists():
        return []
    try:
        result = _run_command(
            ["git", "diff", "--name-only"], worktree_path, timeout=30
        )
    except Exception:  # noqa: BLE001
        return []
    if result.exit_code != 0:
        return []
    return sorted(
        line.strip().replace("\\", "/")
        for line in result.output.splitlines()
        if line.strip()
    )


def _write_reflection(
    job: JobRecord,
    duration_sec: float,
    log_path: Path = REFLECTIONS_LOG_PATH,
) -> dict[str, Any] | None:
    """Append a reflection JSONL line for the given completed job.

    Returns the record written, or None if the job should not be reflected.
    """
    if job.status not in SUCCESS_STATUSES:
        return None
    files_touched = _files_touched_for_job(job)
    reviewer_verdict = None
    if isinstance(job.metadata, dict):
        reviewer_verdict = job.metadata.get("reviewer_verdict")
    record: dict[str, Any] = {
        "project": job.project,
        "command": job.action,
        "files_touched": files_touched,
        "duration_sec": round(duration_sec, 3),
        "cost_usd": float(job.cost_usd or 0),
        "reviewer_verdict": reviewer_verdict,
        "job_id": job.id,
        "status": job.status,
        "ts": datetime.now(timezone.utc).isoformat(),
    }
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True) + "\n")
    except OSError as exc:
        logger.warning("Failed to write reflection: %s", redact_secrets(str(exc)))
        return record
    return record


# -- C2: error classification + retry policy -------------------------------

HOOK_BLOCKED_MARKERS = ("hook_blocked", "blocked destructive", "blocked command", "guardrail")
RISK_DENIED_MARKERS = ("risk gate denied", "auto-merge blocked", "risk_denied", "high-risk changes require")


@dataclass(frozen=True)
class RetryPolicy:
    name: str
    max_attempts: int
    retry_after_sec: float
    backoff: str  # "const" | "exp"


# Ordered policy table — first match wins. Patterns are lowercase substrings.
ERROR_POLICIES: tuple[tuple[str, RetryPolicy], ...] = (
    ("rate limit", RetryPolicy("rate_limit", max_attempts=4, retry_after_sec=30.0, backoff="exp")),
    ("429", RetryPolicy("rate_limit", max_attempts=4, retry_after_sec=30.0, backoff="exp")),
    ("worktree", RetryPolicy("worktree_lock", max_attempts=3, retry_after_sec=5.0, backoff="const")),
    ("is locked by another job", RetryPolicy("worktree_lock", max_attempts=3, retry_after_sec=5.0, backoff="const")),
    ("non-fast-forward", RetryPolicy("git_push_race", max_attempts=3, retry_after_sec=2.0, backoff="exp")),
    ("failed to push some refs", RetryPolicy("git_push_race", max_attempts=3, retry_after_sec=2.0, backoff="exp")),
    ("rejected", RetryPolicy("git_push_race", max_attempts=3, retry_after_sec=2.0, backoff="exp")),
    ("claude 5", RetryPolicy("claude_5xx", max_attempts=3, retry_after_sec=10.0, backoff="exp")),
    ("anthropic 5", RetryPolicy("claude_5xx", max_attempts=3, retry_after_sec=10.0, backoff="exp")),
    ("500 internal", RetryPolicy("claude_5xx", max_attempts=3, retry_after_sec=10.0, backoff="exp")),
    ("502 bad gateway", RetryPolicy("claude_5xx", max_attempts=3, retry_after_sec=10.0, backoff="exp")),
    ("503 service", RetryPolicy("claude_5xx", max_attempts=3, retry_after_sec=10.0, backoff="exp")),
    ("504 gateway", RetryPolicy("claude_5xx", max_attempts=3, retry_after_sec=10.0, backoff="exp")),
    ("connection reset", RetryPolicy("transient_network", max_attempts=4, retry_after_sec=3.0, backoff="exp")),
    ("connection aborted", RetryPolicy("transient_network", max_attempts=4, retry_after_sec=3.0, backoff="exp")),
    ("connection refused", RetryPolicy("transient_network", max_attempts=4, retry_after_sec=3.0, backoff="exp")),
    ("temporary failure", RetryPolicy("transient_network", max_attempts=4, retry_after_sec=3.0, backoff="exp")),
    ("timed out", RetryPolicy("transient_network", max_attempts=4, retry_after_sec=3.0, backoff="exp")),
    ("timeout", RetryPolicy("transient_network", max_attempts=4, retry_after_sec=3.0, backoff="exp")),
    ("dns", RetryPolicy("transient_network", max_attempts=4, retry_after_sec=3.0, backoff="exp")),
)


def classify_error(message: str) -> RetryPolicy | None:
    """Return a retry policy for the given error message, or None if not retryable.

    Hook-blocked and risk-gate-denied errors are intentional stops — never retried.
    """
    if not message:
        return None
    lowered = message.lower()
    for marker in HOOK_BLOCKED_MARKERS:
        if marker in lowered:
            return None
    for marker in RISK_DENIED_MARKERS:
        if marker in lowered:
            return None
    for needle, policy in ERROR_POLICIES:
        if needle in lowered:
            return policy
    return None


def _retry_delay(policy: RetryPolicy, attempt_index: int) -> float:
    """attempt_index is 0-based — the delay before the (attempt_index+1)-th retry."""
    if policy.backoff == "exp":
        return policy.retry_after_sec * (2 ** attempt_index)
    return policy.retry_after_sec


def _persist_attempts(db_path: Path, job_id: str, attempts: list[dict[str, Any]]) -> None:
    """Write attempts JSON to the jobs row; no-op if column/row missing."""
    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.Error:
        return
    try:
        apply_migrations(conn)
        try:
            conn.execute(
                "UPDATE jobs SET attempts_json = ? WHERE id = ?",
                (json.dumps(attempts, sort_keys=True), job_id),
            )
            conn.commit()
        except sqlite3.Error:
            pass
    finally:
        conn.close()

logger = logging.getLogger("operator_v3.runner")

STALE_WORKTREE_HOURS = int(os.environ.get("OPERATOR_WORKTREE_STALE_HOURS", "6"))
LOCK_STALE_SECONDS = int(os.environ.get("OPERATOR_LOCK_STALE_SECONDS", "21600"))
BRANCH_MAX_LEN = 200


@dataclass(frozen=True)
class CommandResult:
    command: str
    exit_code: int
    output: str


class JobRunner:
    """Runs queued Operator jobs and records their lifecycle."""

    def __init__(
        self,
        store: JobStore | None = None,
        feature_builder: Callable[[str, Path], CommandResult] | None = None,
        pr_opener: Callable[[ProjectConfig, Path, str, str, str], str | None] | None = None,
        merge_runner: Callable[[ProjectConfig, str, bool], str] | None = None,
        discord_notifier: Callable[[str, str, str, str], bool] | None = None,
        email_sender: Callable[[str, str, str], bool] | None = None,
    ):
        self.store = store or JobStore()
        self._feature_builder = feature_builder or _default_feature_builder
        self._pr_opener = pr_opener or _default_pr_opener
        self._merge_runner = merge_runner or _default_merge_runner
        self._discord_notifier = discord_notifier or _default_discord_notifier
        self._email_sender = email_sender or _default_email_sender

    def run(self, job_id: str) -> JobRecord:
        """Execute a job with smart retries on classified transient failures.

        Public signature is unchanged — existing callers (smoke harness, daemon,
        tests) see the same `run(job_id) -> JobRecord` contract. Internally we
        delegate each attempt to `_run_once` and loop per the retry policy.
        """
        attempts: list[dict[str, Any]] = []
        attempt_index = 0
        last_result: JobRecord | None = None
        job_started_at = time.monotonic()

        while True:
            start_ts = datetime.now(timezone.utc).isoformat()
            try:
                last_result = self._run_once(job_id)
                error_msg = None
                if last_result.status == "failed":
                    error_msg = str(last_result.metadata.get("error") or "")
            except Exception as exc:  # noqa: BLE001 — want the fence here
                # _run_once should already catch, but belt-and-suspenders.
                error_msg = redact_secrets(str(exc))
                last_result = self.store.update_job(
                    job_id,
                    status="failed",
                    metadata={"error": error_msg},
                )

            attempts.append(
                {
                    "attempt": attempt_index + 1,
                    "started_at": start_ts,
                    "status": last_result.status,
                    "error": redact_secrets(error_msg) if error_msg else None,
                }
            )

            if last_result.status != "failed" or not error_msg:
                break

            policy = classify_error(error_msg)
            if policy is None:
                attempts[-1]["retry_decision"] = "no_policy_or_intentional_stop"
                break
            if attempt_index + 1 >= policy.max_attempts:
                attempts[-1]["retry_decision"] = f"exhausted ({policy.name})"
                break

            delay = _retry_delay(policy, attempt_index)
            attempts[-1]["retry_decision"] = f"retry in {delay:.1f}s ({policy.name})"
            self._sleep(delay)
            attempt_index += 1

        _persist_attempts(self.store.db_path, job_id, attempts)
        # Mirror attempts into job metadata so callers without DB access can see them.
        try:
            merged_meta = {**last_result.metadata, "attempts": attempts}
            last_result = self.store.update_job(last_result.id, metadata=merged_meta)
        except Exception:  # noqa: BLE001
            pass

        # C3: post-job reflection — only on successful terminal statuses.
        duration = time.monotonic() - job_started_at
        try:
            _write_reflection(last_result, duration, log_path=REFLECTIONS_LOG_PATH)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Reflection write failed: %s", redact_secrets(str(exc)))

        return last_result

    # Test hook — overridden in tests to avoid real sleeps.
    def _sleep(self, seconds: float) -> None:  # pragma: no cover - trivial
        if seconds > 0:
            time.sleep(min(seconds, 0.01))

    def _run_once(self, job_id: str) -> JobRecord:
        """Single execution attempt — no retries. Always returns a JobRecord."""
        job = self.store.update_job(job_id, status="running")
        try:
            if job.action == "morning":
                return self._run_script_job(job, "morning-briefing.py")
            if job.action == "review_prs":
                return self._run_script_job(job, "pr-reviewer.py")
            if job.action == "deploy_check":
                return self._run_script_job(job, "deploy-checker.py")
            if job.action == "marketing_pulse":
                return self._run_script_job(job, "marketing-pulse.py", timeout=1800)
            if job.action == "cost_report":
                return self._run_script_job(job, "cost-tracker.py report")
            if job.action == "deck_ag_market_pulse":
                return self._run_ag_market_pulse(job)
            if job.action in {"fleet_status", "fleet_check", "fleet_weakest"}:
                return self._run_fleet(job)
            if job.action == "build":
                return self._run_build(job)
            if job.action == "status":
                return self.store.update_job(job.id, status="complete", metadata={"summary": "Status requested"})
            if job.action in ("pl_analyst", "pl_brief"):
                return self._run_pl_analyst(job)
            if job.action.startswith("pl_"):
                return self._run_pl_engine(job)
            raise RuntimeError(f"Unsupported job action: {job.action}")
        except Exception as exc:
            return self.store.update_job(
                job.id,
                status="failed",
                metadata={**job.metadata, "error": redact_secrets(str(exc))},
            )

    def _run_fleet(self, job: JobRecord) -> JobRecord:
        mode = job.action.replace("fleet_", "")
        run_checks = job.action in {"fleet_check", "fleet_weakest"}
        reports = collect_fleet_report(run_checks=run_checks)
        title = "SaaS Fleet Weakest" if job.action == "fleet_weakest" else "SaaS Fleet Status"
        summary = render_fleet_report(reports, title=title)
        path = write_fleet_report(reports, mode=mode)
        weakest = reports[0].slug if reports else None
        return self.store.update_job(
            job.id,
            status="complete",
            metadata={
                **job.metadata,
                "summary": summary,
                "weakest": weakest,
                "report_path": str(path),
                "run_checks": run_checks,
            },
        )

    def _run_pl_engine(self, job: JobRecord) -> JobRecord:
        """Dispatch pl-engine commands via the adapter module."""
        from .pl_engine import (
            adjustment_status,
            build_pptx,
            explain_cpoh,
            format_brief_result,
            format_pl_result,
            pl_morning,
            pl_status,
            run_pipeline,
            validate_all,
            validate_factory,
        )

        factory = job.project  # factory code or None
        action = job.action

        try:
            if action == "pl_status":
                result = pl_status()
            elif action == "pl_validate":
                result = validate_factory(factory) if factory else validate_all()
            elif action == "pl_pptx":
                if not factory:
                    raise RuntimeError("PPTX build requires a factory code (e.g. !op pl pptx AX02)")
                result = build_pptx(factory)
            elif action == "pl_explain":
                if not factory:
                    raise RuntimeError("Explain requires a factory code (e.g. !op pl explain AX02)")
                result = explain_cpoh(factory)
            elif action == "pl_adjustments":
                if not factory:
                    raise RuntimeError("Adjustments requires a factory code (e.g. !op pl adjustments AX02)")
                result = adjustment_status(factory)
            elif action == "pl_morning":
                result = pl_morning()
            elif action == "pl_pipeline":
                if not factory:
                    raise RuntimeError("Pipeline requires a factory code")
                result = run_pipeline(factory, force="force" in (job.prompt or "").lower())
            else:
                raise RuntimeError(f"Unknown pl-engine action: {action}")

            discord_body = format_pl_result(result)
            try:
                self._discord_notifier(
                    "projects",
                    f"PL Engine — {action.replace('pl_', '')}",
                    discord_body,
                    "green" if result.ok else "red",
                )
            except Exception:
                pass

            status = "complete" if result.ok else "needs_fix"
            return self.store.update_job(
                job.id,
                status=status,
                project="pl-engine",
                metadata={
                    **job.metadata,
                    "summary": format_brief_result(result),
                    "verdict": result.verdict,
                    "exit_code": result.exit_code,
                    "factory": result.factory,
                    "artifacts": result.artifacts,
                    "errors": len(result.errors),
                    "warnings": len(result.warnings),
                    "output_tail": result.raw_output[-4000:],
                    "next_action": result.next_action,
                },
            )
        except Exception as exc:
            return self.store.update_job(
                job.id,
                status="failed",
                project="pl-engine",
                metadata={**job.metadata, "error": redact_secrets(str(exc))},
            )

    def _run_pl_analyst(self, job: JobRecord) -> JobRecord:
        """Run the PL Engine always-on analyst loop or briefing."""
        from .pl_analyst import run_analyst_brief, run_analyst_loop

        factory = job.project  # factory code or None
        try:
            if job.action == "pl_brief":
                loop_result = run_analyst_brief(factory=factory)
            else:
                initial_action = "validate"
                if isinstance(job.metadata, dict) and "initial_action" in job.metadata:
                    initial_action = job.metadata["initial_action"]
                elif isinstance(job.args if hasattr(job, "args") else None, dict):
                    initial_action = job.args.get("initial_action", "validate")
                # Check if args were passed via parsed command
                meta = job.metadata if isinstance(job.metadata, dict) else {}
                if "initial_action" not in meta and job.prompt:
                    # prompt may contain "pipeline" etc.
                    if "pipeline" in job.prompt.lower():
                        initial_action = "pipeline"

                loop_result = run_analyst_loop(
                    factory=factory,
                    initial_action=initial_action,
                )

            discord_body = loop_result.format_discord()
            try:
                self._discord_notifier(
                    "projects",
                    f"PL Analyst — {'brief' if job.action == 'pl_brief' else 'loop'}{' ' + factory if factory else ''}",
                    discord_body,
                    "green" if loop_result.final_verdict.value == "safe_to_use" else (
                        "yellow" if loop_result.final_verdict.value == "use_with_caveats" else "red"
                    ),
                )
            except Exception:
                pass

            status = "complete" if not loop_result.escalated else "needs_manual"
            return self.store.update_job(
                job.id,
                status=status,
                project="pl-engine",
                metadata={
                    **job.metadata,
                    **loop_result.to_metadata(),
                },
            )
        except Exception as exc:
            return self.store.update_job(
                job.id,
                status="failed",
                project="pl-engine",
                metadata={**job.metadata, "error": redact_secrets(str(exc))},
            )

    def _run_script_job(self, job: JobRecord, script_name: str, timeout: int = 900) -> JobRecord:
        if " " in script_name:
            result = _run_command(f"{sys.executable} {script_name}", PACKAGE_ROOT, timeout=timeout)
        else:
            result = _run_command([sys.executable, str(PACKAGE_ROOT / script_name)], PACKAGE_ROOT, timeout=timeout)
        status = "complete" if result.exit_code == 0 else "failed"
        return self.store.update_job(
            job.id,
            status=status,
            metadata={
                **job.metadata,
                "command": result.command,
                "exit_code": result.exit_code,
                "output_tail": result.output[-6000:],
            },
        )

    def _run_ag_market_pulse(self, job: JobRecord) -> JobRecord:
        ag_dir = Path(os.environ.get("OPERATOR_AG_MARKET_PULSE_DIR", PROJECTS_ROOT / "ag-market-pulse"))
        output_dir = ag_dir / "output"

        result = _run_command([sys.executable, "run.py", "full"], ag_dir, timeout=1800)

        latest_deck = _find_latest_pptx(output_dir)
        freshness = _summarize_source_freshness(result.output)
        data_gaps = _extract_data_gaps(result.output)

        success = result.exit_code == 0 and latest_deck is not None
        status = "complete" if success else "failed"

        deck_display = str(latest_deck) if latest_deck else "(no deck found)"
        discord_body_lines = [
            f"Status: {'ok' if success else 'FAILED'}",
            f"Deck: `{deck_display}`",
        ]
        if freshness:
            discord_body_lines.append(f"Source freshness: {freshness}")
        if data_gaps:
            discord_body_lines.append("Data gaps: " + "; ".join(data_gaps[:5]))
        discord_body = "\n".join(discord_body_lines)

        try:
            posted = self._discord_notifier(
                "projects",
                "Ag Market Pulse — deck run",
                discord_body,
                "green" if success else "red",
            )
        except Exception as exc:  # noqa: BLE001
            posted = False
            logger.warning("Discord notify failed: %s", redact_secrets(str(exc)))

        owner_email = os.environ.get("OPERATOR_OWNER_EMAIL", "").strip()
        emailed = False
        if owner_email and success and latest_deck:
            subject = f"Ag Market Pulse deck ready — {latest_deck.name}"
            body = (
                f"Latest deck: {latest_deck}\n"
                f"Source freshness: {freshness or 'n/a'}\n"
                f"Data gaps: {', '.join(data_gaps) if data_gaps else 'none reported'}\n\n"
                "Internal review only. Do NOT forward to Greg, Kari, or external recipients without manual approval."
            )
            try:
                emailed = self._email_sender(owner_email, subject, body)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Email send failed: %s", redact_secrets(str(exc)))
        elif not owner_email:
            logger.warning("OPERATOR_OWNER_EMAIL unset; skipping ag-market-pulse email")

        return self.store.update_job(
            job.id,
            status=status,
            project="ag-market-pulse",
            metadata={
                **job.metadata,
                "command": result.command,
                "exit_code": result.exit_code,
                "output_tail": result.output[-6000:],
                "latest_deck": str(latest_deck) if latest_deck else None,
                "source_freshness": freshness,
                "data_gaps": data_gaps,
                "discord_posted": posted,
                "emailed_owner": emailed,
                "email_policy": "Kruz-only; external recipients require manual approval",
            },
        )

    def _run_build(self, job: JobRecord) -> JobRecord:
        if not job.project:
            raise RuntimeError("Build jobs require a project")
        project = find_project(job.project, load_projects())
        ensure_data_dirs()

        workspace_root = _detect_workspace_root(project.path)
        worktrees_parent = (
            project.path / ".op-builds" if workspace_root is not None else WORKTREES_DIR
        )
        worktrees_parent.mkdir(parents=True, exist_ok=True)
        _reap_stale_worktrees(project, worktrees_parent, STALE_WORKTREE_HOURS)

        branch = _branch_name(project.slug, job.id, job.prompt)
        worktree_path = _worktree_location(
            project.path, project.slug, job.id, workspace_root, WORKTREES_DIR
        )

        with _project_lock(project.slug):
            base_commit = _git_output(["git", "rev-parse", "HEAD"], project.path)

            self.store.update_job(job.id, branch=branch, worktree=str(worktree_path), project=project.slug)
            self._prepare_worktree(project, branch, worktree_path)
            copied_envs = _copy_env_files(project.path, worktree_path)
            if copied_envs:
                logger.info("Copied env files into worktree: %s", ", ".join(copied_envs))

            try:
                feature_result = self._feature_builder(job.prompt, worktree_path)

                if not _has_head_moved(worktree_path, base_commit):
                    _commit_uncommitted(worktree_path, job.prompt)

                changed_files = _changed_files_since(worktree_path, base_commit)
                risk = classify_risk(job.prompt, changed_files, project)
                secret_findings = scan_files_for_secrets([worktree_path / path for path in changed_files])

                if workspace_root is not None:
                    # Worktree lives inside the monorepo; Next/Turbopack will
                    # walk up, find the workspace package.json, and resolve
                    # modules via the real hoisted node_modules. No per-worktree
                    # install needed — the user's monorepo node_modules IS the
                    # dep source. If it is out of date, the build will fail in
                    # a way the user can fix with `npm ci` at the monorepo root.
                    install_results = []
                else:
                    install_results = _install_dependencies(worktree_path)

                project_check_results = [_run_command(command, worktree_path, timeout=900) for command in project.checks]
                checks = install_results + project_check_results
                tests_passed = feature_result.exit_code == 0 and all(r.exit_code == 0 for r in checks)

                reviewer_verdict = "PASS"
                if "REQUEST_CHANGES" in feature_result.output.upper() or feature_result.exit_code != 0:
                    reviewer_verdict = "REQUEST_CHANGES"

                approvals = self.store.approval_count(job.id)
                decision = can_auto_merge(
                    project=project,
                    risk=risk,
                    tests_passed=tests_passed,
                    secret_scan_passed=not secret_findings,
                    reviewer_verdict=reviewer_verdict,
                    ci_green=_ci_green(project, branch),
                    deploy_green=True,
                    approvals=approvals,
                    global_auto_merge_enabled=_auto_merge_enabled(),
                )

                dry_run = _dry_run_enabled()
                pr_body = _render_pr_body(
                    prompt=job.prompt,
                    risk=risk,
                    changed_files=changed_files,
                    checks=checks,
                    reviewer_verdict=reviewer_verdict,
                    secret_findings=secret_findings,
                    decision=decision,
                    dry_run=dry_run,
                )
                pr_title = f"operator: {job.prompt[:68].strip() or 'automated update'}"

                pr_url = None
                if changed_files:
                    pr_url = self._pr_opener(project, worktree_path, branch, pr_title, pr_body) if not dry_run else f"dry-run://{project.repo}/pull/{branch}"

                deploy_result = "manual_required"
                if decision.allowed and pr_url and not dry_run:
                    deploy_result = self._merge_runner(project, branch, False)
                elif dry_run:
                    deploy_result = "dry_run"

                status = "complete" if tests_passed and not secret_findings else "needs_manual"
                if reviewer_verdict == "REQUEST_CHANGES":
                    status = "needs_fix"

                return self.store.update_job(
                    job.id,
                    status=status,
                    risk_tier=risk,
                    pr_url=pr_url,
                    deploy_result=deploy_result,
                    metadata={
                        **job.metadata,
                        "feature_builder_exit": feature_result.exit_code,
                        "feature_builder_tail": feature_result.output[-6000:],
                        "changed_files": changed_files,
                        "secret_findings": [finding.__dict__ for finding in secret_findings],
                        "checks": [result.__dict__ for result in checks],
                        "install_results": [result.__dict__ for result in install_results],
                        "copied_env_files": copied_envs,
                        "reviewer_verdict": reviewer_verdict,
                        "auto_merge_decision": decision.__dict__,
                        "pr_body": pr_body,
                        "dry_run": dry_run,
                    },
                )
            finally:
                _cleanup_worktree(project, worktree_path)

    def _prepare_worktree(self, project: ProjectConfig, branch: str, worktree_path: Path) -> None:
        if worktree_path.exists():
            raise RuntimeError(f"Worktree already exists: {worktree_path}")
        status = _run_command(["git", "status", "--porcelain"], project.path, timeout=60)
        if status.exit_code != 0:
            raise RuntimeError(status.output)
        result = _run_command(
            ["git", "worktree", "add", "-B", branch, str(worktree_path), "HEAD"],
            project.path,
            timeout=180,
        )
        if result.exit_code != 0:
            raise RuntimeError(result.output)


def _default_feature_builder(prompt: str, worktree_path: Path) -> CommandResult:
    return _run_command(
        [sys.executable, str(PACKAGE_ROOT / "feature-builder.py"), prompt, str(worktree_path)],
        PACKAGE_ROOT,
        timeout=3600,
    )


def _default_pr_opener(
    project: ProjectConfig,
    worktree_path: Path,
    branch: str,
    title: str,
    body: str,
) -> str | None:
    push = _run_command(["git", "push", "-u", "origin", branch], worktree_path, timeout=300)
    if push.exit_code != 0:
        raise RuntimeError(push.output)
    pr = _run_command(["gh", "pr", "create", "--title", title, "--body", body], worktree_path, timeout=300)
    if pr.exit_code != 0:
        raise RuntimeError(pr.output)
    lines = [line for line in pr.output.strip().splitlines() if line.strip()]
    return lines[-1] if lines else None


def _default_merge_runner(project: ProjectConfig, branch: str, dry_run: bool) -> str:
    if dry_run:
        return "dry-run merge skipped"
    merge = _run_command(
        ["gh", "pr", "merge", branch, "--squash", "--delete-branch", "--auto"],
        project.path,
        timeout=300,
    )
    if merge.exit_code != 0:
        return f"merge_failed: {merge.output[-1000:]}"
    return "merge_requested"


def _default_discord_notifier(channel: str, title: str, body: str, color: str) -> bool:
    try:
        from .utils.discord import notify_sync  # noqa: WPS433
    except Exception as exc:  # noqa: BLE001
        logger.warning("Discord utility unavailable: %s", exc)
        return False
    return notify_sync(channel, title=title, body=body, color=color, footer="operator-v3 | ag-market-pulse")


def _default_email_sender(to_address: str, subject: str, body: str) -> bool:
    host = os.environ.get("OPERATOR_SMTP_HOST")
    if not host:
        logger.warning("OPERATOR_SMTP_HOST unset; skipping email to %s", to_address)
        return False
    port = int(os.environ.get("OPERATOR_SMTP_PORT", "587"))
    user = os.environ.get("OPERATOR_SMTP_USER", "")
    password = os.environ.get("OPERATOR_SMTP_PASSWORD", "")
    sender = os.environ.get("OPERATOR_SMTP_FROM", user or "operator-v3@localhost")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to_address
    msg.set_content(body)

    with smtplib.SMTP(host, port, timeout=30) as server:
        server.starttls()
        if user:
            server.login(user, password)
        server.send_message(msg)
    return True


def _install_dependencies(worktree_path: Path) -> list[CommandResult]:
    """Install project dependencies inside a fresh worktree.

    Worktrees share the repo's .git but not build artefacts like node_modules,
    so every Node build check fails unless we install first. Returns a list of
    CommandResult entries (one per installer that ran); empty if nothing needed.

    NOTE: this path is for STANDALONE projects only. Workspace-member projects
    (children of an npm-workspaces monorepo that depend on in-tree packages
    like `@suite/*`) cannot be installed in isolation because their private
    deps do not exist in the public registry. For those, call
    `_link_workspace_node_modules` instead.
    """
    results: list[CommandResult] = []
    package_json = worktree_path / "package.json"
    if package_json.exists():
        if (worktree_path / "package-lock.json").exists():
            cmd = "npm ci --no-audit --fund=false --prefer-offline"
        else:
            cmd = "npm install --no-audit --fund=false --prefer-offline"
        results.append(_run_command(cmd, worktree_path, timeout=900))
    return results


def _detect_workspace_root(project_path: Path) -> Path | None:
    """Walk up from `project_path` looking for a parent `package.json` whose
    `workspaces` list declares this directory as a workspace member.

    Returns the monorepo root path, or None if the project is standalone.
    Supports:
      - exact directory name match (`"prospector-pro"`)
      - simple glob pattern (`"packages/*"`)
      - explicit relative path (`"apps/prospector-pro"`)
      - object form `{"packages": [...]}`
    """
    try:
        project_name = project_path.name
        parent = project_path.parent
    except (ValueError, OSError):
        return None

    while parent != parent.parent:
        pkg_json = parent / "package.json"
        if pkg_json.exists():
            try:
                data = json.loads(pkg_json.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                parent = parent.parent
                continue
            workspaces = data.get("workspaces")
            if isinstance(workspaces, dict):
                workspaces = workspaces.get("packages", [])
            if isinstance(workspaces, list):
                for pattern in workspaces:
                    if not isinstance(pattern, str):
                        continue
                    if pattern == project_name:
                        return parent
                    if pattern.endswith("/*"):
                        prefix = pattern[:-2]
                        try:
                            if (parent / prefix).resolve() == project_path.parent.resolve():
                                return parent
                        except OSError:
                            continue
                    elif "/" in pattern:
                        try:
                            if (parent / pattern).resolve() == project_path.resolve():
                                return parent
                        except OSError:
                            continue
        parent = parent.parent
    return None


def _copy_env_files(source: Path, dest: Path) -> list[str]:
    """Copy `.env*` files from `source` into `dest`, skipping examples.

    Git worktrees contain only tracked files from HEAD. Most projects gitignore
    their real env files (`.env.local`, `.env.production.local`, etc.), which
    means a fresh worktree has no credentials — Next.js will then throw at
    build time when server-side imports try to read `process.env.SUPABASE_URL`
    and similar. Copying these files into the worktree is the smallest fix
    that mirrors how every other local/CI build works.

    Returns the list of filenames that were actually copied.
    """
    copied: list[str] = []
    if not source.is_dir() or not dest.is_dir():
        return copied
    try:
        entries = list(source.iterdir())
    except OSError:
        return copied
    for entry in entries:
        if not entry.is_file():
            continue
        name = entry.name
        if not name.startswith(".env"):
            continue
        if name in {".env.example", ".env.sample", ".env.template"}:
            continue
        dest_file = dest / name
        if dest_file.exists():
            continue
        try:
            shutil.copy2(entry, dest_file)
            copied.append(name)
        except OSError as exc:
            logger.warning("Failed to copy %s into worktree: %s", name, exc)
    return copied


def _worktree_location(
    project_path: Path,
    project_slug: str,
    job_id: str,
    workspace_root: Path | None,
    default_worktrees_dir: Path,
) -> Path:
    """Pick the worktree path based on whether the project is a workspace member.

    - Standalone projects: worktree goes OUTSIDE the monorepo at
      `default_worktrees_dir/op-<slug>-<id>`. This keeps Node toolchains from
      walking up and discovering the monorepo's package-lock.json and hoisted
      node_modules when the project is not actually a workspace member.
    - Workspace-member projects: worktree goes INSIDE the child project itself
      at `project_path/.op-builds/op-<slug>-<id>`. This is the only path that
      gives Node's module-resolution walk-up access to BOTH the child's local
      `node_modules/` (where npm installs packages it chose not to hoist, e.g.
      `@base-ui/react` with a peer-dep conflict) AND the monorepo's hoisted
      `node_modules/` (where private in-tree `@suite/*` packages live). Nested
      git worktrees are allowed by git; `.op-builds/` must be gitignored in
      the child repo so the main worktree does not see the nested worktree as
      untracked files.
    """
    dir_name = f"op-{project_slug}-{job_id}"
    if workspace_root is not None:
        return project_path / ".op-builds" / dir_name
    return default_worktrees_dir / dir_name


def _run_command(command: list[str] | str, cwd: Path, timeout: int) -> CommandResult:
    shell = isinstance(command, str)
    display = command if isinstance(command, str) else " ".join(command)
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd),
            shell=shell,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
        )
        return CommandResult(display, completed.returncode, redact_secrets(completed.stdout or ""))
    except FileNotFoundError as exc:
        return CommandResult(display, 127, str(exc))
    except subprocess.TimeoutExpired as exc:
        output = (exc.stdout or "") if isinstance(exc.stdout, str) else ""
        return CommandResult(display, 124, redact_secrets(output + "\nTimed out"))


def _changed_files_porcelain(worktree_path: Path) -> list[str]:
    result = _run_command(["git", "status", "--porcelain"], worktree_path, timeout=60)
    if result.exit_code != 0:
        return []
    files: list[str] = []
    for line in result.output.splitlines():
        if len(line) > 3:
            files.append(line[3:].strip().replace("\\", "/"))
    return files


def _changed_files_since(worktree_path: Path, base_commit: str) -> list[str]:
    result = _run_command(
        ["git", "diff", "--name-only", f"{base_commit}...HEAD"],
        worktree_path,
        timeout=60,
    )
    files: set[str] = set()
    if result.exit_code == 0:
        files.update(line.strip().replace("\\", "/") for line in result.output.splitlines() if line.strip())
    files.update(_changed_files_porcelain(worktree_path))
    return sorted(files)


def _has_head_moved(worktree_path: Path, base_commit: str) -> bool:
    head = _run_command(["git", "rev-parse", "HEAD"], worktree_path, timeout=30)
    if head.exit_code != 0:
        return False
    return head.output.strip() != base_commit.strip()


def _commit_uncommitted(worktree_path: Path, prompt: str) -> None:
    if not _changed_files_porcelain(worktree_path):
        return
    add = _run_command(["git", "add", "."], worktree_path, timeout=120)
    if add.exit_code != 0:
        raise RuntimeError(add.output)
    message = f"operator: {prompt[:72].strip() or 'automated update'}"
    commit = _run_command(
        ["git", "-c", "user.email=operator-v3@localhost", "-c", "user.name=Operator V3", "commit", "-m", message],
        worktree_path,
        timeout=180,
    )
    if commit.exit_code != 0:
        raise RuntimeError(commit.output)


def _git_output(command: list[str], cwd: Path) -> str:
    result = _run_command(command, cwd, timeout=60)
    if result.exit_code != 0:
        raise RuntimeError(result.output)
    return result.output.strip()


def _branch_name(project_slug: str, job_id: str, prompt: str) -> str:
    raw_slug = re.sub(r"[^a-zA-Z0-9]+", "-", prompt.lower()).strip("-") or "task"
    raw_slug = raw_slug[:40].strip("-") or "task"
    short_id = (job_id or uuid.uuid4().hex)[:8]
    safe_project = re.sub(r"[^a-zA-Z0-9_-]+", "-", project_slug).strip("-") or "project"
    branch = f"op/{safe_project}/{raw_slug}-{short_id}"
    if len(branch) > BRANCH_MAX_LEN:
        branch = branch[:BRANCH_MAX_LEN].rstrip("-/")
    return branch


def _dry_run_enabled() -> bool:
    return os.environ.get("OPERATOR_V3_DRY_RUN", "1") != "0"


def _auto_merge_enabled() -> bool:
    return os.environ.get("OPERATOR_AUTO_MERGE_ENABLED", "0") == "1"


def _ci_green(project: ProjectConfig, branch: str) -> bool:
    if shutil.which("gh") is None:
        return False
    result = _run_command(
        ["gh", "run", "list", "--repo", project.repo, "--branch", branch, "--limit", "1", "--json", "conclusion"],
        PROJECTS_ROOT,
        timeout=120,
    )
    return result.exit_code == 0 and '"success"' in result.output


def _render_pr_body(
    *,
    prompt: str,
    risk: str,
    changed_files: list[str],
    checks: list[CommandResult],
    reviewer_verdict: str,
    secret_findings: list[SecretFinding],
    decision: AutonomyDecision,
    dry_run: bool,
) -> str:
    check_lines = [
        f"- `{r.command}` → exit {r.exit_code}" for r in checks
    ] or ["- (no project checks configured)"]
    file_lines = [f"- `{path}`" for path in changed_files[:50]] or ["- (no file changes detected)"]
    if len(changed_files) > 50:
        file_lines.append(f"- ...and {len(changed_files) - 50} more")

    if secret_findings:
        secret_block = "\n".join(
            f"- `{f.path}` line {f.line} — {f.pattern}" for f in secret_findings[:20]
        )
    else:
        secret_block = "- clean"

    auto_merge_line = (
        f"{'allowed' if decision.allowed else 'blocked'} — {decision.reason}"
    )

    return (
        "Automated Operator V3 PR.\n\n"
        f"**Job prompt:** {prompt}\n\n"
        f"## Risk Tier\n{risk}\n\n"
        "## Changed Files\n" + "\n".join(file_lines) + "\n\n"
        "## Checks Run\n" + "\n".join(check_lines) + "\n\n"
        f"## Reviewer Verdict\n{reviewer_verdict}\n\n"
        "## Secret Scan\n" + secret_block + "\n\n"
        f"## Auto-Merge Decision\n{auto_merge_line}\n\n"
        f"_Dry run: {dry_run}_"
    )


@contextmanager
def _project_lock(project_slug: str) -> Iterator[Path]:
    ensure_data_dirs()
    lock_dir = DATA_DIR / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "-", project_slug).strip("-") or "project"
    lock_path = lock_dir / f"{safe}.lock"

    if lock_path.exists():
        if _lock_is_stale(lock_path):
            logger.warning("Reaping stale lock: %s", lock_path)
            try:
                lock_path.unlink()
            except OSError:
                pass
        else:
            raise RuntimeError(f"Project {project_slug} is locked by another job: {lock_path}")

    lock_path.write_text(f"{os.getpid()}\n{datetime.now(timezone.utc).isoformat()}", encoding="utf-8")
    try:
        yield lock_path
    finally:
        try:
            if lock_path.exists():
                lock_path.unlink()
        except OSError:
            pass


def _lock_is_stale(lock_path: Path) -> bool:
    try:
        content = lock_path.read_text(encoding="utf-8").strip().splitlines()
    except OSError:
        return True
    if not content:
        return True
    try:
        pid = int(content[0])
    except ValueError:
        return True
    if not _pid_alive(pid):
        return True
    try:
        age = time.time() - lock_path.stat().st_mtime
    except OSError:
        return True
    return age > LOCK_STALE_SECONDS


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            out = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}"],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False
        return str(pid) in (out.stdout or "")
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _reap_stale_worktrees(project: ProjectConfig, worktrees_root: Path, stale_hours: int) -> list[str]:
    if not worktrees_root.exists():
        return []
    cutoff = time.time() - (stale_hours * 3600)
    reaped: list[str] = []
    for entry in worktrees_root.iterdir():
        if not entry.is_dir():
            continue
        if not entry.name.startswith(f"op-{project.slug}-"):
            continue
        try:
            mtime = entry.stat().st_mtime
        except OSError:
            continue
        if mtime > cutoff:
            continue
        logger.warning("Reaping stale worktree: %s", entry)
        _cleanup_worktree(project, entry)
        reaped.append(str(entry))
    return reaped


def _cleanup_worktree(project: ProjectConfig, worktree_path: Path) -> None:
    if not worktree_path.exists():
        return
    remove = _run_command(
        ["git", "worktree", "remove", "--force", str(worktree_path)],
        project.path,
        timeout=180,
    )
    if remove.exit_code != 0:
        logger.warning("git worktree remove failed for %s: %s", worktree_path, remove.output[-500:])
        try:
            shutil.rmtree(worktree_path, ignore_errors=True)
        except OSError:
            pass
    _run_command(["git", "worktree", "prune"], project.path, timeout=60)


def _find_latest_pptx(output_dir: Path) -> Path | None:
    if not output_dir.exists():
        return None
    candidates = [p for p in output_dir.rglob("*.pptx") if p.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _summarize_source_freshness(output: str) -> str | None:
    for line in output.splitlines():
        lower = line.lower()
        if "freshness" in lower or "last updated" in lower or "data as of" in lower:
            return line.strip()[:200]
    return None


def _extract_data_gaps(output: str) -> list[str]:
    gaps: list[str] = []
    for line in output.splitlines():
        lower = line.lower()
        if "gap" in lower or "missing" in lower or "no data" in lower or "warning" in lower:
            cleaned = line.strip()
            if cleaned and cleaned not in gaps:
                gaps.append(cleaned[:200])
        if len(gaps) >= 10:
            break
    return gaps
