"""Operator V3 live smoke harness.

Runs a fixed sequence of `!op` commands (dry-run by default) and captures
stdout, job ledger IDs, exit codes into a timestamped JSONL file under
`.operator-v3/logs/`. Used as the end-of-sprint sanity check before going
live, and as a CI dry-run guard on every PR.

No network in the default path. `--live` flips off the dry-run env flag for
the duration of the harness (still bounded by the per-command runner gates).
`--record` persists the raw stdout blobs for replay fixtures.
"""

from __future__ import annotations

import io
import json
import os
import sys
import traceback
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .commands import CommandParseError, parse_operator_command
from .paths import DATA_DIR, ensure_data_dirs
from .runner import JobRunner
from .store import JobStore


SMOKE_SEQUENCE: tuple[str, ...] = (
    "!op status",
    "!op help",
    "!op morning",
    "!op review prs",
    "!op deploy check",
    "!op deck ag-market-pulse",
)


@dataclass
class SmokeStepResult:
    command: str
    action: str
    job_id: str | None
    status: str
    exit_code: int
    stdout: str
    error: str | None = None

    def to_dict(self, *, include_stdout: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "command": self.command,
            "action": self.action,
            "job_id": self.job_id,
            "status": self.status,
            "exit_code": self.exit_code,
            "error": self.error,
        }
        if include_stdout:
            payload["stdout"] = self.stdout
        else:
            payload["stdout_bytes"] = len(self.stdout.encode("utf-8"))
        return payload


@dataclass
class SmokeReport:
    started_at: str
    finished_at: str
    live: bool
    record: bool
    log_path: str
    steps: list[SmokeStepResult] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(step.exit_code == 0 for step in self.steps)


def _log_path(now: datetime | None = None) -> Path:
    now = now or datetime.now()
    logs_dir = DATA_DIR / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    stamp = now.strftime("%Y%m%d-%H%M")
    return logs_dir / f"smoke-{stamp}.jsonl"


def _make_runner(store: JobStore | None = None) -> tuple[JobStore, JobRunner]:
    store = store or JobStore()
    runner = JobRunner(store)
    return store, runner


def _run_one(
    raw: str,
    store: JobStore,
    runner: JobRunner,
) -> SmokeStepResult:
    buf = io.StringIO()
    try:
        parsed = parse_operator_command(raw)
    except CommandParseError as exc:
        return SmokeStepResult(
            command=raw,
            action="<parse-error>",
            job_id=None,
            status="parse_error",
            exit_code=2,
            stdout="",
            error=str(exc),
        )

    # `help` is pure — no job row, just print help text.
    if parsed.action == "help":
        from .commands import help_text

        with redirect_stdout(buf):
            print(help_text())
        return SmokeStepResult(
            command=raw,
            action=parsed.action,
            job_id=None,
            status="complete",
            exit_code=0,
            stdout=buf.getvalue(),
        )

    try:
        job = store.create_job(
            parsed.action,
            parsed.prompt,
            parsed.project,
            metadata={"source": "smoke"},
        )
        with redirect_stdout(buf):
            runner.run(job.id)
        final = store.get_job(job.id)
    except Exception as exc:  # noqa: BLE001 — smoke must keep going
        return SmokeStepResult(
            command=raw,
            action=parsed.action,
            job_id=None,
            status="error",
            exit_code=1,
            stdout=buf.getvalue(),
            error=f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
        )

    terminal_ok = {"complete", "needs_manual", "needs_fix"}
    exit_code = 0 if final.status in terminal_ok else 1
    return SmokeStepResult(
        command=raw,
        action=parsed.action,
        job_id=final.id,
        status=final.status,
        exit_code=exit_code,
        stdout=buf.getvalue(),
        error=final.metadata.get("error") if isinstance(final.metadata, dict) else None,
    )


def run_smoke(
    live: bool = False,
    record: bool = False,
    *,
    sequence: tuple[str, ...] = SMOKE_SEQUENCE,
    store: JobStore | None = None,
    runner: JobRunner | None = None,
    log_path: Path | None = None,
    now: Callable[[], datetime] = datetime.now,
) -> SmokeReport:
    """Run the fixed smoke sequence and write a JSONL audit log.

    Defaults:
      * dry-run — sets `OPERATOR_V3_DRY_RUN=1` unless `live=True`.
      * no network — all runner calls still honour dry-run defaults.
      * stdout captured per step; persisted in full only when `record=True`.
    """
    ensure_data_dirs()

    prior_dry = os.environ.get("OPERATOR_V3_DRY_RUN")
    if not live:
        os.environ["OPERATOR_V3_DRY_RUN"] = "1"

    try:
        if store is None or runner is None:
            store, runner = _make_runner(store)

        started = now()
        path = log_path or _log_path(started)

        report = SmokeReport(
            started_at=started.isoformat(timespec="seconds"),
            finished_at="",
            live=live,
            record=record,
            log_path=str(path).replace("\\", "/"),
        )

        with path.open("w", encoding="utf-8") as fh:
            header = {
                "type": "smoke_start",
                "started_at": report.started_at,
                "live": live,
                "record": record,
                "sequence": list(sequence),
            }
            fh.write(json.dumps(header, sort_keys=True) + "\n")
            for raw in sequence:
                step = _run_one(raw, store, runner)
                report.steps.append(step)
                line = {"type": "smoke_step", **step.to_dict(include_stdout=record)}
                fh.write(json.dumps(line, sort_keys=True) + "\n")

            finished = now()
            report.finished_at = finished.isoformat(timespec="seconds")
            footer = {
                "type": "smoke_end",
                "finished_at": report.finished_at,
                "ok": report.ok,
                "steps": len(report.steps),
            }
            fh.write(json.dumps(footer, sort_keys=True) + "\n")

        return report
    finally:
        if not live:
            if prior_dry is None:
                os.environ.pop("OPERATOR_V3_DRY_RUN", None)
            else:
                os.environ["OPERATOR_V3_DRY_RUN"] = prior_dry


def main(argv: list[str] | None = None) -> int:
    """CLI entry point wired from `operator-v3.py smoke`."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="operator-v3 smoke",
        description="Run the Operator V3 live smoke harness (dry-run by default).",
    )
    parser.add_argument("--live", action="store_true", help="Disable dry-run for this smoke pass")
    parser.add_argument("--record", action="store_true", help="Persist full stdout in the JSONL log")
    args = parser.parse_args(argv)

    report = run_smoke(live=args.live, record=args.record)
    print(f"smoke log: {report.log_path}")
    for step in report.steps:
        marker = "OK " if step.exit_code == 0 else "FAIL"
        print(f"  [{marker}] {step.command} -> {step.status} ({step.job_id or '-'})")
    print(f"overall: {'OK' if report.ok else 'FAIL'}  steps={len(report.steps)}")
    return 0 if report.ok else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
