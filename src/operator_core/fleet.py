"""Fleet status and check reporting for the four SaaS products."""

from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable

from .config import ProjectConfig, load_projects
from .paths import DATA_DIR


SAAS_TYPE = "saas"
LOG_DIR = DATA_DIR / "logs"


@dataclass(frozen=True)
class CheckRun:
    command: str
    exit_code: int
    output_tail: str = ""

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


@dataclass(frozen=True)
class DeployRun:
    status: str
    http_status: int | None = None
    error: str | None = None
    elapsed_seconds: float = 0.0

    @property
    def ok(self) -> bool:
        return self.status == "healthy"


@dataclass
class FleetProjectReport:
    slug: str
    path: str
    branch: str
    dirty: bool
    ahead_behind: str
    smoke_doc: bool
    deploy: DeployRun
    checks: list[CheckRun] = field(default_factory=list)
    security_flags: list[str] = field(default_factory=list)
    score: int = 0
    next_fix: str = ""


CommandRunner = Callable[[str, Path, int], CheckRun]
DeployChecker = Callable[[ProjectConfig], DeployRun]


def load_saas_projects() -> list[ProjectConfig]:
    return [project for project in load_projects() if project.type == SAAS_TYPE]


def collect_fleet_report(
    *,
    run_checks: bool = False,
    command_runner: CommandRunner | None = None,
    deploy_checker: DeployChecker | None = None,
) -> list[FleetProjectReport]:
    runner = command_runner or run_command
    check_deploy = deploy_checker or quick_deploy_check
    reports: list[FleetProjectReport] = []

    for project in load_saas_projects():
        branch, dirty, ahead_behind = git_state(project.path)
        checks = [runner(command, project.path, 900) for command in project.checks] if run_checks else []
        deploy = check_deploy(project)
        report = FleetProjectReport(
            slug=project.slug,
            path=str(project.path),
            branch=branch,
            dirty=dirty,
            ahead_behind=ahead_behind,
            smoke_doc=(project.path / "SMOKE.md").exists(),
            deploy=deploy,
            checks=checks,
            security_flags=security_flags(project),
        )
        report.score = score_project(report)
        report.next_fix = next_fix(report)
        reports.append(report)

    return sorted(reports, key=lambda item: (-item.score, item.slug))


def run_command(command: str, cwd: Path, timeout: int = 900) -> CheckRun:
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd),
            shell=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
        )
        return CheckRun(command=command, exit_code=completed.returncode, output_tail=(completed.stdout or "")[-4000:])
    except subprocess.TimeoutExpired as exc:
        output = exc.stdout if isinstance(exc.stdout, str) else ""
        return CheckRun(command=command, exit_code=124, output_tail=(output + "\nTimed out")[-4000:])
    except OSError as exc:
        return CheckRun(command=command, exit_code=127, output_tail=str(exc))


def git_state(path: Path) -> tuple[str, bool, str]:
    result = run_command("git status --short --branch", path, timeout=60)
    if result.exit_code != 0:
        return "unknown", True, "git-status-failed"

    lines = result.output_tail.splitlines()
    branch_line = lines[0] if lines else "## unknown"
    branch = branch_line.removeprefix("## ").strip()
    dirty = any(line and not line.startswith("##") for line in lines)
    ahead_behind = "clean"
    if "[" in branch_line and "]" in branch_line:
        ahead_behind = branch_line.split("[", 1)[1].split("]", 1)[0]
    return branch, dirty, ahead_behind


def quick_deploy_check(project: ProjectConfig) -> DeployRun:
    started = time.monotonic()
    if project.deploy.provider.lower() == "local":
        return DeployRun(status="skipped", error="local provider")

    try:
        req = urllib.request.Request(
            project.deploy_health_url,
            headers={"User-Agent": "operator-v3-fleet/1"},
        )
        with urllib.request.urlopen(req, timeout=15) as response:  # noqa: S310 - registry health URL only
            status = response.status
        ok = 200 <= status < 300 and status == project.health.expected_status
        return DeployRun(
            status="healthy" if ok else "unhealthy",
            http_status=status,
            error=None if ok else f"unexpected status {status}",
            elapsed_seconds=time.monotonic() - started,
        )
    except urllib.error.HTTPError as exc:
        return DeployRun(
            status="unhealthy",
            http_status=exc.code,
            error=f"HTTPError: {exc.code}",
            elapsed_seconds=time.monotonic() - started,
        )
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return DeployRun(
            status="unhealthy",
            error=f"{type(exc).__name__}: {exc}",
            elapsed_seconds=time.monotonic() - started,
        )


def security_flags(project: ProjectConfig) -> list[str]:
    flags: list[str] = []
    slug = project.slug

    if slug == "ai-ops-consulting":
        send_email = project.path / "src/app/api/send-email/route.ts"
        if send_email.exists():
            text = send_email.read_text(encoding="utf-8", errors="replace")
            if "AI_OPS_INTERNAL_API_TOKEN" not in text and "authorization" not in text.lower():
                flags.append("public send-email route")
        config = project.path / "outreach/config.py"
        if config.exists():
            text = config.read_text(encoding="utf-8", errors="replace")
            if "os.getenv(\"RESEND_API_KEY\", \"\")" not in text:
                flags.append("outreach config has secret fallbacks")

    if slug == "prospector-pro":
        enricher = project.path / "src/lib/enricher.ts"
        if enricher.exists():
            text = enricher.read_text(encoding="utf-8", errors="replace")
            if "mockEnrichmentAllowed" not in text:
                flags.append("mock enrichment not production-gated")

    if slug == "deal-brain":
        nurture = project.path / "src/app/api/nurture/route.ts"
        if nurture.exists():
            text = nurture.read_text(encoding="utf-8", errors="replace")
            if "CRON_SECRET not configured" not in text:
                flags.append("nurture route does not fail closed")

    if slug == "ai-voice-receptionist":
        provision = project.path / "src/app/api/onboarding/provision/route.ts"
        if provision.exists():
            text = provision.read_text(encoding="utf-8", errors="replace")
            if "isRetellMockAllowed" not in text:
                flags.append("Retell mock provisioning not production-gated")

    return flags


def score_project(report: FleetProjectReport) -> int:
    score = 0
    for check in report.checks:
        lowered = check.command.lower()
        if check.ok:
            continue
        if "build" in lowered:
            score += 40
        elif "lint" in lowered:
            score += 25
        elif "test" in lowered:
            score += 20
        else:
            score += 15
    if not report.deploy.ok and report.deploy.status != "skipped":
        score += 15
    if report.dirty:
        score += 10
    if report.ahead_behind != "clean":
        score += 5
    if not report.smoke_doc:
        score += 10
    score += 30 * len(report.security_flags)
    return score


def next_fix(report: FleetProjectReport) -> str:
    for check in report.checks:
        if not check.ok:
            return f"fix `{check.command}`"
    if report.security_flags:
        return f"clear security flag: {report.security_flags[0]}"
    if not report.deploy.ok and report.deploy.status != "skipped":
        return "restore deploy health"
    if report.dirty:
        return "review and commit working tree changes"
    if report.ahead_behind != "clean":
        return "push or reconcile branch state"
    if not report.smoke_doc:
        return "add SMOKE.md"
    return "keep green"


def render_fleet_report(reports: list[FleetProjectReport], *, title: str = "SaaS Fleet Status") -> str:
    lines = [f"## {title}", ""]
    for report in reports:
        check_bits = []
        for check in report.checks:
            status = "ok" if check.ok else f"fail {check.exit_code}"
            check_bits.append(f"{check.command}: {status}")
        checks = "; ".join(check_bits) if check_bits else "checks not run"
        deploy = report.deploy.status
        if report.deploy.http_status:
            deploy += f" {report.deploy.http_status}"
        flags = ", ".join(report.security_flags) if report.security_flags else "none"
        lines.append(
            f"- {report.slug}: score {report.score}; deploy {deploy}; "
            f"git {report.branch}; smoke {'yes' if report.smoke_doc else 'no'}; "
            f"checks {checks}; security {flags}; next {report.next_fix}"
        )
    return "\n".join(lines)


def write_fleet_report(reports: list[FleetProjectReport], *, mode: str) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = LOG_DIR / f"fleet-{mode}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    payload = {
        "mode": mode,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "projects": [
            {
                "slug": report.slug,
                "path": report.path,
                "branch": report.branch,
                "dirty": report.dirty,
                "ahead_behind": report.ahead_behind,
                "smoke_doc": report.smoke_doc,
                "deploy": report.deploy.__dict__,
                "checks": [check.__dict__ for check in report.checks],
                "security_flags": report.security_flags,
                "score": report.score,
                "next_fix": report.next_fix,
            }
            for report in reports
        ],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path
