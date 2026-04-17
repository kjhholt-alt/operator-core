from pathlib import Path

from operator_core.config import DeployConfig, HealthConfig, ProjectConfig
from operator_core.fleet import (
    CheckRun,
    DeployRun,
    FleetProjectReport,
    next_fix,
    render_fleet_report,
    score_project,
    security_flags,
)


def _project(slug: str, path: Path) -> ProjectConfig:
    return ProjectConfig(
        slug=slug,
        path=path,
        repo=f"owner/{slug}",
        type="saas",
        deploy=DeployConfig(provider="vercel", url="https://example.com"),
        health=HealthConfig(path="/"),
        checks=["npm run lint", "npm run build", "npm run test"],
        autonomy_tier="tiered_auto_deploy",
        protected_patterns=[],
        auto_merge=True,
    )


def test_score_weights_build_failures_highest():
    report = FleetProjectReport(
        slug="prospector-pro",
        path=".",
        branch="master",
        dirty=False,
        ahead_behind="clean",
        smoke_doc=True,
        deploy=DeployRun(status="healthy", http_status=200),
        checks=[
            CheckRun("npm run lint", 0),
            CheckRun("npm run build", 1),
            CheckRun("npm run test", 0),
        ],
    )

    assert score_project(report) == 40
    report.score = score_project(report)
    assert next_fix(report) == "fix `npm run build`"


def test_render_report_includes_next_fix():
    report = FleetProjectReport(
        slug="deal-brain",
        path=".",
        branch="master...origin/master [ahead 1]",
        dirty=True,
        ahead_behind="ahead 1",
        smoke_doc=False,
        deploy=DeployRun(status="healthy", http_status=200),
        checks=[],
        security_flags=["nurture route does not fail closed"],
        score=55,
        next_fix="clear security flag: nurture route does not fail closed",
    )

    rendered = render_fleet_report([report])

    assert "deal-brain" in rendered
    assert "score 55" in rendered
    assert "clear security flag" in rendered


def test_security_flags_detect_known_ai_ops_gap(tmp_path: Path):
    route = tmp_path / "src/app/api/send-email"
    route.mkdir(parents=True)
    (route / "route.ts").write_text("export async function POST() {}", encoding="utf-8")

    project = _project("ai-ops-consulting", tmp_path)

    assert "public send-email route" in security_flags(project)
