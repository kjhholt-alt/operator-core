from pathlib import Path
from unittest.mock import MagicMock

from operator_core.config import DeployConfig, HealthConfig, ProjectConfig
from operator_core.deploy_health import check_project_deploy, poll_health_url
from operator_core.security import (
    CheckResults,
    can_auto_merge,
    classify_and_decide,
    classify_risk,
    command_is_blocked,
    redact_mapping,
    scan_files_for_secrets,
)


def _project(auto_merge=True):
    return ProjectConfig(
        slug="demo",
        path=Path("."),
        repo="owner/demo",
        type="saas",
        deploy=DeployConfig(provider="vercel", url="https://example.com"),
        health=HealthConfig(path="/"),
        checks=["pytest"],
        autonomy_tier="tiered_auto_deploy",
        protected_patterns=["**/auth/**", ".env*"],
        auto_merge=auto_merge,
    )


def test_blocks_destructive_commands():
    assert command_is_blocked("git reset --hard HEAD")
    assert command_is_blocked("rm -rf important")
    assert command_is_blocked("rm -rf /")
    assert command_is_blocked("git push origin main --force")
    assert command_is_blocked("git push -f origin main")
    assert command_is_blocked("git commit -m 'x' --no-verify")
    assert command_is_blocked("DROP TABLE users;")
    assert command_is_blocked("drop database production")
    assert command_is_blocked("taskkill /F /PID 0")
    assert command_is_blocked("taskkill /PID 0 /F")
    assert command_is_blocked("TRUNCATE TABLE orders")
    assert command_is_blocked(":(){ :|:& };:")
    assert command_is_blocked("echo sk-ant-0123456789abcdef0123")


def test_allows_safe_commands():
    assert command_is_blocked("git status") is None
    assert command_is_blocked("npm run build") is None
    assert command_is_blocked("python -m pytest tests -q") is None
    assert command_is_blocked("ls -la") is None


def test_secret_scan_finds_discord_webhook(tmp_path):
    file_path = tmp_path / "notes.txt"
    file_path.write_text(
        "hook=https://discord.com/api/webhooks/1234567890/abcdefghijklmnopqrstuvwxyz",
        encoding="utf-8",
    )

    findings = scan_files_for_secrets([file_path])

    assert findings
    assert findings[0].line == 1


def test_risk_classification():
    project = _project()

    assert classify_risk("fix docs", ["README.md"], project) == "low"
    assert classify_risk("rebuild pricing page", ["src/page.tsx"], project) == "medium"
    assert classify_risk("change auth callback", ["src/auth/callback.ts"], project) == "high"


def test_auto_merge_policy_requires_global_enablement():
    decision = can_auto_merge(
        project=_project(),
        risk="low",
        tests_passed=True,
        secret_scan_passed=True,
        reviewer_verdict="PASS",
        ci_green=True,
        deploy_green=True,
        global_auto_merge_enabled=False,
    )

    assert not decision.allowed
    assert decision.requires_manual


def test_auto_merge_policy_allows_low_risk_when_enabled():
    decision = can_auto_merge(
        project=_project(),
        risk="low",
        tests_passed=True,
        secret_scan_passed=True,
        reviewer_verdict="PASS",
        ci_green=False,
        deploy_green=False,
        global_auto_merge_enabled=True,
    )

    assert decision.allowed


def _base_checks(**overrides):
    base = dict(
        tests_passed=True,
        secret_scan_passed=True,
        reviewer_verdict="PASS",
        ci_green=True,
        deploy_green=True,
        unresolved_comments=False,
        approvals=2,
        global_auto_merge_enabled=True,
    )
    base.update(overrides)
    return CheckResults(**base)


def test_classify_and_decide_low_risk_docs_allowed():
    decision = classify_and_decide(
        changed_files=["README.md", "docs/guide.md"],
        project_cfg=_project(),
        check_results=_base_checks(),
    )
    assert decision.allowed is True
    assert decision.risk == "low"
    assert decision.reasoning  # has reasoning trail


def test_classify_and_decide_global_disabled_blocks_even_low_risk():
    decision = classify_and_decide(
        changed_files=["README.md"],
        project_cfg=_project(),
        check_results=_base_checks(global_auto_merge_enabled=False),
    )
    assert decision.allowed is False
    assert decision.requires_manual is True
    assert "disabled" in decision.reason.lower()


def test_classify_and_decide_medium_requires_two_approvals_and_ci_and_deploy():
    # only 1 approval — blocked
    decision = classify_and_decide(
        changed_files=["src/components/Pricing.tsx"],
        project_cfg=_project(),
        check_results=_base_checks(approvals=1),
    )
    assert decision.risk == "medium"
    assert decision.allowed is False
    assert "approval" in decision.reason.lower()

    # ci red — blocked
    decision = classify_and_decide(
        changed_files=["src/components/Pricing.tsx"],
        project_cfg=_project(),
        check_results=_base_checks(ci_green=False),
    )
    assert decision.allowed is False
    assert "ci" in decision.reason.lower()

    # deploy red — blocked
    decision = classify_and_decide(
        changed_files=["src/components/Pricing.tsx"],
        project_cfg=_project(),
        check_results=_base_checks(deploy_green=False),
    )
    assert decision.allowed is False
    assert "deploy" in decision.reason.lower()

    # all good — allowed
    decision = classify_and_decide(
        changed_files=["src/components/Pricing.tsx"],
        project_cfg=_project(),
        check_results=_base_checks(),
    )
    assert decision.allowed is True
    assert decision.risk == "medium"


def test_classify_and_decide_high_risk_always_blocked():
    decision = classify_and_decide(
        changed_files=["src/auth/login.ts", "supabase/migrations/001.sql"],
        project_cfg=_project(),
        check_results=_base_checks(approvals=5),
    )
    assert decision.risk == "high"
    assert decision.allowed is False
    assert decision.requires_manual is True


def test_classify_and_decide_protected_path_is_high_risk():
    project = _project()  # protected_patterns includes "**/auth/**"
    decision = classify_and_decide(
        changed_files=["src/auth/session.ts"],
        project_cfg=project,
        check_results=_base_checks(),
    )
    assert decision.risk == "high"
    assert decision.allowed is False


def test_classify_and_decide_failed_secret_scan_blocks():
    decision = classify_and_decide(
        changed_files=["README.md"],
        project_cfg=_project(),
        check_results=_base_checks(secret_scan_passed=False),
    )
    assert decision.allowed is False
    assert "secret" in decision.reason.lower()


def test_redact_mapping_drops_sensitive_keys():
    payload = {
        "tool_name": "Bash",
        "env": {"DISCORD_BOT_TOKEN": "super-secret"},
        "api_key": "sk-ant-0123456789abcdefghij",
        "password": "hunter2",
        "safe": "hello",
        "nested": {"token": "xoxb-abc", "cmd": "ls"},
        "webhook": "https://discord.com/api/webhooks/123456789/AAAAAAAAAAAAAAAAAAAAA",
    }
    scrubbed = redact_mapping(payload)
    assert scrubbed["env"] == "[REDACTED]"
    assert scrubbed["api_key"] == "[REDACTED]"
    assert scrubbed["password"] == "[REDACTED]"
    assert scrubbed["safe"] == "hello"
    assert scrubbed["nested"]["token"] == "[REDACTED]"
    assert scrubbed["nested"]["cmd"] == "ls"
    assert "[REDACTED_SECRET]" in scrubbed["webhook"]


def test_poll_health_url_returns_healthy_on_first_200():
    http_get = MagicMock(return_value=(200, b"ok"))
    sleep = MagicMock()
    t = [0.0]

    def clock():
        return t[0]

    result = poll_health_url(
        "https://example.com/",
        http_get=http_get,
        sleep=sleep,
        clock=clock,
    )
    assert result.status == "healthy"
    assert result.attempts == 1
    assert result.http_status == 200
    sleep.assert_not_called()


def test_poll_health_url_retries_then_succeeds():
    responses = [(503, b""), (502, b""), (200, b"ok")]
    http_get = MagicMock(side_effect=responses)
    sleep = MagicMock()
    t = [0.0]

    def clock():
        return t[0]

    def fake_sleep(seconds):
        t[0] += seconds

    result = poll_health_url(
        "https://example.com/",
        http_get=http_get,
        sleep=fake_sleep,
        clock=clock,
        interval=15.0,
        total_timeout=300.0,
    )
    assert result.status == "healthy"
    assert result.attempts == 3
    assert http_get.call_count == 3


def test_poll_health_url_times_out():
    http_get = MagicMock(return_value=(500, b""))
    t = [0.0]

    def clock():
        return t[0]

    def fake_sleep(seconds):
        t[0] += seconds

    result = poll_health_url(
        "https://example.com/",
        http_get=http_get,
        sleep=fake_sleep,
        clock=clock,
        interval=15.0,
        total_timeout=60.0,
    )
    assert result.status == "unhealthy"
    assert result.last_error is not None


def test_check_project_deploy_skips_local_provider():
    project = ProjectConfig(
        slug="ops",
        path=Path("."),
        repo="owner/ops",
        type="operator",
        deploy=DeployConfig(provider="local", url="http://127.0.0.1:8765"),
        health=HealthConfig(path="/health"),
        checks=[],
        autonomy_tier="guarded",
        protected_patterns=[],
        auto_merge=False,
    )
    http_get = MagicMock()
    result = check_project_deploy(project, http_get=http_get)
    assert result.status == "skipped"
    http_get.assert_not_called()


def test_check_project_deploy_hits_vercel_url():
    project = _project()
    http_get = MagicMock(return_value=(200, b"ok"))
    result = check_project_deploy(project, http_get=http_get, sleep=lambda _: None)
    assert result.status == "healthy"
    called_url = http_get.call_args[0][0]
    assert called_url.startswith("https://example.com")
