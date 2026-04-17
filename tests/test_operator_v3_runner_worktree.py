"""Integration tests for the Operator worktree PR factory."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from operator_core import runner as runner_module
from operator_core.config import DeployConfig, HealthConfig, ProjectConfig
from operator_core.runner import (
    CommandResult,
    JobRunner,
    _branch_name,
    _find_latest_pptx,
    _reap_stale_worktrees,
    _render_pr_body,
)
from operator_core.security import AutonomyDecision, SecretFinding
from operator_core.store import JobStore


pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="git not available")


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        check=True,
        capture_output=True,
        text=True,
    )


def _init_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "-q", "-b", "main")
    _git(path, "config", "user.email", "test@example.com")
    _git(path, "config", "user.name", "Test")
    _git(path, "config", "commit.gpgsign", "false")
    (path / "README.md").write_text("hello\n", encoding="utf-8")
    _git(path, "add", ".")
    _git(path, "commit", "-q", "-m", "initial")


def _make_project(path: Path, slug: str = "demo") -> ProjectConfig:
    return ProjectConfig(
        slug=slug,
        path=path,
        repo=f"owner/{slug}",
        type="saas",
        deploy=DeployConfig(provider="vercel", url="https://example.com"),
        health=HealthConfig(path="/", expected_status=200),
        checks=[],
        autonomy_tier="guarded",
        protected_patterns=[".env*"],
        auto_merge=False,
    )


@pytest.fixture
def operator_env(tmp_path, monkeypatch):
    repo = tmp_path / "demo"
    _init_repo(repo)
    data_dir = tmp_path / ".operator"
    data_dir.mkdir()
    worktrees_dir = tmp_path / "worktrees"
    worktrees_dir.mkdir()

    monkeypatch.setenv("OPERATOR_V3_DRY_RUN", "1")
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_ENABLED", "0")

    monkeypatch.setattr(runner_module, "WORKTREES_DIR", worktrees_dir)
    monkeypatch.setattr(runner_module, "DATA_DIR", data_dir)
    monkeypatch.setattr(runner_module, "ensure_data_dirs", lambda: None)

    store = JobStore(data_dir / "jobs.sqlite3")
    return {
        "repo": repo,
        "tmp_path": tmp_path,
        "store": store,
        "runner_cls": JobRunner,
        "worktrees_dir": worktrees_dir,
        "data_dir": data_dir,
    }


def test_branch_name_is_short_and_safe():
    branch = _branch_name("deal-brain", "abc12345", "Rebuild pricing copy!!!")
    assert branch.startswith("op/deal-brain/")
    assert "!" not in branch
    assert len(branch) < 200

    # Extremely long prompts are truncated.
    long = _branch_name("demo", "abc12345", "x" * 5000)
    assert len(long) < 200


def test_render_pr_body_contains_required_sections():
    body = _render_pr_body(
        prompt="update copy",
        risk="low",
        changed_files=["README.md"],
        checks=[CommandResult("echo ok", 0, "ok")],
        reviewer_verdict="PASS",
        secret_findings=[],
        decision=AutonomyDecision(False, "Global auto-merge is disabled", True),
        dry_run=True,
    )
    for section in (
        "## Risk Tier",
        "## Changed Files",
        "## Checks Run",
        "## Reviewer Verdict",
        "## Secret Scan",
        "## Auto-Merge Decision",
    ):
        assert section in body
    assert "Dry run: True" in body


def test_render_pr_body_flags_secret_findings():
    body = _render_pr_body(
        prompt="add key",
        risk="high",
        changed_files=[".env"],
        checks=[],
        reviewer_verdict="PASS",
        secret_findings=[SecretFinding(path=".env", line=1, pattern="api_key=...")],
        decision=AutonomyDecision(False, "Secret scan failed", True),
        dry_run=True,
    )
    assert "api_key" in body
    assert "blocked" in body.lower()


def test_find_latest_pptx_picks_newest(tmp_path):
    output = tmp_path / "output"
    output.mkdir()
    older = output / "deck-old.pptx"
    newer = output / "deck-new.pptx"
    older.write_bytes(b"x")
    newer.write_bytes(b"y")
    os.utime(older, (1_600_000_000, 1_600_000_000))
    os.utime(newer, (1_700_000_000, 1_700_000_000))

    result = _find_latest_pptx(output)
    assert result == newer


def test_find_latest_pptx_returns_none_when_empty(tmp_path):
    assert _find_latest_pptx(tmp_path / "nope") is None
    empty = tmp_path / "empty"
    empty.mkdir()
    assert _find_latest_pptx(empty) is None


def test_reap_stale_worktrees_only_touches_matching_prefix(tmp_path, monkeypatch):
    worktrees = tmp_path / "wt"
    worktrees.mkdir()
    stale = worktrees / "op-demo-abc"
    stale.mkdir()
    (stale / "file.txt").write_text("x", encoding="utf-8")
    old_time = 1_600_000_000
    os.utime(stale, (old_time, old_time))

    other = worktrees / "op-other-xyz"
    other.mkdir()
    os.utime(other, (old_time, old_time))

    fresh = worktrees / "op-demo-fresh"
    fresh.mkdir()

    project = _make_project(tmp_path / "demo-repo", slug="demo")
    _init_repo(project.path)

    calls: list[Path] = []
    real_cleanup = runner_module._cleanup_worktree

    def fake_cleanup(proj, wt_path):
        calls.append(wt_path)
        shutil.rmtree(wt_path, ignore_errors=True)

    monkeypatch.setattr(runner_module, "_cleanup_worktree", fake_cleanup)
    reaped = _reap_stale_worktrees(project, worktrees, stale_hours=1)

    assert stale in calls
    assert other not in calls  # wrong slug prefix
    assert fresh not in calls  # not old enough
    assert str(stale) in reaped


def test_build_worktree_flow_dry_run(operator_env, monkeypatch):
    runner_cls = operator_env["runner_cls"]
    store = operator_env["store"]
    repo = operator_env["repo"]

    project = _make_project(repo, slug="demo")
    monkeypatch.setattr(runner_module, "find_project", lambda slug, projects=None: project)
    monkeypatch.setattr(runner_module, "load_projects", lambda: [project])

    def fake_feature_builder(prompt, worktree_path):
        target = worktree_path / "NOTES.md"
        target.write_text(f"automated: {prompt}\n", encoding="utf-8")
        subprocess.run(
            ["git", "-c", "user.email=t@t", "-c", "user.name=t", "add", "NOTES.md"],
            cwd=str(worktree_path),
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-m", "feature"],
            cwd=str(worktree_path),
            check=True,
            capture_output=True,
        )
        return CommandResult("feature-builder", 0, "ok")

    pr_calls: list[tuple] = []

    def fake_pr(project, worktree_path, branch, title, body):
        pr_calls.append((branch, title, body))
        return f"https://example.invalid/pr/{branch}"

    def fake_merge(project, branch, dry_run):
        return "dry-run merge skipped"

    runner = runner_cls(
        store=store,
        feature_builder=fake_feature_builder,
        pr_opener=fake_pr,
        merge_runner=fake_merge,
        discord_notifier=lambda *a, **k: True,
        email_sender=lambda *a, **k: True,
    )

    job = store.create_job("build", prompt="Add operator notes file", project="demo")
    result = runner.run(job.id)

    assert result.status in ("complete", "needs_manual", "needs_fix")
    assert result.branch and result.branch.startswith("op/demo/")
    assert "NOTES.md" in result.metadata.get("changed_files", [])
    assert result.metadata.get("dry_run") is True
    assert result.pr_url and "dry-run" in result.pr_url
    pr_body = result.metadata.get("pr_body", "")
    assert "## Changed Files" in pr_body
    assert "## Risk Tier" in pr_body
    assert "NOTES.md" in pr_body
    worktree_path = Path(result.worktree)
    assert not worktree_path.exists()
    lock_file = operator_env["data_dir"] / "locks" / "demo.lock"
    assert not lock_file.exists()


def test_build_rejects_when_project_locked(operator_env, tmp_path, monkeypatch):
    runner_cls = operator_env["runner_cls"]
    store = operator_env["store"]
    data_dir = operator_env["data_dir"]
    repo = operator_env["repo"]

    project = _make_project(repo, slug="demo")
    monkeypatch.setattr(runner_module, "find_project", lambda slug, projects=None: project)
    monkeypatch.setattr(runner_module, "load_projects", lambda: [project])

    lock_dir = data_dir / "locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_file = lock_dir / "demo.lock"
    lock_file.write_text(f"{os.getpid()}\n2099-01-01T00:00:00+00:00", encoding="utf-8")

    runner = runner_cls(
        store=store,
        feature_builder=lambda p, wt: CommandResult("noop", 0, ""),
        pr_opener=lambda *a, **k: None,
        merge_runner=lambda *a, **k: "skipped",
        discord_notifier=lambda *a, **k: True,
        email_sender=lambda *a, **k: True,
    )
    job = store.create_job("build", prompt="Test lock", project="demo")
    result = runner.run(job.id)
    assert result.status == "failed"
    assert "locked" in (result.metadata.get("error") or "").lower()
    assert lock_file.exists()
    lock_file.unlink()


def test_dry_run_default_is_enabled(monkeypatch):
    monkeypatch.delenv("OPERATOR_V3_DRY_RUN", raising=False)
    from operator_core.runner import _dry_run_enabled

    assert _dry_run_enabled() is True

    monkeypatch.setenv("OPERATOR_V3_DRY_RUN", "0")
    assert _dry_run_enabled() is False


def test_install_dependencies_skips_when_no_package_json(tmp_path, monkeypatch):
    from operator_core.runner import _install_dependencies

    calls: list[str] = []
    monkeypatch.setattr(
        runner_module,
        "_run_command",
        lambda cmd, cwd, timeout: calls.append(cmd) or CommandResult(cmd, 0, ""),
    )

    results = _install_dependencies(tmp_path)
    assert results == []
    assert calls == []


def test_install_dependencies_uses_npm_ci_when_lockfile_present(tmp_path, monkeypatch):
    from operator_core.runner import _install_dependencies

    (tmp_path / "package.json").write_text("{}", encoding="utf-8")
    (tmp_path / "package-lock.json").write_text("{}", encoding="utf-8")

    calls: list[str] = []
    monkeypatch.setattr(
        runner_module,
        "_run_command",
        lambda cmd, cwd, timeout: calls.append(cmd) or CommandResult(cmd, 0, "ok"),
    )

    results = _install_dependencies(tmp_path)
    assert len(results) == 1
    assert results[0].exit_code == 0
    assert calls == ["npm ci --no-audit --fund=false --prefer-offline"]


def test_worktree_location_standalone_uses_default_dir(tmp_path):
    from operator_core.runner import _worktree_location

    default = tmp_path / "outside"
    project = tmp_path / "deal-brain"
    loc = _worktree_location(
        project,
        "deal-brain",
        "abcd1234",
        workspace_root=None,
        default_worktrees_dir=default,
    )
    assert loc == default / "op-deal-brain-abcd1234"


def test_worktree_location_workspace_member_goes_inside_child(tmp_path):
    from operator_core.runner import _worktree_location

    workspace = tmp_path / "projects"
    workspace.mkdir()
    project = workspace / "prospector-pro"
    project.mkdir()
    default = tmp_path / "outside"

    loc = _worktree_location(
        project,
        "prospector-pro",
        "deadbeef",
        workspace_root=workspace,
        default_worktrees_dir=default,
    )
    assert loc == project / ".op-builds" / "op-prospector-pro-deadbeef"


def test_copy_env_files_copies_env_local_but_not_example(tmp_path):
    from operator_core.runner import _copy_env_files

    source = tmp_path / "source"
    source.mkdir()
    dest = tmp_path / "dest"
    dest.mkdir()

    (source / ".env.local").write_text("NEXT_PUBLIC_SUPABASE_URL=https://example", encoding="utf-8")
    (source / ".env.production").write_text("STRIPE_SECRET_KEY=sk_live_xxx", encoding="utf-8")
    (source / ".env.example").write_text("# template only", encoding="utf-8")
    (source / ".env.sample").write_text("# template only", encoding="utf-8")
    (source / "package.json").write_text('{"name":"x"}', encoding="utf-8")

    copied = _copy_env_files(source, dest)

    assert sorted(copied) == [".env.local", ".env.production"]
    assert (dest / ".env.local").read_text(encoding="utf-8") == "NEXT_PUBLIC_SUPABASE_URL=https://example"
    assert (dest / ".env.production").exists()
    assert not (dest / ".env.example").exists()
    assert not (dest / ".env.sample").exists()
    assert not (dest / "package.json").exists()


def test_copy_env_files_does_not_overwrite_existing(tmp_path):
    from operator_core.runner import _copy_env_files

    source = tmp_path / "source"
    source.mkdir()
    dest = tmp_path / "dest"
    dest.mkdir()

    (source / ".env.local").write_text("FROM_SOURCE=1", encoding="utf-8")
    (dest / ".env.local").write_text("FROM_DEST=1", encoding="utf-8")

    copied = _copy_env_files(source, dest)

    assert copied == []
    assert (dest / ".env.local").read_text(encoding="utf-8") == "FROM_DEST=1"


def test_detect_workspace_root_finds_exact_match(tmp_path):
    from operator_core.runner import _detect_workspace_root

    (tmp_path / "package.json").write_text(
        '{"name":"monorepo","private":true,"workspaces":["prospector-pro","deal-brain"]}',
        encoding="utf-8",
    )
    child = tmp_path / "prospector-pro"
    child.mkdir()
    (child / "package.json").write_text('{"name":"prospector-pro"}', encoding="utf-8")

    assert _detect_workspace_root(child).resolve() == tmp_path.resolve()


def test_detect_workspace_root_finds_glob_pattern(tmp_path):
    from operator_core.runner import _detect_workspace_root

    (tmp_path / "package.json").write_text(
        '{"name":"monorepo","private":true,"workspaces":["apps/*","packages/*"]}',
        encoding="utf-8",
    )
    (tmp_path / "packages").mkdir()
    child = tmp_path / "packages" / "ui"
    child.mkdir()
    (child / "package.json").write_text('{"name":"@suite/ui"}', encoding="utf-8")

    assert _detect_workspace_root(child).resolve() == tmp_path.resolve()


def test_detect_workspace_root_returns_none_for_standalone(tmp_path):
    from operator_core.runner import _detect_workspace_root

    child = tmp_path / "deal-brain"
    child.mkdir()
    (child / "package.json").write_text('{"name":"deal-brain"}', encoding="utf-8")

    (tmp_path / "package.json").write_text('{"name":"root"}', encoding="utf-8")

    assert _detect_workspace_root(child) is None


def test_detect_workspace_root_ignores_non_matching_workspaces(tmp_path):
    from operator_core.runner import _detect_workspace_root

    (tmp_path / "package.json").write_text(
        '{"name":"monorepo","workspaces":["something-else","another-thing"]}',
        encoding="utf-8",
    )
    child = tmp_path / "deal-brain"
    child.mkdir()
    (child / "package.json").write_text('{"name":"deal-brain"}', encoding="utf-8")

    assert _detect_workspace_root(child) is None


def test_worktrees_dir_default_is_outside_projects_root(monkeypatch, tmp_path):
    monkeypatch.delenv("OPERATOR_WORKTREES_DIR", raising=False)

    # operator-core's paths constants are _LazyPath wrappers — coerce to real Paths.
    # Point at a tmp config so this doesn't depend on the developer's real ~/.operator.
    projects_root = tmp_path / "projects"
    projects_root.mkdir()
    config = tmp_path / "config.toml"
    config.write_text(
        f"""
[user]
github = "tester"
projects_root = "{projects_root.as_posix()}"
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("OPERATOR_CONFIG", str(config))

    import importlib
    from operator_core import settings as settings_mod
    settings_mod.clear_cache()
    from operator_core import paths as paths_module

    importlib.reload(paths_module)
    try:
        default = Path(str(paths_module.WORKTREES_DIR)).resolve()
        pr = Path(str(paths_module.PROJECTS_ROOT)).resolve()
        assert pr not in default.parents, (
            f"WORKTREES_DIR default {default} is inside PROJECTS_ROOT {pr}"
        )
        assert default != pr
        assert default.is_absolute()
    finally:
        settings_mod.clear_cache()
        importlib.reload(paths_module)


def test_install_dependencies_falls_back_to_npm_install_without_lockfile(tmp_path, monkeypatch):
    from operator_core.runner import _install_dependencies

    (tmp_path / "package.json").write_text("{}", encoding="utf-8")

    calls: list[str] = []
    monkeypatch.setattr(
        runner_module,
        "_run_command",
        lambda cmd, cwd, timeout: calls.append(cmd) or CommandResult(cmd, 0, "ok"),
    )

    results = _install_dependencies(tmp_path)
    assert len(results) == 1
    assert calls == ["npm install --no-audit --fund=false --prefer-offline"]
