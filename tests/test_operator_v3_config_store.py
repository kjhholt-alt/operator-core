"""Tests for config.load_projects and the JobStore lifecycle.

The V4 settings loader now reads TOML (not JSON) — adapted accordingly.
"""

from __future__ import annotations

from pathlib import Path

from operator_core.config import find_project, load_projects
from operator_core.store import JobStore


def test_load_projects_required_shape(tmp_path: Path):
    projects_root = tmp_path / "projects"
    projects_root.mkdir()
    (projects_root / "demo").mkdir()

    config_path = tmp_path / "config.toml"
    config_path.write_text(
        f"""
[user]
github = "alice"
projects_root = "{projects_root.as_posix()}"

[[projects]]
slug = "demo"
path = "demo"
repo = "owner/demo"
type = "saas"
autonomy_tier = "guarded"
protected_patterns = [".env*"]
checks = ["pytest"]

[projects.deploy]
provider = "vercel"
url = "https://example.com"

[projects.health]
path = "/"
expected_status = 200
""",
        encoding="utf-8",
    )

    projects = load_projects(config_path)

    assert projects[0].slug == "demo"
    assert find_project("demo", projects).repo == "owner/demo"


def test_job_store_lifecycle(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.sqlite3")

    job = store.create_job("morning", prompt="hello token=super-secret-value")
    updated = store.update_job(job.id, status="complete", metadata={"ok": True})

    assert updated.status == "complete"
    assert updated.metadata == {"ok": True}
    assert store.list_jobs()[0].id == job.id
