"""Smoke tests for settings.py loader."""

from __future__ import annotations

from pathlib import Path

import pytest

from operator_core.settings import (
    ConfigError,
    Settings,
    load_settings,
)


GOOD_CONFIG = """\
[user]
github = "alice"
projects_root = "{projects_root}"

[daemon]
bind = "127.0.0.1"
port = 9000

[discord.channels]
projects = "DISCORD_PROJECTS"

[[projects]]
slug = "demo"
path = "demo"
repo = "alice/demo"
type = "nextjs"
autonomy_tier = "medium"
protected_patterns = []

[projects.deploy]
provider = "vercel"
url = "https://demo.vercel.app"

[projects.health]
path = "/"
expected_status = 200
"""


def test_load_good_config(tmp_path: Path) -> None:
    projects_root = tmp_path / "projects"
    projects_root.mkdir()
    (projects_root / "demo").mkdir()

    config = tmp_path / "config.toml"
    config.write_text(
        GOOD_CONFIG.format(projects_root=str(projects_root).replace("\\", "/")),
        encoding="utf-8",
    )

    settings = load_settings(config)
    assert isinstance(settings, Settings)
    assert settings.github_handle == "alice"
    assert settings.projects_root == projects_root.resolve()
    assert settings.daemon.port == 9000
    assert settings.discord_channels == {"projects": "DISCORD_PROJECTS"}
    assert len(settings.projects) == 1
    assert settings.projects[0].slug == "demo"
    assert settings.projects[0].path == (projects_root / "demo").resolve()
    assert settings.projects[0].deploy.url == "https://demo.vercel.app"


def test_missing_projects_root_raises(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        '[user]\ngithub = "alice"\n', encoding="utf-8"
    )
    with pytest.raises(ConfigError) as ei:
        load_settings(config)
    assert "projects_root" in str(ei.value)


def test_missing_github_raises(tmp_path: Path) -> None:
    projects_root = tmp_path / "projects"
    projects_root.mkdir()
    config = tmp_path / "config.toml"
    config.write_text(
        f'[user]\nprojects_root = "{projects_root.as_posix()}"\n',
        encoding="utf-8",
    )
    with pytest.raises(ConfigError) as ei:
        load_settings(config)
    assert "github" in str(ei.value).lower()


def test_duplicate_slugs_raise(tmp_path: Path) -> None:
    projects_root = tmp_path / "projects"
    projects_root.mkdir()
    (projects_root / "a").mkdir()
    (projects_root / "b").mkdir()

    config = tmp_path / "config.toml"
    config.write_text(
        f"""
[user]
github = "alice"
projects_root = "{projects_root.as_posix()}"

[[projects]]
slug = "dup"
path = "a"
repo = "alice/a"
type = "python"
autonomy_tier = "medium"

[projects.deploy]
provider = "vercel"
url = "https://a.example"

[projects.health]
path = "/"

[[projects]]
slug = "dup"
path = "b"
repo = "alice/b"
type = "python"
autonomy_tier = "medium"

[projects.deploy]
provider = "vercel"
url = "https://b.example"

[projects.health]
path = "/"
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError) as ei:
        load_settings(config)
    assert "dup" in str(ei.value).lower()


def test_missing_config_file_raises(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as ei:
        load_settings(tmp_path / "no-such-file.toml")
    assert "not found" in str(ei.value).lower()


def test_absolute_project_path_preserved(tmp_path: Path) -> None:
    abs_path = tmp_path / "other-place"
    abs_path.mkdir()
    projects_root = tmp_path / "projects"
    projects_root.mkdir()

    config = tmp_path / "config.toml"
    config.write_text(
        f"""
[user]
github = "alice"
projects_root = "{projects_root.as_posix()}"

[[projects]]
slug = "elsewhere"
path = "{abs_path.as_posix()}"
repo = "alice/elsewhere"
type = "python"
autonomy_tier = "low"

[projects.deploy]
provider = "vercel"
url = "https://elsewhere.example"

[projects.health]
path = "/"
""",
        encoding="utf-8",
    )
    settings = load_settings(config)
    assert settings.projects[0].path == abs_path.resolve()
