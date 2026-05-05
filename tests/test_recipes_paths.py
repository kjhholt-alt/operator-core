"""Tests for recipes._paths.

The helper is the choke-point for cross-host path resolution. Lock in
the precedence order so a future refactor doesn't silently re-introduce
the host-specific bug it was created to fix.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from recipes._paths import projects_dir, project_subpath


# ---------------------------------------------------------------------------
# projects_dir() precedence
# ---------------------------------------------------------------------------


def test_projects_dir_uses_env_var_when_set(tmp_path, monkeypatch):
    target = tmp_path / "some_other_root"
    target.mkdir()
    monkeypatch.setenv("OPERATOR_PROJECTS_DIR", str(target))
    assert projects_dir() == target


def test_projects_dir_env_var_returned_even_if_path_absent(tmp_path, monkeypatch):
    """Explicit env wins even if the path doesn't exist (loud failure later
    is better than silently picking a different dir)."""
    nonexistent = tmp_path / "definitely-not-there"
    monkeypatch.setenv("OPERATOR_PROJECTS_DIR", str(nonexistent))
    assert projects_dir() == nonexistent


def test_projects_dir_falls_back_to_cwd_when_no_settings_no_env(monkeypatch, tmp_path):
    monkeypatch.delenv("OPERATOR_PROJECTS_DIR", raising=False)
    # Forcibly poison settings so it returns no projects_root.
    import operator_core.paths as op_paths

    def _broken(*args, **kwargs):
        raise RuntimeError("settings unavailable")

    monkeypatch.setattr(op_paths, "_projects_root", _broken)

    # Force home to a path with no Desktop/Projects or Projects subdir.
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    monkeypatch.chdir(tmp_path)

    result = projects_dir()
    # On Windows Path.home() consults USERPROFILE; on POSIX, HOME.
    # Either way the fallback should land at cwd since neither subdir exists.
    assert result == tmp_path


def test_projects_dir_prefers_home_desktop_projects_over_cwd(monkeypatch, tmp_path):
    monkeypatch.delenv("OPERATOR_PROJECTS_DIR", raising=False)
    import operator_core.paths as op_paths
    monkeypatch.setattr(op_paths, "_projects_root", lambda: (_ for _ in ()).throw(RuntimeError()))

    fake_home = tmp_path / "home"
    desktop_projects = fake_home / "Desktop" / "Projects"
    desktop_projects.mkdir(parents=True)
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv("USERPROFILE", str(fake_home))

    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    assert projects_dir() == desktop_projects


def test_projects_dir_uses_home_projects_when_desktop_projects_missing(monkeypatch, tmp_path):
    monkeypatch.delenv("OPERATOR_PROJECTS_DIR", raising=False)
    import operator_core.paths as op_paths
    monkeypatch.setattr(op_paths, "_projects_root", lambda: (_ for _ in ()).throw(RuntimeError()))

    fake_home = tmp_path / "home"
    home_projects = fake_home / "Projects"
    home_projects.mkdir(parents=True)
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv("USERPROFILE", str(fake_home))
    monkeypatch.chdir(tmp_path)

    assert projects_dir() == home_projects


def test_projects_dir_custom_env_var_name(tmp_path, monkeypatch):
    target = tmp_path / "alt_root"
    target.mkdir()
    monkeypatch.setenv("MY_CUSTOM_PROJECTS_DIR", str(target))
    monkeypatch.delenv("OPERATOR_PROJECTS_DIR", raising=False)
    assert projects_dir(env_var="MY_CUSTOM_PROJECTS_DIR") == target


# ---------------------------------------------------------------------------
# project_subpath()
# ---------------------------------------------------------------------------


def test_project_subpath_joins_under_projects_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_PROJECTS_DIR", str(tmp_path))
    assert project_subpath("foo", "bar.csv") == tmp_path / "foo" / "bar.csv"


def test_project_subpath_explicit_env_override_wins(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_PROJECTS_DIR", str(tmp_path))
    explicit = tmp_path / "completely" / "different.csv"
    monkeypatch.setenv("MY_OVERRIDE", str(explicit))
    result = project_subpath("foo", "bar.csv", env_var="MY_OVERRIDE")
    assert result == explicit


def test_project_subpath_no_env_falls_through_to_join(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_PROJECTS_DIR", str(tmp_path))
    monkeypatch.delenv("UNSET_VAR", raising=False)
    assert project_subpath("a", env_var="UNSET_VAR") == tmp_path / "a"
