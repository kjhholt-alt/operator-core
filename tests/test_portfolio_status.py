"""Tests for the portfolio status reader."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from operator_core import portfolio_status


def _write_doc(path: Path, **fields) -> None:
    # Default project name is repo dir, walking past container dirs like .status
    parent = path.parent
    default_proj = parent.parent.name if parent.name in {".status", "data", "docs"} else parent.name
    base = {
        "schema_version": "status-spec/v1",
        "project": fields.pop("project", default_proj),
        "ts": "2026-05-05T00:00:00Z",
        "health": fields.pop("health", "green"),
    }
    base.update(fields)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(base), encoding="utf-8")


def test_collect_finds_in_tree_status_spec(tmp_path):
    repo = tmp_path / "my-app"
    _write_doc(repo / ".status" / "status-spec.json")
    out = portfolio_status.collect(projects_root=tmp_path)
    assert len(out) == 1
    assert out[0].project == "my-app"
    assert out[0].health == "green"


def test_collect_skips_non_status_spec_json(tmp_path):
    repo = tmp_path / "decoy"
    repo.mkdir()
    (repo / "status-spec.json").write_text(json.dumps(
        {"schema_version": "something/else", "ts": "2026-05-05T00:00:00Z"}
    ), encoding="utf-8")
    out = portfolio_status.collect(projects_root=tmp_path)
    assert out == []


def test_collect_skips_dot_dirs(tmp_path):
    _write_doc((tmp_path / ".hidden") / "status-spec.json")
    out = portfolio_status.collect(projects_root=tmp_path)
    assert out == []


def test_collect_skips_underscore_dirs(tmp_path):
    _write_doc((tmp_path / "_archive") / "status-spec.json")
    out = portfolio_status.collect(projects_root=tmp_path)
    assert out == []


def test_collect_extra_path_appears_first(tmp_path):
    extra = tmp_path / "extra.json"
    _write_doc(extra, project="from-extra")
    other = tmp_path / "repo"
    _write_doc(other / "status-spec.json", project="from-repo")
    out = portfolio_status.collect(projects_root=tmp_path, extra_path=extra)
    assert [s.project for s in out[:2]] == ["from-extra", "from-repo"]


def test_collect_handles_unreadable_doc(tmp_path):
    repo = tmp_path / "bad"
    repo.mkdir()
    (repo / "status-spec.json").write_text("not json {{", encoding="utf-8")
    out = portfolio_status.collect(projects_root=tmp_path)
    assert out == []


def test_collect_picks_first_candidate_path(tmp_path):
    """If a repo has multiple candidate paths, only one wins."""
    repo = tmp_path / "doubled"
    _write_doc(repo / "status-spec.json")
    _write_doc(repo / ".status" / "status-spec.json")
    out = portfolio_status.collect(projects_root=tmp_path)
    assert len(out) == 1


def test_overall_health_red_wins(tmp_path):
    _write_doc((tmp_path / "a") / "status-spec.json", health="green")
    _write_doc((tmp_path / "b") / "status-spec.json", health="yellow")
    _write_doc((tmp_path / "c") / "status-spec.json", health="red")
    out = portfolio_status.collect(projects_root=tmp_path)
    assert portfolio_status.overall_health(out) == "red"


def test_overall_health_yellow_wins_over_green(tmp_path):
    _write_doc((tmp_path / "a") / "status-spec.json", health="green")
    _write_doc((tmp_path / "b") / "status-spec.json", health="yellow")
    out = portfolio_status.collect(projects_root=tmp_path)
    assert portfolio_status.overall_health(out) == "yellow"


def test_overall_health_unknown_with_no_inputs():
    assert portfolio_status.overall_health([]) == "unknown"


def test_render_table_handles_empty():
    out = portfolio_status.render_table([])
    assert "No status-spec/v1 documents found" in out


def test_render_table_includes_overall(tmp_path):
    _write_doc((tmp_path / "a") / "status-spec.json", health="yellow", summary="x")
    statuses = portfolio_status.collect(projects_root=tmp_path)
    rendered = portfolio_status.render_table(statuses)
    assert "OVERALL: yellow" in rendered
    assert "a" in rendered


def test_render_json_is_valid(tmp_path):
    _write_doc((tmp_path / "a") / "status-spec.json", health="green")
    statuses = portfolio_status.collect(projects_root=tmp_path)
    payload = json.loads(portfolio_status.render_json(statuses))
    assert payload["overall"] == "green"
    assert payload["count"] == 1
    assert payload["projects"][0]["project"] == "a"


def test_summary_with_only_newlines_does_not_crash(tmp_path):
    _write_doc((tmp_path / "a") / "status-spec.json", summary="\n\n")
    statuses = portfolio_status.collect(projects_root=tmp_path)
    out = portfolio_status.render_table(statuses)
    assert "OVERALL" in out
