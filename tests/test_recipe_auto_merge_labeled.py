"""Unit tests for the auto_merge_labeled recipe."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest

from operator_core.recipes import RecipeContext


def _load_module():
    repo_root = Path(__file__).resolve().parent.parent
    src = repo_root / "recipes" / "auto_merge_labeled.py"
    spec = importlib.util.spec_from_file_location("operator_recipes_auto_merge_labeled", src)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _ctx(**ov):
    base = dict(
        recipe_name="auto_merge_labeled",
        correlation_id="t",
        env={}, clients={}, cost_so_far=0.0, cost_budget_usd=0.0, dry_run=False,
    )
    base.update(ov)
    return RecipeContext(**base)


def _pr(number, *, labels=("auto-merge",), changed_files=1, additions=1, deletions=0,
         head_ref="feat/x", head_sha="deadbeef", title="title"):
    return {
        "number": number, "title": title,
        "head": {"ref": head_ref, "sha": head_sha},
        "changed_files": changed_files,
        "additions": additions, "deletions": deletions,
        "labels": [{"name": n} for n in labels],
    }


def test_metadata():
    mod = _load_module()
    r = mod.AutoMergeLabeled()
    assert r.name == "auto_merge_labeled"
    assert r.schedule == "*/15 * * * *"


def test_quiet_when_disabled(monkeypatch):
    mod = _load_module()
    monkeypatch.delenv("OPERATOR_AUTO_MERGE_LABELED", raising=False)
    r = mod.AutoMergeLabeled()
    result = asyncio.run(r.query(_ctx()))
    assert result["enabled"] is False
    assert result["merged"] == []


def test_quiet_when_no_token(monkeypatch):
    mod = _load_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_LABELED", "1")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    r = mod.AutoMergeLabeled()
    result = asyncio.run(r.query(_ctx()))
    assert result["enabled"] is True
    assert "GITHUB_TOKEN not set" in result["errors"][0]


def test_default_repos_used_when_env_empty(monkeypatch):
    mod = _load_module()
    monkeypatch.delenv("OPERATOR_AUTO_MERGE_LABELED_REPOS", raising=False)
    assert mod._resolve_repos() == ("kjhholt-alt/operator-core",)


def test_env_repos_override(monkeypatch):
    mod = _load_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_LABELED_REPOS", "a/b, c/d ,e/f")
    assert mod._resolve_repos() == ("a/b", "c/d", "e/f")


def test_skips_when_ci_pending(monkeypatch):
    mod = _load_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_LABELED", "1")
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setattr(mod, "_list_labeled_open_prs", lambda *a, **kw: [_pr(11)])
    monkeypatch.setattr(mod, "_combined_status", lambda *a, **kw: "pending")
    monkeypatch.setattr(mod, "_squash_merge", lambda *a, **kw: pytest.fail("must not merge while pending"))

    r = mod.AutoMergeLabeled()
    result = asyncio.run(r.query(_ctx()))
    assert result["merged"] == []
    assert any(s["reason"] == "ci_pending" for s in result["skipped"])


def test_skips_empty_diff(monkeypatch):
    mod = _load_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_LABELED", "1")
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setattr(mod, "_list_labeled_open_prs",
                         lambda *a, **kw: [_pr(12, changed_files=0, additions=0, deletions=0)])
    monkeypatch.setattr(mod, "_combined_status", lambda *a, **kw: "success")
    monkeypatch.setattr(mod, "_squash_merge", lambda *a, **kw: pytest.fail("must not merge empty"))

    r = mod.AutoMergeLabeled()
    result = asyncio.run(r.query(_ctx()))
    assert result["merged"] == []
    assert any(s["reason"] == "empty_diff" for s in result["skipped"])


def test_merges_when_green_and_labeled(monkeypatch):
    mod = _load_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_LABELED", "1")
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_LABELED_REPOS", "owner/repo")

    monkeypatch.setattr(mod, "_list_labeled_open_prs",
                         lambda token, repo, label: [_pr(42, title="bump dep")])
    monkeypatch.setattr(mod, "_combined_status", lambda *a, **kw: "success")

    comments, merges = [], []
    monkeypatch.setattr(mod, "_post_comment",
                         lambda t, r, n, b: comments.append((r, n, b)) or {})
    monkeypatch.setattr(mod, "_squash_merge",
                         lambda t, r, n: merges.append((r, n)) or {"merged": True})

    rec = mod.AutoMergeLabeled()
    result = asyncio.run(rec.query(_ctx()))
    assert merges == [("owner/repo", 42)]
    assert comments[0][0] == "owner/repo"
    assert "auto_merge_labeled" in comments[0][2]
    assert result["merged"][0]["pr"] == 42

    msg = asyncio.run(rec.format(_ctx(), result))
    assert "owner/repo" in msg
    assert "PR #42" in msg


def test_dry_run_does_not_merge(monkeypatch):
    mod = _load_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_LABELED", "1")
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setattr(mod, "_list_labeled_open_prs", lambda *a, **kw: [_pr(7)])
    monkeypatch.setattr(mod, "_combined_status", lambda *a, **kw: "success")
    monkeypatch.setattr(mod, "_squash_merge",
                         lambda *a, **kw: pytest.fail("must not merge in dry run"))
    rec = mod.AutoMergeLabeled()
    result = asyncio.run(rec.query(_ctx(dry_run=True)))
    assert len(result["merged"]) == 1
    assert result["merged"][0].get("dry_run") is True


def test_merge_api_error_surfaced(monkeypatch):
    mod = _load_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_LABELED", "1")
    monkeypatch.setenv("GITHUB_TOKEN", "fake")
    monkeypatch.setattr(mod, "_list_labeled_open_prs", lambda *a, **kw: [_pr(99)])
    monkeypatch.setattr(mod, "_combined_status", lambda *a, **kw: "success")
    monkeypatch.setattr(mod, "_post_comment", lambda *a, **kw: {})
    monkeypatch.setattr(mod, "_squash_merge",
                         lambda *a, **kw: {"error": True, "status": 405, "detail": "Method Not Allowed"})

    rec = mod.AutoMergeLabeled()
    result = asyncio.run(rec.query(_ctx()))
    assert result["merged"] == []
    assert any("405" in e or "Method Not Allowed" in e for e in result["errors"])


def test_list_labeled_filter_excludes_unlabeled(monkeypatch):
    """Belt-and-suspenders: helper must NOT return PRs without the label."""
    mod = _load_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_LABELED", "1")
    monkeypatch.setenv("GITHUB_TOKEN", "fake")

    def fake_gh(token, method, url, payload=None):
        return [
            _pr(1, labels=("auto-merge",), title="should merge"),
            _pr(2, labels=("dependencies",), title="should NOT merge"),
            _pr(3, labels=(), title="no labels at all"),
        ]
    monkeypatch.setattr(mod, "_gh", fake_gh)
    out = mod._list_labeled_open_prs("tok", "x/y", "auto-merge")
    assert [p["number"] for p in out] == [1]
