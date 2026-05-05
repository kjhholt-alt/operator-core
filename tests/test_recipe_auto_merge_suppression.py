"""Unit tests for the auto_merge_suppression recipe."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest

from operator_core.recipes import RecipeContext


def _load_recipe_module():
    repo_root = Path(__file__).resolve().parent.parent
    src = repo_root / "recipes" / "auto_merge_suppression.py"
    spec = importlib.util.spec_from_file_location("operator_recipes_auto_merge_suppression", src)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _ctx(**overrides):
    base = dict(
        recipe_name="auto_merge_suppression",
        correlation_id="test-corr",
        env={}, clients={}, cost_so_far=0.0,
        cost_budget_usd=0.0, dry_run=False,
    )
    base.update(overrides)
    return RecipeContext(**base)


def test_metadata():
    mod = _load_recipe_module()
    r = mod.AutoMergeSuppression()
    assert r.name == "auto_merge_suppression"
    assert r.schedule == "*/30 * * * *"
    assert "outreach" in r.tags


def test_quiet_when_feature_disabled(monkeypatch):
    mod = _load_recipe_module()
    monkeypatch.delenv("OPERATOR_AUTO_MERGE_SUPPRESSION_PR", raising=False)
    r = mod.AutoMergeSuppression()
    result = asyncio.run(r.query(_ctx()))
    assert result["enabled"] is False
    assert result["merged"] == []
    msg = asyncio.run(r.format(_ctx(), result))
    assert msg == ""


def test_quiet_when_no_token(monkeypatch):
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_SUPPRESSION_PR", "1")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    r = mod.AutoMergeSuppression()
    result = asyncio.run(r.query(_ctx()))
    assert result["enabled"] is True
    assert "GITHUB_TOKEN not set" in result["errors"][0]


def test_skips_non_auto_branches(monkeypatch):
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_SUPPRESSION_PR", "1")
    monkeypatch.setenv("GITHUB_TOKEN", "fake-tok")
    fake_prs = [
        {"number": 1, "title": "hand PR", "head": {"ref": "feat/manual", "sha": "deadbeef"},
         "changed_files": 1, "additions": 1, "deletions": 0},
    ]
    monkeypatch.setattr(mod, "_list_open_auto_prs", lambda *a, **kw: fake_prs)
    monkeypatch.setattr(mod, "_combined_status", lambda *a, **kw: "success")
    monkeypatch.setattr(mod, "_squash_merge", lambda *a, **kw: pytest.fail("must not merge non-auto branch"))

    r = mod.AutoMergeSuppression()
    result = asyncio.run(r.query(_ctx()))
    # _list_open_auto_prs filters by prefix in the real impl; we monkeypatched
    # it to return a non-auto branch on purpose. The defensive in-loop check
    # should still skip it.
    assert result["merged"] == []


def test_merges_when_ci_green(monkeypatch):
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_SUPPRESSION_PR", "1")
    monkeypatch.setenv("GITHUB_TOKEN", "fake-tok")

    fake_prs = [
        {"number": 42, "title": "auto-suppress: 5 names",
         "head": {"ref": "auto-suppress/20260505123456", "sha": "abc123"},
         "changed_files": 1, "additions": 5, "deletions": 0},
    ]
    merges_called = []
    comments_called = []

    monkeypatch.setattr(mod, "_list_open_auto_prs", lambda *a, **kw: fake_prs)
    monkeypatch.setattr(mod, "_combined_status", lambda *a, **kw: "success")
    monkeypatch.setattr(mod, "_post_comment", lambda t, r, n, b: comments_called.append((n, b)) or {})
    monkeypatch.setattr(mod, "_squash_merge", lambda t, r, n: merges_called.append(n) or {"merged": True})

    r = mod.AutoMergeSuppression()
    result = asyncio.run(r.query(_ctx()))
    assert merges_called == [42]
    assert comments_called == [(42, comments_called[0][1])]
    assert "Auto-merging" in comments_called[0][1]
    assert result["merged"][0]["pr"] == 42

    msg = asyncio.run(r.format(_ctx(), result))
    assert "merged PR #42" in msg


def test_skips_when_ci_pending(monkeypatch):
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_SUPPRESSION_PR", "1")
    monkeypatch.setenv("GITHUB_TOKEN", "fake-tok")
    fake_prs = [{"number": 7, "title": "auto-suppress: ...",
                 "head": {"ref": "auto-suppress/x", "sha": "x"},
                 "changed_files": 1, "additions": 1, "deletions": 0}]
    monkeypatch.setattr(mod, "_list_open_auto_prs", lambda *a, **kw: fake_prs)
    monkeypatch.setattr(mod, "_combined_status", lambda *a, **kw: "pending")
    monkeypatch.setattr(mod, "_squash_merge", lambda *a, **kw: pytest.fail("must not merge while pending"))
    r = mod.AutoMergeSuppression()
    result = asyncio.run(r.query(_ctx()))
    assert result["merged"] == []
    assert result["skipped"][0]["reason"] == "ci_pending"


def test_skips_empty_diff(monkeypatch):
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_SUPPRESSION_PR", "1")
    monkeypatch.setenv("GITHUB_TOKEN", "fake-tok")
    fake_prs = [{"number": 99, "title": "empty",
                 "head": {"ref": "auto-suppress/empty", "sha": "y"},
                 "changed_files": 0, "additions": 0, "deletions": 0}]
    monkeypatch.setattr(mod, "_list_open_auto_prs", lambda *a, **kw: fake_prs)
    monkeypatch.setattr(mod, "_combined_status", lambda *a, **kw: "success")
    monkeypatch.setattr(mod, "_squash_merge", lambda *a, **kw: pytest.fail("must not merge empty diff"))
    r = mod.AutoMergeSuppression()
    result = asyncio.run(r.query(_ctx()))
    assert result["skipped"][0]["reason"] == "empty_diff"


def test_dry_run_does_not_merge(monkeypatch):
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_AUTO_MERGE_SUPPRESSION_PR", "1")
    monkeypatch.setenv("GITHUB_TOKEN", "fake-tok")
    fake_prs = [{"number": 5, "title": "dr",
                 "head": {"ref": "auto-suppress/dr", "sha": "z"},
                 "changed_files": 1, "additions": 3, "deletions": 0}]
    monkeypatch.setattr(mod, "_list_open_auto_prs", lambda *a, **kw: fake_prs)
    monkeypatch.setattr(mod, "_combined_status", lambda *a, **kw: "success")
    monkeypatch.setattr(mod, "_squash_merge", lambda *a, **kw: pytest.fail("must not merge in dry run"))
    r = mod.AutoMergeSuppression()
    result = asyncio.run(r.query(_ctx(dry_run=True)))
    assert result["merged"] == [{"pr": 5, "title": "dr", "branch": "auto-suppress/dr", "dry_run": True}]
