"""Tests for the weekly operator review generator."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from operator_core import review
from operator_core.review import (
    REQUIRED_SECTIONS,
    gather_cost_delta,
    gather_hook_blocks,
    gather_job_stats,
    generate_weekly_review,
)
from operator_core.store import JobStore
from operator_core.utils import status as status_mod


NOW = datetime(2026, 4, 11, 18, 0, tzinfo=timezone.utc)


def _seed_jobs(store: JobStore) -> None:
    j1 = store.create_job("morning", project="operator-ai")
    store.update_job(j1.id, status="done")
    j2 = store.create_job("deploy_check", project="dealbrain")
    store.update_job(j2.id, status="failed")
    old = store.create_job("ancient", project="archived")
    with store._connect() as conn:
        conn.execute(
            "UPDATE jobs SET created_at=?, updated_at=? WHERE id=?",
            ("2026-03-20T06:00:00+00:00", "2026-03-20T06:00:00+00:00", old.id),
        )


def _seed_hooks(path, cutoff_iso: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"ts": "2026-04-10T06:00:00+00:00", "blocked": True, "reason": "destructive", "tool_name": "Bash"},
        {"ts": "2026-04-10T07:00:00+00:00", "blocked": False, "reason": None, "tool_name": "Bash"},
        {"ts": "2026-02-01T00:00:00+00:00", "blocked": True, "reason": "old", "tool_name": "Bash"},
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")


def _seed_costs(path):
    path.write_text(
        "2026-04-09T06:00:00+00:00,morning,0.40,1,1,\n"
        "2026-04-10T06:00:00+00:00,deploy,0.10,1,1,\n"
        "2026-04-02T06:00:00+00:00,morning,0.20,1,1,\n"
        "2026-03-01T06:00:00+00:00,old,9.99,1,1,\n"
        "bad,row,nope,1,1,\n",
        encoding="utf-8",
    )


def test_gather_job_stats_window_only(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    _seed_jobs(store)
    cutoff = NOW - timedelta(days=7)
    stats = gather_job_stats(store, cutoff)
    assert stats["total"] == 2
    assert stats["by_status"].get("done") == 1
    assert stats["by_status"].get("failed") == 1
    assert len(stats["failed"]) == 1
    assert stats["failed"][0]["action"] == "deploy_check"


def test_gather_hook_blocks_respects_cutoff(tmp_path):
    log = tmp_path / "hooks.jsonl"
    _seed_hooks(log, "")
    cutoff = NOW - timedelta(days=7)
    blocks = gather_hook_blocks(log, cutoff)
    assert len(blocks) == 1
    assert blocks[0]["reason"] == "destructive"


def test_gather_cost_delta_computes_prior_vs_window(tmp_path):
    path = tmp_path / "costs.csv"
    _seed_costs(path)
    cutoff = NOW - timedelta(days=7)
    delta = gather_cost_delta(path, cutoff)
    assert round(delta["window_total"], 2) == 0.50
    assert round(delta["prior_window_total"], 2) == 0.20
    assert round(delta["delta"], 2) == 0.30


def test_generate_weekly_review_with_fake_client(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    _seed_jobs(store)
    hooks_log = tmp_path / "hooks.jsonl"
    _seed_hooks(hooks_log, "")
    costs = tmp_path / "costs.csv"
    _seed_costs(costs)
    status_path = tmp_path / "status.json"
    status_mod.set_deploy_health("operator-ai", "ok", status_path)
    reviews_dir = tmp_path / "reviews"

    canned = (
        "# Operator Weekly Review\n\n"
        "## What ran\n- operator-ai morning x1\n\n"
        "## What failed\n- dealbrain deploy_check\n\n"
        "## Cost delta\n- +$0.30\n\n"
        "## Top 3 recommendations\n- Fix dealbrain\n- Watch costs\n- Keep shipping\n"
    )
    calls: list[str] = []

    def fake_client(prompt: str) -> str:
        calls.append(prompt)
        return canned

    result = generate_weekly_review(
        now=NOW,
        claude_client=fake_client,
        store=store,
        status_path=status_path,
        costs_csv=costs,
        hooks_log=hooks_log,
        reviews_dir=reviews_dir,
    )

    assert result["path"].exists()
    md = result["path"].read_text(encoding="utf-8")
    for section in REQUIRED_SECTIONS:
        assert section in md
    assert "dealbrain" in md
    assert len(calls) == 1
    assert "Jobs total: 2" in calls[0]
    assert "operator-ai" in calls[0]
    assert result["week"].startswith("2026-W")


def test_generate_weekly_review_patches_missing_sections(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    reviews_dir = tmp_path / "reviews"

    def partial_client(_prompt: str) -> str:
        return "# Operator Weekly Review\n\n## What ran\n- nothing\n"

    result = generate_weekly_review(
        now=NOW,
        claude_client=partial_client,
        store=store,
        status_path=tmp_path / "status.json",
        costs_csv=tmp_path / "missing.csv",
        hooks_log=tmp_path / "missing-hooks.jsonl",
        reviews_dir=reviews_dir,
    )
    md = result["markdown"]
    for section in REQUIRED_SECTIONS:
        assert section in md


def test_generate_weekly_review_offline_fallback(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    _seed_jobs(store)
    result = generate_weekly_review(
        now=NOW,
        claude_client=None,
        store=store,
        status_path=tmp_path / "status.json",
        costs_csv=tmp_path / "missing.csv",
        hooks_log=tmp_path / "missing.jsonl",
        reviews_dir=tmp_path / "reviews",
    )
    md = result["markdown"]
    for section in REQUIRED_SECTIONS:
        assert section in md
    assert "Total jobs: 2" in md


def test_cli_week_invokes_generator_without_network(tmp_path, monkeypatch):
    captured: dict = {}

    def fake_gen():
        path = tmp_path / "review.md"
        path.write_text("# Operator Weekly Review\n", encoding="utf-8")
        return {"path": path, "markdown": "# Operator Weekly Review\n", "week": "2026-W15"}

    monkeypatch.setattr(review, "generate_weekly_review", lambda: fake_gen())
    monkeypatch.setattr(review, "_post_to_discord", lambda md, week: captured.setdefault("posted", (md, week)) or True)

    rc = review._cli(["week"])
    assert rc == 0
    assert captured["posted"][1] == "2026-W15"


def test_cli_rejects_unknown_subcommand():
    assert review._cli([]) == 2
    assert review._cli(["wat"]) == 2


# --- weekly_review recipe -----------------------------------------------------

def test_weekly_review_classifies_and_sorts_auto_merged_prs(tmp_path, monkeypatch):
    import asyncio
    from unittest.mock import MagicMock

    from recipes import weekly_review as wr

    monkeypatch.setattr(wr, "WEEKLY_REVIEW_HTML", tmp_path / "weekly-review.html")
    monkeypatch.setattr(wr, "WEEKLY_REVIEW_JSON", tmp_path / "weekly-review.json")

    search_rows = {
        "items": [
        {
            "number": 1,
            "title": "small reviewed",
            "repository_url": "https://api.github.com/repos/kjhholt-alt/status-spec",
            "closed_at": "2026-05-05T10:00:00Z",
            "html_url": "https://example/pr/1",
        },
        {
            "number": 2,
            "title": "large autonomous merge",
            "repository_url": "https://api.github.com/repos/kjhholt-alt/operator-core",
            "closed_at": "2026-05-06T10:00:00Z",
            "html_url": "https://example/pr/2",
        },
        {
            "number": 3,
            "title": "medium autonomous merge",
            "repository_url": "https://api.github.com/repos/kjhholt-alt/portfolio",
            "closed_at": "2026-05-04T10:00:00Z",
            "html_url": "https://example/pr/3",
        },
        ]
    }

    def fake_run_gh(args, *, timeout=60.0):
        if args[:2] == ["api", "search/issues"]:
            return 0, json.dumps(search_rows), ""
        if args[:2] == ["api", "repos/kjhholt-alt/status-spec/pulls/1"]:
            return 0, json.dumps({"additions": 10, "deletions": 2, "changed_files": 1, "merged_at": "2026-05-05T10:00:00Z"}), ""
        if args[:2] == ["api", "repos/kjhholt-alt/operator-core/pulls/2"]:
            return 0, json.dumps({"additions": 500, "deletions": 100, "changed_files": 12, "merged_at": "2026-05-06T10:00:00Z"}), ""
        if args[:2] == ["api", "repos/kjhholt-alt/portfolio/pulls/3"]:
            return 0, json.dumps({"additions": 100, "deletions": 50, "changed_files": 3, "merged_at": "2026-05-04T10:00:00Z"}), ""
        if args[0] == "api" and "status-spec/pulls/1/reviews" in args[1]:
            return 0, json.dumps([{"user": {"login": "reviewer", "type": "User"}}]), ""
        if args[0] == "api":
            return 0, "[]", ""
        raise AssertionError(args)

    monkeypatch.setattr(wr, "_run_gh", fake_run_gh)
    recipe = wr.WeeklyReview()
    ctx = MagicMock()
    ctx.logger = MagicMock()
    data = asyncio.run(recipe.query(ctx))
    result = asyncio.run(recipe.analyze(ctx, data))
    body = asyncio.run(recipe.format(ctx, result))

    assert result["total"] == 3
    assert [pr["number"] for pr in result["auto_merged"]] == [2, 3]
    assert [pr["number"] for pr in result["human_reviewed"]] == [1]
    assert "large autonomous merge" in body
    assert wr.WEEKLY_REVIEW_HTML.exists()
    assert wr.WEEKLY_REVIEW_JSON.exists()
    assert "Largest auto-merged PRs" in wr.WEEKLY_REVIEW_HTML.read_text(encoding="utf-8")


def test_weekly_review_query_tolerates_gh_failure(monkeypatch):
    import asyncio
    from unittest.mock import MagicMock

    from recipes import weekly_review as wr

    monkeypatch.setattr(wr, "_run_gh", lambda *a, **kw: (1, "", "offline"))
    recipe = wr.WeeklyReview()
    ctx = MagicMock()
    ctx.logger = MagicMock()
    data = asyncio.run(recipe.query(ctx))
    result = asyncio.run(recipe.analyze(ctx, data))
    assert result["total"] == 0
    assert result["auto_merged"] == []
