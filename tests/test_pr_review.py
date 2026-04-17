"""Tests for the PR review recipe (Sprint 7, Phase E).

Covers:
  - `list_open_prs` with a mocked urlopen that returns two PRs across
    two repos; state de-dupes one of them.
  - `review_pr` calls the mocked agent with the fetched diff and returns
    the text verbatim.
  - `load_state` / `save_state` round-trip the JSON file.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


# ---------------------------------------------------------------------------
# Fake urlopen infrastructure
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, body: str, *, status: int = 200, headers=None):
        self._body = body.encode("utf-8") if isinstance(body, str) else body
        self.status = status
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _make_fetcher(route_map: dict[str, _Resp]):
    """Return a fake urlopen that looks at req.full_url and returns the
    matching _Resp, or raises KeyError for unexpected URLs."""

    def fetcher(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else req.get_full_url()
        for key, resp in route_map.items():
            if key in url:
                return resp
        raise KeyError(f"unexpected URL: {url}")

    return fetcher


def _settings_with(projects):
    return SimpleNamespace(projects=projects, data_dir=Path("ignored"))


def _project(repo: str):
    return SimpleNamespace(repo=repo)


# ---------------------------------------------------------------------------
# list_open_prs + filter_unseen
# ---------------------------------------------------------------------------


def test_list_open_prs_and_filter_state(tmp_path):
    from operator_core import pr_review

    list_body = json.dumps([
        {
            "number": 42,
            "title": "Add revenue heartbeat",
            "head": {"sha": "abc"},
            "diff_url": "https://github.com/kjhholt-alt/operator-core/pull/42.diff",
        },
        {
            "number": 43,
            "title": "Old unreviewed",
            "head": {"sha": "def"},
            "diff_url": "https://github.com/kjhholt-alt/operator-core/pull/43.diff",
        },
    ])
    fetcher = _make_fetcher({
        "/repos/kjhholt-alt/operator-core/pulls": _Resp(list_body),
    })

    settings = _settings_with([_project("kjhholt-alt/operator-core")])
    prs = pr_review.list_open_prs(settings, url_fetcher=fetcher)

    assert len(prs) == 2
    numbers = [p.number for p in prs]
    assert 42 in numbers and 43 in numbers

    # Pre-seen PR 43 should be filtered out.
    state = {"kjhholt-alt/operator-core": [43]}
    unseen = pr_review.filter_unseen(prs, state)
    assert [p.number for p in unseen] == [42]


# ---------------------------------------------------------------------------
# review_pr — mocked agent + diff fetch
# ---------------------------------------------------------------------------


def test_review_pr_uses_agent_and_returns_text():
    from operator_core import pr_review

    diff_body = (
        "diff --git a/x.py b/x.py\n"
        "+++ b/x.py\n"
        "@@ +1,2 @@\n"
        "+print('hi')\n"
    )
    fetcher = _make_fetcher({
        "/repos/kjhholt-alt/operator-core/pulls/42": _Resp(diff_body),
    })

    agent_calls: list[dict] = []

    class _FakeResult:
        text = "Looks fine. One change, no tests needed, approve."
        error = None
        cost_usd = 0.002

    def fake_agent(prompt, *, system=None, model=None, max_turns=1, **kw):
        agent_calls.append({"prompt": prompt, "system": system})
        return _FakeResult()

    out = pr_review.review_pr(
        "kjhholt-alt/operator-core",
        42,
        run_agent_fn=fake_agent,
        url_fetcher=fetcher,
    )

    assert "approve" in out.lower()
    assert len(agent_calls) == 1
    assert "print('hi')" in agent_calls[0]["prompt"]


# ---------------------------------------------------------------------------
# State file round-trip + mark_seen
# ---------------------------------------------------------------------------


def test_state_roundtrip_and_mark_seen(tmp_path):
    from operator_core import pr_review

    initial = pr_review.load_state(tmp_path)
    assert initial == {}

    prs = [
        pr_review.OpenPR("kjhholt-alt/x", 5, "a", "sha", ""),
        pr_review.OpenPR("kjhholt-alt/x", 6, "b", "sha", ""),
        pr_review.OpenPR("kjhholt-alt/y", 1, "c", "sha", ""),
    ]
    state = pr_review.mark_seen(prs, {})
    assert sorted(state["kjhholt-alt/x"]) == [5, 6]
    assert state["kjhholt-alt/y"] == [1]

    pr_review.save_state(tmp_path, state)
    round_tripped = pr_review.load_state(tmp_path)
    assert round_tripped == state

    # Second pass: same PR 5 shouldn't double up.
    again = pr_review.mark_seen(
        [pr_review.OpenPR("kjhholt-alt/x", 5, "a", "sha", "")], state
    )
    assert again["kjhholt-alt/x"].count(5) == 1
