"""Tests for the V4 morning-briefing pipeline (Sprint 7, Phase B).

Covers:
  - `collect_context` runs without network: no Supabase creds, no git
    repo, still returns a well-formed dict.
  - `run_once` calls the mocked agent, posts to the mocked Discord
    webhook, and returns the expected fields.
"""

from __future__ import annotations

from pathlib import Path

import pytest


def test_collect_context_no_network(monkeypatch, tmp_path):
    """With no Supabase env + no git repo, context is empty-safe."""
    from operator_core import briefing as briefing_mod

    # Force the deploy fetch to return []; scrub SUPABASE_* env.
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)
    monkeypatch.delenv("NEXT_PUBLIC_SUPABASE_URL", raising=False)
    monkeypatch.delenv("NEXT_PUBLIC_SUPABASE_ANON_KEY", raising=False)

    class _FakeSettings:
        projects = []
        db_path = tmp_path / "does-not-exist.sqlite"

    ctx = briefing_mod.collect_context(_FakeSettings())
    assert ctx["projects"] == []
    assert ctx["deploys_last_5"] == []
    assert ctx["cost_24h_usd"] == 0.0
    assert ctx["jobs_24h"] == 0
    assert "generated_at" in ctx


def test_run_once_calls_agent_and_posts(monkeypatch, tmp_path):
    """Full pipeline with mocks: context collected, prompt built, agent
    called, Discord notify called, error=None, posted=True."""
    from operator_core import briefing as briefing_mod

    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SERVICE_ROLE_KEY", raising=False)

    class _FakeSettings:
        projects = []
        db_path = tmp_path / "empty.sqlite"

    agent_calls: list[dict] = []

    class _FakeResult:
        text = "Ship revenue panel. Focus on /kruz. Everything else can wait today."
        cost_usd = 0.0017
        error = None

    def fake_run_agent(prompt, *, system=None, model=None, max_turns=1, **kw):
        agent_calls.append({
            "prompt": prompt, "system": system, "model": model
        })
        return _FakeResult()

    posted_calls: list[dict] = []

    def fake_notify(**kwargs):
        posted_calls.append(kwargs)
        return True

    result = briefing_mod.run_once(
        settings=_FakeSettings(),
        run_agent_fn=fake_run_agent,
        notify_fn=fake_notify,
    )

    assert len(agent_calls) == 1
    assert agent_calls[0]["model"] == "claude-sonnet-4-6"
    assert "last 24 hours" in agent_calls[0]["prompt"].lower()

    assert len(posted_calls) == 1
    posted = posted_calls[0]
    assert posted["channel"] == "claude_chat"
    assert posted["title"] == "Morning brief"
    assert "revenue panel" in posted["body"]

    assert result["posted"] is True
    assert result["error"] is None
    assert result["cost_usd"] == pytest.approx(0.0017, rel=1e-3)
    assert "revenue panel" in result["text"]
