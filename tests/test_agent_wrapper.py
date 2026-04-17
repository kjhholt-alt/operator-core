"""Tests for the claude-agent-sdk wrapper in operator_core.agent.

Covers:
  - Missing SDK: returns AgentResult with error and 0 cost, no crash.
  - Happy path with mocked `claude_agent_sdk.query`: text stitched from
    TextBlocks, tokens + cost captured from ResultMessage.
  - Ledger row gets written with `action="agent.run"` and correct cost.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import pytest


def _isolated_store(tmp_path: Path):
    from operator_core.store import JobStore

    return JobStore(db_path=tmp_path / "jobs.sqlite")


def test_agent_missing_sdk_returns_error(tmp_path, monkeypatch):
    """When claude_agent_sdk is unimportable, we return an error result."""
    from operator_core import agent as agent_mod

    # Force `import claude_agent_sdk` inside run_agent to raise ImportError.
    original_import = __builtins__["__import__"] if isinstance(
        __builtins__, dict
    ) else __import__

    def fake_import(name, *args, **kwargs):
        if name == "claude_agent_sdk" or name.startswith("claude_agent_sdk."):
            raise ImportError("mocked missing SDK")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)

    store = _isolated_store(tmp_path)
    result = agent_mod.run_agent(
        "hello", system="be brief", model="claude-sonnet-4-6", store=store
    )

    assert result.ok is False
    assert "not installed" in (result.error or "")
    assert result.cost_usd == 0.0
    assert result.duration_ms >= 0

    # Ledger got a failed row for agent.run.
    jobs = store.list_jobs()
    assert any(j.action == "agent.run" and j.status == "failed" for j in jobs)


def test_agent_happy_path_captures_text_and_cost(tmp_path, monkeypatch):
    """Mock the SDK query; check text, tokens, cost, and ledger row."""
    from operator_core import agent as agent_mod

    # Build fake sdk objects first.
    class _FakeText:
        def __init__(self, text):
            self.text = text

    class _FakeAssistant:
        def __init__(self, texts, model="claude-sonnet-4-6"):
            self.content = [_FakeText(t) for t in texts]
            self.model = model

    class _FakeResult:
        def __init__(self, cost, inp, out):
            self.total_cost_usd = cost
            self.usage = {
                "input_tokens": inp,
                "output_tokens": out,
                "cache_read_input_tokens": 0,
            }
            self.is_error = False
            self.result = None
            self.stop_reason = "end_turn"

    class _FakeOptions:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    async def fake_query(*, prompt, options, transport=None):
        yield _FakeAssistant(["one move for today: ship revenue panel."])
        yield _FakeResult(cost=0.0031, inp=120, out=48)

    fake_mod = mock.MagicMock()
    fake_mod.query = fake_query
    fake_mod.AssistantMessage = _FakeAssistant
    fake_mod.ResultMessage = _FakeResult
    fake_mod.TextBlock = _FakeText
    fake_mod.ClaudeAgentOptions = _FakeOptions

    monkeypatch.setitem(sys.modules, "claude_agent_sdk", fake_mod)

    store = _isolated_store(tmp_path)
    result = agent_mod.run_agent(
        "brief me",
        system="you are the operator",
        model="claude-sonnet-4-6",
        store=store,
    )

    assert result.ok
    assert "ship revenue panel" in result.text
    assert result.input_tokens == 120
    assert result.output_tokens == 48
    assert result.cost_usd == pytest.approx(0.0031, rel=1e-3)
    assert result.duration_ms >= 0

    jobs = store.list_jobs()
    agent_jobs = [j for j in jobs if j.action == "agent.run"]
    assert len(agent_jobs) == 1
    assert agent_jobs[0].status == "complete"
    assert agent_jobs[0].cost_usd == pytest.approx(0.0031, rel=1e-3)
    assert agent_jobs[0].metadata.get("model") == "claude-sonnet-4-6"


def test_agent_cost_cap_marks_error(tmp_path, monkeypatch):
    """If total cost exceeds the cap, result is flagged as an error."""
    from operator_core import agent as agent_mod

    class _FakeText:
        def __init__(self, text):
            self.text = text

    class _FakeAssistant:
        def __init__(self):
            self.content = [_FakeText("expensive")]
            self.model = "claude-opus-4-7"

    class _FakeResult:
        def __init__(self):
            self.total_cost_usd = 1.25
            self.usage = {"input_tokens": 5000, "output_tokens": 1000}
            self.is_error = False
            self.result = None
            self.stop_reason = "end_turn"

    class _FakeOptions:
        def __init__(self, **kwargs):
            pass

    async def fake_query(*, prompt, options, transport=None):
        yield _FakeAssistant()
        yield _FakeResult()

    fake_mod = mock.MagicMock()
    fake_mod.query = fake_query
    fake_mod.AssistantMessage = _FakeAssistant
    fake_mod.ResultMessage = _FakeResult
    fake_mod.TextBlock = _FakeText
    fake_mod.ClaudeAgentOptions = _FakeOptions

    monkeypatch.setitem(sys.modules, "claude_agent_sdk", fake_mod)

    store = _isolated_store(tmp_path)
    result = agent_mod.run_agent(
        "go big",
        model="claude-opus-4-7",
        max_cost_usd=0.10,
        store=store,
    )

    assert result.ok is False
    assert "cost cap exceeded" in (result.error or "")
    assert result.cost_usd == pytest.approx(1.25)

    agent_jobs = [j for j in store.list_jobs() if j.action == "agent.run"]
    assert agent_jobs and agent_jobs[0].status == "failed"
