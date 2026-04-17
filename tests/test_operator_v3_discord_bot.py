"""Tests for operator_core.discord_bot — pure-unit, no live Discord connection."""

from __future__ import annotations

import asyncio
import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

# discord.py may not be installed in CI. Provide a minimal stub so import works.
if "discord" not in sys.modules:
    stub = types.ModuleType("discord")

    class _Intents:
        message_content = False

        @classmethod
        def default(cls):
            return cls()

    class _Client:
        def __init__(self, *args, **kwargs):
            self.user = "stub-bot"
            self._handlers = {}

        def event(self, fn):
            self._handlers[fn.__name__] = fn
            return fn

        def run(self, token):
            pass

    stub.Intents = _Intents
    stub.Client = _Client
    sys.modules["discord"] = stub


from operator_core import discord_bot
from operator_core.commands import help_text
from operator_core.discord_bot import (
    OperatorDiscordBot,
    build_status_payload,
    format_job_result,
)
from operator_core.scheduler import ScheduledTask
from operator_core.store import JobStore


def _make_message(content: str, author_id: str = "1"):
    message = MagicMock()
    message.content = content
    message.author.id = author_id
    message.author.bot = False
    message.author.name = "tester"
    message.channel.id = 42
    reply = MagicMock()
    reply.id = 7
    reply.edit = AsyncMock(return_value=None)
    reply.reply = AsyncMock(return_value=None)
    message.reply = AsyncMock(return_value=reply)
    return message, reply


def test_help_text_lists_every_command_and_safety_notes():
    text = help_text()
    for needle in [
        "!op status",
        "!op jobs",
        "!op help",
        "!op morning",
        "!op review prs",
        "!op build",
        "!op deploy check",
        "!op deck ag-market-pulse",
        "!op stop",
        "!op approve",
        "Auto-merge is OFF",
        "dry-run",
        "owner-locked",
    ]:
        assert needle in text, f"help_text missing: {needle}"


def test_build_status_payload_includes_uptime_scheduler_cost_jobs(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job = store.create_job("morning", project=None)
    store.update_job(job.id, status="complete")

    state_path = tmp_path / "scheduler.json"
    state_path.write_text(json.dumps({"morning-briefing": "2026-04-10"}), encoding="utf-8")

    status_path = tmp_path / "status.json"
    status_path.write_text(
        json.dumps(
            {
                "last_updated": "2026-04-10T06:00:00",
                "services": {"green": 3, "yellow": 1, "red": 0},
            }
        ),
        encoding="utf-8",
    )

    cost_path = tmp_path / "costs.csv"
    cost_path.write_text(
        "2026-04-10T06:01:00,morning-briefing,0.42,12,45,\n"
        "2026-04-10T06:05:00,pr-review,0.18,5,20,\n"
        "2026-04-09T06:00:00,stale,99.00,1,1,\n",
        encoding="utf-8",
    )

    tasks = [ScheduledTask("morning-briefing", "morning", "06:00")]
    now = datetime(2026, 4, 10, 12, 0, tzinfo=timezone.utc)
    start_ts = now - timedelta(hours=2, minutes=5)

    text = build_status_payload(
        store,
        now=now,
        tasks=tasks,
        scheduler_state_path=state_path,
        status_path=status_path,
        cost_log_path=cost_path,
        start_ts=start_ts,
    )

    assert "Operator V3 status" in text
    assert "Uptime: 2h05m" in text
    assert "morning-briefing" in text
    assert "last=2026-04-10" in text
    assert "Deploys:" in text and "3 green" in text
    assert "Today's Claude cost: $0.60" in text
    assert "Latest 5 jobs" in text
    assert job.id in text


def test_build_status_payload_handles_missing_artifacts(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    text = build_status_payload(
        store,
        now=datetime(2026, 4, 10, 8, 0, tzinfo=timezone.utc),
        tasks=[ScheduledTask("demo", "morning", "06:00")],
        scheduler_state_path=tmp_path / "nope.json",
        status_path=tmp_path / "nostatus.json",
        cost_log_path=tmp_path / "nocosts.csv",
        start_ts=None,
    )
    assert "Uptime: not tracked" in text
    assert "last=never" in text
    assert "no status file" in text
    assert "Today's Claude cost: n/a" in text
    assert "none yet" in text


def test_format_job_result_includes_pr_url_for_build(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job = store.create_job("build", prompt="test", project="deal-brain")
    updated = store.update_job(
        job.id,
        status="complete",
        pr_url="https://github.com/kjhholt-alt/deal-brain/pull/42",
        risk_tier="low",
        deploy_result="dry-run merge skipped",
    )
    text = format_job_result(updated)
    assert "build" in text
    assert "complete" in text
    assert "pull/42" in text
    assert "risk: low" in text
    assert "dry-run merge skipped" in text


def test_format_job_result_deck_surfaces_latest_path(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job = store.create_job("deck_ag_market_pulse", project="ag-market-pulse")
    updated = store.update_job(
        job.id,
        status="complete",
        metadata={"latest_deck": "C:/decks/ag-2026-04-10.pptx"},
    )
    text = format_job_result(updated)
    assert "ag-2026-04-10.pptx" in text


@pytest.fixture
def bot(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_OWNER_DISCORD_ID", "1")
    discord_bot.DAEMON_START_TS = None
    store = JobStore(tmp_path / "jobs.sqlite3")
    runner = MagicMock()
    b = OperatorDiscordBot(store=store, runner=runner)
    return b


def test_non_owner_rejected_and_logged(bot):
    message, _reply = _make_message("!op status", author_id="999")

    asyncio.run(bot.handle_message(message))

    message.reply.assert_awaited_once()
    call_args = message.reply.await_args.args[0]
    assert "owner-locked" in call_args
    jobs = bot.store.list_jobs()
    assert any(j.action == "rejected_non_owner" and j.status == "rejected" for j in jobs)


def test_help_command_sends_full_help(bot):
    message, _reply = _make_message("!op help", author_id="1")
    asyncio.run(bot.handle_message(message))
    message.reply.assert_awaited_once()
    payload = message.reply.await_args.args[0]
    assert "Operator V3 commands" in payload
    assert "dry-run" in payload


def test_status_command_uses_payload_builder(bot, tmp_path, monkeypatch):
    # operator-core's STATUS_PATH is a _LazyPath which lacks .read_text(); patch
    # it to a tmp file so build_status_payload can read a real file.
    status_path = tmp_path / "status.json"
    status_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(discord_bot, "STATUS_PATH", status_path)

    message, _reply = _make_message("!op status", author_id="1")
    asyncio.run(bot.handle_message(message))
    payload = message.reply.await_args.args[0]
    assert "Operator V3 status" in payload


def test_queued_command_creates_job_and_tracks_progress(bot, monkeypatch):
    message, reply = _make_message("!op morning", author_id="1")

    # Prevent the background runner thread from racing the event loop.
    monkeypatch.setattr(
        discord_bot.threading,
        "Thread",
        lambda *a, **kw: MagicMock(start=lambda: None),
    )

    async def scenario():
        await bot.handle_message(message)

    asyncio.run(scenario())

    # First reply is a "queued" message with job id
    message.reply.assert_awaited_once()
    first = message.reply.await_args.args[0]
    assert "Queued `morning`" in first
    assert "queued" in first

    jobs = bot.store.list_jobs()
    assert any(j.action == "morning" for j in jobs)
    # Progress handle should have been registered
    morning_jobs = [j for j in jobs if j.action == "morning"]
    assert morning_jobs
