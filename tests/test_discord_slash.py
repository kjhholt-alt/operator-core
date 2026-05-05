"""Tests for the /op slash command mirrors (Queue D2 stretch).

These tests never touch the real discord.py runtime. They use simple
doubles that mimic the `tree.command(name=..., description=...)` decorator
pattern from `discord.app_commands.CommandTree`.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from operator_core.discord_slash import (
    SLASH_COMMANDS,
    SlashRegistration,
    default_enqueue_factory,
    register_slash_commands,
)
from operator_core.store import JobStore


@dataclass
class FakeTree:
    """Stand-in for discord.app_commands.CommandTree."""

    registered: dict[str, Any] = field(default_factory=dict)
    last_description: dict[str, str] = field(default_factory=dict)

    def command(self, *, name: str, description: str):
        def decorator(callback):
            self.registered[name] = callback
            self.last_description[name] = description
            return callback
        return decorator


@dataclass
class FakeBot:
    """Stand-in for discord.Client / discord.Bot."""

    tree: FakeTree = field(default_factory=FakeTree)


@dataclass
class FakeInteractionResponse:
    sent: list[str] = field(default_factory=list)

    async def send_message(self, content: str) -> None:
        self.sent.append(content)


@dataclass
class FakeInteraction:
    response: FakeInteractionResponse = field(default_factory=FakeInteractionResponse)


def test_register_adds_expected_command_names():
    bot = FakeBot()
    captured: list[str] = []

    reg = register_slash_commands(bot, enqueue=lambda cmd: captured.append(cmd) or "job-1")

    assert isinstance(reg, SlashRegistration)
    assert set(reg.command_names) == {"status", "morning", "deploy_check", "gate_review"}
    assert set(bot.tree.registered.keys()) == {"status", "morning", "deploy_check", "gate_review"}


def test_descriptions_are_non_empty():
    bot = FakeBot()
    register_slash_commands(bot, enqueue=lambda cmd: "job")
    for name, description in bot.tree.last_description.items():
        assert description, f"slash command {name} has empty description"


def test_status_callback_routes_to_op_status():
    bot = FakeBot()
    captured: list[str] = []
    register_slash_commands(bot, enqueue=lambda cmd: (captured.append(cmd), "job-7")[1])

    callback = bot.tree.registered["status"]
    interaction = FakeInteraction()
    asyncio.run(callback(interaction))

    assert captured == ["!op status"]
    assert len(interaction.response.sent) == 1
    assert "!op status" in interaction.response.sent[0]
    assert "job-7" in interaction.response.sent[0]


def test_morning_callback_routes_to_op_morning():
    bot = FakeBot()
    captured: list[str] = []
    register_slash_commands(bot, enqueue=lambda cmd: captured.append(cmd) or "job-x")

    callback = bot.tree.registered["morning"]
    asyncio.run(callback(FakeInteraction()))
    assert captured == ["!op morning"]


def test_deploy_check_callback_routes_to_op_deploy_check():
    bot = FakeBot()
    captured: list[str] = []
    register_slash_commands(bot, enqueue=lambda cmd: captured.append(cmd) or "job-y")

    callback = bot.tree.registered["deploy_check"]
    asyncio.run(callback(FakeInteraction()))
    assert captured == ["!op deploy check"]


def test_default_enqueue_factory_creates_real_jobs(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    enqueue = default_enqueue_factory(store)

    bot = FakeBot()
    register_slash_commands(bot, enqueue=enqueue)

    callback = bot.tree.registered["status"]
    asyncio.run(callback(FakeInteraction()))

    jobs = store.list_jobs(5)
    assert len(jobs) == 1
    assert jobs[0].action == "status"
    assert jobs[0].metadata.get("source") == "discord_slash"


def test_register_without_store_or_enqueue_raises():
    bot = FakeBot()
    with pytest.raises(ValueError):
        register_slash_commands(bot)


def test_slash_commands_constant_matches_shipped_set():
    """Catch silent additions/removals of slash commands.

    Shipped: status, morning, deploy_check + gate_review (Reply Copilot v2).
    Adjust this set when adding a new slash command intentionally.
    """
    names = {entry[0] for entry in SLASH_COMMANDS}
    assert names == {"status", "morning", "deploy_check", "gate_review"}


def test_register_with_custom_command_subset():
    bot = FakeBot()
    register_slash_commands(
        bot,
        enqueue=lambda cmd: "job-id",
        commands=[("status", "only status", "!op status")],
    )
    assert list(bot.tree.registered.keys()) == ["status"]
