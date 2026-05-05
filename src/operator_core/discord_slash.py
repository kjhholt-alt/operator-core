"""Slash command mirrors for Operator V3 (Queue D2, stretch).

Mirrors the top `!op` text commands as `/op <sub>` slash commands. This
module never imports discord.py at module scope — the import happens
inside `register_slash_commands` so the rest of the package stays
importable in test environments that do not have discord.py installed.

Future integration is a single line from `operator_v3/discord_bot.py`
inside `OperatorDiscordBot.__init__` (after `self.client = ...`):

    from .discord_slash import register_slash_commands
    register_slash_commands(self.client, store=self.store, runner=self.runner)

We intentionally ship a minimal V1: `/op status`, `/op morning`,
`/op deploy_check`. The rest of the `!op` surface (build, review prs,
deck, stop, approve, schedule list/add/remove) is deferred to a follow-up
commit so the stretch goal ships something working tonight.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable


# Exported so `discord_bot.py` (and tests) can iterate / introspect the
# command set without importing discord.py. Each entry is (name, help text,
# `!op`-equivalent text command) — the text command is what gets parsed and
# enqueued via the existing `parse_operator_command` path.
SLASH_COMMANDS: list[tuple[str, str, str]] = [
    ("status", "Daemon uptime, scheduler health, recent jobs, deploys, cost.", "!op status"),
    ("morning", "Run the morning briefing now.", "!op morning"),
    ("deploy_check", "Check health of the 4 SaaS apps.", "!op deploy check"),
    ("gate_review", "Show next pending Sender Gate disagreement (cut-over triage).", "!op gate-review"),
]


@dataclass(frozen=True)
class SlashRegistration:
    """Return value of `register_slash_commands` — names of registered commands."""

    command_names: tuple[str, ...]


class SlashUnavailable(RuntimeError):
    """Raised when discord.py is not installed."""


def _resolve_app_commands_tree(client: Any):
    """Locate the `app_commands.CommandTree` attached to the given client.

    Accepts either a real `discord.Client` (uses `.tree`) or a test double
    that already exposes a `tree` attribute. Returns the tree-like object
    that supports a `.command(name=..., description=...)` decorator.
    """
    tree = getattr(client, "tree", None)
    if tree is not None:
        return tree
    # Try to create one lazily for real discord.py clients that were not
    # constructed with an explicit tree (discord.Bot subclasses already
    # attach one).
    try:
        import discord  # type: ignore
    except ImportError as exc:  # pragma: no cover — exercised in real deploy
        raise SlashUnavailable("discord.py is not installed") from exc
    try:
        tree = discord.app_commands.CommandTree(client)
    except Exception as exc:  # pragma: no cover
        raise SlashUnavailable(f"could not build CommandTree: {exc}") from exc
    client.tree = tree
    return tree


def _make_callback(text_command: str, enqueue: Callable[[str], Any]) -> Callable[..., Any]:
    """Build the async callback a slash command registers.

    The callback delegates to `enqueue(text_command)` which performs the
    same `parse_operator_command` + `store.create_job` dance as the text
    bot. We keep it as a plain coroutine so tests can await it without a
    running event loop.
    """

    async def _callback(interaction: Any) -> None:
        try:
            result = enqueue(text_command)
        except Exception as exc:  # pragma: no cover — mocked in tests
            await interaction.response.send_message(f"Operator V3 error: {exc}")
            return
        message = f"Queued `{text_command}` ({result})" if result else f"Queued `{text_command}`"
        await interaction.response.send_message(message)

    return _callback


def default_enqueue_factory(store: Any, runner: Any | None = None) -> Callable[[str], str]:
    """Build an `enqueue(text_command) -> job_id` closure over a JobStore.

    `runner` is accepted for future parity with the text bot (which runs
    the job on a background thread) but is not required — the scheduler
    will pick up queued jobs on its next tick if nothing triggers them
    directly. The text-command bot runs jobs eagerly; a future commit can
    add the same eager behavior here once we decide how to share threads.
    """
    from .commands import parse_operator_command

    def _enqueue(text_command: str) -> str:
        parsed = parse_operator_command(text_command)
        job = store.create_job(
            parsed.action,
            parsed.prompt,
            parsed.project,
            metadata={"source": "discord_slash"},
        )
        return job.id

    return _enqueue


def register_slash_commands(
    client: Any,
    *,
    store: Any | None = None,
    runner: Any | None = None,
    enqueue: Callable[[str], Any] | None = None,
    commands: Iterable[tuple[str, str, str]] | None = None,
) -> SlashRegistration:
    """Attach Operator slash commands to the given discord client/bot.

    Either `store` or a custom `enqueue(text_command)` callable must be
    provided — the former is used by production wiring, the latter by
    tests that want to assert on the exact text command routed through.
    """
    if enqueue is None:
        if store is None:
            raise ValueError("register_slash_commands needs either enqueue or store")
        enqueue = default_enqueue_factory(store, runner)

    tree = _resolve_app_commands_tree(client)
    entries = list(commands) if commands is not None else list(SLASH_COMMANDS)
    registered: list[str] = []

    for name, description, text_command in entries:
        callback = _make_callback(text_command, enqueue)
        decorator = tree.command(name=name, description=description)
        decorator(callback)
        registered.append(name)

    return SlashRegistration(command_names=tuple(registered))


__all__ = [
    "SLASH_COMMANDS",
    "SlashRegistration",
    "SlashUnavailable",
    "default_enqueue_factory",
    "register_slash_commands",
]
