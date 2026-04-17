"""Discord command surface for Operator V3."""

from __future__ import annotations

import asyncio
import csv
import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .commands import CommandParseError, help_text, parse_operator_command
from .paths import PACKAGE_ROOT, SCHEDULER_STATE_PATH, STATUS_PATH
from .runner import JobRunner
from .scheduler import DEFAULT_TASKS, ScheduledTask
from .store import JobRecord, JobStore


# In-memory daemon start marker. Set on bot init so !op status can report uptime.
DAEMON_START_TS: datetime | None = None


class DiscordUnavailable(RuntimeError):
    """Raised when discord.py is not installed."""


@dataclass
class JobProgressHandle:
    """Tracks the Discord reply that should be edited as a job progresses."""

    channel_id: int
    message_id: int
    reply: Any  # discord.Message — kept untyped so tests can use stubs


class OperatorDiscordBot:
    def __init__(self, store: JobStore, runner: JobRunner):
        try:
            import discord
        except ImportError as exc:
            raise DiscordUnavailable("Install discord.py to enable the Discord bot") from exc

        global DAEMON_START_TS
        if DAEMON_START_TS is None:
            DAEMON_START_TS = datetime.now(timezone.utc)

        self.discord = discord
        self.store = store
        self.runner = runner
        self.owner_id = os.environ.get("OPERATOR_OWNER_DISCORD_ID", "")
        self._progress: dict[str, JobProgressHandle] = {}
        self._progress_lock = threading.Lock()
        intents = discord.Intents.default()
        intents.message_content = True
        self.client = discord.Client(intents=intents)
        try:
            from .discord_slash import register_slash_commands
            register_slash_commands(self.client, store=self.store, runner=self.runner)
        except Exception as exc:
            print(f"Slash command registration skipped: {exc}")
        self._wire_events()

    def _wire_events(self) -> None:
        @self.client.event
        async def on_ready():
            print(f"Operator V3 Discord bot logged in as {self.client.user}")
            await self._post_startup_notification()

        @self.client.event
        async def on_message(message):
            if message.author.bot:
                return
            if not message.content.strip().lower().startswith("!op"):
                return
            await self.handle_message(message)

    async def handle_message(self, message) -> None:
        """Public entry for tests — handles one inbound Discord message."""
        if self.owner_id and str(message.author.id) != self.owner_id:
            self._log_rejected_attempt(message)
            await message.reply("Operator V3 is owner-locked.")
            return

        try:
            parsed = parse_operator_command(message.content)
        except CommandParseError as exc:
            await message.reply(str(exc))
            return

        if parsed.action == "help":
            await message.reply(help_text())
            return
        if parsed.action == "status":
            await message.reply(build_status_payload(self.store))
            return
        if parsed.action == "jobs":
            await message.reply(_format_jobs(self.store.list_jobs()))
            return
        if parsed.action == "stop":
            self.store.update_job(parsed.job_id or "", status="cancel_requested")
            await message.reply(f"Cancel requested for `{parsed.job_id}`.")
            return
        if parsed.action == "approve":
            approvals = self.store.approve(parsed.job_id or "", str(message.author.id))
            await message.reply(f"Approval recorded for `{parsed.job_id}` ({approvals}/2).")
            return

        job_metadata: dict[str, Any] = {"source": "discord"}
        if parsed.args:
            job_metadata.update(parsed.args)
        job = self.store.create_job(
            parsed.action,
            parsed.prompt,
            parsed.project,
            metadata=job_metadata,
        )
        reply = await message.reply(
            f"Queued `{parsed.action}` as job `{job.id}` — status: queued."
        )
        self._track_progress(job.id, message, reply)

        loop = asyncio.get_running_loop()
        threading.Thread(
            target=self._run_with_progress,
            args=(job.id, loop),
            daemon=True,
        ).start()

    def _track_progress(self, job_id: str, message, reply) -> None:
        with self._progress_lock:
            self._progress[job_id] = JobProgressHandle(
                channel_id=getattr(message.channel, "id", 0),
                message_id=getattr(reply, "id", 0),
                reply=reply,
            )

    def _run_with_progress(self, job_id: str, loop: asyncio.AbstractEventLoop) -> None:
        self._schedule_edit(job_id, loop, f"Job `{job_id}`: started")
        try:
            self.runner.run(job_id)
        except Exception as exc:  # pragma: no cover — runner catches its own
            self._schedule_edit(job_id, loop, f"Job `{job_id}` crashed: {exc}")
            return

        try:
            final = self.store.get_job(job_id)
        except KeyError:
            return
        self._schedule_followup(job_id, loop, format_job_result(final))

    def _schedule_edit(self, job_id: str, loop, content: str) -> None:
        handle = self._progress.get(job_id)
        if handle is None:
            return
        self._dispatch(loop, handle.reply.edit(content=content))

    def _schedule_followup(self, job_id: str, loop, content: str) -> None:
        handle = self._progress.pop(job_id, None)
        if handle is None:
            return
        self._dispatch(loop, handle.reply.reply(content))

    @staticmethod
    def _dispatch(loop, value) -> None:
        if loop is None or loop.is_closed():
            if asyncio.iscoroutine(value):
                value.close()
            return
        if asyncio.iscoroutine(value):
            try:
                asyncio.run_coroutine_threadsafe(value, loop)
            except RuntimeError:
                value.close()

    def _log_rejected_attempt(self, message) -> None:
        try:
            self.store.create_job(
                "rejected_non_owner",
                prompt=message.content[:200],
                metadata={
                    "source": "discord",
                    "author_id": str(message.author.id),
                    "author_name": getattr(message.author, "name", ""),
                },
            )
            self.store.update_job(
                self.store.list_jobs(1)[0].id,
                status="rejected",
            )
        except Exception:
            pass

    async def _post_startup_notification(self) -> None:
        try:
            from .utils.discord import notify  # type: ignore
        except Exception:
            return
        start_ts = DAEMON_START_TS.isoformat() if DAEMON_START_TS else "unknown"
        try:
            notify(
                "automations",
                title="Operator V3 daemon online",
                body=f"Uptime tracking started at `{start_ts}`.",
                color="green",
                footer="operator-v3",
            )
        except Exception:
            pass

    def run(self, token: str) -> None:
        self.client.run(token)


def _format_jobs(jobs: list[JobRecord]) -> str:
    if not jobs:
        return "No jobs yet."
    lines = ["Recent Operator V3 jobs:"]
    for job in jobs[:10]:
        project = f" `{job.project}`" if job.project else ""
        lines.append(f"- `{job.id}` {job.action}{project}: {job.status}")
    return "\n".join(lines)


def format_job_result(job: JobRecord) -> str:
    """Build a short human-readable completion summary for a finished job."""
    headline = f"Job `{job.id}` {job.action} → **{job.status}**"
    extras: list[str] = []

    if job.action == "build":
        if job.pr_url:
            extras.append(f"PR: {job.pr_url}")
        if job.risk_tier:
            extras.append(f"risk: {job.risk_tier}")
        if job.deploy_result:
            extras.append(f"deploy: {job.deploy_result}")
    elif job.action == "deck_ag_market_pulse":
        latest = job.metadata.get("latest_deck") if isinstance(job.metadata, dict) else None
        if latest:
            extras.append(f"deck: {latest}")
    elif job.action == "deploy_check":
        failing = _extract_failing_services(job)
        if failing:
            extras.append("failing: " + ", ".join(failing))
        else:
            extras.append("all services green")

    elif job.action.startswith("pl_"):
        if isinstance(job.metadata, dict):
            verdict = job.metadata.get("verdict")
            if verdict:
                extras.append(verdict)
            factory = job.metadata.get("factory")
            if factory:
                extras.append(f"factory: {factory}")
            artifacts = job.metadata.get("artifacts")
            if isinstance(artifacts, list) and artifacts:
                extras.append(f"artifacts: {len(artifacts)}")
            next_act = job.metadata.get("next_action")
            if next_act and job.status != "complete":
                extras.append(f"next: {next_act}")

    if isinstance(job.metadata, dict):
        exit_code = job.metadata.get("exit_code")
        if exit_code not in (None, 0) and job.action != "build":
            extras.append(f"exit={exit_code}")

    if job.cost_usd:
        extras.append(f"cost ${job.cost_usd:.2f}")

    if not extras:
        return headline
    return headline + "\n" + " · ".join(extras)


def _extract_failing_services(job: JobRecord) -> list[str]:
    failing: list[str] = []
    if STATUS_PATH.exists():
        try:
            status = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return failing
        for name, info in (status.get("projects") or {}).items():
            if isinstance(info, dict) and info.get("status") in {"red", "yellow"}:
                failing.append(f"{name}:{info.get('status')}")
    return failing


def build_status_payload(
    store: JobStore,
    now: datetime | None = None,
    tasks: list[ScheduledTask] | None = None,
    scheduler_state_path: Path | None = None,
    status_path: Path | None = None,
    cost_log_path: Path | None = None,
    start_ts: datetime | None = None,
) -> str:
    """Build the !op status reply text. Pure function for easy testing."""
    now = now or datetime.now(timezone.utc)
    tasks = tasks if tasks is not None else list(DEFAULT_TASKS)
    scheduler_state_path = scheduler_state_path or SCHEDULER_STATE_PATH
    status_path = status_path or STATUS_PATH
    cost_log_path = cost_log_path or (PACKAGE_ROOT / "costs.csv")
    start_ts = start_ts or DAEMON_START_TS

    parts: list[str] = ["**Operator V3 status**"]

    try:
        from .remote import is_public_bind
        if is_public_bind():
            parts.append("⚠️ PUBLIC BIND: /remote/command is reachable from 0.0.0.0. Rotate `OPERATOR_REMOTE_SECRET` regularly.")
    except Exception:
        pass

    if start_ts is not None:
        uptime = now - start_ts
        parts.append(f"Uptime: {_format_duration(uptime.total_seconds())} (since {start_ts.isoformat()})")
    else:
        parts.append("Uptime: not tracked (daemon not started via bot)")

    scheduler_section = _format_scheduler_section(tasks, scheduler_state_path, now)
    parts.append(scheduler_section)

    deploy_section = _format_deploy_section(status_path)
    if deploy_section:
        parts.append(deploy_section)

    cost_today = _today_cost(cost_log_path, now)
    parts.append(f"Today's Claude cost: {cost_today}")

    jobs = store.list_jobs(5)
    if jobs:
        parts.append("Latest 5 jobs:")
        for job in jobs:
            duration = _job_duration(job)
            parts.append(
                f"- `{job.id}` {job.action}: {job.status} ({duration})"
            )
    else:
        parts.append("Latest 5 jobs: none yet")

    return "\n".join(parts)


def _format_scheduler_section(
    tasks: list[ScheduledTask],
    state_path: Path,
    now: datetime,
) -> str:
    state: dict[str, str] = {}
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            state = {}
    lines = ["Scheduler:"]
    for task in tasks:
        last = state.get(task.key, "never")
        next_run = _next_run_label(task, now)
        lines.append(f"- {task.key} ({task.cadence} @ {task.time_hhmm}): last={last} next={next_run}")
    return "\n".join(lines)


def _next_run_label(task: ScheduledTask, now: datetime) -> str:
    try:
        hour, minute = (int(p) for p in task.time_hhmm.split(":", 1))
    except ValueError:
        return "unknown"
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now:
        return f"tomorrow {task.time_hhmm}"
    return f"today {task.time_hhmm}"


def _format_deploy_section(status_path: Path) -> str | None:
    if not status_path.exists():
        return "Deploys: no status file yet"
    try:
        status = json.loads(status_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return "Deploys: status file not valid JSON"
    services = status.get("services") or {}
    last_updated = status.get("last_updated", "unknown")
    if services:
        return (
            "Deploys: "
            f"{services.get('green', 0)} green, "
            f"{services.get('yellow', 0)} yellow, "
            f"{services.get('red', 0)} red "
            f"(updated {last_updated})"
        )
    return None


def _today_cost(cost_log_path: Path, now: datetime) -> str:
    if not cost_log_path.exists():
        return "n/a"
    today = now.date().isoformat()
    total = 0.0
    found = False
    try:
        with open(cost_log_path, "r", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            for row in reader:
                if not row or len(row) < 3:
                    continue
                ts, _script, cost = row[0], row[1], row[2]
                if ts.startswith(today):
                    try:
                        total += float(cost)
                        found = True
                    except ValueError:
                        continue
    except OSError:
        return "n/a"
    return f"${total:.2f}" if found else "$0.00"


def _job_duration(job: JobRecord) -> str:
    try:
        created = datetime.fromisoformat(job.created_at)
        updated = datetime.fromisoformat(job.updated_at)
    except ValueError:
        return "?"
    return _format_duration((updated - created).total_seconds())


def _format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}h{minutes:02d}m"
