"""Minimal local scheduler for the Morning Ops Loop.

Also exposes CRUD over a versioned `config/schedule.json` file so the
`!op schedule list/add/remove` Discord surface can read and write entries
without touching code. The CRUD surface is intentionally file-based — it
does not mutate the in-memory `DEFAULT_TASKS` tuple used by the
background MorningOpsScheduler, so one half can be refactored without
breaking the other.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .paths import CONFIG_DIR, SCHEDULER_STATE_PATH, ensure_data_dirs
from .runner import JobRunner
from .store import JobStore


SCHEDULE_CONFIG_PATH = CONFIG_DIR / "schedule.json"
SCHEDULE_CONFIG_VERSION = 1


@dataclass(frozen=True)
class ScheduledTask:
    key: str
    action: str
    time_hhmm: str
    cadence: str = "daily"
    project: str | None = None
    prompt: str = ""


DEFAULT_TASKS = [
    ScheduledTask("morning-briefing", "morning", "06:00"),
    ScheduledTask("pr-review", "review_prs", "06:10"),
    ScheduledTask("deploy-check", "deploy_check", "06:20"),
    ScheduledTask("marketing-pulse", "marketing_pulse", "06:30"),
    ScheduledTask("ag-market-pulse", "deck_ag_market_pulse", "06:40", cadence="monthly", project="ag-market-pulse"),
    ScheduledTask("cost-report", "cost_report", "21:00", cadence="weekly"),
]


class MorningOpsScheduler:
    def __init__(
        self,
        store: JobStore,
        runner: JobRunner,
        tasks: list[ScheduledTask] | None = None,
        state_path: Path = SCHEDULER_STATE_PATH,
    ):
        ensure_data_dirs()
        self.store = store
        self.runner = runner
        self.tasks = tasks or DEFAULT_TASKS
        self.state_path = state_path
        self._stop = threading.Event()

    def start_background(self) -> threading.Thread:
        thread = threading.Thread(target=self.run_forever, name="operator-v3-scheduler", daemon=True)
        thread.start()
        return thread

    def stop(self) -> None:
        self._stop.set()

    def run_forever(self) -> None:
        while not self._stop.is_set():
            self.tick()
            self._stop.wait(60)

    def tick(self, now: datetime | None = None) -> list[str]:
        now = now or datetime.now()
        state = self._load_state()
        launched: list[str] = []
        for task in self.tasks:
            if not self._due(task, now, state):
                continue
            job = self.store.create_job(task.action, prompt=task.prompt, project=task.project, metadata={"schedule": task.key})
            state[task.key] = self._period_key(task, now)
            self._save_state(state)
            launched.append(job.id)
            threading.Thread(target=self.runner.run, args=(job.id,), daemon=True).start()
        return launched

    def _due(self, task: ScheduledTask, now: datetime, state: dict[str, str]) -> bool:
        hour, minute = [int(part) for part in task.time_hhmm.split(":", 1)]
        if now.hour < hour or (now.hour == hour and now.minute < minute):
            return False
        period = self._period_key(task, now)
        return state.get(task.key) != period

    def _period_key(self, task: ScheduledTask, now: datetime) -> str:
        if task.cadence == "monthly":
            return now.strftime("%Y-%m")
        if task.cadence == "weekly":
            return now.strftime("%Y-W%U")
        return now.strftime("%Y-%m-%d")

    def _load_state(self) -> dict[str, str]:
        if not self.state_path.exists():
            return {}
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _save_state(self, state: dict[str, str]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


# ---------------------------------------------------------------------------
# Cron-parity CRUD (Queue D3)
# ---------------------------------------------------------------------------
#
# The Discord bot will wire these as `!op schedule list/add/remove` in a
# future 1-line change — see `discord_slash.py` for the slash-command mirror
# and the integration note in the V4 sprint report. None of these helpers
# touch the running scheduler; they only read and write the on-disk
# `config/schedule.json` file.


class ScheduleConfigError(ValueError):
    """Raised when the schedule config file is malformed or a CRUD call is invalid."""


def _empty_config() -> dict[str, Any]:
    return {"version": SCHEDULE_CONFIG_VERSION, "schedules": []}


def load_schedule_config(path: Path = SCHEDULE_CONFIG_PATH) -> dict[str, Any]:
    """Load `schedule.json`, returning the empty template if the file is missing.

    Validates the top-level shape and raises `ScheduleConfigError` for any
    structural problem. An older `version` is tolerated and upgraded in
    memory — on-disk upgrade happens on the next write.
    """
    if not path.exists():
        return _empty_config()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ScheduleConfigError(f"schedule.json is not valid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise ScheduleConfigError("schedule.json top-level must be an object")
    schedules = raw.get("schedules")
    if not isinstance(schedules, list):
        raise ScheduleConfigError("schedule.json 'schedules' must be a list")
    raw.setdefault("version", SCHEDULE_CONFIG_VERSION)
    return raw


def save_schedule_config(config: dict[str, Any], path: Path = SCHEDULE_CONFIG_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": SCHEDULE_CONFIG_VERSION,
        "schedules": list(config.get("schedules") or []),
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def list_schedules(path: Path = SCHEDULE_CONFIG_PATH) -> list[dict[str, Any]]:
    """Return the list of schedule entries currently on disk."""
    return list(load_schedule_config(path).get("schedules") or [])


def add_schedule(
    name: str,
    cron: str,
    command: str,
    path: Path = SCHEDULE_CONFIG_PATH,
) -> dict[str, Any]:
    """Add a new schedule entry. Raises on duplicate name or empty fields.

    `cron` is validated only as "non-empty" — we do not embed a full cron
    parser, the enforcing layer is the host scheduler (Windows Task
    Scheduler or the in-process MorningOpsScheduler).
    """
    if not name or not name.strip():
        raise ScheduleConfigError("schedule name is required")
    if not cron or not cron.strip():
        raise ScheduleConfigError("cron expression is required")
    if not command or not command.strip():
        raise ScheduleConfigError("command is required")

    config = load_schedule_config(path)
    schedules: list[dict[str, Any]] = list(config.get("schedules") or [])
    for entry in schedules:
        if isinstance(entry, dict) and entry.get("name") == name:
            raise ScheduleConfigError(f"schedule '{name}' already exists")

    new_entry = {"name": name.strip(), "cron": cron.strip(), "command": command.strip()}
    schedules.append(new_entry)
    config["schedules"] = schedules
    save_schedule_config(config, path)
    return new_entry


def remove_schedule(name: str, path: Path = SCHEDULE_CONFIG_PATH) -> bool:
    """Remove the named schedule. Returns True if something was removed."""
    if not name or not name.strip():
        raise ScheduleConfigError("schedule name is required")
    config = load_schedule_config(path)
    schedules: list[dict[str, Any]] = list(config.get("schedules") or [])
    filtered = [e for e in schedules if not (isinstance(e, dict) and e.get("name") == name)]
    if len(filtered) == len(schedules):
        return False
    config["schedules"] = filtered
    save_schedule_config(config, path)
    return True


def format_schedule_list(path: Path = SCHEDULE_CONFIG_PATH) -> str:
    """Human-readable rendering for `!op schedule list`."""
    entries = list_schedules(path)
    if not entries:
        return "No schedules configured."
    lines = ["**Operator V3 schedules**"]
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name", "?")
        cron = entry.get("cron", "?")
        command = entry.get("command", "?")
        lines.append(f"- `{name}` ({cron}) -> {command}")
    return "\n".join(lines)
