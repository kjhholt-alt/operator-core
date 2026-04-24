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
    description: str = ""


DEFAULT_TASKS = [
    ScheduledTask(
        "morning-briefing", "morning", "06:00",
        description="Cross-project morning briefing - pipeline status, PRs, deploys",
    ),
    ScheduledTask(
        "pr-review", "review_prs", "06:10",
        description="Auto-review open PRs across all tracked repos",
    ),
    ScheduledTask(
        "deploy-check", "deploy_check", "06:20",
        description="Ping every project deploy URL and flag anything non-200",
    ),
    ScheduledTask(
        "marketing-pulse", "marketing_pulse", "06:30",
        description="Daily marketing metrics + outreach pipeline report",
    ),
    ScheduledTask(
        "lead-digest", "lead_digest", "06:45",
        description="Signup-first lead queue sync + follow-up digest metrics",
    ),
    ScheduledTask(
        "ag-market-pulse", "deck_ag_market_pulse", "06:40",
        cadence="monthly", project="ag-market-pulse",
        description="Monthly ag market intel deck (PPTX + email)",
    ),
    ScheduledTask(
        "cost-report", "cost_report", "21:00", cadence="weekly",
        description="Weekly Claude / infra spend breakdown",
    ),
    ScheduledTask(
        "nightly-demand-plan", "nightly_demand_plan", "21:05",
        description="Nightly signup-first plan, experiment registry, and follow-up queue",
    ),
    ScheduledTask(
        "demand-review", "demand_review", "21:15", cadence="weekly",
        description="Weekly portfolio demand scoreboard + experiment backlog",
    ),
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
            if is_task_disabled(task.key):
                continue
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


# ---------------------------------------------------------------------------
# Enable / disable per-task toggle (layered on top of DEFAULT_TASKS)
# ---------------------------------------------------------------------------
#
# Disabled task keys live in `schedule.json` under top-level "disabled": [].
# MorningOpsScheduler.tick() skips tasks whose key appears in that set.
# This lets `operator tasks disable morning-briefing` stop the daemon
# from launching a job without editing source or touching Task Scheduler.


def _load_disabled(path: Path = SCHEDULE_CONFIG_PATH) -> set[str]:
    cfg = load_schedule_config(path)
    raw = cfg.get("disabled") or []
    if not isinstance(raw, list):
        return set()
    return {str(k) for k in raw}


def _save_disabled(disabled: set[str], path: Path = SCHEDULE_CONFIG_PATH) -> None:
    cfg = load_schedule_config(path)
    payload = {
        "version": SCHEDULE_CONFIG_VERSION,
        "schedules": list(cfg.get("schedules") or []),
        "disabled": sorted(disabled),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def is_task_disabled(key: str, path: Path = SCHEDULE_CONFIG_PATH) -> bool:
    """Return True if the given DEFAULT_TASKS key is user-disabled."""
    return key in _load_disabled(path)


def disable_task(key: str, path: Path = SCHEDULE_CONFIG_PATH) -> bool:
    """Disable a task. Returns True if the state changed."""
    disabled = _load_disabled(path)
    if key in disabled:
        return False
    disabled.add(key)
    _save_disabled(disabled, path)
    return True


def enable_task(key: str, path: Path = SCHEDULE_CONFIG_PATH) -> bool:
    """Enable a task. Returns True if the state changed."""
    disabled = _load_disabled(path)
    if key not in disabled:
        return False
    disabled.remove(key)
    _save_disabled(disabled, path)
    return True


def list_all_tasks(
    state_path: Path = SCHEDULER_STATE_PATH,
    config_path: Path = SCHEDULE_CONFIG_PATH,
) -> list[dict[str, Any]]:
    """Merge DEFAULT_TASKS + custom schedules into one user-facing list.

    Each entry:
      { "key", "action", "time", "cadence", "project", "description",
        "enabled", "last_run" (period key or None), "kind": "builtin"|"custom" }
    """
    disabled = _load_disabled(config_path)

    # Last-run state for built-ins lives in scheduler-state.json.
    try:
        state_raw = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    except json.JSONDecodeError:
        state_raw = {}

    out: list[dict[str, Any]] = []
    for t in DEFAULT_TASKS:
        out.append({
            "key": t.key,
            "action": t.action,
            "time": t.time_hhmm,
            "cadence": t.cadence,
            "project": t.project,
            "description": t.description,
            "enabled": t.key not in disabled,
            "last_run": state_raw.get(t.key),
            "kind": "builtin",
        })

    for entry in list_schedules(config_path):
        if not isinstance(entry, dict):
            continue
        name = entry.get("name", "?")
        out.append({
            "key": name,
            "action": entry.get("command", "?"),
            "time": entry.get("cron", "?"),
            "cadence": "custom",
            "project": None,
            "description": entry.get("description", ""),
            "enabled": name not in disabled,
            "last_run": state_raw.get(name),
            "kind": "custom",
        })

    return out


def find_task(key: str) -> ScheduledTask | None:
    """Look up a DEFAULT_TASK by key."""
    for t in DEFAULT_TASKS:
        if t.key == key:
            return t
    return None
