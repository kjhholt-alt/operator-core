"""Schedule loader for ``schedules/schedule.yaml``.

Replaces the per-script ``run-*.bat`` files. The schedule file lists each
recipe with a cron expression; ``operator schedule install`` registers each
as a Windows Task Scheduler task that invokes ``operator run <name>``.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger("operator.recipe.schedule")


@dataclass
class ScheduledRecipe:
    name: str
    cron: str
    enabled: bool = True
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "cron": self.cron, "enabled": self.enabled, "notes": self.notes}


@dataclass
class Schedule:
    version: int = 1
    recipes: list[ScheduledRecipe] = field(default_factory=list)

    def find(self, name: str) -> ScheduledRecipe | None:
        for r in self.recipes:
            if r.name == name:
                return r
        return None


# --- minimal YAML parser (avoids hard PyYAML dependency) ----------------------
#
# The schedule file is intentionally simple:
#
#   version: 1
#   recipes:
#     - name: morning_briefing
#       cron: "0 7 * * *"
#       enabled: true
#       notes: daily briefing
#
# We support exactly that shape. Anything more complicated belongs in code.

_KEY_RE = re.compile(r"^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:\s*(.*?)\s*$")


def _parse_value(raw: str) -> Any:
    raw = raw.strip()
    if not raw:
        return ""
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    if raw.startswith("'") and raw.endswith("'"):
        return raw[1:-1]
    if raw.lower() == "true":
        return True
    if raw.lower() == "false":
        return False
    if raw.lower() in {"null", "~"}:
        return None
    if raw.isdigit():
        return int(raw)
    return raw


def parse_schedule_yaml(text: str) -> Schedule:
    schedule = Schedule()
    current: dict[str, Any] | None = None
    in_recipes = False
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        stripped = line.lstrip()
        indent = len(line) - len(stripped)

        if indent == 0:
            in_recipes = False
            m = _KEY_RE.match(line)
            if not m:
                continue
            key, val = m.group(1), m.group(2)
            if key == "recipes":
                in_recipes = True
                continue
            if key == "version":
                schedule.version = int(_parse_value(val))
            continue

        if not in_recipes:
            continue

        # list item start: "- name: foo"
        if stripped.startswith("- "):
            if current is not None:
                schedule.recipes.append(_to_scheduled(current))
            current = {}
            stripped = stripped[2:].lstrip()
            m = _KEY_RE.match(stripped)
            if m:
                current[m.group(1)] = _parse_value(m.group(2))
            continue

        # continuation key inside the current item
        m = _KEY_RE.match(stripped)
        if m and current is not None:
            current[m.group(1)] = _parse_value(m.group(2))

    if current is not None:
        schedule.recipes.append(_to_scheduled(current))
    return schedule


def _to_scheduled(d: dict[str, Any]) -> ScheduledRecipe:
    return ScheduledRecipe(
        name=str(d.get("name", "")),
        cron=str(d.get("cron", "")),
        enabled=bool(d.get("enabled", True)),
        notes=str(d.get("notes", "")),
    )


def load_schedule(path: Path | str) -> Schedule:
    target = Path(path)
    if not target.exists():
        return Schedule()
    return parse_schedule_yaml(target.read_text(encoding="utf-8"))


# --- cron -> Windows Task Scheduler translation -------------------------------

def cron_to_schtasks(cron: str) -> list[str]:
    """Translate a cron expression to ``schtasks /Create`` flags.

    Supported patterns (the only ones we use today):
      "M H * * *"        -> /SC DAILY /ST HH:MM
      "M H * * 0-6"      -> /SC WEEKLY /D <days> /ST HH:MM
      "M H D * *"        -> /SC MONTHLY /D <day> /ST HH:MM
      "*/N * * * *"      -> /SC MINUTE /MO N

    Anything else returns ``["UNSUPPORTED", cron]`` so the installer can warn.
    """
    parts = cron.split()
    if len(parts) != 5:
        return ["UNSUPPORTED", cron]
    minute, hour, dom, month, dow = parts

    if minute.startswith("*/") and hour == "*" and dom == "*" and month == "*" and dow == "*":
        n = minute[2:]
        return ["/SC", "MINUTE", "/MO", n]

    # hourly with interval ("M */N * * *" -> /SC HOURLY /MO N /ST <minute past>)
    if (
        hour.startswith("*/")
        and dom == "*"
        and month == "*"
        and dow == "*"
        and minute.isdigit()
    ):
        n = hour[2:]
        return ["/SC", "HOURLY", "/MO", n, "/ST", f"00:{int(minute):02d}"]

    # daily
    if dom == "*" and month == "*" and dow == "*" and minute.isdigit() and hour.isdigit():
        return ["/SC", "DAILY", "/ST", f"{int(hour):02d}:{int(minute):02d}"]

    # weekly
    if dom == "*" and month == "*" and dow != "*" and minute.isdigit() and hour.isdigit():
        days = _cron_dow_to_schtasks(dow)
        return ["/SC", "WEEKLY", "/D", days, "/ST", f"{int(hour):02d}:{int(minute):02d}"]

    # monthly
    if dow == "*" and month == "*" and dom != "*" and minute.isdigit() and hour.isdigit():
        return ["/SC", "MONTHLY", "/D", dom, "/ST", f"{int(hour):02d}:{int(minute):02d}"]

    return ["UNSUPPORTED", cron]


_DOW_MAP = {"0": "SUN", "1": "MON", "2": "TUE", "3": "WED", "4": "THU", "5": "FRI", "6": "SAT", "7": "SUN"}


def _cron_dow_to_schtasks(dow: str) -> str:
    parts: list[str] = []
    for chunk in dow.split(","):
        if "-" in chunk:
            a, b = chunk.split("-", 1)
            try:
                ai, bi = int(a), int(b)
            except ValueError:
                continue
            for i in range(ai, bi + 1):
                parts.append(_DOW_MAP.get(str(i), ""))
        else:
            parts.append(_DOW_MAP.get(chunk, ""))
    return ",".join(p for p in parts if p)


def install_windows_tasks(schedule: Schedule, *, prefix: str = "operator-recipe-", dry_run: bool = False) -> list[dict[str, Any]]:
    """Register each enabled recipe as a Windows Task Scheduler task.

    Returns a list of result dicts for reporting; on dry-run nothing is
    actually executed but the planned commands are reported.
    """
    plans: list[dict[str, Any]] = []
    schtasks = shutil.which("schtasks") or "schtasks"

    for recipe in schedule.recipes:
        if not recipe.enabled:
            plans.append({"recipe": recipe.name, "skipped": "disabled"})
            continue
        flags = cron_to_schtasks(recipe.cron)
        if flags and flags[0] == "UNSUPPORTED":
            plans.append({"recipe": recipe.name, "error": f"unsupported cron: {recipe.cron}"})
            continue

        task_name = f"{prefix}{recipe.name}"
        # Use ``py`` per Kruz's preference (3.14 has SDK).
        cmd_str = f'py -m operator_core.cli recipe run {recipe.name}'
        argv = [
            schtasks, "/Create", "/F", "/TN", task_name,
            "/TR", cmd_str,
            *flags,
        ]
        plan = {"recipe": recipe.name, "task": task_name, "argv": argv}
        if dry_run:
            plan["dry_run"] = True
            plans.append(plan)
            continue
        try:
            res = subprocess.run(argv, capture_output=True, text=True, check=False)
            plan["returncode"] = res.returncode
            plan["stdout"] = res.stdout.strip()
            plan["stderr"] = res.stderr.strip()
        except OSError as exc:
            plan["error"] = str(exc)
        plans.append(plan)
    return plans


def list_windows_tasks(prefix: str = "operator-recipe-") -> list[str]:
    """Return registered task names matching ``prefix``."""
    schtasks = shutil.which("schtasks") or "schtasks"
    try:
        res = subprocess.run([schtasks, "/Query", "/FO", "CSV", "/NH"], capture_output=True, text=True, check=False)
    except OSError:
        return []
    names: list[str] = []
    for line in res.stdout.splitlines():
        if not line:
            continue
        # CSV lines: "TaskName","NextRunTime","Status"
        first = line.split(",", 1)[0].strip().strip('"')
        # schtasks puts "\TaskName" -- strip the leading backslash.
        first = first.lstrip("\\")
        if first.startswith(prefix):
            names.append(first)
    return names
