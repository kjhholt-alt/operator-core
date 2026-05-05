"""Schedule loader for ``schedules/schedule.yaml``.

Replaces the per-script ``run-*.bat`` files. The schedule file lists each
recipe with a cron expression; ``operator schedule install`` registers each
recipe with the host scheduler:

  * Windows -- ``schtasks`` (Task Scheduler) via ``install_windows_tasks``.
  * macOS   -- ``launchctl`` LaunchAgents under ``~/Library/LaunchAgents``.
  * Linux   -- systemd-timer units under ``~/.config/systemd/user``.

The cross-platform dispatch lives in :func:`install_tasks` /
:func:`uninstall_tasks` / :func:`list_installed_tasks`. The Windows-specific
helpers stay public so existing tests + callers keep working.
"""

from __future__ import annotations

import logging
import os
import platform
import re
import shutil
import subprocess
import sys
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

    # every hour at fixed minute ("M * * * *" -> /SC HOURLY /MO 1 /ST 00:M)
    if (
        hour == "*"
        and dom == "*"
        and month == "*"
        and dow == "*"
        and minute.isdigit()
    ):
        return ["/SC", "HOURLY", "/MO", "1", "/ST", f"00:{int(minute):02d}"]

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


def uninstall_windows_tasks(prefix: str = "operator-recipe-", *, dry_run: bool = False) -> list[dict[str, Any]]:
    """Delete every registered ``operator-recipe-*`` task. Returns per-task results."""
    schtasks = shutil.which("schtasks") or "schtasks"
    out: list[dict[str, Any]] = []
    for name in list_windows_tasks(prefix):
        argv = [schtasks, "/Delete", "/F", "/TN", name]
        if dry_run:
            out.append({"task": name, "dry_run": True, "argv": argv})
            continue
        try:
            res = subprocess.run(argv, capture_output=True, text=True, check=False)
            out.append({"task": name, "returncode": res.returncode, "stdout": res.stdout.strip(), "stderr": res.stderr.strip()})
        except OSError as exc:
            out.append({"task": name, "error": str(exc)})
    return out


# ============================================================================
# Cross-platform host detection + dispatch
# ============================================================================
#
# Why the dispatch lives here instead of shelling out to a `cron`-shaped
# fallback everywhere: each host has a *better* native primitive (launchd
# tracks plist state, systemd-timer survives reboots cleanly, schtasks owns
# the Windows event log integration). Falling back to crontab loses the
# per-host signal that "operator is registered". So we do the right thing
# per host, and only use crontab as a manual escape hatch.

HOST_WINDOWS = "windows"
HOST_MACOS = "macos"
HOST_LINUX = "linux"

ScheduleHost = str  # Literal[HOST_WINDOWS, HOST_MACOS, HOST_LINUX]


def detect_host() -> ScheduleHost:
    """Return the host scheduler family for the current process.

    Override with ``OPERATOR_SCHEDULER_HOST`` (``windows`` | ``macos`` |
    ``linux``) to force-test cross-platform code paths.
    """
    forced = os.environ.get("OPERATOR_SCHEDULER_HOST", "").strip().lower()
    if forced in {HOST_WINDOWS, HOST_MACOS, HOST_LINUX}:
        return forced
    if sys.platform.startswith("win"):
        return HOST_WINDOWS
    if sys.platform == "darwin":
        return HOST_MACOS
    return HOST_LINUX


# --- macOS launchd ------------------------------------------------------------

def cron_to_launchd(cron: str) -> dict[str, Any] | None:
    """Translate a cron expression to a launchd ``StartCalendarInterval`` dict.

    launchd does not support */N intervals natively; for those we synthesize
    a ``StartInterval`` (seconds) instead by computing the period.

    Returns ``None`` for unsupported cron shapes.
    """
    parts = cron.split()
    if len(parts) != 5:
        return None
    minute, hour, dom, month, dow = parts

    # */N minutes -> StartInterval (seconds)
    if minute.startswith("*/") and hour == "*" and dom == "*" and month == "*" and dow == "*":
        try:
            n = int(minute[2:])
        except ValueError:
            return None
        return {"StartInterval": n * 60}

    # */N hours -> StartInterval
    if (
        hour.startswith("*/")
        and dom == "*"
        and month == "*"
        and dow == "*"
        and minute.isdigit()
    ):
        try:
            n = int(hour[2:])
        except ValueError:
            return None
        return {"StartInterval": n * 3600}

    # hourly at fixed minute ("M * * * *" -> StartInterval=3600 starting on the minute)
    if (
        hour == "*"
        and dom == "*"
        and month == "*"
        and dow == "*"
        and minute.isdigit()
    ):
        return {"StartCalendarInterval": {"Minute": int(minute)}}

    interval: dict[str, Any] = {}
    if minute.isdigit():
        interval["Minute"] = int(minute)
    if hour.isdigit():
        interval["Hour"] = int(hour)
    if dom.isdigit():
        interval["Day"] = int(dom)
    if month.isdigit():
        interval["Month"] = int(month)
    if dow.isdigit():
        interval["Weekday"] = int(dow)

    # Need at least one calendar field to be meaningful.
    if not interval:
        return None
    return {"StartCalendarInterval": interval}


def _plist_xml(label: str, program_argv: list[str], schedule_block: dict[str, Any]) -> str:
    """Render a minimal launchd plist for one operator recipe."""
    args_xml = "\n".join(f"        <string>{_xml_escape(a)}</string>" for a in program_argv)

    lines: list[str] = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
        '<plist version="1.0">',
        '<dict>',
        '    <key>Label</key>',
        f'    <string>{_xml_escape(label)}</string>',
        '    <key>ProgramArguments</key>',
        '    <array>',
        args_xml,
        '    </array>',
        '    <key>RunAtLoad</key>',
        '    <false/>',
    ]

    if "StartInterval" in schedule_block:
        lines.append('    <key>StartInterval</key>')
        lines.append(f'    <integer>{int(schedule_block["StartInterval"])}</integer>')
    elif "StartCalendarInterval" in schedule_block:
        cal = schedule_block["StartCalendarInterval"]
        lines.append('    <key>StartCalendarInterval</key>')
        lines.append('    <dict>')
        for k, v in cal.items():
            lines.append(f'        <key>{k}</key>')
            lines.append(f'        <integer>{int(v)}</integer>')
        lines.append('    </dict>')

    lines.append('</dict>')
    lines.append('</plist>')
    return "\n".join(lines) + "\n"


def _xml_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def install_macos_tasks(
    schedule: Schedule,
    *,
    prefix: str = "dev.operator.recipe.",
    dry_run: bool = False,
    agents_dir: Path | None = None,
    program: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Write launchd plists for each enabled recipe and load them."""
    home = Path.home()
    target_dir = agents_dir or (home / "Library" / "LaunchAgents")
    program = program or [sys.executable, "-m", "operator_core.cli", "recipe", "run"]

    plans: list[dict[str, Any]] = []
    for recipe in schedule.recipes:
        if not recipe.enabled:
            plans.append({"recipe": recipe.name, "skipped": "disabled"})
            continue
        sched = cron_to_launchd(recipe.cron)
        if sched is None:
            plans.append({"recipe": recipe.name, "error": f"unsupported cron: {recipe.cron}"})
            continue
        label = f"{prefix}{recipe.name}"
        plist_path = target_dir / f"{label}.plist"
        plist_text = _plist_xml(label, [*program, recipe.name], sched)
        plan: dict[str, Any] = {"recipe": recipe.name, "label": label, "plist": str(plist_path)}
        if dry_run:
            plan["dry_run"] = True
            plan["plist_text"] = plist_text
            plans.append(plan)
            continue
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            plist_path.write_text(plist_text, encoding="utf-8")
            launchctl = shutil.which("launchctl") or "launchctl"
            # bootout first (ignore errors — fresh installs won't have it loaded).
            subprocess.run([launchctl, "bootout", f"gui/{os.getuid()}/{label}"], capture_output=True, text=True, check=False)
            res = subprocess.run(
                [launchctl, "bootstrap", f"gui/{os.getuid()}", str(plist_path)],
                capture_output=True, text=True, check=False,
            )
            plan["returncode"] = res.returncode
            plan["stderr"] = res.stderr.strip()
        except OSError as exc:
            plan["error"] = str(exc)
        plans.append(plan)
    return plans


def list_macos_tasks(prefix: str = "dev.operator.recipe.", *, agents_dir: Path | None = None) -> list[str]:
    target_dir = agents_dir or (Path.home() / "Library" / "LaunchAgents")
    if not target_dir.exists():
        return []
    return sorted(
        p.stem for p in target_dir.glob(f"{prefix}*.plist")
    )


def uninstall_macos_tasks(
    prefix: str = "dev.operator.recipe.",
    *,
    dry_run: bool = False,
    agents_dir: Path | None = None,
) -> list[dict[str, Any]]:
    target_dir = agents_dir or (Path.home() / "Library" / "LaunchAgents")
    if not target_dir.exists():
        return []
    out: list[dict[str, Any]] = []
    launchctl = shutil.which("launchctl") or "launchctl"
    for plist_path in sorted(target_dir.glob(f"{prefix}*.plist")):
        label = plist_path.stem
        plan: dict[str, Any] = {"task": label, "plist": str(plist_path)}
        if dry_run:
            plan["dry_run"] = True
            out.append(plan)
            continue
        try:
            subprocess.run(
                [launchctl, "bootout", f"gui/{os.getuid()}/{label}"],
                capture_output=True, text=True, check=False,
            )
            plist_path.unlink(missing_ok=True)
            plan["removed"] = True
        except OSError as exc:
            plan["error"] = str(exc)
        out.append(plan)
    return out


# --- Linux systemd-timer ------------------------------------------------------

def cron_to_systemd_oncalendar(cron: str) -> str | None:
    """Translate a cron expression to a systemd ``OnCalendar=`` value.

    Supported shapes mirror ``cron_to_schtasks``. Returns ``None`` for
    unsupported shapes (caller should fall back or report).
    """
    parts = cron.split()
    if len(parts) != 5:
        return None
    minute, hour, dom, month, dow = parts

    # */N minutes
    if minute.startswith("*/") and hour == "*" and dom == "*" and month == "*" and dow == "*":
        return f"*:0/{minute[2:]}"

    # */N hours, fixed minute
    if (
        hour.startswith("*/")
        and dom == "*"
        and month == "*"
        and dow == "*"
        and minute.isdigit()
    ):
        return f"*-*-* 0/{hour[2:]}:{int(minute):02d}:00"

    # hourly at fixed minute ("M * * * *")
    if (
        hour == "*"
        and dom == "*"
        and month == "*"
        and dow == "*"
        and minute.isdigit()
    ):
        return f"*:{int(minute):02d}:00"

    # daily H:M
    if dom == "*" and month == "*" and dow == "*" and minute.isdigit() and hour.isdigit():
        return f"*-*-* {int(hour):02d}:{int(minute):02d}:00"

    # weekly: cron 0=Sun..6=Sat -> systemd Sun..Sat
    if dom == "*" and month == "*" and dow != "*" and minute.isdigit() and hour.isdigit():
        days = _cron_dow_to_systemd(dow)
        if not days:
            return None
        return f"{days} *-*-* {int(hour):02d}:{int(minute):02d}:00"

    # monthly: specific day
    if dow == "*" and month == "*" and dom.isdigit() and minute.isdigit() and hour.isdigit():
        return f"*-*-{int(dom):02d} {int(hour):02d}:{int(minute):02d}:00"

    return None


_DOW_SYSTEMD = {"0": "Sun", "1": "Mon", "2": "Tue", "3": "Wed", "4": "Thu", "5": "Fri", "6": "Sat", "7": "Sun"}


def _cron_dow_to_systemd(dow: str) -> str:
    parts: list[str] = []
    for chunk in dow.split(","):
        if "-" in chunk:
            a, b = chunk.split("-", 1)
            try:
                ai, bi = int(a), int(b)
            except ValueError:
                continue
            for i in range(ai, bi + 1):
                v = _DOW_SYSTEMD.get(str(i))
                if v and v not in parts:
                    parts.append(v)
        else:
            v = _DOW_SYSTEMD.get(chunk)
            if v and v not in parts:
                parts.append(v)
    return ",".join(parts)


def _systemd_unit_text(*, recipe: ScheduledRecipe, on_calendar: str, program_argv: list[str]) -> tuple[str, str]:
    exec_start = " ".join(_systemd_quote(a) for a in [*program_argv, recipe.name])
    service = (
        "[Unit]\n"
        f"Description=Operator recipe: {recipe.name}\n\n"
        "[Service]\n"
        "Type=oneshot\n"
        f"ExecStart={exec_start}\n"
    )
    timer = (
        "[Unit]\n"
        f"Description=Timer for operator recipe {recipe.name}\n\n"
        "[Timer]\n"
        f"OnCalendar={on_calendar}\n"
        "Persistent=true\n\n"
        "[Install]\n"
        "WantedBy=timers.target\n"
    )
    return service, timer


def _systemd_quote(arg: str) -> str:
    if any(ch.isspace() for ch in arg) or '"' in arg:
        return '"' + arg.replace('"', '\\"') + '"'
    return arg


def install_linux_tasks(
    schedule: Schedule,
    *,
    prefix: str = "operator-recipe-",
    dry_run: bool = False,
    units_dir: Path | None = None,
    program: list[str] | None = None,
) -> list[dict[str, Any]]:
    target_dir = units_dir or (Path.home() / ".config" / "systemd" / "user")
    program = program or [sys.executable, "-m", "operator_core.cli", "recipe", "run"]

    plans: list[dict[str, Any]] = []
    for recipe in schedule.recipes:
        if not recipe.enabled:
            plans.append({"recipe": recipe.name, "skipped": "disabled"})
            continue
        on_cal = cron_to_systemd_oncalendar(recipe.cron)
        if on_cal is None:
            plans.append({"recipe": recipe.name, "error": f"unsupported cron: {recipe.cron}"})
            continue
        unit_name = f"{prefix}{recipe.name}"
        service_path = target_dir / f"{unit_name}.service"
        timer_path = target_dir / f"{unit_name}.timer"
        service_text, timer_text = _systemd_unit_text(
            recipe=recipe, on_calendar=on_cal, program_argv=program,
        )
        plan: dict[str, Any] = {
            "recipe": recipe.name,
            "unit": unit_name,
            "service_path": str(service_path),
            "timer_path": str(timer_path),
            "on_calendar": on_cal,
        }
        if dry_run:
            plan["dry_run"] = True
            plan["service_text"] = service_text
            plan["timer_text"] = timer_text
            plans.append(plan)
            continue
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            service_path.write_text(service_text, encoding="utf-8")
            timer_path.write_text(timer_text, encoding="utf-8")
            systemctl = shutil.which("systemctl") or "systemctl"
            subprocess.run([systemctl, "--user", "daemon-reload"], capture_output=True, text=True, check=False)
            res = subprocess.run(
                [systemctl, "--user", "enable", "--now", f"{unit_name}.timer"],
                capture_output=True, text=True, check=False,
            )
            plan["returncode"] = res.returncode
            plan["stderr"] = res.stderr.strip()
        except OSError as exc:
            plan["error"] = str(exc)
        plans.append(plan)
    return plans


def list_linux_tasks(prefix: str = "operator-recipe-", *, units_dir: Path | None = None) -> list[str]:
    target_dir = units_dir or (Path.home() / ".config" / "systemd" / "user")
    if not target_dir.exists():
        return []
    return sorted({p.stem for p in target_dir.glob(f"{prefix}*.timer")})


def uninstall_linux_tasks(
    prefix: str = "operator-recipe-",
    *,
    dry_run: bool = False,
    units_dir: Path | None = None,
) -> list[dict[str, Any]]:
    target_dir = units_dir or (Path.home() / ".config" / "systemd" / "user")
    if not target_dir.exists():
        return []
    systemctl = shutil.which("systemctl") or "systemctl"
    out: list[dict[str, Any]] = []
    for timer_path in sorted(target_dir.glob(f"{prefix}*.timer")):
        unit = timer_path.stem
        service_path = target_dir / f"{unit}.service"
        plan: dict[str, Any] = {"task": unit, "timer_path": str(timer_path)}
        if dry_run:
            plan["dry_run"] = True
            out.append(plan)
            continue
        try:
            subprocess.run([systemctl, "--user", "disable", "--now", f"{unit}.timer"], capture_output=True, text=True, check=False)
            timer_path.unlink(missing_ok=True)
            service_path.unlink(missing_ok=True)
            subprocess.run([systemctl, "--user", "daemon-reload"], capture_output=True, text=True, check=False)
            plan["removed"] = True
        except OSError as exc:
            plan["error"] = str(exc)
        out.append(plan)
    return out


# --- top-level dispatch -------------------------------------------------------

def install_tasks(
    schedule: Schedule,
    *,
    host: ScheduleHost | None = None,
    dry_run: bool = False,
    prefix: str | None = None,
) -> list[dict[str, Any]]:
    """Install all enabled recipes on the current host scheduler."""
    h = host or detect_host()
    if h == HOST_WINDOWS:
        return install_windows_tasks(schedule, prefix=prefix or "operator-recipe-", dry_run=dry_run)
    if h == HOST_MACOS:
        return install_macos_tasks(schedule, prefix=prefix or "dev.operator.recipe.", dry_run=dry_run)
    if h == HOST_LINUX:
        return install_linux_tasks(schedule, prefix=prefix or "operator-recipe-", dry_run=dry_run)
    raise ValueError(f"unknown scheduler host: {h!r}")


def uninstall_tasks(
    *,
    host: ScheduleHost | None = None,
    dry_run: bool = False,
    prefix: str | None = None,
) -> list[dict[str, Any]]:
    h = host or detect_host()
    if h == HOST_WINDOWS:
        return uninstall_windows_tasks(prefix=prefix or "operator-recipe-", dry_run=dry_run)
    if h == HOST_MACOS:
        return uninstall_macos_tasks(prefix=prefix or "dev.operator.recipe.", dry_run=dry_run)
    if h == HOST_LINUX:
        return uninstall_linux_tasks(prefix=prefix or "operator-recipe-", dry_run=dry_run)
    raise ValueError(f"unknown scheduler host: {h!r}")


def list_installed_tasks(*, host: ScheduleHost | None = None, prefix: str | None = None) -> list[str]:
    h = host or detect_host()
    if h == HOST_WINDOWS:
        return list_windows_tasks(prefix or "operator-recipe-")
    if h == HOST_MACOS:
        return list_macos_tasks(prefix or "dev.operator.recipe.")
    if h == HOST_LINUX:
        return list_linux_tasks(prefix or "operator-recipe-")
    raise ValueError(f"unknown scheduler host: {h!r}")


def status_report(
    schedule: Schedule,
    *,
    host: ScheduleHost | None = None,
    prefix: str | None = None,
) -> dict[str, Any]:
    """Return a compact dict for ``operator schedule status``: which recipes
    in the schedule are currently registered with the host scheduler, and which
    aren't."""
    h = host or detect_host()
    installed = set(list_installed_tasks(host=h, prefix=prefix))
    if h == HOST_MACOS:
        eff_prefix = prefix or "dev.operator.recipe."
    else:
        eff_prefix = prefix or "operator-recipe-"

    rows: list[dict[str, Any]] = []
    for r in schedule.recipes:
        task_name = f"{eff_prefix}{r.name}"
        rows.append({
            "recipe": r.name,
            "cron": r.cron,
            "enabled": r.enabled,
            "task": task_name,
            "installed": task_name in installed,
        })
    orphans = sorted(installed - {f"{eff_prefix}{r.name}" for r in schedule.recipes})
    return {
        "host": h,
        "prefix": eff_prefix,
        "rows": rows,
        "orphans": orphans,
    }
