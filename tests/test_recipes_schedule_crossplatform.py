"""Tests for the cross-platform schedule installer (cycle 2).

The Windows-only ``cron_to_schtasks`` + ``install_windows_tasks`` paths
are covered in test_recipes_framework.py. This file covers the new
launchd / systemd-timer / dispatch surface added so the same
``operator schedule install`` works on Mac and Linux home machines.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from operator_core.recipes.schedule import (
    HOST_LINUX,
    HOST_MACOS,
    HOST_WINDOWS,
    Schedule,
    ScheduledRecipe,
    cron_to_launchd,
    cron_to_systemd_oncalendar,
    detect_host,
    install_linux_tasks,
    install_macos_tasks,
    install_tasks,
    list_installed_tasks,
    list_linux_tasks,
    list_macos_tasks,
    status_report,
    uninstall_linux_tasks,
    uninstall_macos_tasks,
    uninstall_tasks,
)


# --- detect_host -------------------------------------------------------------

class TestDetectHost:
    def test_env_override_windows(self, monkeypatch):
        monkeypatch.setenv("OPERATOR_SCHEDULER_HOST", "windows")
        assert detect_host() == HOST_WINDOWS

    def test_env_override_macos(self, monkeypatch):
        monkeypatch.setenv("OPERATOR_SCHEDULER_HOST", "macos")
        assert detect_host() == HOST_MACOS

    def test_env_override_linux(self, monkeypatch):
        monkeypatch.setenv("OPERATOR_SCHEDULER_HOST", "linux")
        assert detect_host() == HOST_LINUX

    def test_env_override_invalid_falls_back(self, monkeypatch):
        monkeypatch.setenv("OPERATOR_SCHEDULER_HOST", "bogus")
        assert detect_host() in {HOST_WINDOWS, HOST_MACOS, HOST_LINUX}


# --- cron -> launchd ---------------------------------------------------------

class TestCronToLaunchd:
    def test_daily(self):
        assert cron_to_launchd("0 7 * * *") == {"StartCalendarInterval": {"Minute": 0, "Hour": 7}}

    def test_minute_interval_uses_start_interval(self):
        assert cron_to_launchd("*/15 * * * *") == {"StartInterval": 900}

    def test_hourly_interval(self):
        assert cron_to_launchd("0 */4 * * *") == {"StartInterval": 14400}

    def test_weekly_specific_dow(self):
        # cron 0=Sun, launchd 0=Sun -> Weekday=1 for Mon
        out = cron_to_launchd("30 9 * * 1")
        assert out == {"StartCalendarInterval": {"Minute": 30, "Hour": 9, "Weekday": 1}}

    def test_unsupported_returns_none(self):
        assert cron_to_launchd("not a cron") is None
        assert cron_to_launchd("0 0 0 0 0 0") is None


# --- cron -> systemd OnCalendar ----------------------------------------------

class TestCronToSystemd:
    def test_daily(self):
        assert cron_to_systemd_oncalendar("0 7 * * *") == "*-*-* 07:00:00"

    def test_minute_interval(self):
        assert cron_to_systemd_oncalendar("*/5 * * * *") == "*:0/5"

    def test_hourly_interval(self):
        assert cron_to_systemd_oncalendar("0 */6 * * *") == "*-*-* 0/6:00:00"

    def test_weekly_range(self):
        # Mon-Fri 09:00
        out = cron_to_systemd_oncalendar("0 9 * * 1-5")
        assert out == "Mon,Tue,Wed,Thu,Fri *-*-* 09:00:00"

    def test_monthly_dom(self):
        assert cron_to_systemd_oncalendar("0 0 15 * *") == "*-*-15 00:00:00"

    def test_unsupported(self):
        assert cron_to_systemd_oncalendar("not cron") is None


# --- install_macos_tasks (dry_run, no launchctl) -----------------------------

def _sample_schedule() -> Schedule:
    return Schedule(
        version=1,
        recipes=[
            ScheduledRecipe(name="alpha", cron="0 7 * * *", enabled=True, notes="daily morning"),
            ScheduledRecipe(name="beta", cron="*/30 * * * *", enabled=True, notes="every 30m"),
            ScheduledRecipe(name="gamma", cron="0 0 * * *", enabled=False, notes="disabled"),
            ScheduledRecipe(name="bad", cron="garbage", enabled=True, notes="unsupported"),
        ],
    )


class TestInstallMacos:
    def test_dry_run_renders_plists_for_enabled(self, tmp_path):
        plans = install_macos_tasks(_sample_schedule(), dry_run=True, agents_dir=tmp_path)
        recipes = {p["recipe"]: p for p in plans}

        assert recipes["alpha"]["dry_run"] is True
        assert "<key>StartCalendarInterval</key>" in recipes["alpha"]["plist_text"]
        assert "<key>Hour</key>" in recipes["alpha"]["plist_text"]

        assert recipes["beta"]["dry_run"] is True
        assert "<key>StartInterval</key>" in recipes["beta"]["plist_text"]
        assert "<integer>1800</integer>" in recipes["beta"]["plist_text"]

        assert recipes["gamma"]["skipped"] == "disabled"
        assert "unsupported cron" in recipes["bad"]["error"]

    def test_dry_run_plist_label_uses_prefix(self, tmp_path):
        plans = install_macos_tasks(_sample_schedule(), dry_run=True, agents_dir=tmp_path, prefix="x.test.")
        alpha = next(p for p in plans if p["recipe"] == "alpha")
        assert alpha["label"] == "x.test.alpha"
        assert alpha["plist"].endswith("x.test.alpha.plist")

    def test_list_and_uninstall_dry_run(self, tmp_path):
        # Pre-create a couple of plist files to simulate installed tasks.
        (tmp_path / "dev.operator.recipe.alpha.plist").write_text("<plist/>", encoding="utf-8")
        (tmp_path / "dev.operator.recipe.beta.plist").write_text("<plist/>", encoding="utf-8")
        (tmp_path / "unrelated.plist").write_text("<plist/>", encoding="utf-8")

        names = list_macos_tasks(agents_dir=tmp_path)
        assert names == ["dev.operator.recipe.alpha", "dev.operator.recipe.beta"]

        plans = uninstall_macos_tasks(dry_run=True, agents_dir=tmp_path)
        assert {p["task"] for p in plans} == {"dev.operator.recipe.alpha", "dev.operator.recipe.beta"}
        # Files still present (dry run).
        assert (tmp_path / "dev.operator.recipe.alpha.plist").exists()


# --- install_linux_tasks (dry_run, no systemctl) -----------------------------

class TestInstallLinux:
    def test_dry_run_writes_unit_text(self, tmp_path):
        plans = install_linux_tasks(_sample_schedule(), dry_run=True, units_dir=tmp_path)
        recipes = {p["recipe"]: p for p in plans}

        assert recipes["alpha"]["dry_run"] is True
        assert recipes["alpha"]["on_calendar"] == "*-*-* 07:00:00"
        assert "OnCalendar=*-*-* 07:00:00" in recipes["alpha"]["timer_text"]
        assert "[Service]" in recipes["alpha"]["service_text"]
        assert "ExecStart=" in recipes["alpha"]["service_text"]

        assert recipes["beta"]["on_calendar"] == "*:0/30"
        assert recipes["gamma"]["skipped"] == "disabled"
        assert "unsupported cron" in recipes["bad"]["error"]

    def test_list_and_uninstall_dry_run(self, tmp_path):
        (tmp_path / "operator-recipe-alpha.timer").write_text("[Timer]\n", encoding="utf-8")
        (tmp_path / "operator-recipe-alpha.service").write_text("[Service]\n", encoding="utf-8")
        (tmp_path / "operator-recipe-beta.timer").write_text("[Timer]\n", encoding="utf-8")
        (tmp_path / "operator-recipe-beta.service").write_text("[Service]\n", encoding="utf-8")
        (tmp_path / "unrelated.timer").write_text("[Timer]\n", encoding="utf-8")

        names = list_linux_tasks(units_dir=tmp_path)
        assert names == ["operator-recipe-alpha", "operator-recipe-beta"]

        plans = uninstall_linux_tasks(dry_run=True, units_dir=tmp_path)
        assert {p["task"] for p in plans} == {"operator-recipe-alpha", "operator-recipe-beta"}
        # Files still present.
        assert (tmp_path / "operator-recipe-alpha.timer").exists()


# --- top-level dispatch ------------------------------------------------------

class TestDispatch:
    def test_install_tasks_macos_dispatch(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OPERATOR_SCHEDULER_HOST", "macos")
        # We call the macOS path directly by passing host explicitly.
        plans = install_tasks(_sample_schedule(), host=HOST_MACOS, dry_run=True)
        # Expect at least the enabled recipes to have a plan with `label`.
        labels = [p.get("label") for p in plans if p.get("label")]
        assert any("alpha" in lbl for lbl in labels)

    def test_install_tasks_linux_dispatch(self):
        plans = install_tasks(_sample_schedule(), host=HOST_LINUX, dry_run=True)
        oncals = [p.get("on_calendar") for p in plans if p.get("on_calendar")]
        assert "*-*-* 07:00:00" in oncals

    def test_install_tasks_unknown_host_raises(self):
        with pytest.raises(ValueError):
            install_tasks(_sample_schedule(), host="solaris", dry_run=True)

    def test_uninstall_tasks_unknown_host_raises(self):
        with pytest.raises(ValueError):
            uninstall_tasks(host="solaris")

    def test_list_installed_tasks_unknown_host_raises(self):
        with pytest.raises(ValueError):
            list_installed_tasks(host="haiku")


# --- status_report -----------------------------------------------------------

class TestStatusReport:
    def test_status_report_finds_installed_and_orphans(self, tmp_path, monkeypatch):
        from operator_core.recipes import schedule as mod

        (tmp_path / "operator-recipe-alpha.timer").write_text("", encoding="utf-8")
        (tmp_path / "operator-recipe-orphan.timer").write_text("", encoding="utf-8")

        # Capture the original BEFORE monkeypatching to avoid infinite recursion.
        real_list_linux = mod.list_linux_tasks

        def fake_list(prefix="operator-recipe-", *, units_dir=None):
            return real_list_linux(prefix, units_dir=tmp_path)

        monkeypatch.setattr(mod, "list_linux_tasks", fake_list)

        report = status_report(_sample_schedule(), host=HOST_LINUX)
        recipes = {r["recipe"]: r for r in report["rows"]}
        assert recipes["alpha"]["installed"] is True
        assert recipes["beta"]["installed"] is False
        assert "operator-recipe-orphan" in report["orphans"]
