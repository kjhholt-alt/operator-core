"""Tests for the cron-parity CRUD surface in operator_core.scheduler (Queue D3)."""

from __future__ import annotations

import json

import pytest

from operator_core.scheduler import (
    DEFAULT_TASKS,
    SCHEDULE_CONFIG_VERSION,
    ScheduleConfigError,
    add_schedule,
    format_schedule_list,
    list_schedules,
    load_schedule_config,
    remove_schedule,
    save_schedule_config,
)


def test_list_schedules_missing_file_returns_empty(tmp_path):
    path = tmp_path / "schedule.json"
    assert list_schedules(path) == []


def test_load_returns_versioned_empty_template(tmp_path):
    path = tmp_path / "schedule.json"
    config = load_schedule_config(path)
    assert config == {"version": SCHEDULE_CONFIG_VERSION, "schedules": []}


def test_add_schedule_writes_versioned_file(tmp_path):
    path = tmp_path / "schedule.json"
    add_schedule("morning", "0 6 * * *", "!op morning", path=path)
    assert path.exists()
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["version"] == SCHEDULE_CONFIG_VERSION
    assert len(raw["schedules"]) == 1
    assert raw["schedules"][0]["name"] == "morning"
    assert raw["schedules"][0]["cron"] == "0 6 * * *"
    assert raw["schedules"][0]["command"] == "!op morning"


def test_add_and_list_round_trip(tmp_path):
    path = tmp_path / "schedule.json"
    add_schedule("morning", "0 6 * * *", "!op morning", path=path)
    add_schedule("deploy-check", "20 6 * * *", "!op deploy check", path=path)

    entries = list_schedules(path)
    assert [e["name"] for e in entries] == ["morning", "deploy-check"]
    assert [e["cron"] for e in entries] == ["0 6 * * *", "20 6 * * *"]


def test_add_duplicate_name_raises(tmp_path):
    path = tmp_path / "schedule.json"
    add_schedule("morning", "0 6 * * *", "!op morning", path=path)
    with pytest.raises(ScheduleConfigError, match="already exists"):
        add_schedule("morning", "0 7 * * *", "!op morning", path=path)


def test_add_empty_fields_raises(tmp_path):
    path = tmp_path / "schedule.json"
    with pytest.raises(ScheduleConfigError):
        add_schedule("", "0 6 * * *", "!op morning", path=path)
    with pytest.raises(ScheduleConfigError):
        add_schedule("a", "", "!op morning", path=path)
    with pytest.raises(ScheduleConfigError):
        add_schedule("a", "0 6 * * *", "", path=path)


def test_remove_schedule_deletes_entry(tmp_path):
    path = tmp_path / "schedule.json"
    add_schedule("morning", "0 6 * * *", "!op morning", path=path)
    add_schedule("pulse", "30 6 * * *", "!op marketing pulse", path=path)

    removed = remove_schedule("morning", path=path)
    assert removed is True
    remaining = list_schedules(path)
    assert [e["name"] for e in remaining] == ["pulse"]


def test_remove_missing_returns_false(tmp_path):
    path = tmp_path / "schedule.json"
    add_schedule("morning", "0 6 * * *", "!op morning", path=path)
    assert remove_schedule("nope", path=path) is False
    assert len(list_schedules(path)) == 1


def test_remove_empty_name_raises(tmp_path):
    path = tmp_path / "schedule.json"
    with pytest.raises(ScheduleConfigError):
        remove_schedule("", path=path)


def test_malformed_json_raises(tmp_path):
    path = tmp_path / "schedule.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(ScheduleConfigError):
        load_schedule_config(path)


def test_top_level_must_be_object(tmp_path):
    path = tmp_path / "schedule.json"
    path.write_text("[]", encoding="utf-8")
    with pytest.raises(ScheduleConfigError, match="object"):
        load_schedule_config(path)


def test_schedules_must_be_list(tmp_path):
    path = tmp_path / "schedule.json"
    path.write_text(json.dumps({"version": 1, "schedules": {}}), encoding="utf-8")
    with pytest.raises(ScheduleConfigError, match="list"):
        load_schedule_config(path)


def test_save_normalizes_missing_version(tmp_path):
    path = tmp_path / "schedule.json"
    save_schedule_config({"schedules": [{"name": "x", "cron": "* * * * *", "command": "!op status"}]}, path=path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["version"] == SCHEDULE_CONFIG_VERSION
    assert raw["schedules"][0]["name"] == "x"


def test_format_schedule_list_empty(tmp_path):
    path = tmp_path / "schedule.json"
    assert "No schedules" in format_schedule_list(path)


def test_format_schedule_list_with_entries(tmp_path):
    path = tmp_path / "schedule.json"
    add_schedule("morning", "0 6 * * *", "!op morning", path=path)
    rendered = format_schedule_list(path)
    assert "morning" in rendered
    assert "0 6 * * *" in rendered
    assert "!op morning" in rendered


def test_shipped_template_file_is_valid():
    """The versioned template committed at config/schedule.json must parse."""
    from operator_core.scheduler import SCHEDULE_CONFIG_PATH

    if not SCHEDULE_CONFIG_PATH.exists():
        pytest.skip("template not materialized in this environment")
    config = load_schedule_config(SCHEDULE_CONFIG_PATH)
    assert config["version"] == SCHEDULE_CONFIG_VERSION
    assert isinstance(config["schedules"], list)


def test_default_tasks_include_lead_digest():
    lead_task = next((task for task in DEFAULT_TASKS if task.key == "lead-digest"), None)

    assert lead_task is not None
    assert lead_task.action == "lead_digest"
    assert lead_task.cadence == "daily"


def test_default_tasks_include_demand_review():
    task = next((task for task in DEFAULT_TASKS if task.key == "demand-review"), None)

    assert task is not None
    assert task.action == "demand_review"
    assert task.cadence == "weekly"


def test_default_tasks_include_nightly_demand_plan():
    task = next((task for task in DEFAULT_TASKS if task.key == "nightly-demand-plan"), None)

    assert task is not None
    assert task.action == "nightly_demand_plan"
    assert task.cadence == "daily"
