"""Tests for C4 risk-tier learning loop in operator_core.security."""

from __future__ import annotations

from pathlib import Path

import pytest

from operator_core import memory as memory_module
from operator_core import security as security_module
from operator_core.config import DeployConfig, HealthConfig, ProjectConfig
from operator_core.security import (
    AUTO_MERGE_ROLLBACK_KEY,
    AUTO_MERGE_SUCCESS_KEY,
    HIGH_RISK_PATH_PATTERNS,
    classify_risk,
    record_auto_merge_rollback,
    record_auto_merge_success,
)


def _make_project(slug: str = "demo") -> ProjectConfig:
    return ProjectConfig(
        slug=slug,
        path=Path("/tmp") / slug,
        repo=f"owner/{slug}",
        type="saas",
        deploy=DeployConfig(provider="vercel", url="https://example.com"),
        health=HealthConfig(path="/", expected_status=200),
        checks=[],
        autonomy_tier="guarded",
        protected_patterns=[".env*"],
        auto_merge=False,
    )


@pytest.fixture
def mem_db(tmp_path, monkeypatch):
    db = tmp_path / "learning.sqlite3"
    monkeypatch.setattr(memory_module, "DB_PATH", db)
    memory_module.reset_default_store_for_tests()
    return db


def test_learning_disabled_default_no_change(mem_db, monkeypatch):
    monkeypatch.delenv("OPERATOR_RISK_LEARNING_ENABLED", raising=False)
    project = _make_project()
    for _ in range(10):
        record_auto_merge_success(project.slug)
    risk = classify_risk("tweak config", ["next.config.js"], project)
    assert risk == "medium"


def test_learning_enabled_but_no_successes_no_change(mem_db, monkeypatch):
    monkeypatch.setenv("OPERATOR_RISK_LEARNING_ENABLED", "1")
    project = _make_project()
    risk = classify_risk("tweak config", ["next.config.js"], project)
    assert risk == "medium"


def test_learning_enabled_insufficient_successes_no_change(mem_db, monkeypatch):
    monkeypatch.setenv("OPERATOR_RISK_LEARNING_ENABLED", "1")
    project = _make_project()
    for _ in range(4):
        record_auto_merge_success(project.slug)
    risk = classify_risk("tweak config", ["next.config.js"], project)
    assert risk == "medium"


def test_learning_enabled_clean_history_relaxes_single_config(mem_db, monkeypatch):
    monkeypatch.setenv("OPERATOR_RISK_LEARNING_ENABLED", "1")
    project = _make_project()
    for _ in range(5):
        record_auto_merge_success(project.slug)
    risk = classify_risk("tweak config", ["next.config.js"], project)
    assert risk == "low"


def test_learning_rollback_blocks_relaxation(mem_db, monkeypatch):
    monkeypatch.setenv("OPERATOR_RISK_LEARNING_ENABLED", "1")
    project = _make_project()
    for _ in range(10):
        record_auto_merge_success(project.slug)
    record_auto_merge_rollback(project.slug)
    risk = classify_risk("tweak config", ["next.config.js"], project)
    assert risk == "medium"


def test_learning_relaxation_only_for_single_file(mem_db, monkeypatch):
    monkeypatch.setenv("OPERATOR_RISK_LEARNING_ENABLED", "1")
    project = _make_project()
    for _ in range(5):
        record_auto_merge_success(project.slug)
    risk = classify_risk("tweak configs", ["next.config.js", "tsconfig.json"], project)
    assert risk == "medium"


def test_learning_relaxation_only_for_config_extensions(mem_db, monkeypatch):
    monkeypatch.setenv("OPERATOR_RISK_LEARNING_ENABLED", "1")
    project = _make_project()
    for _ in range(5):
        record_auto_merge_success(project.slug)
    risk = classify_risk("refactor", ["src/app.ts"], project)
    assert risk == "medium"


def test_learning_never_relaxes_high_risk_path(mem_db, monkeypatch):
    """Hard cap: .env files stay high even with perfect history."""
    monkeypatch.setenv("OPERATOR_RISK_LEARNING_ENABLED", "1")
    project = _make_project()
    for _ in range(100):
        record_auto_merge_success(project.slug)
    risk = classify_risk("update env", [".env"], project)
    assert risk == "high"


def test_learning_never_relaxes_high_risk_word_in_prompt(mem_db, monkeypatch):
    monkeypatch.setenv("OPERATOR_RISK_LEARNING_ENABLED", "1")
    project = _make_project()
    for _ in range(10):
        record_auto_merge_success(project.slug)
    risk = classify_risk("update stripe payment config", ["next.config.js"], project)
    assert risk == "high"


def test_learning_never_relaxes_auth_path(mem_db, monkeypatch):
    monkeypatch.setenv("OPERATOR_RISK_LEARNING_ENABLED", "1")
    project = _make_project()
    for _ in range(10):
        record_auto_merge_success(project.slug)
    risk = classify_risk("routine edit", ["src/auth/session.json"], project)
    assert risk == "high"


def test_counters_persist_across_calls(mem_db):
    project = _make_project()
    assert record_auto_merge_success(project.slug) == 1
    assert record_auto_merge_success(project.slug) == 2
    assert record_auto_merge_rollback(project.slug) == 1
    assert memory_module.recall(project.slug, AUTO_MERGE_SUCCESS_KEY) == "2"
    assert memory_module.recall(project.slug, AUTO_MERGE_ROLLBACK_KEY) == "1"


def test_classify_risk_unchanged_for_projects_with_no_learning_opt_in(mem_db, monkeypatch):
    monkeypatch.delenv("OPERATOR_RISK_LEARNING_ENABLED", raising=False)
    project = _make_project()
    assert classify_risk("update docs", ["README.md"], project) == "low"
    assert classify_risk("refactor", ["src/a.ts"], project) == "medium"
    assert classify_risk("update stripe", ["src/a.ts"], project) == "high"


def test_high_risk_path_patterns_nonempty():
    joined = " ".join(HIGH_RISK_PATH_PATTERNS)
    assert "env" in joined
    assert "auth" in joined
