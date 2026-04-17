"""Tests for operator_core.memory and store_migrations."""

from __future__ import annotations

import sqlite3

import pytest

from operator_core import memory as memory_module
from operator_core.memory import ProjectMemory
from operator_core.store_migrations import MIGRATIONS, apply_migrations
from operator_core.store import JobStore


@pytest.fixture
def mem_db(tmp_path, monkeypatch):
    db_path = tmp_path / "operator.sqlite3"
    # Repoint default store at temp db
    monkeypatch.setattr(memory_module, "DB_PATH", db_path)
    memory_module.reset_default_store_for_tests()
    return db_path


def test_remember_recall_roundtrip(mem_db):
    store = ProjectMemory(mem_db)
    entry = store.remember("demo", "last_green_sha", "abc123", source_job_id="job1")
    assert entry.project == "demo"
    assert entry.value == "abc123"

    assert store.recall("demo", "last_green_sha") == "abc123"
    assert store.recall("demo", "nope") is None
    assert store.recall("other", "last_green_sha") is None


def test_remember_upserts_existing_key(mem_db):
    store = ProjectMemory(mem_db)
    store.remember("demo", "k", "v1", source_job_id="j1")
    store.remember("demo", "k", "v2", source_job_id="j2")
    assert store.recall("demo", "k") == "v2"
    # Only one row
    listing = store.list_project("demo")
    assert listing == {"k": "v2"}


def test_list_project_scoped(mem_db):
    store = ProjectMemory(mem_db)
    store.remember("a", "x", "1")
    store.remember("a", "y", "2")
    store.remember("b", "x", "99")
    assert store.list_project("a") == {"x": "1", "y": "2"}
    assert store.list_project("b") == {"x": "99"}
    assert store.list_project("missing") == {}


def test_remember_requires_project_and_key(mem_db):
    store = ProjectMemory(mem_db)
    with pytest.raises(ValueError):
        store.remember("", "k", "v")
    with pytest.raises(ValueError):
        store.remember("p", "", "v")


def test_forget_removes_key(mem_db):
    store = ProjectMemory(mem_db)
    store.remember("demo", "k", "v")
    assert store.forget("demo", "k") is True
    assert store.recall("demo", "k") is None
    assert store.forget("demo", "k") is False


def test_increment(mem_db):
    store = ProjectMemory(mem_db)
    assert store.increment("demo", "count") == 1
    assert store.increment("demo", "count") == 2
    assert store.increment("demo", "count", delta=3) == 5
    # Non-integer current resets
    store.remember("demo", "count", "not_a_number")
    assert store.increment("demo", "count") == 1


def test_module_level_api_uses_singleton(mem_db):
    memory_module.remember("demo", "foo", "bar")
    assert memory_module.recall("demo", "foo") == "bar"
    assert memory_module.list_project("demo") == {"foo": "bar"}
    assert memory_module.increment("demo", "n") == 1


def test_apply_migrations_is_idempotent(tmp_path):
    db = tmp_path / "m.sqlite3"
    conn = sqlite3.connect(db)
    try:
        first = apply_migrations(conn)
        second = apply_migrations(conn)
        assert set(first) >= {1}  # at least the project_memory migration
        assert second == []  # nothing new
        # schema_migrations has one row per migration, no dupes
        rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
        assert sorted(r[0] for r in rows) == sorted(v for v, _ in MIGRATIONS)
    finally:
        conn.close()


def test_apply_migrations_adds_attempts_column_when_jobs_exists(tmp_path):
    db = tmp_path / "jobs.sqlite3"
    # Create the job store first (creates the jobs table)
    store = JobStore(db)
    # Now apply migrations against a fresh connection
    conn = sqlite3.connect(db)
    try:
        apply_migrations(conn)
        cols = [row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()]
        assert "attempts_json" in cols
        # Second apply — still fine, column check prevents duplicate
        apply_migrations(conn)
        cols2 = [row[1] for row in conn.execute("PRAGMA table_info(jobs)").fetchall()]
        assert cols2.count("attempts_json") == 1
    finally:
        conn.close()


def test_memory_coexists_with_job_store(tmp_path, monkeypatch):
    """Memory store and JobStore share a DB file without stepping on each other."""
    db = tmp_path / "shared.sqlite3"
    job_store = JobStore(db)
    monkeypatch.setattr(memory_module, "DB_PATH", db)
    memory_module.reset_default_store_for_tests()
    mem = ProjectMemory(db)

    job = job_store.create_job("status", prompt="hi")
    mem.remember(job.project or "demo", "last_job", job.id, source_job_id=job.id)
    assert mem.recall("demo", "last_job") == job.id
