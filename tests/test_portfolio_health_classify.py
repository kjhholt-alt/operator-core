"""Focused unit tests for portfolio_health._classify_health.

These do NOT require templated-dashboards to be installed (unlike
test_portfolio_health.py which exercises the full recipe). They exist
to lock in the status-spec/v1 contract: a status doc with an invalid
health value MUST classify as bad, never silently pass as good.

Bug repro context (2026-05-06):
  A status-spec/v1 doc with `health: "degraded"` was classifying as
  ("good", "ok"). Fix: refuse to trust a doc that doesn't pass minimal
  v1 validation; classify it as bad with a reason naming the violation.
"""
from __future__ import annotations

import pytest

from recipes.portfolio_health import _classify_health, _is_valid_status_spec_doc


def test_invalid_health_value_classifies_as_bad() -> None:
    """The headline bug: 'degraded' was passing as 'good'."""
    doc = {
        "schema_version": "status-spec/v1",
        "project": "foo",
        "ts": "2026-05-06T12:00:00Z",
        "health": "degraded",
    }
    tone, reason = _classify_health({}, doc, [], None)
    assert tone == "bad"
    assert "invalid" in reason.lower()
    assert "degraded" in reason


def test_unknown_health_value_classifies_as_bad() -> None:
    doc = {
        "schema_version": "status-spec/v1",
        "project": "foo",
        "ts": "2026-05-06T12:00:00Z",
        "health": "unknown",
    }
    tone, reason = _classify_health({}, doc, [], None)
    assert tone == "bad"
    assert "invalid" in reason.lower()


def test_wrong_schema_version_classifies_as_bad() -> None:
    doc = {
        "schema_version": "status-spec/v2",
        "project": "foo",
        "ts": "2026-05-06T12:00:00Z",
        "health": "green",
    }
    tone, reason = _classify_health({}, doc, [], None)
    assert tone == "bad"
    assert "schema_version" in reason


def test_missing_schema_version_classifies_as_bad() -> None:
    """A health-only doc with no schema_version isn't a v1 doc — refuse it."""
    doc = {"health": "green"}
    tone, reason = _classify_health({}, doc, [], None)
    assert tone == "bad"


def test_valid_green_classifies_as_good() -> None:
    doc = {
        "schema_version": "status-spec/v1",
        "project": "foo",
        "ts": "2026-05-06T12:00:00Z",
        "health": "green",
        "summary": "all systems clean",
    }
    tone, reason = _classify_health({}, doc, [], None)
    assert tone == "good"
    assert reason == "all systems clean"


def test_valid_yellow_classifies_as_warn() -> None:
    doc = {
        "schema_version": "status-spec/v1",
        "project": "foo",
        "ts": "2026-05-06T12:00:00Z",
        "health": "yellow",
    }
    tone, reason = _classify_health({}, doc, [], None)
    assert tone == "warn"


def test_valid_red_classifies_as_bad() -> None:
    doc = {
        "schema_version": "status-spec/v1",
        "project": "foo",
        "ts": "2026-05-06T12:00:00Z",
        "health": "red",
        "summary": "broken pipeline",
    }
    tone, reason = _classify_health({}, doc, [], None)
    assert tone == "bad"
    assert reason == "broken pipeline"


def test_no_status_doc_falls_back_to_components() -> None:
    """When a project has no status doc but recipes are erroring, classify bad."""
    fallback = [{"name": "ci_triage", "status": "error"}]
    tone, reason = _classify_health({}, None, fallback, None)
    assert tone == "bad"
    assert "ci_triage" in reason


def test_no_status_doc_no_components_is_neutral() -> None:
    tone, reason = _classify_health({}, None, [], None)
    assert tone == "neutral"


# --- _is_valid_status_spec_doc -------------------------------------

def test_validator_accepts_minimal_valid_doc() -> None:
    ok, _ = _is_valid_status_spec_doc({
        "schema_version": "status-spec/v1",
        "project": "x",
        "ts": "2026-05-06T12:00:00Z",
        "health": "green",
    })
    assert ok is True


def test_validator_rejects_missing_health() -> None:
    ok, reason = _is_valid_status_spec_doc({"schema_version": "status-spec/v1"})
    assert ok is False
    # "project" is checked before "health" now, so the message names project.
    # Either is acceptable -- we just want the doc to be REJECTED.
    assert "project" in reason or "health" in reason


def test_validator_rejects_missing_project() -> None:
    """schema_version + health alone aren't enough -- project required."""
    ok, reason = _is_valid_status_spec_doc({
        "schema_version": "status-spec/v1",
        "ts": "2026-05-06T12:00:00Z",
        "health": "green",
    })
    assert ok is False
    assert "project" in reason


def test_validator_rejects_missing_ts() -> None:
    ok, reason = _is_valid_status_spec_doc({
        "schema_version": "status-spec/v1",
        "project": "x",
        "health": "green",
    })
    assert ok is False
    assert "ts" in reason


def test_validator_rejects_malformed_ts() -> None:
    ok, reason = _is_valid_status_spec_doc({
        "schema_version": "status-spec/v1",
        "project": "x",
        "ts": "yesterday",  # not ISO-8601
        "health": "green",
    })
    assert ok is False
    assert "ts" in reason


def test_validator_rejects_unknown_top_level_keys() -> None:
    """schema requires additionalProperties: false on top level."""
    ok, reason = _is_valid_status_spec_doc({
        "schema_version": "status-spec/v1",
        "project": "x",
        "ts": "2026-05-06T12:00:00Z",
        "health": "green",
        "rogue_field": "should reject",
    })
    assert ok is False
    assert "rogue_field" in reason or "unknown" in reason.lower()


def test_validator_rejects_invalid_project_pattern() -> None:
    ok, reason = _is_valid_status_spec_doc({
        "schema_version": "status-spec/v1",
        "project": "x y z",  # spaces not allowed by ^[A-Za-z0-9._-]+$
        "ts": "2026-05-06T12:00:00Z",
        "health": "green",
    })
    assert ok is False
    assert "project" in reason


def test_validator_accepts_full_valid_doc() -> None:
    ok, _ = _is_valid_status_spec_doc({
        "schema_version": "status-spec/v1",
        "project": "ax02-engine",
        "ts": "2026-05-06T12:00:00Z",
        "health": "yellow",
        "summary": "ok",
        "subsystems": [{"name": "x", "health": "green"}],
        "counters": {"foo": 1},
        "last_event": {"ts": "2026-05-06T12:00:00Z", "type": "x"},
        "links": [{"label": "x", "href": "https://example.com"}],
        "extensions": {"k": "v"},
    })
    assert ok is True


# --- bug repro lock-ins -----------------------------------------------------

def test_classify_rejects_partial_doc_without_required_fields() -> None:
    """A doc with health but missing project/ts must NOT classify as good.

    Repro from 2026-05-06: {"schema_version":"status-spec/v1","health":"green"}
    was passing as good with the previous looser validator.
    """
    partial = {"schema_version": "status-spec/v1", "health": "green"}
    tone, reason = _classify_health({}, partial, [], None)
    assert tone == "bad"


def test_classify_rejects_doc_with_rogue_top_level_key() -> None:
    """Repro: same valid-looking doc + rogue field was classifying as good."""
    rogue = {
        "schema_version": "status-spec/v1",
        "project": "x",
        "ts": "2026-05-06T12:00:00Z",
        "health": "green",
        "rogue": "should reject",
    }
    tone, reason = _classify_health({}, rogue, [], None)
    assert tone == "bad"
