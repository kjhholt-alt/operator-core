"""Tests for the gate_audit reader / cut-over decision logic."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from operator_core import outreach_audit


def _write(path: Path, events: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for ev in events:
            fh.write(json.dumps(ev) + "\n")


def _ev(product: str, agreement: str, **extra) -> dict:
    payload = {"product": product, "agreement": agreement, "lead_hash": "a" * 16}
    payload.update(extra)
    return {
        "ts": "2026-05-05T15:00:00Z",
        "stream": "gate_audit",
        "event_type": "decision",
        "payload": payload,
    }


def test_collect_groups_by_product(tmp_path):
    p = tmp_path / "g.ndjson"
    _write(p, [
        _ev("outreach-engine", "match"),
        _ev("outreach-engine", "match"),
        _ev("outreach-engine", "would_block_new"),
        _ev("ai-ops-consulting", "match"),
    ])
    out = outreach_audit.collect([p])
    assert [s.product for s in out] == ["ai-ops-consulting", "outreach-engine"]
    oe = [s for s in out if s.product == "outreach-engine"][0]
    assert oe.total == 3
    assert oe.match == 2
    assert oe.would_block_new == 1
    assert oe.match_pct == pytest.approx(66.6667, rel=1e-3)


def test_cutover_ready_threshold(tmp_path):
    p = tmp_path / "g.ndjson"
    # 95 match, 5 different reason -> 95% match -> ready at default
    events = [_ev("oe", "match") for _ in range(95)] + \
             [_ev("oe", "both_block_diff_reason") for _ in range(5)]
    _write(p, events)
    out = outreach_audit.collect([p])
    assert out[0].cutover_ready(threshold_pct=95.0) is True
    assert out[0].cutover_ready(threshold_pct=96.0) is False


def test_cutover_NOT_ready_with_any_would_allow_new(tmp_path):
    """A single would_allow_new flips ready to False regardless of match%."""
    p = tmp_path / "g.ndjson"
    events = [_ev("oe", "match") for _ in range(99)] + [_ev("oe", "would_allow_new")]
    _write(p, events)
    out = outreach_audit.collect([p])
    assert out[0].match_pct == 99.0
    assert out[0].would_allow_new == 1
    assert out[0].cutover_ready() is False


def test_cutover_NOT_ready_with_zero_total():
    """Empty bucket isn't ready -- nothing to base a decision on."""
    s = outreach_audit.ProductSummary(product="x")
    assert s.cutover_ready() is False


def test_collect_filters_by_since(tmp_path):
    p = tmp_path / "g.ndjson"
    old = {**_ev("oe", "match"), "ts": "2026-04-01T00:00:00Z"}
    fresh = {**_ev("oe", "match"), "ts": "2026-05-05T15:00:00Z"}
    _write(p, [old, fresh])
    cutoff = datetime(2026, 5, 1, tzinfo=timezone.utc)
    out = outreach_audit.collect([p], since=cutoff)
    assert out[0].total == 1


def test_collect_skips_malformed_lines(tmp_path):
    p = tmp_path / "g.ndjson"
    p.write_text("not json\n" + json.dumps(_ev("oe", "match")) + "\n", encoding="utf-8")
    out = outreach_audit.collect([p])
    assert out[0].total == 1


def test_collect_skips_events_missing_product_or_agreement(tmp_path):
    p = tmp_path / "g.ndjson"
    _write(p, [
        {"ts": "2026-05-05T15:00:00Z", "payload": {"agreement": "match"}},  # no product
        {"ts": "2026-05-05T15:00:00Z", "payload": {"product": "oe"}},        # no agreement
        _ev("oe", "match"),
    ])
    out = outreach_audit.collect([p])
    assert out[0].total == 1


def test_collect_caps_samples_at_5(tmp_path):
    p = tmp_path / "g.ndjson"
    _write(p, [
        _ev("oe", "would_block_new", lead_business_name=f"Biz{i}",
            gate_block_label="step:x") for i in range(10)
    ])
    out = outreach_audit.collect([p])
    assert out[0].would_block_new == 10
    assert len(out[0].sample_would_block) == 5


def test_render_table_shows_ready_no(tmp_path):
    p = tmp_path / "g.ndjson"
    _write(p, [_ev("oe", "would_allow_new"), _ev("oe", "match")])
    out = outreach_audit.collect([p])
    rendered = outreach_audit.render_table(out, threshold=95.0)
    assert "no" in rendered
    assert "DANGER" in rendered  # would_allow_new is the danger sample


def test_render_table_shows_ready_yes(tmp_path):
    p = tmp_path / "g.ndjson"
    _write(p, [_ev("oe", "match") for _ in range(20)])
    out = outreach_audit.collect([p])
    rendered = outreach_audit.render_table(out, threshold=95.0)
    assert "YES" in rendered


def test_render_table_handles_empty():
    assert "No gate_audit" in outreach_audit.render_table([], threshold=95.0)


def test_render_json_is_valid(tmp_path):
    p = tmp_path / "g.ndjson"
    _write(p, [_ev("oe", "match"), _ev("oe", "would_block_new")])
    out = outreach_audit.collect([p])
    payload = json.loads(outreach_audit.render_json(out, threshold=95.0))
    assert payload["threshold_pct"] == 95.0
    assert payload["products"][0]["product"] == "oe"
    assert payload["products"][0]["total"] == 2


def test_overall_ready_requires_all_products(tmp_path):
    p = tmp_path / "g.ndjson"
    _write(p, [
        _ev("oe", "match"),
        _ev("ai", "would_allow_new"),
        _ev("ai", "match"),
    ])
    out = outreach_audit.collect([p])
    # oe is ready (1/1 match) but ai isn't (would_allow_new) -> overall not ready
    assert outreach_audit.overall_ready(out, 95.0) is False


def test_overall_ready_empty_is_false():
    assert outreach_audit.overall_ready([], 95.0) is False


def test_parse_since_24h_relative():
    cutoff = outreach_audit._parse_since("24h")
    delta = datetime.now(timezone.utc) - cutoff
    assert timedelta(hours=23) < delta < timedelta(hours=25)


def test_parse_since_7d_relative():
    cutoff = outreach_audit._parse_since("7d")
    delta = datetime.now(timezone.utc) - cutoff
    assert timedelta(days=6, hours=23) < delta < timedelta(days=7, hours=1)


def test_parse_since_iso_8601():
    cutoff = outreach_audit._parse_since("2026-01-01T00:00:00Z")
    assert cutoff == datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_parse_since_none_returns_none():
    assert outreach_audit._parse_since(None) is None


# -- triage integration --


def test_cutover_ready_requires_fully_triaged(tmp_path):
    """A would_block_new disagreement leaves a row in the queue. Until
    that row is resolved, cut-over is NOT ready -- even with 100% match."""
    from operator_core import gate_review

    audit_path = tmp_path / "g.ndjson"
    db_path = tmp_path / "review.sqlite"
    # 100 match + 1 would_block_new -> match% = 99% > threshold
    events = [_ev("oe", "match") for _ in range(99)] + [
        _ev("oe", "would_block_new", lead_business_name="Acme")
    ]
    _write(audit_path, events)
    # Seed the queue with that one disagreement (still pending).
    gate_review.ingest_events(events, db_path=db_path)

    out = outreach_audit.collect([audit_path], triage_db_path=db_path)
    s = out[0]
    assert s.match_pct >= 95.0
    assert s.would_allow_new == 0
    assert s.triage_pending == 1
    assert s.cutover_ready() is False  # blocked by pending review

    # Now resolve the queue item.
    item = gate_review.list_pending("oe", db_path=db_path)[0]
    gate_review.resolve(item.id, "approved_gate", db_path=db_path)
    out2 = outreach_audit.collect([audit_path], triage_db_path=db_path)
    assert out2[0].cutover_ready() is True
    assert out2[0].triaged_pct == 100.0


def test_cutover_ready_no_disagreements_no_queue_rows(tmp_path):
    """Pure happy-path: 100% match, never any disagreements queued."""
    audit_path = tmp_path / "g.ndjson"
    db_path = tmp_path / "review.sqlite"
    _write(audit_path, [_ev("oe", "match") for _ in range(20)])
    out = outreach_audit.collect([audit_path], triage_db_path=db_path)
    s = out[0]
    assert s.triage_total == 0
    assert s.triaged_pct == 100.0  # vacuously
    assert s.cutover_ready() is True


def test_render_table_includes_triage_column(tmp_path):
    audit_path = tmp_path / "g.ndjson"
    _write(audit_path, [_ev("oe", "match")])
    out = outreach_audit.collect([audit_path], include_triage=False)
    rendered = outreach_audit.render_table(out, threshold=95.0)
    assert "TRIAGE%" in rendered
    assert "triaged% == 100" in rendered


def test_render_json_includes_triage_fields(tmp_path):
    audit_path = tmp_path / "g.ndjson"
    _write(audit_path, [_ev("oe", "match")])
    out = outreach_audit.collect([audit_path], include_triage=False)
    payload = json.loads(outreach_audit.render_json(out, threshold=95.0))
    p = payload["products"][0]
    assert "triage_total" in p
    assert "triage_pending" in p
    assert "triaged_pct" in p
