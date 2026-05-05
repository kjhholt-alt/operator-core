"""Tests for the auto-suppression PR builder."""
from __future__ import annotations

from pathlib import Path

import pytest

from operator_core import gate_review, suppression_pr


def _seed_approved(db_path: Path, *, business_name: str = "Acme",
                   product: str = "outreach-engine") -> int:
    """Helper: add one would_block_new event and resolve as approved_gate."""
    gate_review.ingest_events([{
        "ts": "2026-05-05T15:00:00Z",
        "stream": "gate_audit",
        "event_type": "decision",
        "payload": {
            "product": product,
            "agreement": "would_block_new",
            "lead_hash": business_name.lower().replace(" ", "")[:16].ljust(16, "0"),
            "lead_business_name": business_name,
            "gate_block_label": f"network_scrub:business_name:{business_name.lower()}",
        },
    }], db_path=db_path)
    item = gate_review.list_pending(product, db_path=db_path)[0]
    gate_review.resolve(item.id, "approved_gate", note="legitimate block",
                          resolved_by="test", db_path=db_path)
    return item.id


# -- helpers --


def test_extract_business_name_from_field():
    item = gate_review.ReviewItem(
        id=1, product="oe", lead_hash="x" * 16,
        business_name="Acme Co", agreement="would_block_new",
        gate_block_label=None, legacy_block_reason=None,
        first_seen_ts="t", last_seen_ts="t", hit_count=1,
        status="approved_gate", resolution_note=None,
        resolved_by=None, resolved_ts=None,
    )
    assert suppression_pr._extract_business_name(item) == "Acme Co"


def test_extract_business_name_from_label_when_no_field():
    item = gate_review.ReviewItem(
        id=1, product="oe", lead_hash="x" * 16,
        business_name=None, agreement="would_block_new",
        gate_block_label="network_scrub:business_name:nightowl pools",
        legacy_block_reason=None,
        first_seen_ts="t", last_seen_ts="t", hit_count=1,
        status="approved_gate", resolution_note=None,
        resolved_by=None, resolved_ts=None,
    )
    assert suppression_pr._extract_business_name(item) == "nightowl pools"


def test_extract_business_name_returns_none_when_missing():
    item = gate_review.ReviewItem(
        id=1, product="oe", lead_hash="x" * 16,
        business_name=None, agreement="would_block_new",
        gate_block_label=None,
        legacy_block_reason=None,
        first_seen_ts="t", last_seen_ts="t", hit_count=1,
        status="approved_gate", resolution_note=None,
        resolved_by=None, resolved_ts=None,
    )
    assert suppression_pr._extract_business_name(item) is None


def test_add_business_names_appends_idempotent(tmp_path):
    yml = (
        "scrub:\n"
        "  business_names:\n"
        '    - "Existing Corp"\n'
    )
    new_yml, added = suppression_pr._add_business_names(yml, ["New One", "Existing Corp"])
    assert added == ["New One"]  # Existing Corp was already present
    assert "New One" in new_yml
    # Existing entry not duplicated
    assert new_yml.count("Existing Corp") == 1


def test_add_business_names_creates_anchor_when_missing():
    yml = "scrub:\n  emails:\n    - 'foo@bar.com'\n"
    new_yml, added = suppression_pr._add_business_names(yml, ["Fresh Co"])
    assert added == ["Fresh Co"]
    assert "business_names:" in new_yml
    assert "Fresh Co" in new_yml


def test_add_business_names_case_insensitive_dedup():
    yml = "  business_names:\n    - 'acme'\n"
    new_yml, added = suppression_pr._add_business_names(yml, ["ACME", "other"])
    assert added == ["other"]


# -- build_plan --


def test_build_plan_returns_none_when_queue_empty(tmp_path):
    db = tmp_path / "g.sqlite"
    yml_path = tmp_path / "network_scrub.yml"
    plan = suppression_pr.build_plan(yml_path, db_path=db)
    assert plan is None


def test_build_plan_returns_none_when_only_pending(tmp_path):
    db = tmp_path / "g.sqlite"
    gate_review.ingest_events([{
        "ts": "2026-05-05T15:00:00Z",
        "payload": {"product": "oe", "agreement": "would_block_new",
                    "lead_hash": "x" * 16, "lead_business_name": "Foo"},
    }], db_path=db)
    # NOT resolved
    plan = suppression_pr.build_plan(tmp_path / "network_scrub.yml", db_path=db)
    assert plan is None


def test_build_plan_with_approved_items(tmp_path):
    db = tmp_path / "g.sqlite"
    yml_path = tmp_path / "network_scrub.yml"
    _seed_approved(db, business_name="Acme")
    _seed_approved(db, business_name="Beta", product="prospector-pro")
    plan = suppression_pr.build_plan(yml_path, db_path=db)
    assert plan is not None
    assert sorted(plan.new_business_names) == ["Acme", "Beta"]
    assert "Acme" in plan.yml_content
    assert "Beta" in plan.yml_content
    assert plan.branch_name.startswith("auto-suppress/")
    assert "approved_gate" in plan.pr_body or "gate-review" in plan.pr_body


def test_build_plan_skips_already_in_yml(tmp_path):
    db = tmp_path / "g.sqlite"
    yml_path = tmp_path / "network_scrub.yml"
    yml_path.write_text(
        "scrub:\n  business_names:\n    - 'Acme'\n",
        encoding="utf-8",
    )
    _seed_approved(db, business_name="Acme")
    plan = suppression_pr.build_plan(yml_path, db_path=db)
    # Acme is already in the file -> nothing actually added -> no plan
    assert plan is None


def test_build_plan_pr_body_lists_each_item(tmp_path):
    db = tmp_path / "g.sqlite"
    _seed_approved(db, business_name="OneCorp")
    _seed_approved(db, business_name="TwoCorp", product="ai-ops-consulting")
    plan = suppression_pr.build_plan(tmp_path / "network_scrub.yml", db_path=db)
    assert "OneCorp" in plan.pr_body
    assert "TwoCorp" in plan.pr_body
    assert "outreach-engine" in plan.pr_body
    assert "ai-ops-consulting" in plan.pr_body


# -- mark_items_suppressed --


def test_mark_items_suppressed_flips_status(tmp_path):
    db = tmp_path / "g.sqlite"
    _seed_approved(db, business_name="X")
    items = [i for i in
             [gate_review.get_item(1, db_path=db)]
             if i is not None]
    n = suppression_pr.mark_items_suppressed(items, db_path=db)
    assert n == 1
    item = gate_review.get_item(1, db_path=db)
    assert item.status == "suppressed"


# -- open_pr --


def test_open_pr_no_token_returns_error(tmp_path, monkeypatch):
    """Without GITHUB_TOKEN we never hit the network."""
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    plan = suppression_pr.SuppressionPlan(
        items=[],
        new_business_names=["X"],
        pr_title="test",
        pr_body="b",
        branch_name="auto-suppress/x",
        yml_content="scrub:\n  business_names:\n    - 'X'\n",
    )
    result = suppression_pr.open_pr(plan, token=None)
    assert result.get("error") is True
    assert "GITHUB_TOKEN" in result.get("message", "")
