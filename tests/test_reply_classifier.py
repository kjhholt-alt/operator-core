"""Tests for the reply auto-classifier (rules + recipe)."""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest

from operator_core import replies, reply_classifier
from operator_core.recipes import RecipeContext


# --- pure rule tests --------------------------------------------------------

class TestPatterns:
    @pytest.mark.parametrize("body", [
        "please unsubscribe me",
        "Please opt out of future emails",
        "stop emailing me",
        "remove me from your list",
        "do not contact me again",
        "Please stop emailing me, thanks",
        "take me off your list",
        "Not interested.",
    ])
    def test_opt_out_matches(self, body):
        rule, phrase = reply_classifier.classify_message_body(body)
        assert rule == "opt_out", f"expected opt_out for {body!r}, got {rule}"

    @pytest.mark.parametrize("body", [
        "Mail Delivery Failed: Returning message to sender",
        "From: MAILER-DAEMON@example.com",
        "Postmaster says: undeliverable",
        "Recipient address rejected: User unknown",
        "550 5.1.1 The email account does not exist",
        "Address not found",
        "No such user here",
    ])
    def test_hard_bounce_matches(self, body):
        rule, _ = reply_classifier.classify_message_body(body)
        assert rule == "hard_bounce"

    @pytest.mark.parametrize("body", [
        "I am out of the office until Monday",
        "Automatic Reply: Thank you for your message",
        "Auto-reply: I'll respond when I'm back",
        "I will be out of the office through next week",
        "I am currently on vacation and will respond when I return",
        "Vacation reply: limited email access",
    ])
    def test_ooo_matches(self, body):
        rule, _ = reply_classifier.classify_message_body(body)
        assert rule == "out_of_office"

    @pytest.mark.parametrize("body", [
        "Hi, this looks really interesting -- can we set up time next week?",
        "Yes I'd love to learn more about pricing",
        "Forwarding to my team for review",
        "What does the onboarding look like?",
        "",
        None,
    ])
    def test_neutral_or_positive_does_not_match(self, body):
        rule, _ = reply_classifier.classify_message_body(body)
        assert rule is None, f"unexpected match for {body!r}: {rule}"


# --- store-backed integration tests -----------------------------------------

@pytest.fixture
def store(tmp_path, monkeypatch):
    db = tmp_path / "replies.sqlite"
    s = replies.ReplyStore(db_path=db)
    return s


def _seed_thread(store, *, sender="someone@example.com", subject="Re: outreach", body="hello"):
    """Push one inbound message via the public API and return the thread."""
    thread = store.upsert_thread_for_incoming(
        sender_email=sender, sender_name="Some Name",
        subject=subject, body_md=body,
    )
    return thread


def test_classify_pending_closes_opt_out(store):
    t = _seed_thread(store, sender="a@example.com", body="please unsubscribe me from this list")
    assert store.get_thread(t.thread_id).status == replies.STATUS_NEW
    res = reply_classifier.classify_pending(store=store)
    assert res.auto_closed == 1
    assert res.rules_fired["opt_out"] == 1
    assert store.get_thread(t.thread_id).status == replies.STATUS_CLOSED


def test_classify_pending_skips_positive(store):
    t = _seed_thread(store, sender="b@example.com",
                     body="Yes please send the deck and let's chat next week")
    res = reply_classifier.classify_pending(store=store)
    assert res.auto_closed == 0
    assert store.get_thread(t.thread_id).status == replies.STATUS_NEW


def test_classify_pending_handles_mixed_batch(store):
    a = _seed_thread(store, sender="a@example.com", body="please unsubscribe")
    b = _seed_thread(store, sender="b@example.com", body="I am out of the office until Monday")
    c = _seed_thread(store, sender="c@example.com", body="What does pricing look like?")
    d = _seed_thread(store, sender="d@example.com", body="MAILER-DAEMON: undeliverable")

    res = reply_classifier.classify_pending(store=store)
    assert res.auto_closed == 3
    assert res.rules_fired["opt_out"] == 1
    assert res.rules_fired["out_of_office"] == 1
    assert res.rules_fired["hard_bounce"] == 1

    statuses = {tid.sender_email: store.get_thread(tid.thread_id).status
                for tid in (a, b, c, d)}
    assert statuses["a@example.com"] == replies.STATUS_CLOSED
    assert statuses["b@example.com"] == replies.STATUS_CLOSED
    assert statuses["c@example.com"] == replies.STATUS_NEW   # positive stays open
    assert statuses["d@example.com"] == replies.STATUS_CLOSED


def test_classify_pending_dry_run_does_not_close(store):
    t = _seed_thread(store, sender="e@example.com", body="please unsubscribe")
    res = reply_classifier.classify_pending(store=store, dry_run=True)
    assert res.auto_closed == 1            # would-close count
    assert store.get_thread(t.thread_id).status == replies.STATUS_NEW   # untouched


# --- recipe wrapper ---------------------------------------------------------

def _load_recipe_module():
    repo_root = Path(__file__).resolve().parent.parent
    src = repo_root / "recipes" / "reply_classifier_sweep.py"
    spec = importlib.util.spec_from_file_location("operator_recipes_reply_classifier_sweep", src)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _ctx(**ov):
    base = dict(
        recipe_name="reply_classifier_sweep",
        correlation_id="t",
        env={}, clients={}, cost_so_far=0.0, cost_budget_usd=0.0, dry_run=False,
    )
    base.update(ov)
    return RecipeContext(**base)


def test_recipe_metadata():
    mod = _load_recipe_module()
    r = mod.ReplyClassifierSweep()
    assert r.name == "reply_classifier_sweep"
    assert r.schedule == "*/15 * * * *"


def test_recipe_quiet_when_disabled(monkeypatch):
    mod = _load_recipe_module()
    monkeypatch.setenv("OPERATOR_REPLY_AUTO_CLASSIFY", "0")
    rec = mod.ReplyClassifierSweep()
    result = asyncio.run(rec.query(_ctx()))
    assert result["enabled"] is False
    assert asyncio.run(rec.format(_ctx(), result)) == ""


def test_recipe_quiet_on_zero_closures(monkeypatch):
    mod = _load_recipe_module()
    from operator_core import reply_classifier as rc
    monkeypatch.setattr(rc, "classify_pending",
                         lambda **kw: rc.ClassifierResult(
                             inspected=0, auto_closed=0, rules_fired={"opt_out": 0}, thread_ids_closed=[]))
    rec = mod.ReplyClassifierSweep()
    result = asyncio.run(rec.query(_ctx()))
    assert result["auto_closed"] == 0
    assert asyncio.run(rec.format(_ctx(), result)) == ""


def test_recipe_emits_when_closures_happen(monkeypatch):
    mod = _load_recipe_module()
    from operator_core import reply_classifier as rc
    monkeypatch.setattr(rc, "classify_pending",
                         lambda **kw: rc.ClassifierResult(
                             inspected=10, auto_closed=3,
                             rules_fired={"opt_out": 2, "out_of_office": 1, "hard_bounce": 0},
                             thread_ids_closed=["a", "b", "c"]))
    rec = mod.ReplyClassifierSweep()
    result = asyncio.run(rec.query(_ctx()))
    msg = asyncio.run(rec.format(_ctx(), result))
    assert "auto-closed: 3" in msg
    assert "opt_out: 2" in msg
    assert "out_of_office: 1" in msg


def test_recipe_dry_run_does_not_post(monkeypatch):
    mod = _load_recipe_module()
    from operator_core import reply_classifier as rc
    monkeypatch.setattr(rc, "classify_pending",
                         lambda **kw: rc.ClassifierResult(
                             inspected=5, auto_closed=2,
                             rules_fired={"opt_out": 2}, thread_ids_closed=[]))
    rec = mod.ReplyClassifierSweep()
    result = asyncio.run(rec.query(_ctx(dry_run=True)))
    assert asyncio.run(rec.format(_ctx(), result)) == ""
