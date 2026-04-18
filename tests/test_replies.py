"""Tests for the reply ledger (Sprint 8, Phase A).

Covers:
  - Adding an inbound reply creates a thread (NEW) and one IN message.
  - A second inbound on the same sender+subject collapses into the same
    thread via normalized thread_id (Re: prefix stripped).
  - save_draft transitions NEW -> DRAFTING, stores one OUT message,
    subsequent save_draft replaces the pending draft (last-write-wins).
  - mark_ready / mark_sent transition status and sent_at is populated.
  - summary() counts unread/drafting/ready + sent_7d.
"""

from __future__ import annotations

import argparse
from pathlib import Path


def test_incoming_creates_thread_and_collapses_subject(tmp_path):
    from operator_core.replies import ReplyStore, STATUS_NEW

    store = ReplyStore(tmp_path / "replies.sqlite3")
    t1 = store.upsert_thread_for_incoming(
        sender_email="kruz@example.com",
        sender_name="Kruz",
        subject="Your outreach email",
        body_md="the link didn't work",
    )
    assert t1.status == STATUS_NEW
    assert t1.sender_email == "kruz@example.com"
    assert t1.subject == "Your outreach email"

    # Re: reply lands on the same thread.
    t2 = store.upsert_thread_for_incoming(
        sender_email="Kruz@Example.com",
        sender_name="Kruz",
        subject="Re: Your outreach email",
        body_md="still broken",
    )
    assert t2.thread_id == t1.thread_id

    messages = store.list_messages(t1.thread_id)
    assert len(messages) == 2
    assert all(m.direction == "in" for m in messages)


def test_save_draft_and_transitions(tmp_path):
    from operator_core.replies import (
        ReplyStore,
        STATUS_DRAFTING,
        STATUS_READY,
        STATUS_SENT,
    )

    store = ReplyStore(tmp_path / "replies.sqlite3")
    thread = store.upsert_thread_for_incoming(
        sender_email="a@b.com",
        sender_name="A",
        subject="hi",
        body_md="hello",
    )
    tid = thread.thread_id

    # Save a draft.
    t2 = store.save_draft(tid, body_md="thanks for the note — broken link fix incoming")
    assert t2.status == STATUS_DRAFTING
    out_messages = [m for m in store.list_messages(tid) if m.direction == "out"]
    assert len(out_messages) == 1
    assert "broken link" in out_messages[0].body_md
    assert out_messages[0].sent_at is None

    # Save a second draft — replaces the first.
    t3 = store.save_draft(
        tid,
        body_md="updated draft after custom DD",
        dd_notes_md="**Sender:** A@B.com — runs Lakeside Fiberglass.",
    )
    assert t3.status == STATUS_DRAFTING
    assert "custom DD" in t3.dd_notes_md or "Lakeside" in t3.dd_notes_md
    out_messages = [m for m in store.list_messages(tid) if m.direction == "out"]
    assert len(out_messages) == 1  # replaced, not appended

    # Ready -> Sent.
    assert store.mark_ready(tid).status == STATUS_READY
    sent_thread = store.mark_sent(tid)
    assert sent_thread.status == STATUS_SENT
    # The out message now has a sent_at.
    out_messages = [m for m in store.list_messages(tid) if m.direction == "out"]
    assert out_messages[0].sent_at is not None


def test_summary_counts_and_sent_7d(tmp_path):
    from operator_core.replies import ReplyStore, STATUS_NEW

    store = ReplyStore(tmp_path / "replies.sqlite3")
    # Two NEW threads.
    store.upsert_thread_for_incoming(
        sender_email="a@b.com", sender_name=None, subject="s1", body_md="x"
    )
    store.upsert_thread_for_incoming(
        sender_email="c@d.com", sender_name=None, subject="s2", body_md="x"
    )
    # Third thread we take all the way to SENT.
    t3 = store.upsert_thread_for_incoming(
        sender_email="e@f.com", sender_name=None, subject="s3", body_md="x"
    )
    store.save_draft(t3.thread_id, body_md="reply")
    store.mark_ready(t3.thread_id)
    store.mark_sent(t3.thread_id)

    summary = store.summary()
    assert summary[STATUS_NEW] == 2
    assert summary["SENT"] == 1
    assert summary["DRAFTING"] == 0
    assert summary["sent_7d"] == 1
    assert summary["unread"] == 2  # NEW + DRAFTING + READY; DRAFTING/READY are 0


def test_close_thread_transitions_to_closed(tmp_path):
    from operator_core.replies import ReplyStore, STATUS_CLOSED

    store = ReplyStore(tmp_path / "replies.sqlite3")
    thread = store.upsert_thread_for_incoming(
        sender_email="close@example.com",
        sender_name="Closer",
        subject="not interested",
        body_md="please remove me",
    )
    closed = store.close_thread(thread.thread_id)
    assert closed.status == STATUS_CLOSED


def test_remote_sync_upserts_thread_and_replaces_message_slice(tmp_path, monkeypatch):
    from operator_core.replies import ReplyStore

    calls: list[tuple[str, str, object]] = []

    class _Resp:
        def raise_for_status(self):
            return None

    def _record(method, url, **kwargs):
        calls.append((method, url, kwargs.get("json")))
        return _Resp()

    monkeypatch.setenv("OPERATOR_REPLY_SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("OPERATOR_REPLY_SUPABASE_KEY", "service-role")
    monkeypatch.setattr("operator_core.replies.requests.post", lambda url, **kwargs: _record("POST", url, **kwargs))
    monkeypatch.setattr("operator_core.replies.requests.delete", lambda url, **kwargs: _record("DELETE", url, **kwargs))

    store = ReplyStore(tmp_path / "replies.sqlite3")
    thread = store.upsert_thread_for_incoming(
        sender_email="sync@example.com",
        sender_name="Sync",
        subject="mirror me",
        body_md="first inbound",
    )
    store.save_draft(thread.thread_id, body_md="draft v1")
    store.save_draft(thread.thread_id, body_md="draft v2", dd_notes_md="keep latest")
    store.mark_ready(thread.thread_id)

    thread_posts = [payload for method, url, payload in calls if method == "POST" and url.endswith("/outreach_reply_threads")]
    message_posts = [payload for method, url, payload in calls if method == "POST" and url.endswith("/outreach_reply_messages")]
    message_deletes = [url for method, url, _ in calls if method == "DELETE" and "outreach_reply_messages" in url]

    assert thread_posts, "thread mirror upsert never ran"
    assert message_posts, "message mirror upsert never ran"
    assert message_deletes, "message mirror replacement delete never ran"

    latest_thread = thread_posts[-1][0]
    assert latest_thread["thread_id"] == thread.thread_id
    assert latest_thread["status"] == "READY"
    assert "latest_draft_preview" not in latest_thread
    latest_messages = message_posts[-1]
    out_bodies = [m["body_md"] for m in latest_messages if m["direction"] == "out"]
    assert out_bodies == ["draft v2"]


def test_replies_parser_exposes_ready_and_close_commands():
    from operator_core.cli import build_parser

    parser = build_parser()
    args = parser.parse_args(["replies", "mark-ready", "abc123"])
    assert isinstance(args, argparse.Namespace)
    assert args.replies_command == "mark-ready"
    assert args.thread_id == "abc123"

    args = parser.parse_args(["replies", "close", "deadbeef"])
    assert args.replies_command == "close"
    assert args.thread_id == "deadbeef"
