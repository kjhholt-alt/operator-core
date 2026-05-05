"""Auto-suppression PR builder.

When a `would_block_new` review item is resolved as `approved_gate`, the
business should join the canonical block list permanently so the gate
keeps blocking it regardless of which product attempts the send.

This module:
  1. Walks the gate_review queue for approved_gate rows still in
     `pending` suppression state.
  2. Extracts business names (the gate-block-label is the source).
  3. Writes a patch to outreach-common's `config/network_scrub.yml`.
  4. Optionally opens a PR via GitHub REST.
  5. Marks the queue rows as `suppressed`.

Intentionally does NOT auto-merge. Human review on the PR is the safety
gate.
"""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from . import gate_review

OUTREACH_COMMON_REPO = "kjhholt-alt/outreach-common"
DEFAULT_BRANCH = "master"
SCRUB_YML_PATH = "config/network_scrub.yml"


@dataclass
class SuppressionPlan:
    """A buildable patch that turns N queue items into a network_scrub PR."""
    items: List[gate_review.ReviewItem]
    new_business_names: List[str]
    pr_title: str
    pr_body: str
    branch_name: str
    yml_content: str  # full file contents to commit


def _extract_business_name(item: gate_review.ReviewItem) -> Optional[str]:
    """Prefer the explicit business_name; fall back to parsing the gate label."""
    if item.business_name:
        return item.business_name.strip()
    if not item.gate_block_label:
        return None
    # label shape: "network_scrub:business_name:all around town"
    parts = item.gate_block_label.split(":", 2)
    if len(parts) == 3 and parts[1] == "business_name":
        return parts[2].strip()
    return None


def _candidate_items(db_path: Optional[Path] = None) -> List[gate_review.ReviewItem]:
    """approved_gate items where the gate decided to BLOCK (would_block_new)."""
    out: List[gate_review.ReviewItem] = []
    with gate_review.open_db(db_path) as conn:
        cur = conn.execute(
            "SELECT * FROM review_items "
            "WHERE status='approved_gate' AND agreement='would_block_new' "
            "ORDER BY first_seen_ts ASC"
        )
        for row in cur.fetchall():
            out.append(gate_review.ReviewItem(**{k: row[k] for k in row.keys()}))
    return out


def _read_existing_yml(path: Path) -> str:
    """Return the file contents, or a minimal seed if it doesn't exist."""
    if path.is_file():
        return path.read_text(encoding="utf-8")
    return (
        "# network_scrub.yml -- auto-managed by `operator outreach suppression-pr`.\n"
        "# Hand-edit anything outside the `business_names:` list freely;\n"
        "# the auto-suppression flow only appends new entries.\n"
        "scrub:\n"
        "  business_names:\n"
    )


def _add_business_names(yml: str, names_to_add: Iterable[str]) -> tuple[str, List[str]]:
    """Insert names under scrub.business_names. Returns (new_yml, actually_added).

    Idempotent: names already present are skipped.
    """
    # Match list items quoted with " or ' or unquoted.
    existing_lower = set()
    for m in re.finditer(
        r'''^\s*-\s*(?:"([^"\n]+)"|'([^'\n]+)'|([^\s'"#][^\n#]*?))\s*(?:#.*)?$''',
        yml,
        re.MULTILINE,
    ):
        val = m.group(1) or m.group(2) or m.group(3) or ""
        existing_lower.add(val.strip().lower())

    actually_added = []
    for name in names_to_add:
        if name.lower() not in existing_lower:
            actually_added.append(name)
            existing_lower.add(name.lower())

    if not actually_added:
        return yml, actually_added

    # Find the business_names: anchor; insert under it.
    anchor = re.search(r"^\s*business_names:\s*$", yml, re.MULTILINE)
    if not anchor:
        # Append a fresh block at the end of file.
        block = "\n  business_names:\n"
        for name in actually_added:
            block += f'    - "{name}"\n'
        return yml.rstrip() + block, actually_added

    # Insert each new entry right after the business_names: line.
    insert_at = anchor.end()
    new_lines = "".join(f'    - "{name}"\n' for name in actually_added)
    out = yml[:insert_at] + "\n" + new_lines + yml[insert_at:]
    # Strip the extra leading newline if business_names: was already followed by entries
    # (no perfect way without a yaml parser, but the pattern stays valid).
    return out, actually_added


def build_plan(scrub_yml_path: Path,
               db_path: Optional[Path] = None) -> Optional[SuppressionPlan]:
    """Build a patch from currently-eligible queue items. Returns None when there's
    nothing to do."""
    candidates = _candidate_items(db_path)
    if not candidates:
        return None
    names = []
    items_with_name = []
    for item in candidates:
        name = _extract_business_name(item)
        if name:
            names.append(name)
            items_with_name.append(item)
    if not names:
        return None

    base_yml = _read_existing_yml(scrub_yml_path)
    new_yml, actually_added = _add_business_names(base_yml, names)
    if not actually_added:
        return None

    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    branch = f"auto-suppress/{ts}"
    title = f"chore(scrub): add {len(actually_added)} business name(s) from gate-review approvals"
    body_lines = [
        "Auto-generated by `operator outreach suppression-pr`. Each entry is a",
        "business that gate-review marked `approved_gate` (the gate was right",
        "to block; legacy let it through).",
        "",
        "## Names added",
    ]
    for item in items_with_name:
        name = _extract_business_name(item)
        body_lines.append(
            f"- `{name}` -- product `{item.product}`, "
            f"first seen {item.first_seen_ts}, "
            f"hit_count {item.hit_count}"
        )
        if item.resolution_note:
            body_lines.append(f"  - note: {item.resolution_note}")
    body_lines.extend([
        "",
        "## Verify",
        "1. Confirm each name belongs on the canonical block list (gate is right).",
        "2. Merge this PR.",
        "3. Adapters with shadow mode on will see `match` instead of `would_block_new` next batch.",
    ])
    return SuppressionPlan(
        items=items_with_name,
        new_business_names=actually_added,
        pr_title=title,
        pr_body="\n".join(body_lines),
        branch_name=branch,
        yml_content=new_yml,
    )


def mark_items_suppressed(items: Iterable[gate_review.ReviewItem],
                          db_path: Optional[Path] = None) -> int:
    """After a successful PR open, flip these queue rows to `suppressed`."""
    n = 0
    for item in items:
        gate_review.resolve(
            item.id, "suppressed",
            note="auto-suppression PR opened",
            resolved_by="operator-core/suppression_pr",
            db_path=db_path,
        )
        n += 1
    return n


# ---------------------------------------------------------------------------
# GitHub REST integration. Only called when --open-pr is passed; bare
# `build-only` mode prints the patch + branch name without touching GitHub.
# ---------------------------------------------------------------------------


def _gh_request(token: str, method: str, url: str,
                payload: Optional[dict] = None) -> dict:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req) as r:
            text = r.read().decode("utf-8")
            return json.loads(text) if text else {}
    except urllib.error.HTTPError as e:
        return {"error": True, "status": e.code, "body": e.read().decode("utf-8")}


def open_pr(plan: SuppressionPlan,
            *,
            token: Optional[str] = None,
            repo: str = OUTREACH_COMMON_REPO,
            base: str = DEFAULT_BRANCH,
            scrub_path: str = SCRUB_YML_PATH) -> dict:
    """Push branch + commit + open PR via GitHub REST. Returns dict with
    PR URL or error detail. Caller should verify the response before
    marking queue items suppressed."""
    token = token or os.environ.get("GITHUB_TOKEN")
    if not token:
        return {"error": True, "message": "GITHUB_TOKEN not set"}

    # 1. Resolve base branch SHA
    ref = _gh_request(token, "GET", f"https://api.github.com/repos/{repo}/git/ref/heads/{base}")
    if ref.get("error"):
        return ref
    base_sha = ref["object"]["sha"]

    # 2. Create the new branch
    create_ref = _gh_request(
        token, "POST",
        f"https://api.github.com/repos/{repo}/git/refs",
        {"ref": f"refs/heads/{plan.branch_name}", "sha": base_sha},
    )
    if create_ref.get("error"):
        return create_ref

    # 3. Get current file SHA (if exists) for update; else fresh create
    file_path = scrub_path
    existing = _gh_request(
        token, "GET",
        f"https://api.github.com/repos/{repo}/contents/{file_path}?ref={base}",
    )
    file_sha = existing.get("sha") if not existing.get("error") else None

    import base64
    content_b64 = base64.b64encode(plan.yml_content.encode("utf-8")).decode("ascii")
    commit_payload = {
        "message": plan.pr_title,
        "content": content_b64,
        "branch": plan.branch_name,
    }
    if file_sha:
        commit_payload["sha"] = file_sha
    put = _gh_request(
        token, "PUT",
        f"https://api.github.com/repos/{repo}/contents/{file_path}",
        commit_payload,
    )
    if put.get("error"):
        return put

    # 4. Open the PR
    pr_payload = {
        "title": plan.pr_title,
        "head": plan.branch_name,
        "base": base,
        "body": plan.pr_body,
    }
    pr = _gh_request(
        token, "POST",
        f"https://api.github.com/repos/{repo}/pulls",
        pr_payload,
    )
    return pr
