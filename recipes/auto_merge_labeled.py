"""auto_merge_labeled -- merge any green PR labeled `auto-merge` on watched repos.

Generalization of ``auto_merge_suppression``. That recipe is purpose-built
for outreach-common's ``auto-suppress/*`` branches; this one fires on any
PR carrying the ``auto-merge`` label across a configurable repo list.

Use cases:
- Dependabot bumps you've already eyeballed: label `auto-merge`, walk away.
- Doc-only PRs from the daemon's own automations.
- Trivial rename / cleanup PRs you opened yourself.

Opt-in via ``OPERATOR_AUTO_MERGE_LABELED=1``. Repo allow-list:
``OPERATOR_AUTO_MERGE_LABELED_REPOS`` (comma-separated owner/repo).
Default repo list is just ``kjhholt-alt/operator-core`` so this never
silently sprawls onto other repos.

Defensive rules (same as auto_merge_suppression):
- PR must carry the ``auto-merge`` label.
- Combined check-runs status must be ``success`` (pending / failure /
  none all skip).
- PR must have a non-empty diff.
- Posts a paper-trail comment before the merge.
- Squash-merge for clean history.
- Any HTTP error is logged and skipped -- never raises.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any, Optional

from operator_core.recipes import Recipe, RecipeContext, register_recipe


DEFAULT_REPOS = ("kjhholt-alt/operator-core",)
AUTO_LABEL = "auto-merge"


@register_recipe
class AutoMergeLabeled(Recipe):
    name = "auto_merge_labeled"
    version = "1.0.0"
    description = (
        "Merge any green PR carrying the `auto-merge` label on watched repos. "
        "Generalization of auto_merge_suppression -- works on any repo, any "
        "branch, gated by label. Opt-in via OPERATOR_AUTO_MERGE_LABELED=1."
    )
    cost_budget_usd = 0.0
    schedule = "*/15 * * * *"
    timeout_sec = 120
    discord_channel = "automations"
    requires_clients = ()
    tags = ("repo-ops", "every-15m")

    async def verify(self, ctx: RecipeContext) -> bool:
        return True

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        if os.environ.get("OPERATOR_AUTO_MERGE_LABELED", "").strip().lower() not in {"1", "true", "yes"}:
            return {"enabled": False, "merged": [], "skipped": [], "errors": []}

        token = os.environ.get("GITHUB_TOKEN")
        if not token:
            ctx.logger.debug("auto_merge_labeled.no_token")
            return {"enabled": True, "merged": [], "skipped": [], "errors": ["GITHUB_TOKEN not set"]}

        repos = _resolve_repos()
        merged: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        errors: list[str] = []

        for repo in repos:
            try:
                prs = _list_labeled_open_prs(token, repo, AUTO_LABEL)
            except _AutoMergeError as exc:
                errors.append(f"{repo}: list_prs: {exc}")
                continue
            for pr in prs:
                number = pr["number"]
                title = pr["title"]
                head_sha = pr["head"]["sha"]
                head_ref = pr["head"]["ref"]

                # CI gate.
                ci_state = _combined_status(token, repo, head_sha)
                if ci_state != "success":
                    skipped.append({"repo": repo, "pr": number, "reason": f"ci_{ci_state}"})
                    continue

                # Non-empty diff gate.
                if (pr.get("changed_files", 0) == 0
                        or pr.get("additions", 0) + pr.get("deletions", 0) == 0):
                    skipped.append({"repo": repo, "pr": number, "reason": "empty_diff"})
                    continue

                if ctx.dry_run:
                    merged.append({"repo": repo, "pr": number, "title": title,
                                   "branch": head_ref, "dry_run": True})
                    continue

                _post_comment(token, repo, number,
                              "Auto-merging via `operator_core.recipes.auto_merge_labeled`. "
                              f"Label `{AUTO_LABEL}` present, CI is green, diff is non-empty.")
                res = _squash_merge(token, repo, number)
                if isinstance(res, dict) and res.get("error"):
                    errors.append(f"{repo} PR#{number}: merge: {res.get('detail') or res.get('error')}")
                    continue
                merged.append({"repo": repo, "pr": number, "title": title, "branch": head_ref})

        return {"enabled": True, "merged": merged, "skipped": skipped, "errors": errors}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        if not result.get("enabled"):
            return ""
        merged = result.get("merged") or []
        errors = result.get("errors") or []
        if not merged and not errors:
            return ""
        lines = ["**auto_merge_labeled** -- this tick:"]
        for m in merged:
            tag = " (dry-run)" if m.get("dry_run") else ""
            lines.append(f"- merged `{m['repo']}` PR #{m['pr']}: {m['title']}{tag}")
        for e in errors:
            lines.append(f"- :warning: {e}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _AutoMergeError(Exception):
    """Raised by _list_labeled_open_prs only -- merge errors stay as dicts."""


def _resolve_repos() -> tuple[str, ...]:
    raw = os.environ.get("OPERATOR_AUTO_MERGE_LABELED_REPOS", "").strip()
    if not raw:
        return DEFAULT_REPOS
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return tuple(parts) if parts else DEFAULT_REPOS


def _gh(token: str, method: str, url: str, payload: Optional[dict] = None) -> dict:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url, data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read().decode("utf-8") or "{}"
            return json.loads(data)
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode("utf-8")
        except Exception:
            detail = str(e)
        return {"error": True, "status": e.code, "detail": detail[:500]}
    except urllib.error.URLError as e:
        return {"error": True, "detail": str(e)}


def _list_labeled_open_prs(token: str, repo: str, label: str) -> list[dict]:
    """Return open PRs in `repo` carrying `label`."""
    res = _gh(token, "GET",
              f"https://api.github.com/repos/{repo}/pulls?state=open&per_page=50")
    if isinstance(res, dict) and res.get("error"):
        raise _AutoMergeError(str(res.get("detail") or res.get("status")))
    if not isinstance(res, list):
        return []
    out = []
    for pr in res:
        labels = pr.get("labels") or []
        names = {lbl.get("name") for lbl in labels if isinstance(lbl, dict)}
        if label in names:
            out.append(pr)
    return out


def _combined_status(token: str, repo: str, sha: str) -> str:
    res = _gh(token, "GET",
              f"https://api.github.com/repos/{repo}/commits/{sha}/check-runs?per_page=50")
    if isinstance(res, dict) and res.get("error"):
        return "error"
    runs = (res or {}).get("check_runs") or []
    if not runs:
        return "none"
    if any(r.get("status") != "completed" for r in runs):
        return "pending"
    if all(r.get("conclusion") in {"success", "skipped", "neutral"} for r in runs):
        return "success"
    return "failure"


def _post_comment(token: str, repo: str, pr_number: int, body: str) -> dict:
    return _gh(token, "POST",
               f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments",
               {"body": body})


def _squash_merge(token: str, repo: str, pr_number: int) -> dict:
    return _gh(token, "PUT",
               f"https://api.github.com/repos/{repo}/pulls/{pr_number}/merge",
               {"merge_method": "squash"})
