"""auto_merge_suppression -- merge clean auto-suppression PRs.

Closes the very last manual step in the Sender Gate cut-over loop:

  shadow event -> ingest -> auto-classify -> auto-PR -> *AUTO-MERGE* -> done

Opt-in via ``OPERATOR_AUTO_MERGE_SUPPRESSION_PR=1`` (default off, because
auto-merging code into a shared repo is the kind of thing you want to be
deliberate about turning on).

Safety rules baked in:
- Only merges PRs whose head branch starts with ``auto-suppress/``.
  Hand-opened PRs are never touched.
- Only merges PRs with a non-empty diff (defensive: if the branch
  somehow ended up identical to base, skip).
- Only merges when **all** required check-runs are ``success``. Pending,
  failure, or no checks at all => skip and try again next tick.
- Uses GitHub's "squash" merge for clean history.
- Writes a comment on the PR before merging so there's a paper trail.
- On any HTTP error, logs and skips -- never raises out of the recipe.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any, Optional

from operator_core.recipes import Recipe, RecipeContext, register_recipe


OUTREACH_COMMON_REPO = "kjhholt-alt/outreach-common"
AUTO_BRANCH_PREFIX = "auto-suppress/"


@register_recipe
class AutoMergeSuppression(Recipe):
    name = "auto_merge_suppression"
    version = "1.0.0"
    description = (
        "Merge auto-suppress/* PRs on outreach-common when CI is green. "
        "Closes the loop so shadow-mode disagreements flow to merged "
        "without human clicks. Opt-in via OPERATOR_AUTO_MERGE_SUPPRESSION_PR=1."
    )
    cost_budget_usd = 0.0
    schedule = "*/30 * * * *"  # every 30 minutes
    timeout_sec = 90
    discord_channel = "automations"
    requires_clients = ()
    tags = ("outreach", "cut-over", "every-30m")

    async def verify(self, ctx: RecipeContext) -> bool:
        # Recipe is a no-op without GITHUB_TOKEN; that's fine, it just
        # won't do anything when called. Verify always passes so the
        # scheduler keeps it on the schedule.
        return True

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        if os.environ.get("OPERATOR_AUTO_MERGE_SUPPRESSION_PR", "").strip().lower() not in {"1", "true", "yes"}:
            return {"enabled": False, "merged": [], "skipped": [], "errors": []}

        token = os.environ.get("GITHUB_TOKEN")
        if not token:
            ctx.logger.debug("auto_merge.no_token")
            return {"enabled": True, "merged": [], "skipped": [], "errors": ["GITHUB_TOKEN not set"]}

        repo = os.environ.get("OPERATOR_OUTREACH_COMMON_REPO", OUTREACH_COMMON_REPO)
        prs = _list_open_auto_prs(token, repo)
        merged: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        errors: list[str] = []

        for pr in prs:
            head_ref = pr["head"]["ref"]
            number = pr["number"]
            title = pr["title"]

            # Defensive: only auto-suppress branches.
            if not head_ref.startswith(AUTO_BRANCH_PREFIX):
                skipped.append({"pr": number, "reason": "not_auto_branch", "branch": head_ref})
                continue

            head_sha = pr["head"]["sha"]

            # Check CI status.
            ci_state = _combined_status(token, repo, head_sha)
            if ci_state != "success":
                skipped.append({"pr": number, "reason": f"ci_{ci_state}", "branch": head_ref})
                continue

            # Defensive: non-empty diff.
            if pr.get("changed_files", 0) == 0 or pr.get("additions", 0) + pr.get("deletions", 0) == 0:
                skipped.append({"pr": number, "reason": "empty_diff", "branch": head_ref})
                continue

            if ctx.dry_run:
                merged.append({"pr": number, "title": title, "branch": head_ref, "dry_run": True})
                continue

            # Comment, then squash-merge.
            _post_comment(token, repo, number,
                          "Auto-merging via `operator_core.recipes.auto_merge_suppression`. "
                          "Branch is `auto-suppress/*`, CI is green, diff is non-empty.")
            res = _squash_merge(token, repo, number)
            if res.get("error"):
                errors.append(f"PR#{number}: {res['error']}")
                continue
            merged.append({"pr": number, "title": title, "branch": head_ref})

        return {"enabled": True, "merged": merged, "skipped": skipped, "errors": errors}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        if not result.get("enabled"):
            return ""  # quiet when feature is off
        merged = result.get("merged") or []
        errors = result.get("errors") or []
        if not merged and not errors:
            return ""  # nothing to report
        lines = ["**auto_merge_suppression** -- result of this tick:"]
        for m in merged:
            tag = " (dry-run)" if m.get("dry_run") else ""
            lines.append(f"- merged PR #{m['pr']}: `{m['title']}`{tag}")
        for e in errors:
            lines.append(f"- :warning: {e}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# GitHub REST helpers (kept local so this recipe has zero new deps)
# ---------------------------------------------------------------------------

def _gh_request(token: str, method: str, url: str, payload: Optional[dict] = None) -> dict:
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


def _list_open_auto_prs(token: str, repo: str) -> list[dict]:
    res = _gh_request(token, "GET", f"https://api.github.com/repos/{repo}/pulls?state=open&per_page=50")
    if isinstance(res, dict) and res.get("error"):
        return []
    if not isinstance(res, list):
        return []
    return [pr for pr in res if pr.get("head", {}).get("ref", "").startswith(AUTO_BRANCH_PREFIX)]


def _combined_status(token: str, repo: str, sha: str) -> str:
    """Return 'success', 'pending', 'failure', 'none', or 'error'."""
    # Prefer check-runs (newer Actions API) over the older statuses endpoint.
    res = _gh_request(token, "GET", f"https://api.github.com/repos/{repo}/commits/{sha}/check-runs?per_page=50")
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
    return _gh_request(
        token, "POST",
        f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments",
        {"body": body},
    )


def _squash_merge(token: str, repo: str, pr_number: int) -> dict:
    return _gh_request(
        token, "PUT",
        f"https://api.github.com/repos/{repo}/pulls/{pr_number}/merge",
        {"merge_method": "squash"},
    )
