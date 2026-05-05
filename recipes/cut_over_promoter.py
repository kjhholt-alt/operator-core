"""cut_over_promoter -- the very last hands-off step in the Sender Gate flow.

When a product has been audit-report READY for a continuous threshold
duration (default 24h), this recipe opens a PR on the product's repo
that flips the cut-over flag from audit-only to live (i.e.
``OUTREACH_COMMON_AUDIT_ONLY=true`` -> ``false``, AND
``ROUTE_VIA_OUTREACH_COMMON=false`` -> ``true``).

The PR is small, one-file, and titled clearly so the operator can
review in 30 seconds and merge. The recipe NEVER auto-merges this PR --
flipping a product to live is the one decision we want a human to
sign off on. Auto-suppression PRs in the same loop ARE auto-merged
because they're additive (just expanding a block list); a flag flip
changes the runtime behavior of a product in production.

Opt-in via ``OPERATOR_CUTOVER_PROMOTER=1``. Off by default.

Per-product config lives in a JSON map at ``OPERATOR_CUTOVER_PROMOTER_CONFIG``
(default: ``~/.operator/data/outreach/cutover_targets.json``). Shape::

    {
      "oe": {
        "repo": "kjhholt-alt/outreach-engine",
        "config_path": ".env.production",
        "audit_only_pattern": "OUTREACH_COMMON_AUDIT_ONLY=true",
        "audit_only_replacement": "OUTREACH_COMMON_AUDIT_ONLY=false",
        "route_pattern": "ROUTE_VIA_OUTREACH_COMMON=false",
        "route_replacement": "ROUTE_VIA_OUTREACH_COMMON=true",
        "base_branch": "main"
      }
    }
"""

from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional

from operator_core import cutover_streak, outreach_audit
from operator_core.recipes import Recipe, RecipeContext, register_recipe


DEFAULT_STREAK_THRESHOLD_SECONDS = 24 * 60 * 60   # 24h
DEFAULT_AUDIT_MATCH_THRESHOLD = 95.0


@register_recipe
class CutOverPromoter(Recipe):
    name = "cut_over_promoter"
    version = "1.0.0"
    description = (
        "Open the flag-flip PR on a product repo once it has been "
        "audit-report READY for a 24h streak. Final hands-off step in "
        "the Sender Gate cut-over loop. Opt-in via OPERATOR_CUTOVER_PROMOTER=1."
    )
    cost_budget_usd = 0.0
    schedule = "0 * * * *"   # every hour
    timeout_sec = 120
    discord_channel = "automations"
    requires_clients = ()
    tags = ("outreach", "cut-over", "hourly")

    async def verify(self, ctx: RecipeContext) -> bool:
        return True

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        if not _is_enabled():
            return {"enabled": False, "checked": [], "promoted": [], "errors": []}

        try:
            threshold_s = int(os.environ.get("OPERATOR_CUTOVER_STREAK_SECONDS", DEFAULT_STREAK_THRESHOLD_SECONDS))
        except ValueError:
            threshold_s = DEFAULT_STREAK_THRESHOLD_SECONDS

        try:
            match_threshold = float(os.environ.get("OPERATOR_CUTOVER_MATCH_PCT", DEFAULT_AUDIT_MATCH_THRESHOLD))
        except ValueError:
            match_threshold = DEFAULT_AUDIT_MATCH_THRESHOLD

        targets = _load_targets()
        if not targets:
            return {"enabled": True, "checked": [], "promoted": [], "errors": ["no targets configured"]}

        # 1. Run audit-report once across all known logs.
        try:
            paths = outreach_audit.default_audit_paths()
            events = list(outreach_audit._iter_events(paths))
        except (FileNotFoundError, OSError) as exc:
            return {"enabled": True, "checked": [], "promoted": [],
                    "errors": [f"audit log unreadable: {exc}"]}

        summaries = outreach_audit.collect(paths)
        by_product = {s.product: s for s in summaries}

        checked: list[dict[str, Any]] = []
        promoted: list[dict[str, Any]] = []
        errors: list[str] = []

        for product, cfg in targets.items():
            summary = by_product.get(product)
            is_ready = bool(summary and summary.cutover_ready(match_threshold))
            streak = cutover_streak.record_check(product, is_ready)
            sec = cutover_streak.streak_seconds(product)
            row = {
                "product": product,
                "ready_now": is_ready,
                "streak_seconds": int(sec),
                "streak_threshold_seconds": threshold_s,
                "already_promoted": bool(streak.promoted_ts),
            }
            checked.append(row)

            if not is_ready:
                continue
            if streak.promoted_ts:
                continue
            if sec < threshold_s:
                continue

            # Streak threshold met + not yet promoted. Open the flip PR.
            if ctx.dry_run:
                promoted.append({**row, "dry_run": True, "pr_url": None})
                continue

            token = os.environ.get("GITHUB_TOKEN")
            if not token:
                errors.append(f"{product}: GITHUB_TOKEN not set")
                continue

            try:
                pr_url = _open_flip_pr(token, product, cfg)
            except _PromoterError as exc:
                errors.append(f"{product}: {exc}")
                continue

            cutover_streak.mark_promoted(product, pr_url)
            promoted.append({**row, "pr_url": pr_url})

        return {"enabled": True, "checked": checked, "promoted": promoted, "errors": errors}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        if not result.get("enabled"):
            return ""
        promoted = result.get("promoted") or []
        errors = result.get("errors") or []
        if not promoted and not errors:
            return ""
        lines = ["**cut_over_promoter** -- flag-flip PRs:"]
        for p in promoted:
            tag = " (dry-run)" if p.get("dry_run") else ""
            url = p.get("pr_url") or ""
            lines.append(f"- {p['product']}: streak {p['streak_seconds']}s -> opened PR{tag} {url}")
        for e in errors:
            lines.append(f"- :warning: {e}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _is_enabled() -> bool:
    return os.environ.get("OPERATOR_CUTOVER_PROMOTER", "").strip().lower() in {"1", "true", "yes"}


def _config_path() -> Path:
    override = os.environ.get("OPERATOR_CUTOVER_PROMOTER_CONFIG")
    if override:
        return Path(override)
    return Path.home() / ".operator" / "data" / "outreach" / "cutover_targets.json"


def _load_targets() -> dict[str, dict[str, str]]:
    path = _config_path()
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, str]] = {}
    for product, cfg in raw.items():
        if not isinstance(cfg, dict):
            continue
        # Required keys; skip silently if any missing so a malformed
        # entry doesn't break the whole tick.
        required = ("repo", "config_path", "audit_only_pattern",
                    "audit_only_replacement", "route_pattern", "route_replacement")
        if any(k not in cfg for k in required):
            continue
        out[product] = {**cfg, "base_branch": cfg.get("base_branch", "main")}
    return out


# ---------------------------------------------------------------------------
# GitHub flag-flip PR
# ---------------------------------------------------------------------------

class _PromoterError(Exception):
    """Raised when a flag-flip PR cannot be opened."""


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
        raise _PromoterError(f"HTTP {e.code}: {detail[:300]}")
    except urllib.error.URLError as e:
        raise _PromoterError(f"network: {e}")


def _open_flip_pr(token: str, product: str, cfg: dict[str, str]) -> str:
    """Push a single-file branch + open a PR. Returns the PR URL."""
    from datetime import datetime, timezone

    repo = cfg["repo"]
    file_path = cfg["config_path"]
    base = cfg["base_branch"]

    # 1. Resolve base branch SHA.
    ref = _gh(token, "GET", f"https://api.github.com/repos/{repo}/git/ref/heads/{base}")
    base_sha = ref["object"]["sha"]

    # 2. Fetch the current file contents.
    existing = _gh(token, "GET",
                    f"https://api.github.com/repos/{repo}/contents/{file_path}?ref={base}")
    content_b64 = existing.get("content") or ""
    file_sha = existing.get("sha")
    try:
        text = base64.b64decode(content_b64).decode("utf-8")
    except Exception as exc:
        raise _PromoterError(f"could not decode {file_path}: {exc}")

    # 3. Apply both pattern -> replacement substitutions.
    new_text = text
    changes_made = 0
    for pat_key, repl_key in (
        ("audit_only_pattern", "audit_only_replacement"),
        ("route_pattern", "route_replacement"),
    ):
        pat, repl = cfg[pat_key], cfg[repl_key]
        if pat in new_text and pat != repl:
            new_text = new_text.replace(pat, repl)
            changes_made += 1

    if changes_made == 0:
        raise _PromoterError(
            f"no patterns matched in {file_path} (already flipped?). "
            "Streak will be marked promoted to avoid retrying."
        )

    # 4. Create the new branch.
    ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    branch = f"auto-cutover/{product}-{ts}"
    _gh(token, "POST",
         f"https://api.github.com/repos/{repo}/git/refs",
         {"ref": f"refs/heads/{branch}", "sha": base_sha})

    # 5. Commit the updated file on that branch.
    new_b64 = base64.b64encode(new_text.encode("utf-8")).decode("ascii")
    title = f"chore(cut-over): promote {product} to live (gate routing on)"
    payload = {
        "message": title,
        "content": new_b64,
        "branch": branch,
    }
    if file_sha:
        payload["sha"] = file_sha
    _gh(token, "PUT",
         f"https://api.github.com/repos/{repo}/contents/{file_path}",
         payload)

    # 6. Open the PR.
    body_lines = [
        f"Auto-opened by `operator_core.recipes.cut_over_promoter` after {product} "
        f"stayed audit-report READY for the configured streak duration.",
        "",
        "Patches applied to `" + file_path + "`:",
        f"- `{cfg['audit_only_pattern']}` -> `{cfg['audit_only_replacement']}`",
        f"- `{cfg['route_pattern']}` -> `{cfg['route_replacement']}`",
        "",
        "## Verify before merging",
        "1. Spot-check the diff -- flag values are exactly what you expect.",
        "2. Re-run `operator outreach audit-report` if you want a fresh signal.",
        "3. Merge. The next outreach send will route through outreach-common's gate.",
        "",
        "_This PR is NOT auto-merged. Cut-over to live requires a human click._",
    ]
    pr = _gh(token, "POST",
              f"https://api.github.com/repos/{repo}/pulls",
              {"title": title, "head": branch, "base": base, "body": "\n".join(body_lines)})
    return pr.get("html_url") or f"https://github.com/{repo}/pulls"
