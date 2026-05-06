"""weekly_review -- spot-check autonomously merged PRs from the last 7 days."""

from __future__ import annotations

import datetime as dt
import html
import importlib.util
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


ORG = "kjhholt-alt"


def _war_room_dir() -> Path:
    override = os.environ.get("OPERATOR_WAR_ROOM_DIR")
    if override:
        return Path(override)
    projects_override = os.environ.get("OPERATOR_PROJECTS_DIR")
    if projects_override:
        return Path(projects_override) / "war-room"
    home_projects = Path.home() / "Desktop" / "Projects"
    if home_projects.exists():
        return home_projects / "war-room"
    repo_projects = Path(__file__).resolve().parents[2]
    if (repo_projects / "operator-core").exists():
        return repo_projects / "war-room"
    return Path.cwd() / "war-room"


WAR_ROOM_DIR = _war_room_dir()
WEEKLY_REVIEW_HTML = WAR_ROOM_DIR / "weekly-review.html"
WEEKLY_REVIEW_JSON = WAR_ROOM_DIR / "weekly-review.json"


def _run_gh(args: list[str], *, timeout: float = 60.0) -> tuple[int, str, str]:
    gh = shutil.which("gh") or "gh"
    try:
        res = subprocess.run(
            [gh, *args],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )
        return res.returncode, res.stdout, res.stderr
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 124, "", str(exc)


def _iso_utc(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _repo_name(pr: dict[str, Any]) -> str:
    repo = pr.get("repository") or {}
    if isinstance(repo, dict):
        return str(repo.get("nameWithOwner") or repo.get("fullName") or repo.get("name") or "")
    return str(repo or "")


def _repo_from_api_url(value: str) -> str:
    marker = "/repos/"
    if marker not in value:
        return ""
    return value.split(marker, 1)[1].strip("/")


def _repo_short(repo: str) -> str:
    return repo.split("/", 1)[1] if "/" in repo else repo


def _size(pr: dict[str, Any]) -> int:
    return int(pr.get("additions") or 0) + int(pr.get("deletions") or 0)


def _reviewer_login(review: dict[str, Any]) -> str:
    user = review.get("user") if isinstance(review.get("user"), dict) else {}
    return str(user.get("login") or review.get("author", {}).get("login") or "")


def _has_human_review(reviews: list[dict[str, Any]]) -> bool:
    for review in reviews:
        login = _reviewer_login(review)
        user = review.get("user") if isinstance(review.get("user"), dict) else {}
        user_type = str(user.get("type") or "").lower()
        if login and not login.endswith("[bot]") and user_type != "bot":
            return True
    return False


def _fetch_reviews(repo: str, number: int) -> list[dict[str, Any]]:
    rc, out, _err = _run_gh(
        ["api", f"repos/{repo}/pulls/{number}/reviews", "--paginate"],
        timeout=30,
    )
    if rc != 0 or not out.strip():
        return []
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else []


def _fetch_pr_stats(repo: str, number: int) -> dict[str, Any]:
    rc, out, _err = _run_gh(["api", f"repos/{repo}/pulls/{number}"], timeout=30)
    if rc != 0 or not out.strip():
        return {}
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return {}
    return {
        "additions": int(data.get("additions") or 0),
        "deletions": int(data.get("deletions") or 0),
        "changedFiles": int(data.get("changed_files") or data.get("changedFiles") or data.get("files") or 0),
        "mergedAt": str(data.get("merged_at") or data.get("mergedAt") or ""),
    }


def _collect_merged_prs(since: dt.datetime, *, limit: int = 100) -> list[dict[str, Any]]:
    query = f"org:{ORG} is:pr is:merged merged:>={since.date().isoformat()}"
    rc, out, _err = _run_gh(
        [
            "api",
            "search/issues",
            "-f",
            f"q={query}",
            "-f",
            f"per_page={limit}",
        ],
        timeout=90,
    )
    if rc != 0 or not out.strip():
        return []
    try:
        data = json.loads(out)
    except json.JSONDecodeError:
        return []
    rows = data.get("items", []) if isinstance(data, dict) else []
    out_rows: list[dict[str, Any]] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        repo = _repo_from_api_url(str(row.get("repository_url") or ""))
        out_rows.append({
            "number": row.get("number"),
            "title": row.get("title"),
            "repository": repo,
            "mergedAt": row.get("closed_at"),
            "url": row.get("html_url") or row.get("url"),
            "author": row.get("user"),
        })
    return out_rows


def _enrich_pr(pr: dict[str, Any]) -> dict[str, Any]:
    repo = _repo_name(pr)
    number = int(pr.get("number") or 0)
    if repo and number and any(pr.get(k) is None for k in ("additions", "deletions", "changedFiles")):
        pr = {**pr, **_fetch_pr_stats(repo, number)}
    reviews = _fetch_reviews(repo, number) if repo and number else []
    reviewed = _has_human_review(reviews)
    return {
        "repo": repo,
        "repo_short": _repo_short(repo),
        "number": number,
        "title": str(pr.get("title") or ""),
        "url": str(pr.get("url") or ""),
        "merged_at": str(pr.get("mergedAt") or pr.get("merged_at") or ""),
        "additions": int(pr.get("additions") or 0),
        "deletions": int(pr.get("deletions") or 0),
        "files": int(pr.get("changedFiles") or pr.get("files") or 0),
        "reviews": len(reviews),
        "human_reviewed": reviewed,
        "classification": "human-reviewed" if reviewed else "auto-merged",
    }


def _fallback_html(result: dict[str, Any]) -> str:
    auto_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(pr['repo_short'])}</td>"
        f"<td>#{int(pr['number'])}</td>"
        f"<td>{html.escape(pr['title'])}</td>"
        f"<td>{int(pr['additions'])}</td>"
        f"<td>{int(pr['deletions'])}</td>"
        f"<td>{int(pr['files'])}</td>"
        f"<td>{html.escape(pr['merged_at'])}</td>"
        "</tr>"
        for pr in result.get("auto_merged", [])
    )
    reviewed_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(pr['repo_short'])}</td>"
        f"<td>#{int(pr['number'])}</td>"
        f"<td>{html.escape(pr['title'])}</td>"
        f"<td>{int(pr['reviews'])}</td>"
        f"<td>{html.escape(pr['merged_at'])}</td>"
        "</tr>"
        for pr in result.get("human_reviewed", [])
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Weekly Review</title>
  <style>
    body {{ font-family: Segoe UI, Arial, sans-serif; margin: 32px; color: #17202a; }}
    table {{ border-collapse: collapse; width: 100%; margin: 16px 0 32px; }}
    th, td {{ border-bottom: 1px solid #ddd; padding: 8px; text-align: left; }}
    .kpis {{ display: flex; gap: 16px; margin: 16px 0; }}
    .kpi {{ border: 1px solid #ddd; padding: 12px; min-width: 120px; }}
  </style>
</head>
<body>
  <h1>Weekly Review</h1>
  <p>{html.escape(result.get("window", ""))}</p>
  <div class="kpis">
    <div class="kpi"><strong>{result.get("total", 0)}</strong><br>Total merged</div>
    <div class="kpi"><strong>{len(result.get("auto_merged", []))}</strong><br>Auto-merged</div>
    <div class="kpi"><strong>{len(result.get("human_reviewed", []))}</strong><br>Reviewed</div>
  </div>
  <h2>Largest auto-merged PRs</h2>
  <table><thead><tr><th>Repo</th><th>PR</th><th>Title</th><th>+</th><th>-</th><th>Files</th><th>Merged</th></tr></thead><tbody>{auto_rows}</tbody></table>
  <h2>Human-reviewed PRs</h2>
  <table><thead><tr><th>Repo</th><th>PR</th><th>Title</th><th>Reviews</th><th>Merged</th></tr></thead><tbody>{reviewed_rows}</tbody></table>
</body>
</html>
"""


def _write_html(result: dict[str, Any], out: Path | None = None) -> Path:
    out = out or WEEKLY_REVIEW_HTML
    out.parent.mkdir(parents=True, exist_ok=True)
    if importlib.util.find_spec("dashboards") is not None:
        from dashboards import Dashboard, render  # type: ignore

        d = Dashboard(
            "Weekly Review",
            subtitle=result.get("window", ""),
            theme="palantir",
        )
        d.section("Overview", layout="grid")
        d.kpi("Merged PRs", result.get("total", 0), tone="neutral")
        d.kpi("Auto-merged", len(result.get("auto_merged", [])), tone="warn")
        d.kpi("Human-reviewed", len(result.get("human_reviewed", [])), tone="good")
        d.section("Largest auto-merged PRs")
        auto_rows = [
            [pr["repo_short"], f"#{pr['number']}", pr["title"], str(_size(pr)), str(pr["files"]), pr["merged_at"]]
            for pr in result.get("auto_merged", [])[:25]
        ]
        if auto_rows:
            d.table(headers=["Repo", "PR", "Title", "Size", "Files", "Merged"], rows=auto_rows)
        else:
            d.callout("No auto-merged PRs in the review window.", tone="good")
        d.section("Human-reviewed PRs")
        reviewed_rows = [
            [pr["repo_short"], f"#{pr['number']}", pr["title"], str(pr["reviews"]), pr["merged_at"]]
            for pr in result.get("human_reviewed", [])[:25]
        ]
        if reviewed_rows:
            d.table(headers=["Repo", "PR", "Title", "Reviews", "Merged"], rows=reviewed_rows)
        else:
            d.callout("No human-reviewed PRs in the review window.", tone="neutral")
        render(d.build(), "html", out=out)
    else:
        out.write_text(_fallback_html(result), encoding="utf-8")
    return out


def _write_json(result: dict[str, Any], out: Path | None = None) -> Path:
    out = out or WEEKLY_REVIEW_JSON
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return out


@register_recipe
class WeeklyReview(Recipe):
    name = "weekly_review"
    version = "1.0.0"
    description = "Surface merged PRs from the last 7 days that had no human review"
    cost_budget_usd = 0.0
    schedule = "0 9 * * 0"
    timeout_sec = 300
    discord_channel = "automations"
    requires_clients = ("discord",)
    tags = ("weekly", "review", "code-review")

    async def verify(self, ctx: RecipeContext) -> bool:
        if shutil.which("gh") is None:
            ctx.logger.warning("weekly_review.no_gh_cli")
            return False
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        now = dt.datetime.now(dt.timezone.utc)
        since = now - dt.timedelta(days=7)
        prs = [_enrich_pr(pr) for pr in _collect_merged_prs(since)]
        return {"generated_at": _iso_utc(now), "since": _iso_utc(since), "prs": prs}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        prs = list(data.get("prs", []))
        auto = [pr for pr in prs if not pr.get("human_reviewed")]
        reviewed = [pr for pr in prs if pr.get("human_reviewed")]
        auto.sort(key=lambda pr: (_size(pr), int(pr.get("files") or 0)), reverse=True)
        reviewed.sort(key=lambda pr: pr.get("merged_at", ""), reverse=True)
        return {
            **data,
            "total": len(prs),
            "auto_merged": auto,
            "human_reviewed": reviewed,
            "window": f"{data.get('since', '')} to {data.get('generated_at', '')}",
        }

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        html_path = _write_html(result)
        json_path = _write_json(result)
        auto = result.get("auto_merged", [])
        reviewed = result.get("human_reviewed", [])
        lines = [
            f"**Weekly review** -- {result.get('total', 0)} merged PR(s) in last 7 days",
            f"- auto-merged/no human review: {len(auto)}",
            f"- human-reviewed: {len(reviewed)}",
            f"- cockpit: http://127.0.0.1:8765/cockpit#review",
            f"- artifacts: {json_path.name}, {html_path.name}",
        ]
        if auto:
            lines.append("")
            lines.append("Largest auto-merged PRs:")
            for pr in auto[:10]:
                lines.append(
                    f"- {pr['repo_short']} #{pr['number']}: {pr['title'][:90]} "
                    f"(+{pr['additions']}/-{pr['deletions']}, {pr['files']} files)"
                )
        body = "\n".join(lines)
        return body[:1990]
