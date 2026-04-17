"""Automated PR review recipe (Sprint 7, Phase E).

Walks every tracked project's GitHub repo, finds open pull requests we
haven't already reviewed, asks Claude for a review, and posts it as an
issue comment (safer than a real review submission — never auto-approves
or requests-changes on someone's work).

State file `<data_dir>/pr_review_state.json` maps repo → list of seen
PR numbers so we don't double-comment.

Every external call is timeboxed and degrades gracefully — the daemon
runs this unattended and must never hang or crash the scheduler.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_MODEL = "claude-sonnet-4-6"
GITHUB_API = "https://api.github.com"


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OpenPR:
    repo: str
    number: int
    title: str
    head_sha: str
    diff_url: str


# ---------------------------------------------------------------------------
# GitHub REST helpers (timeboxed, never raise)
# ---------------------------------------------------------------------------


def _gh_request(
    path: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    token: str | None = None,
    accept: str = "application/vnd.github+json",
    timeout: float = 5.0,
    url_fetcher=None,
) -> Any:
    """Thin wrapper around urllib for the GitHub REST API. Returns parsed
    JSON on 2xx, None on anything else. Never raises."""
    from urllib.request import Request, urlopen

    token = token if token is not None else os.environ.get("GITHUB_TOKEN", "")
    url = path if path.startswith("http") else f"{GITHUB_API}{path}"
    data = None
    headers = {
        "Accept": accept,
        "User-Agent": "OperatorCore-PRReview",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=headers, method=method)
    opener = url_fetcher or urlopen
    try:
        with opener(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return None
            try:
                return json.loads(raw)
            except ValueError:
                return raw
    except Exception:  # noqa: BLE001 — callers tolerate None
        return None


def list_open_prs(
    settings, *, limit: int = 30, url_fetcher=None
) -> list[OpenPR]:
    """Return OpenPR rows for every tracked project's GitHub repo."""
    out: list[OpenPR] = []
    for p in getattr(settings, "projects", []) or []:
        repo = getattr(p, "repo", "")
        if not repo or "/" not in repo:
            continue
        data = _gh_request(
            f"/repos/{repo}/pulls?state=open&per_page={limit}",
            url_fetcher=url_fetcher,
        )
        if not isinstance(data, list):
            continue
        for item in data:
            try:
                out.append(
                    OpenPR(
                        repo=repo,
                        number=int(item["number"]),
                        title=str(item.get("title") or ""),
                        head_sha=str(
                            (item.get("head") or {}).get("sha") or ""
                        ),
                        diff_url=str(item.get("diff_url") or ""),
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
    return out


def _fetch_diff_text(repo: str, number: int, *, url_fetcher=None) -> str:
    """Best-effort diff fetch. Truncated to 20k chars to keep prompts cheap."""
    data = _gh_request(
        f"/repos/{repo}/pulls/{number}",
        accept="application/vnd.github.v3.diff",
        url_fetcher=url_fetcher,
    )
    if not isinstance(data, str):
        return ""
    if len(data) > 20_000:
        return data[:20_000] + "\n\n... (diff truncated)"
    return data


REVIEW_SYSTEM = (
    "You are a pragmatic senior code reviewer. You read diffs and write "
    "short, specific comments. Call out risky changes. Suggest test cases "
    "only when the diff is missing obvious coverage. Approve boring diffs "
    "in one line. Never be snarky."
)


def review_pr(
    repo: str,
    number: int,
    *,
    run_agent_fn=None,
    url_fetcher=None,
    model: str = DEFAULT_MODEL,
    max_cost_usd: float = 0.10,
) -> str:
    """Fetch the diff, ask Claude, return the review text (possibly empty)."""
    diff = _fetch_diff_text(repo, number, url_fetcher=url_fetcher)
    if not diff:
        return ""

    prompt = (
        f"Review this pull request (repo={repo}, number={number}). "
        "Flag anything that looks risky, suggest missing test coverage "
        "if any, and call out tidy/boring diffs explicitly.\n\n"
        f"```\n{diff}\n```\n\n"
        "Return plain markdown. Keep it under 20 lines."
    )

    if run_agent_fn is None:
        from .agent import run_agent as _run_agent

        run_agent_fn = _run_agent

    result = run_agent_fn(
        prompt,
        system=REVIEW_SYSTEM,
        model=model,
        max_turns=1,
        max_cost_usd=max_cost_usd,
    )
    text = getattr(result, "text", "") or ""
    if getattr(result, "error", None) and not text:
        return ""
    return text


def post_review_comment(
    repo: str,
    number: int,
    text: str,
    *,
    url_fetcher=None,
) -> bool:
    """Post the review as an issue comment. Returns True on 2xx."""
    if not text.strip():
        return False
    res = _gh_request(
        f"/repos/{repo}/issues/{number}/comments",
        method="POST",
        body={"body": text.strip()},
        url_fetcher=url_fetcher,
    )
    return res is not None


# ---------------------------------------------------------------------------
# State de-dupe
# ---------------------------------------------------------------------------


def _state_path(data_dir: Path) -> Path:
    return Path(str(data_dir)) / "pr_review_state.json"


def load_state(data_dir: Path) -> dict[str, list[int]]:
    p = _state_path(data_dir)
    if not p.exists():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {
        str(k): [int(n) for n in v if isinstance(n, (int, float))]
        for k, v in raw.items()
        if isinstance(v, list)
    }


def save_state(data_dir: Path, state: dict[str, list[int]]) -> None:
    p = _state_path(data_dir)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps(state, indent=2, sort_keys=True), encoding="utf-8"
    )


def filter_unseen(
    prs: list[OpenPR], state: dict[str, list[int]]
) -> list[OpenPR]:
    out: list[OpenPR] = []
    for pr in prs:
        seen = set(state.get(pr.repo) or [])
        if pr.number in seen:
            continue
        out.append(pr)
    return out


def mark_seen(
    prs: list[OpenPR], state: dict[str, list[int]]
) -> dict[str, list[int]]:
    """Return a new state dict with each PR appended to its repo's list."""
    new_state = {k: list(v) for k, v in state.items()}
    for pr in prs:
        lst = new_state.setdefault(pr.repo, [])
        if pr.number not in lst:
            lst.append(pr.number)
    return new_state


# ---------------------------------------------------------------------------
# Scheduler entry point
# ---------------------------------------------------------------------------


def run_once(
    *,
    settings=None,
    run_agent_fn=None,
    url_fetcher=None,
    post_fn=None,
) -> dict[str, Any]:
    """Look for new PRs, review each, post the comment, persist state.

    Returns a dict with `reviewed` (list of {repo, number, posted}) plus
    aggregate stats. Never raises.
    """
    if settings is None:
        try:
            from .settings import load_settings

            settings = load_settings()
        except Exception:
            return {"reviewed": [], "error": "settings unavailable"}

    data_dir = getattr(settings, "data_dir", Path.home() / ".operator" / "data")
    state = load_state(Path(str(data_dir)))

    open_prs = list_open_prs(settings, url_fetcher=url_fetcher)
    unseen = filter_unseen(open_prs, state)

    reviewed: list[dict[str, Any]] = []
    for pr in unseen:
        text = review_pr(
            pr.repo,
            pr.number,
            run_agent_fn=run_agent_fn,
            url_fetcher=url_fetcher,
        )
        posted = False
        if text:
            poster = post_fn or post_review_comment
            posted = poster(
                pr.repo, pr.number, text, url_fetcher=url_fetcher
            ) if post_fn is None else poster(pr.repo, pr.number, text)
        reviewed.append({
            "repo": pr.repo,
            "number": pr.number,
            "posted": posted,
            "text_len": len(text),
        })

    state = mark_seen(unseen, state)
    try:
        save_state(Path(str(data_dir)), state)
    except OSError:
        pass

    return {
        "reviewed": reviewed,
        "open_prs": len(open_prs),
        "new_prs": len(unseen),
    }
