"""morning_briefing -- v2 daily portfolio briefing rendered to HTML.

Single-page aggregator. Reads existing recipe outputs (portfolio_health
IR, reply ledger, status-spec emissions) and composes them into one
Palantir-themed dashboard at war-room/morning.html. Discord post links
to the file and surfaces the top 3 things Kruz should look at first.

Dogfoods every lib shipped 2026-05-05: status-spec, events-ndjson,
templated-dashboards v0.2.0, recipes framework, outreach-common gate
digest output, portfolio_health IR.

Design: docs/MORNING_BRIEFING_V2_DESIGN.md
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Optional

from operator_core.recipes import Recipe, RecipeContext, register_recipe
from recipes._paths import projects_dir

PROJECTS_DIR = projects_dir()
WAR_ROOM_DIR = PROJECTS_DIR / "war-room"
PORTFOLIO_HEALTH_IR = WAR_ROOM_DIR / "portfolio-health.ir.json"
MORNING_HTML = WAR_ROOM_DIR / "morning.html"
MORNING_MD = WAR_ROOM_DIR / "morning.md"

STATUS_DIR = Path.home() / ".operator" / "data" / "status"
STALE_THRESHOLD_HOURS = 48
HOT_LEAD_CATEGORIES = {"interested", "hostile"}

# Tracked repos for PR + commit aggregation. Keep in sync with
# portfolio_health.TRACKED_PROJECTS but use a smaller curated set
# for the daily briefing — the noisy ones get in the way.
TRACKED_REPOS = (
    "operator-core",
    "outreach-common",
    "ai-ops-consulting",
    "prospector-pro",
    "outreach-engine",
    "pool-prospector",
    "hosted-agents",
    "templated-dashboards",
    "events-ndjson",
    "status-spec",
    "agentbridge",
)


class SectionResult:
    """Per-section output. ``error`` is a soft signal: render proceeds with
    a small error band rather than crashing the whole briefing.

    Plain class (not @dataclass) because dataclass + ``from __future__ import
    annotations`` + importlib spec_from_file_location interact badly when the
    recipe registry imports under a synthetic module name -- ``cls.__module__``
    points at a name not yet in sys.modules and dataclass trips on the lookup.
    """

    __slots__ = ("title", "payload", "error", "score")

    def __init__(
        self,
        title: str,
        payload: Any = None,
        error: Optional[str] = None,
        score: int = 0,
    ) -> None:
        self.title = title
        self.payload = payload
        self.error = error
        self.score = score

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"SectionResult(title={self.title!r}, payload={self.payload!r}, "
            f"error={self.error!r}, score={self.score!r})"
        )


# ---------------------------------------------------------------------
# Aggregator helpers — one per section
# ---------------------------------------------------------------------


async def _gather_overnight_replies() -> SectionResult:
    """Section 2: count replies in last 24h grouped by classifier outcome."""
    try:
        from operator_core import replies

        threads = replies.list_threads(limit=200)
        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=24)
        recent = [
            t
            for t in threads
            if (t.get("last_inbound_at") or "")
            and dt.datetime.fromisoformat(t["last_inbound_at"].replace("Z", "+00:00")) >= cutoff
        ]
        by_category: dict[str, list[dict]] = {}
        for t in recent:
            cat = t.get("category") or "new"
            by_category.setdefault(cat, []).append(t)
        hot = sum(len(by_category.get(c, [])) for c in HOT_LEAD_CATEGORIES)
        return SectionResult(
            title="Overnight replies",
            payload={"by_category": by_category, "total": len(recent), "hot": hot},
            score=100 if hot > 0 else 0,
        )
    except Exception as exc:  # pragma: no cover - defensive
        return SectionResult(title="Overnight replies", error=str(exc)[:200])


async def _gather_portfolio_health() -> SectionResult:
    """Section 3: read portfolio-health.ir.json, slice Overview KPIs."""
    if not PORTFOLIO_HEALTH_IR.exists():
        return SectionResult(
            title="Portfolio health",
            error="portfolio_health not run yet (war-room/portfolio-health.ir.json missing)",
        )
    try:
        ir = json.loads(PORTFOLIO_HEALTH_IR.read_text(encoding="utf-8"))
        overview = next(
            (s for s in ir.get("sections", []) if s.get("title") == "Overview"),
            None,
        )
        if not overview:
            return SectionResult(title="Portfolio health", error="no Overview section in IR")
        kpis = {
            c.get("label", "?"): c.get("value", 0)
            for c in overview.get("components", [])
            if c.get("type") == "kpi_tile"
        }
        red = int(kpis.get("Red", 0) or 0)
        return SectionResult(
            title="Portfolio health",
            payload={"kpis": kpis, "ir_subtitle": ir.get("subtitle", "")},
            score=70 if red > 0 else 0,
        )
    except Exception as exc:
        return SectionResult(title="Portfolio health", error=str(exc)[:200])


async def _gather_open_prs() -> SectionResult:
    """Section 4: open PRs across tracked repos via gh CLI."""
    rows: list[dict] = []
    overall_score = 0
    for repo in TRACKED_REPOS:
        try:
            proc = subprocess.run(
                [
                    "gh",
                    "pr",
                    "list",
                    "--repo",
                    f"kjhholt-alt/{repo}",
                    "--state",
                    "open",
                    "--json",
                    "number,title,createdAt,mergeStateStatus",
                ],
                capture_output=True,
                text=True,
                timeout=20,
            )
            if proc.returncode != 0:
                continue
            for pr in json.loads(proc.stdout or "[]"):
                created = dt.datetime.fromisoformat(pr["createdAt"].replace("Z", "+00:00"))
                age_hours = (dt.datetime.now(dt.timezone.utc) - created).total_seconds() / 3600
                if age_hours > 24 and pr.get("mergeStateStatus") == "CLEAN":
                    overall_score = max(overall_score, 80)
                rows.append(
                    {
                        "repo": repo,
                        "number": pr["number"],
                        "title": pr["title"][:80],
                        "age_hours": round(age_hours, 1),
                        "state": pr.get("mergeStateStatus", "?"),
                    }
                )
        except Exception:
            continue
    return SectionResult(title="Open PRs", payload={"prs": rows}, score=overall_score)


async def _gather_waitlist_signups(ctx: RecipeContext) -> SectionResult:
    """Section 5: hosted-agents waitlist signups in last 24h."""
    try:
        from operator_core import revenue

        sb = ctx.clients.get("supabase")
        if sb is None:
            return SectionResult(title="Waitlist signups", error="supabase client unavailable")
        actions, metrics = revenue.collect_waitlists(sb)
        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=24)
        recent_signups: dict[str, int] = {}
        for action in actions:
            ts_raw = (action.metadata or {}).get("created_at") if hasattr(action, "metadata") else None
            if ts_raw:
                try:
                    ts = dt.datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                    if ts >= cutoff:
                        recent_signups[action.target_slug] = recent_signups.get(action.target_slug, 0) + 1
                except Exception:
                    continue
        spike_threshold = 3
        score = 35 if any(v >= spike_threshold for v in recent_signups.values()) else 0
        return SectionResult(
            title="Waitlist signups",
            payload={"recent_24h": recent_signups, "totals": metrics},
            score=score,
        )
    except Exception as exc:
        return SectionResult(title="Waitlist signups", error=str(exc)[:200])


async def _gather_audit_pipeline() -> SectionResult:
    """Section 6: AI Ops audit pipeline state from status-spec + events."""
    try:
        ai_ops_status = STATUS_DIR / "ai-ops-consulting.json"
        recent_audit = None
        if ai_ops_status.exists():
            doc = json.loads(ai_ops_status.read_text(encoding="utf-8"))
            last = doc.get("last_event") or {}
            if last.get("type") == "audit_completed":
                recent_audit = last
        return SectionResult(
            title="AI Ops audit",
            payload={"recent_audit": recent_audit},
            score=40 if recent_audit else 0,
        )
    except Exception as exc:
        return SectionResult(title="AI Ops audit", error=str(exc)[:200])


async def _gather_pool_prospector() -> SectionResult:
    """Section 7: pool-prospector lead pipeline + uncle-demo state."""
    try:
        pp_dir = PROJECTS_DIR / "pool-prospector"
        uncle_demo_dir = pp_dir / "examples" / "uncle_demo"
        latest_demo = None
        if uncle_demo_dir.exists():
            dirs = sorted(
                [d for d in uncle_demo_dir.iterdir() if d.is_dir()],
                reverse=True,
            )
            if dirs:
                latest_demo = dirs[0].name
        # status-spec for pool-prospector if it emits one
        status_doc = STATUS_DIR / "pool-prospector.json"
        leads_count = None
        if status_doc.exists():
            doc = json.loads(status_doc.read_text(encoding="utf-8"))
            leads_count = (doc.get("counters") or {}).get("leads")
        return SectionResult(
            title="Pool Prospector",
            payload={"latest_uncle_demo": latest_demo, "leads_count": leads_count},
        )
    except Exception as exc:
        return SectionResult(title="Pool Prospector", error=str(exc)[:200])


async def _gather_cost_rollup() -> SectionResult:
    """Section 8: cost rollup last 24h from events-ndjson cost stream."""
    try:
        cost_path_env = os.environ.get("OPERATOR_COST_NDJSON_PATH")
        cost_path = Path(cost_path_env) if cost_path_env else (
            Path.home() / ".operator" / "data" / "events" / "cost.ndjson"
        )
        if not cost_path.exists():
            return SectionResult(title="Cost (24h)", payload={"total_usd": 0.0, "by_recipe": {}})
        cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=24)
        total = 0.0
        by_recipe: dict[str, float] = {}
        for line in cost_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except Exception:
                continue
            ts_raw = event.get("ts") or ""
            if not ts_raw:
                continue
            try:
                ts = dt.datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except Exception:
                continue
            if ts < cutoff:
                continue
            payload = event.get("payload") or {}
            cost = float(payload.get("cost_usd", 0.0))
            total += cost
            recipe = event.get("source") or payload.get("recipe") or "unknown"
            by_recipe[recipe] = by_recipe.get(recipe, 0.0) + cost
        # rolling 7d avg for spike detection
        all_total = 0.0
        seven_cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=7)
        for line in cost_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                ts = dt.datetime.fromisoformat((event.get("ts") or "").replace("Z", "+00:00"))
            except Exception:
                continue
            if ts < seven_cutoff:
                continue
            all_total += float((event.get("payload") or {}).get("cost_usd", 0.0))
        seven_avg = all_total / 7.0 if all_total else 0.0
        spike = total > seven_avg * 3 and total > 1.0
        return SectionResult(
            title="Cost (24h)",
            payload={"total_usd": round(total, 4), "by_recipe": by_recipe, "spike": spike, "seven_avg": round(seven_avg, 4)},
            score=30 if spike else 0,
        )
    except Exception as exc:
        return SectionResult(title="Cost (24h)", error=str(exc)[:200])


async def _gather_recent_commits() -> SectionResult:
    """Section 9: commits across tracked repos in last 24h."""
    rows: list[dict] = []
    for repo in TRACKED_REPOS:
        repo_dir = PROJECTS_DIR / repo
        if not (repo_dir / ".git").exists():
            continue
        try:
            proc = subprocess.run(
                [
                    "git",
                    "log",
                    '--since="24 hours ago"',
                    "--pretty=format:%h|%s|%cr",
                    "--no-merges",
                ],
                cwd=str(repo_dir),
                capture_output=True,
                text=True,
                timeout=15,
            )
            for line in (proc.stdout or "").splitlines():
                if "|" not in line:
                    continue
                sha, subject, when = line.split("|", 2)
                rows.append({"repo": repo, "sha": sha, "subject": subject[:80], "when": when})
        except Exception:
            continue
    return SectionResult(title="Recent commits (24h)", payload={"commits": rows[:30]})


async def _gather_stale_alerts() -> SectionResult:
    """Section 10: stale status-spec emitters + BLOCKER docs."""
    stale_projects: list[dict] = []
    blockers: list[str] = []
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=STALE_THRESHOLD_HOURS)

    if STATUS_DIR.exists():
        for f in STATUS_DIR.glob("*.json"):
            try:
                doc = json.loads(f.read_text(encoding="utf-8"))
                ts_raw = doc.get("ts") or doc.get("last_event", {}).get("ts") or ""
                ts = dt.datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                if ts < cutoff:
                    stale_projects.append({"project": f.stem, "ts": ts_raw})
            except Exception:
                continue

    for repo in TRACKED_REPOS:
        docs_dir = PROJECTS_DIR / repo / "docs"
        if not docs_dir.exists():
            continue
        for blocker_doc in docs_dir.glob("*BLOCKER*.md"):
            blockers.append(f"{repo}/docs/{blocker_doc.name}")

    score = 50 if stale_projects else 0
    return SectionResult(
        title="Stale alerts",
        payload={"stale_projects": stale_projects, "blockers": blockers[:20]},
        score=score,
    )


# ---------------------------------------------------------------------
# Ranker — "Today's 3 things"
# ---------------------------------------------------------------------


def _rank_today(sections: dict[str, SectionResult]) -> list[dict]:
    """Rule-based ranker. No LLM. Returns a list of 1-3 callout payloads."""
    ranked: list[dict] = []

    replies = sections.get("Overnight replies")
    if replies and not replies.error:
        hot = (replies.payload or {}).get("hot", 0)
        if hot > 0:
            ranked.append(
                {
                    "score": 100,
                    "tone": "warn",
                    "kicker": "HOT LEAD",
                    "title": f"{hot} interested or hostile reply{'ies' if hot != 1 else ''} overnight",
                    "body": "Open the replies section below; auto-classifier flagged these for human review.",
                }
            )

    prs = sections.get("Open PRs")
    if prs and not prs.error:
        stuck = [
            p for p in (prs.payload or {}).get("prs", []) if p["age_hours"] > 24 and p["state"] == "CLEAN"
        ]
        if stuck:
            ranked.append(
                {
                    "score": 80,
                    "tone": "good",
                    "kicker": "READY TO MERGE",
                    "title": f"{len(stuck)} PR{'s' if len(stuck) != 1 else ''} clean + green for >24h",
                    "body": ", ".join(f"{p['repo']}#{p['number']}" for p in stuck[:5]),
                }
            )

    ph = sections.get("Portfolio health")
    if ph and not ph.error:
        kpis = (ph.payload or {}).get("kpis", {})
        red = int(kpis.get("Red", 0) or 0)
        if red > 0:
            ranked.append(
                {
                    "score": 70,
                    "tone": "bad",
                    "kicker": "RED",
                    "title": f"{red} project{'s' if red != 1 else ''} reporting health=red",
                    "body": "See portfolio health section below; investigate the worst offender first.",
                }
            )

    stale = sections.get("Stale alerts")
    if stale and not stale.error:
        stale_list = (stale.payload or {}).get("stale_projects", [])
        if stale_list:
            ranked.append(
                {
                    "score": 50,
                    "tone": "warn",
                    "kicker": "STALE",
                    "title": f"{len(stale_list)} project{'s' if len(stale_list) != 1 else ''} stale >48h",
                    "body": ", ".join(p["project"] for p in stale_list[:5]),
                }
            )

    audit = sections.get("AI Ops audit")
    if audit and not audit.error and (audit.payload or {}).get("recent_audit"):
        ranked.append(
            {
                "score": 40,
                "tone": "good",
                "kicker": "AUDIT READY",
                "title": "Tier 1 audit completed in last 24h",
                "body": "Review the deliverable in ai-ops-consulting/examples/showcase_audit/.",
            }
        )

    waitlist = sections.get("Waitlist signups")
    if waitlist and not waitlist.error:
        recent = (waitlist.payload or {}).get("recent_24h", {})
        spikes = [(k, v) for k, v in recent.items() if v >= 3]
        if spikes:
            ranked.append(
                {
                    "score": 35,
                    "tone": "good",
                    "kicker": "SIGNUP SPIKE",
                    "title": f"{', '.join(f'{k}: {v}' for k, v in spikes)}",
                    "body": "New waitlist activity over baseline — consider personal follow-up.",
                }
            )

    cost = sections.get("Cost (24h)")
    if cost and not cost.error and (cost.payload or {}).get("spike"):
        total = cost.payload.get("total_usd", 0.0)
        ranked.append(
            {
                "score": 30,
                "tone": "warn",
                "kicker": "COST SPIKE",
                "title": f"24h cost ${total:.2f} > 3x rolling 7d avg",
                "body": "Check the cost section for which recipe is responsible.",
            }
        )

    ranked.sort(key=lambda r: -r["score"])
    top3 = ranked[:3]

    while len(top3) < 3:
        top3.append(
            {
                "score": 0,
                "tone": "neutral",
                "kicker": "CLEAR",
                "title": "Nothing surfaced for this slot",
                "body": "No replies, no stale projects, no cost spikes, no PRs awaiting merge.",
            }
        )

    return top3


# ---------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------


def _build_dashboard_ir(sections: dict[str, SectionResult], today: list[dict]) -> Any:
    """Construct templated-dashboards Dashboard IR + return the builder."""
    from dashboards import Dashboard

    d = Dashboard(
        f"Morning briefing -- {dt.date.today().isoformat()}",
        subtitle=f"{len([t for t in today if t['kicker'] != 'CLEAR'])} thing{'s' if len([t for t in today if t['kicker'] != 'CLEAR']) != 1 else ''} to look at first",
        theme="palantir",
    )

    # Section 1 -- today's 3 things
    d.section("Today's 3 things", layout="grid")
    for item in today:
        d.callout(
            f"[{item['kicker']}] {item['title']}\n\n{item['body']}",
            tone=item["tone"],
        )

    # Section 2 -- overnight replies
    replies = sections.get("Overnight replies")
    d.section("Overnight replies")
    if replies and not replies.error:
        p = replies.payload or {}
        rows = []
        for cat, items in (p.get("by_category") or {}).items():
            rows.append([cat, str(len(items))])
        if rows:
            d.table(headers=["Category", "Count"], rows=rows)
        d.kpi("Total replies", p.get("total", 0), tone="neutral")
        d.kpi("Hot leads", p.get("hot", 0), tone="warn" if p.get("hot", 0) > 0 else "neutral")
    elif replies and replies.error:
        d.callout('[' + str("ERROR") + '] ' + str("Reply ledger unavailable") + chr(10) + chr(10) + str(replies.error), tone="bad")
    else:
        d.callout('[' + str("CLEAR") + '] ' + str("0 replies overnight") + chr(10) + chr(10) + str("Quiet inbox."), tone="neutral")

    # Section 3 -- portfolio health
    ph = sections.get("Portfolio health")
    d.section("Portfolio health")
    if ph and not ph.error:
        kpis = (ph.payload or {}).get("kpis", {})
        for label in ("Tracked", "Green", "Yellow", "Red", "Unknown"):
            tone = {
                "Green": "good",
                "Yellow": "warn",
                "Red": "bad",
            }.get(label, "neutral")
            d.kpi(label, int(kpis.get(label, 0) or 0), tone=tone)
    else:
        msg = (ph.error if ph else "no data")
        d.callout('[' + str("MISSING") + '] ' + str("Portfolio health not available") + chr(10) + chr(10) + str(msg), tone="warn")

    # Section 4 -- open PRs
    prs = sections.get("Open PRs")
    d.section("Open PRs")
    if prs and not prs.error:
        rows = [[p["repo"], f"#{p['number']}", p["title"], f"{p['age_hours']}h", p["state"]] for p in (prs.payload or {}).get("prs", [])]
        if rows:
            d.table(headers=["Repo", "PR", "Title", "Age", "State"], rows=rows)
        else:
            d.callout('[' + str("CLEAR") + '] ' + str("0 open PRs") + chr(10) + chr(10) + str("All clear."), tone="neutral")
    else:
        d.callout('[' + str("ERROR") + '] ' + str("PR query failed") + chr(10) + chr(10) + str(prs.error if prs else "no data"), tone="bad")

    # Section 5 -- waitlist signups
    wl = sections.get("Waitlist signups")
    d.section("Waitlist signups (24h)")
    if wl and not wl.error:
        recent = (wl.payload or {}).get("recent_24h", {})
        if recent:
            rows = [[k, str(v)] for k, v in recent.items()]
            d.table(headers=["Product", "New signups (24h)"], rows=rows)
        else:
            d.callout('[' + str("CLEAR") + '] ' + str("0 new signups") + chr(10) + chr(10) + str("No fresh waitlist activity."), tone="neutral")
    else:
        d.callout('[' + str("ERROR") + '] ' + str("Waitlist query failed") + chr(10) + chr(10) + str(wl.error if wl else "no data"), tone="warn")

    # Section 6 -- AI Ops audit
    audit = sections.get("AI Ops audit")
    d.section("AI Ops audit")
    if audit and not audit.error:
        recent = (audit.payload or {}).get("recent_audit")
        if recent:
            d.callout(
                kicker="AUDIT",
                title="audit_completed",
                body=f"At {recent.get('ts', '')}: {recent.get('summary', '')}",
                tone="good",
            )
        else:
            d.callout('[' + str("QUIET") + '] ' + str("No audits in 24h") + chr(10) + chr(10) + str("Pipeline idle."), tone="neutral")
    else:
        d.callout('[' + str("ERROR") + '] ' + str("Audit status unavailable") + chr(10) + chr(10) + str(audit.error if audit else "no data"), tone="warn")

    # Section 7 -- pool prospector
    pp = sections.get("Pool Prospector")
    d.section("Pool Prospector")
    if pp and not pp.error:
        p = pp.payload or {}
        body_parts = []
        if p.get("latest_uncle_demo"):
            body_parts.append(f"Latest uncle demo: {p['latest_uncle_demo']}")
        if p.get("leads_count") is not None:
            body_parts.append(f"Leads tracked: {p['leads_count']}")
        if not body_parts:
            body_parts.append("No status emitter wired yet.")
        d.callout("[POOL] Pipeline state" + chr(10) + chr(10) + " | ".join(body_parts), tone="neutral")
    else:
        d.callout('[' + str("ERROR") + '] ' + str("Pool Prospector unavailable") + chr(10) + chr(10) + str(pp.error if pp else "no data"), tone="warn")

    # Section 8 -- cost rollup
    cost = sections.get("Cost (24h)")
    d.section("Cost (24h)")
    if cost and not cost.error:
        p = cost.payload or {}
        d.kpi("24h spend (USD)", f"${p.get('total_usd', 0):.4f}", tone="warn" if p.get("spike") else "neutral")
        d.kpi("7d avg/day", f"${p.get('seven_avg', 0):.4f}", tone="neutral")
        if p.get("by_recipe"):
            rows = sorted(((k, f"${v:.4f}") for k, v in p["by_recipe"].items()), key=lambda r: r[1], reverse=True)[:10]
            d.table(headers=["Recipe", "Cost"], rows=[[r[0], r[1]] for r in rows])
    else:
        d.callout('[' + str("ERROR") + '] ' + str("Cost rollup unavailable") + chr(10) + chr(10) + str(cost.error if cost else "no data"), tone="warn")

    # Section 9 -- recent commits
    commits = sections.get("Recent commits (24h)")
    d.section("Recent commits (24h)")
    if commits and not commits.error:
        rows = [[c["repo"], c["sha"], c["subject"], c["when"]] for c in (commits.payload or {}).get("commits", [])]
        if rows:
            d.table(headers=["Repo", "SHA", "Subject", "When"], rows=rows[:30])
        else:
            d.callout('[' + str("QUIET") + '] ' + str("0 commits in 24h") + chr(10) + chr(10) + str("No recent activity."), tone="neutral")
    else:
        d.callout('[' + str("ERROR") + '] ' + str("Commit log unavailable") + chr(10) + chr(10) + str(commits.error if commits else "no data"), tone="warn")

    # Section 10 -- stale alerts
    stale = sections.get("Stale alerts")
    d.section("Stale alerts")
    if stale and not stale.error:
        p = stale.payload or {}
        stale_list = p.get("stale_projects", [])
        blockers = p.get("blockers", [])
        if stale_list:
            rows = [[s["project"], s["ts"]] for s in stale_list]
            d.table(headers=["Project", "Last status"], rows=rows)
        if blockers:
            d.callout("[BLOCKERS] " + str(len(blockers)) + " blocker doc(s) on disk" + chr(10) + chr(10) + chr(10).join(blockers[:10]), tone="warn")
        if not stale_list and not blockers:
            d.callout('[' + str("CLEAR") + '] ' + str("Nothing stale") + chr(10) + chr(10) + str("All status emitters fresh."), tone="good")
    else:
        d.callout('[' + str("ERROR") + '] ' + str("Stale-alert scan failed") + chr(10) + chr(10) + str(stale.error if stale else "no data"), tone="warn")

    d.footer(f"Generated by operator-core morning_briefing v2 at {dt.datetime.now().isoformat(timespec='seconds')}")
    return d


# ---------------------------------------------------------------------
# Recipe
# ---------------------------------------------------------------------


@register_recipe
class MorningBriefing(Recipe):
    name = "morning_briefing"
    version = "2.0.0"
    description = "Daily portfolio briefing -- single HTML page at war-room/morning.html"
    cost_budget_usd = 0.05  # rule-based ranker, no LLM
    schedule = "0 7 * * *"
    timeout_sec = 300
    discord_channel = "projects"
    requires_clients = ()  # discord + supabase optional; we degrade gracefully
    tags = ("daily", "briefing")

    async def verify(self, ctx: RecipeContext) -> bool:
        # Always pass verify -- we degrade gracefully per section.
        # Just ensure the war-room dir is writable.
        WAR_ROOM_DIR.mkdir(parents=True, exist_ok=True)
        return WAR_ROOM_DIR.exists()

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        results = await asyncio.gather(
            _gather_overnight_replies(),
            _gather_portfolio_health(),
            _gather_open_prs(),
            _gather_waitlist_signups(ctx),
            _gather_audit_pipeline(),
            _gather_pool_prospector(),
            _gather_cost_rollup(),
            _gather_recent_commits(),
            _gather_stale_alerts(),
            return_exceptions=False,
        )
        sections = {r.title: r for r in results}
        return {"sections": sections}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        sections = data["sections"]
        today = _rank_today(sections)
        return {"sections": sections, "today": today}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        from dashboards import render

        sections = result["sections"]
        today = result["today"]
        builder = _build_dashboard_ir(sections, today)
        ir = builder.build()
        WAR_ROOM_DIR.mkdir(parents=True, exist_ok=True)
        render(ir, "html", out=MORNING_HTML)
        render(ir, "markdown", out=MORNING_MD)
        return MORNING_MD.read_text(encoding="utf-8")

    async def post(self, ctx: RecipeContext, message: str) -> None:
        discord = ctx.clients.get("discord")
        if discord is None:
            ctx.logger.info("morning_briefing.no_discord_skip", extra={"path": str(MORNING_HTML)})
            return
        # Build a short embed: top 3 things + link to morning.html
        try:
            today = (await self.analyze(ctx, await self.query(ctx))).get("today", [])  # short but keeps single source of truth
        except Exception:
            today = []
        lines = [f"**Morning briefing -- {dt.date.today().isoformat()}**", ""]
        for item in today:
            lines.append(f"**{item['kicker']}** -- {item['title']}")
        lines.append("")
        lines.append(f"Full HTML: file:///{MORNING_HTML.as_posix()}")
        await discord.post(self.discord_channel, "\n".join(lines))
