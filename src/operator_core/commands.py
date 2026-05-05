"""Discord command parsing for Operator V3."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


PREFIX = "!op"


@dataclass(frozen=True)
class ParsedCommand:
    action: str
    project: str | None = None
    prompt: str = ""
    job_id: str | None = None
    args: dict[str, str] = field(default_factory=dict)


class CommandParseError(ValueError):
    """Raised when an Operator command cannot be parsed."""


def help_text() -> str:
    lines = [
        "**Operator V3 commands** (owner-only)",
        "",
        "__Status__",
        "`!op status` - daemon uptime, scheduler health, recent jobs, deploys, cost",
        "`!op jobs` - list the 10 most recent jobs",
        "`!op fleet status` - rank the 4 SaaS apps without running local checks",
        "`!op fleet check` - run configured checks and write a fleet report",
        "`!op fleet weakest` - show the weakest SaaS and next fix",
        "`!op help` - this message",
        "",
        "__Morning Ops__",
        "`!op morning` - run morning briefing now",
        "`!op review prs` - run PR reviewer",
        "",
        "__Build__",
        "`!op build <project>: <request>` - worktree PR factory (dry-run default)",
        "`!op stop <job_id>` - request cancel of a running job",
        "`!op approve <job_id>` - record an approval for a medium-risk job",
        "",
        "__Deploy__",
        "`!op deploy check` - check health of 4 SaaS apps",
        "",
        "__Deck__",
        "`!op deck ag-market-pulse` - build weekly ag market deck (Kruz-only email)",
        "",
        "__PL Engine__",
        "`!op pl status` - validate all active factories, show readiness",
        "`!op pl validate [factory]` - run validation checks (e.g. `!op pl validate AX02`)",
        "`!op pl pptx <factory>` - build overhead review PPTX",
        "`!op pl explain <factory>` - CPOH explainability trace",
        "`!op pl adjustments <factory>` - show manual adjustment status",
        "`!op pl morning` - PL engine morning readiness brief",
        "",
        "__PL Analyst (always-on analysis loop)__",
        "`!op pl analyst [factory]` - run analyst loop (validate → Claude → chain safe steps)",
        "`!op pl analyst pipeline <factory>` - analyst loop starting from full pipeline",
        "`!op pl brief [factory]` - quick analyst briefing (state + risks + verdict)",
        "",
        "__Portfolio Brain__",
        "`!op portfolio` - full portfolio snapshot (all projects)",
        "`!op priorities` - ranked next actions for human + agents",
        "`!op blocked` - items waiting on human input",
        "`!op brief` - portfolio briefing (morning-style)",
        "`!op analyze [project]` - run analysis on a project or portfolio",
        "`!op sprint` - sprint recommendation",
        "`!op recipes` - list available agent recipes",
        "",
        "__Sender Gate Cut-Over__",
        "`!op gate-review` - show next pending gate disagreement",
        "`!op gate-review <product>` - filter to one product",
        "`!op gate-resolve <id> <status> [note]` - resolve a queue item",
        "    statuses: approved_gate / approved_legacy / fix_gate / fix_legacy / suppressed",
        "",
        "__Safety__",
        "- Auto-merge is OFF by default (`OPERATOR_AUTO_MERGE_ENABLED=0`)",
        "- Builds default to dry-run (`OPERATOR_V3_DRY_RUN=1`)",
        "- All commands owner-locked; non-owner attempts are logged",
    ]
    return "\n".join(lines)


def parse_operator_command(message: str) -> ParsedCommand:
    """Parse a Discord message into an Operator V3 command."""
    text = message.strip()
    if not text.lower().startswith(PREFIX):
        raise CommandParseError("Message does not start with !op")

    body = text[len(PREFIX) :].strip()
    lowered = body.lower()

    if lowered in {"", "help"}:
        return ParsedCommand(action="help")
    if lowered == "status":
        return ParsedCommand(action="status")
    if lowered == "morning":
        return ParsedCommand(action="morning")
    if lowered == "review prs":
        return ParsedCommand(action="review_prs")
    if lowered == "deploy check":
        return ParsedCommand(action="deploy_check")
    if lowered == "jobs":
        return ParsedCommand(action="jobs")
    if lowered == "fleet status":
        return ParsedCommand(action="fleet_status")
    if lowered == "fleet check":
        return ParsedCommand(action="fleet_check")
    if lowered == "fleet weakest":
        return ParsedCommand(action="fleet_weakest")
    if lowered == "deck ag-market-pulse":
        return ParsedCommand(action="deck_ag_market_pulse", project="ag-market-pulse")

    stop_match = re.fullmatch(r"stop\s+([A-Za-z0-9_.-]+)", body, flags=re.IGNORECASE)
    if stop_match:
        return ParsedCommand(action="stop", job_id=stop_match.group(1))

    approve_match = re.fullmatch(r"approve\s+([A-Za-z0-9_.-]+)", body, flags=re.IGNORECASE)
    if approve_match:
        return ParsedCommand(action="approve", job_id=approve_match.group(1))

    build_match = re.fullmatch(r"build\s+([^:]+):\s*(.+)", body, flags=re.IGNORECASE | re.DOTALL)
    if build_match:
        return ParsedCommand(
            action="build",
            project=build_match.group(1).strip(),
            prompt=build_match.group(2).strip(),
        )

    # ── PL Engine commands ─────────────────────────────────────────────────
    if lowered == "pl":
        return ParsedCommand(action="pl_status", project="pl-engine")
    pl_match = re.fullmatch(r"pl\s+(.*)", body, flags=re.IGNORECASE | re.DOTALL)
    if pl_match:
        pl_body = pl_match.group(1).strip()
        pl_lower = pl_body.lower()

        if pl_lower in {"", "status"}:
            return ParsedCommand(action="pl_status", project="pl-engine")
        if pl_lower == "morning":
            return ParsedCommand(action="pl_morning", project="pl-engine")

        # !op pl validate [factory]
        val_match = re.fullmatch(r"validate(?:\s+(\S+))?", pl_body, flags=re.IGNORECASE)
        if val_match:
            factory = val_match.group(1)
            return ParsedCommand(
                action="pl_validate",
                project=factory.upper() if factory else None,
            )

        # !op pl pptx <factory>
        pptx_match = re.fullmatch(r"pptx\s+(\S+)", pl_body, flags=re.IGNORECASE)
        if pptx_match:
            return ParsedCommand(
                action="pl_pptx",
                project=pptx_match.group(1).upper(),
            )

        # !op pl explain <factory> [topic]
        explain_match = re.fullmatch(r"explain\s+(\S+)(?:\s+(.+))?", pl_body, flags=re.IGNORECASE)
        if explain_match:
            return ParsedCommand(
                action="pl_explain",
                project=explain_match.group(1).upper(),
                prompt=(explain_match.group(2) or "cpoh").strip(),
            )

        # !op pl adjustments <factory>
        adj_match = re.fullmatch(r"adjustments?\s+(\S+)", pl_body, flags=re.IGNORECASE)
        if adj_match:
            return ParsedCommand(
                action="pl_adjustments",
                project=adj_match.group(1).upper(),
            )

        # !op pl analyst [factory] OR !op pl analyst pipeline <factory>
        analyst_match = re.fullmatch(
            r"analyst(?:\s+(pipeline|validate)\s+(\S+)|\s+(\S+))?",
            pl_body, flags=re.IGNORECASE,
        )
        if analyst_match:
            if analyst_match.group(1):
                # !op pl analyst pipeline AX02
                initial_action = analyst_match.group(1).lower()
                factory = analyst_match.group(2).upper()
                return ParsedCommand(
                    action="pl_analyst",
                    project=factory,
                    args={"initial_action": initial_action},
                )
            factory = analyst_match.group(3)
            return ParsedCommand(
                action="pl_analyst",
                project=factory.upper() if factory else None,
            )

        # !op pl brief [factory]
        brief_match = re.fullmatch(r"brief(?:\s+(\S+))?", pl_body, flags=re.IGNORECASE)
        if brief_match:
            factory = brief_match.group(1)
            return ParsedCommand(
                action="pl_brief",
                project=factory.upper() if factory else None,
            )

        raise CommandParseError(f"Unknown pl-engine command: `{pl_body}`. Try `!op pl status`.")

    # ── Portfolio / daemon-brain commands ─────────────────────────────────
    if lowered == "portfolio":
        return ParsedCommand(action="portfolio")
    if lowered == "priorities":
        return ParsedCommand(action="priorities")
    if lowered in {"blocked", "blockers"}:
        return ParsedCommand(action="blocked")
    if lowered in {"brief", "briefing"}:
        return ParsedCommand(action="portfolio_brief")
    if lowered.startswith("analyze "):
        project = body.split(None, 1)[1].strip() if len(body.split(None, 1)) > 1 else None
        return ParsedCommand(action="analyze", project=project)
    if lowered == "analyze":
        return ParsedCommand(action="analyze")
    if lowered == "recipes":
        return ParsedCommand(action="recipes")
    if lowered == "sprint":
        return ParsedCommand(action="sprint_recommend")

    # gate-review (Reply Copilot v2)
    if lowered == "gate-review":
        return ParsedCommand(action="gate_review_next")
    gate_review_product_match = re.fullmatch(
        r"gate-review\s+(\S+)", body, flags=re.IGNORECASE
    )
    if gate_review_product_match:
        return ParsedCommand(
            action="gate_review_next",
            project=gate_review_product_match.group(1),
        )

    # gate-resolve <id> <status> [optional note words]
    gate_resolve_match = re.fullmatch(
        r"gate-resolve\s+(\d+)\s+(approved_gate|approved_legacy|fix_gate|fix_legacy|suppressed)(?:\s+(.+))?",
        body,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if gate_resolve_match:
        return ParsedCommand(
            action="gate_resolve",
            job_id=gate_resolve_match.group(1),
            args={
                "status": gate_resolve_match.group(2).lower(),
                "note": (gate_resolve_match.group(3) or "").strip(),
            },
        )

    raise CommandParseError(f"Unknown command. {help_text()}")
