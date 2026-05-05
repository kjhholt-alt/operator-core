from operator_core.commands import CommandParseError, parse_operator_command


def test_parse_build_command():
    parsed = parse_operator_command("!op build deal-brain: rebuild pricing copy")

    assert parsed.action == "build"
    assert parsed.project == "deal-brain"
    assert parsed.prompt == "rebuild pricing copy"


def test_parse_morning_and_deploy_commands():
    assert parse_operator_command("!op morning").action == "morning"
    assert parse_operator_command("!op review prs").action == "review_prs"
    assert parse_operator_command("!op deploy check").action == "deploy_check"
    assert parse_operator_command("!op fleet status").action == "fleet_status"
    assert parse_operator_command("!op fleet check").action == "fleet_check"
    assert parse_operator_command("!op fleet weakest").action == "fleet_weakest"


def test_parse_rejects_non_operator_message():
    try:
        parse_operator_command("status")
    except CommandParseError as exc:
        assert "!op" in str(exc)
    else:
        raise AssertionError("Expected CommandParseError")


# Gate-review (Reply Copilot v2)


def test_parse_gate_review_no_arg():
    p = parse_operator_command("!op gate-review")
    assert p.action == "gate_review_next"
    assert p.project is None


def test_parse_gate_review_with_product():
    p = parse_operator_command("!op gate-review outreach-engine")
    assert p.action == "gate_review_next"
    assert p.project == "outreach-engine"


def test_parse_gate_resolve_minimal():
    p = parse_operator_command("!op gate-resolve 17 approved_gate")
    assert p.action == "gate_resolve"
    assert p.job_id == "17"
    assert p.args["status"] == "approved_gate"
    assert p.args["note"] == ""


def test_parse_gate_resolve_with_note():
    p = parse_operator_command("!op gate-resolve 5 approved_legacy this is a note about it")
    assert p.action == "gate_resolve"
    assert p.job_id == "5"
    assert p.args["status"] == "approved_legacy"
    assert p.args["note"] == "this is a note about it"


def test_parse_gate_resolve_rejects_unknown_status():
    try:
        parse_operator_command("!op gate-resolve 1 totally_bogus")
    except CommandParseError:
        return
    raise AssertionError("Expected CommandParseError on unknown status")


def test_parse_gate_resolve_supports_all_valid_statuses():
    for s in ("approved_gate", "approved_legacy", "fix_gate", "fix_legacy", "suppressed"):
        p = parse_operator_command(f"!op gate-resolve 1 {s}")
        assert p.args["status"] == s
