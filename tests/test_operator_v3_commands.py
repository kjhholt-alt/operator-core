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
