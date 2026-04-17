"""Tests for operator_core.secrets_audit.

All tests scan a tmp_path tree — never the real repo.
"""

from __future__ import annotations

from pathlib import Path

from operator_core import secrets_audit


FAKE_URL = "https://discord.com/api/webhooks/123456789012345678/AbCdEf-ghiJKL_mnoPQR0123456789abcdefGHIJklMNopQRSTuvWXyz01234567"


def _populate(tmp_path: Path) -> None:
    (tmp_path / "clean.py").write_text(
        "print('hello world')\n# totally normal module\n",
        encoding="utf-8",
    )
    (tmp_path / "leaky.py").write_text(
        f"WEBHOOK = '{FAKE_URL}'\n",
        encoding="utf-8",
    )
    (tmp_path / "env_ref.py").write_text(
        "import os\n"
        f"# reference: {FAKE_URL}\n"
        "url = os.environ['DISCORD_WEBHOOK_URL']\n",
        encoding="utf-8",
    )


def test_scan_tree_flags_only_leaky_file(tmp_path: Path):
    _populate(tmp_path)
    findings = secrets_audit.scan_tree(tmp_path)
    paths = {f.path for f in findings}
    assert paths == {"leaky.py"}
    assert findings[0].webhook_id == "123456789012345678"


def test_scan_tree_ignores_env_filenames(tmp_path: Path):
    (tmp_path / ".env").write_text(
        f"DISCORD_WEBHOOK_URL={FAKE_URL}\n",
        encoding="utf-8",
    )
    (tmp_path / ".env.local").write_text(
        f"DISCORD_WEBHOOK_URL={FAKE_URL}\n",
        encoding="utf-8",
    )
    findings = secrets_audit.scan_tree(tmp_path)
    assert findings == []


def test_scan_tree_ignores_common_junk_dirs(tmp_path: Path):
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "__pycache__" / "leaky.cpython.pyc").write_bytes(b"")
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_text(FAKE_URL, encoding="utf-8")
    (tmp_path / "node_modules").mkdir()
    (tmp_path / "node_modules" / "bad.js").write_text(
        f"const u = '{FAKE_URL}'\n", encoding="utf-8"
    )
    findings = secrets_audit.scan_tree(tmp_path)
    assert findings == []


def test_scan_tree_respects_user_gitignore(tmp_path: Path):
    (tmp_path / ".gitignore").write_text("private/\n", encoding="utf-8")
    (tmp_path / "private").mkdir()
    (tmp_path / "private" / "hidden.py").write_text(
        f"url = '{FAKE_URL}'\n", encoding="utf-8"
    )
    findings = secrets_audit.scan_tree(tmp_path)
    assert findings == []


def test_cli_exits_nonzero_on_findings(tmp_path: Path, capsys):
    _populate(tmp_path)
    code = secrets_audit.main([str(tmp_path)])
    assert code == 1
    out = capsys.readouterr()
    assert "leaky.py" in out.out
    assert "violations" in out.err


def test_cli_exits_zero_when_clean(tmp_path: Path, capsys):
    (tmp_path / "ok.py").write_text("x = 1\n", encoding="utf-8")
    code = secrets_audit.main([str(tmp_path)])
    assert code == 0
    out = capsys.readouterr()
    assert "clean" in out.out


def test_env_reference_heuristic():
    assert secrets_audit._is_env_reference("url = os.environ['X']")
    assert secrets_audit._is_env_reference("const url = process.env.DISCORD")
    assert secrets_audit._is_env_reference("url: ${DISCORD_WEBHOOK_URL}")
    assert not secrets_audit._is_env_reference("url = 'https://discord.com/...'")
