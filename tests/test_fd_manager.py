"""Static guards on the PowerShell entry point.

Covers: EC06
Lot: N4
Competency: C5

WHY A TEST FILE FOR A SHELL SCRIPT. fd-manager.ps1 is the only way any of this is
run, and it has broken three times in ways no Python test could see:

  - twice CORRUPTED by my own slice-based edits, once leaving a stray line
    outside the switch that ran on every invocation, once duplicating a block;
  - once REFUSING to start, because -Drop was declared [string] while in
    PowerShell the comma is the ARRAY operator, so -Drop txn_hour,txn_weekday
    hands over two strings and binding fails before a single line runs.

None of these is a logic bug, so none of them had a natural home. They are all
visible in the file's bytes, which is what this file reads. PowerShell is not
installed in the authoring environment, so this is a static check -- weaker
than running it, and it would have caught all three.
"""
from __future__ import annotations

import re
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "fd-manager.ps1"


def _bytes() -> bytes:
    return SCRIPT.read_bytes()


def _text() -> str:
    # newline="" so the line endings are the FILE's and not Python's universal
    # translation of them. Reading with the default would report zero CRLF on a
    # CRLF file -- measuring the instrument instead of the file, which has
    # already misled me once in this project.
    with open(SCRIPT, encoding="utf-8", newline="") as handle:
        return handle.read()


def test_the_file_opens_with_its_help_block():
    """A stray line pushed before `<#` sits outside the switch and runs on every
    invocation -- which is how a corrupted edit stayed invisible for an hour."""
    assert _bytes().startswith(b"<#")


def test_there_is_no_byte_order_mark():
    """Set-Content -Encoding utf8 adds one, and docker compose then reads a
    variable named <BOM>POSTGRES_USER on the first line."""
    assert not _bytes().startswith(b"\xef\xbb\xbf")


def test_the_line_endings_are_uniform():
    data = _bytes()
    assert data.count(b"\r\n") > 0
    assert data.count(b"\n") == data.count(b"\r\n")


def test_the_braces_balance():
    """The check that caught a duplicated block: 153 against 154."""
    text = _text()
    assert text.count("{") == text.count("}")


def test_every_declared_task_has_a_branch():
    """A task listed in the ValidateSet but absent from the switch falls through
    in silence: the script exits 0 having done nothing."""
    text = _text()
    validate = text[text.index("[ValidateSet("):]
    validate = validate[:validate.index(")]")]
    declared = set(re.findall(r"'([a-z][a-z-]*)'", validate))
    assert len(declared) > 20, declared

    branches = set(re.findall(r"^    '([a-z][a-z-]*)' ", text, re.MULTILINE))
    branches |= set(re.findall(r"^    '([a-z][a-z-]*)'\s*\{", text,
                               re.MULTILINE))
    missing = sorted(declared - branches)
    assert not missing, "taches declarees sans branche : %s" % missing


def test_every_branch_is_a_declared_task():
    """The reverse: a branch nobody can reach, usually a rename half-applied."""
    text = _text()
    validate = text[text.index("[ValidateSet("):]
    validate = validate[:validate.index(")]")]
    declared = set(re.findall(r"'([a-z][a-z-]*)'", validate))
    branches = set(re.findall(r"^    '([a-z][a-z-]*)'\s*\{", text,
                              re.MULTILINE))
    assert not sorted(branches - declared)


def test_the_feature_list_parameter_takes_an_array():
    """THE defect of 16 November 2026. In PowerShell the comma is the array
    operator, so -Drop txn_hour,txn_weekday is two strings; bound to a [string]
    it fails with a transformation error before the script runs."""
    text = _text()
    assert "[string[]]$Drop" in text
    assert "[string]$Drop," not in text


def test_the_feature_list_is_joined_before_it_is_used():
    """Interpolating the array itself prints the names space-separated, which is
    exactly the shape Write-ConfFile refuses -- and the message on screen would
    then disagree with what was sent."""
    text = _text()
    assert "$dropped = ($Drop -join ',')" in text
    assert "$values += $dropped" in text
    assert "$values += $Drop\n" not in text.replace("\r\n", "\n")


def test_the_conf_writer_refuses_a_value_holding_a_space():
    """bash reads the conf back by UNQUOTED command substitution: one space and
    the JSON arrives split into three arguments."""
    text = _text()
    assert "La valeur ne doit pas contenir d'espace" in text


def test_the_conf_pairs_are_joined_without_a_space():
    assert "($pairs -join ',')" in _text()


def test_no_double_quote_is_handed_to_a_native_command():
    """PowerShell's argument layer eats them, so the conf is written byte for
    byte with [char]34 instead."""
    assert "[char]34" in _text()
