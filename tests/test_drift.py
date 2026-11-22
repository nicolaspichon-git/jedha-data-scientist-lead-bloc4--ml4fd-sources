"""The injected drift: the writer, and its agreement with the reader.

Block: B4.
Lot: B4-L4.
Competency: B4-C5, B4-C6.
Covers: B4-EC39, B4-EC51.

ONE FILE, TWO PROGRAMS, AND THAT IS THE WHOLE RISK. tools/drift.py writes
runtime/drift.json and producer/app.py reads it, in different languages of the
same project and in different containers. If they ever disagree -- on the key,
on the path, or on what counts as "no drift" -- the switch goes silently dead:
drift_factor() answers 1.0 to anything it cannot parse, so a demonstration
would run, inject nothing, measure nothing, and report no drift. The tests
below exist for that failure mode and not for the arithmetic.
"""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

import pytest
import yaml

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "tools" / "drift.py"
PRODUCER = ROOT / "producer" / "app.py"


def _module():
    spec = importlib.util.spec_from_file_location("drift_tool", MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tool = _module()
producer_source = PRODUCER.read_text(encoding="utf-8")


def test_the_key_the_writer_uses_is_the_key_the_producer_reads():
    """The one that costs the demonstration if it ever drifts: a writer and a
    reader that disagree on the key leave the switch dead, and drift_factor()
    reports no drift rather than an error."""
    assert tool.FACTOR_KEY == "factor"
    assert '[FACTOR_KEY]' in MODULE.read_text(encoding="utf-8")
    assert '["factor"]' in producer_source


def test_both_sides_resolve_the_path_from_the_same_declaration():
    """sources.drift_switch was declared and read by NOBODY until 2026-11-20:
    the producer carried its own literal. A line in governance.yml that no
    program reads is a line that can move without consequence, until the day it
    matters."""
    from ml4fd import config

    assert config.load().drift_switch, "sources.drift_switch is empty"
    assert 'declared["sources"]["drift_switch"]' in producer_source
    # POSITIVELY, AND NOT BY HUNTING THE OLD LITERAL. The first version of this
    # test asserted the literal "/opt/project/runtime/drift.json" was gone from
    # the file, and failed -- on the docstring that explains why it went. The
    # habit of recording history in the file makes a negative text match a bad
    # test twice over: it fails on the explanation, and it would pass on an
    # assignment spelled any other way.
    bindings = [line for line in producer_source.splitlines()
                if line.startswith("DRIFT_FILE")]
    assert bindings == ["DRIFT_FILE = drift_switch(DECLARED)"], bindings


def test_the_switch_is_read_per_request_and_not_at_import():
    """The help of `fd-manager drift` promises no restart is needed. That
    promise is true only while the read happens inside the request handler:
    hoisted to module level it would be read once, the switch would need the
    container recreated, and the help would lie."""
    handler = producer_source.split("def one_transaction")[1]
    assert "drift_factor(DRIFT_FILE)" in handler
    before = producer_source.split("def one_transaction")[0]
    assert "drift_factor(DRIFT_FILE)" not in before


@pytest.mark.parametrize("payload,expected", [
    (None, 1.0),                        # no file at all
    ("not json", 1.0),                  # unreadable
    ('{"nothing": 2}', 1.0),            # the key missing
    ('{"factor": 0}', 1.0),             # refused by the producer's own rule
    ('{"factor": -3}', 1.0),
    ('{"factor": 1}', 1.0),
    ('{"factor": 3}', 3.0),
    ('{"factor": 2.5, "note": "x"}', 2.5),   # extra keys are none of its business
])
def test_the_report_agrees_with_the_producer_about_what_is_in_force(
        payload, expected, tmp_path):
    """in_force() restates drift_factor()'s rule, so this table is the contract
    of both. A reporting tool that disagreed with the service about what is in
    force would be worse than no tool."""
    path = tmp_path / "drift.json"
    if payload is not None:
        path.write_text(payload, encoding="utf-8")
    assert tool.in_force(path) == expected


def test_the_producer_keeps_the_three_escapes_this_table_rests_on():
    """The table above is only a contract while the producer still answers 1.0
    to the unreadable and to the non-positive. Read from its source, because
    importing it would need fastapi and slowapi."""
    reader = producer_source.split("def drift_factor")[1].split("\ndef ")[0]
    assert "except Exception" in reader
    assert "return 1.0" in reader
    assert "value if value > 0 else 1.0" in reader


def test_a_factor_at_or_below_zero_is_refused_rather_than_written(tmp_path,
                                                                  monkeypatch):
    """Writing it would be a switch that says one thing and does another: the
    producer reads a non-positive factor as no drift."""
    path = tmp_path / "drift.json"
    monkeypatch.setattr(tool, "switch_path", lambda settings, root: path)
    assert tool.main(["--factor", "0"]) == 1
    assert tool.main(["--factor", "-2"]) == 1
    assert not path.exists()


def test_off_writes_a_factor_of_one_rather_than_deleting(tmp_path, monkeypatch):
    """A deleted file also means no drift, but then nothing says whether the
    drift was ever on. The written 1.0 carries its own date."""
    path = tmp_path / "drift.json"
    monkeypatch.setattr(tool, "switch_path", lambda settings, root: path)
    assert tool.main(["--factor", "4"]) == 0
    assert tool.in_force(path) == 4.0

    assert tool.main(["--off"]) == 0
    assert path.exists(), "--off must leave a trace"
    assert tool.in_force(path) == 1.0
    assert "set_at" in json.loads(path.read_text(encoding="utf-8"))


def test_the_two_switches_cannot_be_asked_for_at_once(tmp_path, monkeypatch):
    """--factor and --off together have no meaning, and guessing one of them
    would be the tool deciding what the operator meant."""
    path = tmp_path / "drift.json"
    monkeypatch.setattr(tool, "switch_path", lambda settings, root: path)
    assert tool.main(["--factor", "3", "--off"]) == 1
    assert not path.exists()


def test_the_manager_can_name_this_task():
    """Two lists of the same thing: the ValidateSet and the help table. A task
    reachable from one and not the other is the defect of 2026-11-17 again."""
    script = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    assert "'drift'," in script
    assert "n = 'drift';" in script
    assert "tools/drift.py" in script

def test_the_manager_translates_its_options_into_the_tools_flags():
    """TWO SPELLINGS OF ONE OPTION. The task is named in PowerShell, so -Factor
    and -Off are its options; tools/drift.py keeps POSIX flags so it stays
    runnable on its own inside the container. A rename on either side would
    leave the manager passing a flag argparse refuses, and the operator would
    read a usage message about a tool they never typed."""
    script = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    assert "[double]$Factor," in script
    assert "[switch]$Off," in script
    assert "$extra += '--off'" in script
    assert "$extra += @('--factor', $Factor)" in script

    source = MODULE.read_text(encoding="utf-8")
    assert '"--factor"' in source and '"--off"' in source


def test_the_help_offers_the_option_the_task_accepts():
    """An operator reads the help and types what it shows. The first version of
    this task forwarded @Rest and documented --factor, which PowerShell would
    have taken as a positional argument rather than as an option."""
    script = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    entry = script.split("n = 'drift';")[1].split("}")[0]
    assert "-Factor" in entry, entry
    assert "--factor" not in entry, entry

def test_the_switch_lives_where_both_containers_can_reach_it():
    """THE CROSS-CONTAINER HANDSHAKE, and the reason the path is not free. The
    writer runs in the scheduler container and the reader in the producer one;
    runtime/ is the only path the first mounts read-write and the second mounts
    read-only. A switch declared anywhere else would be written where nobody
    reads it -- and drift_factor() answers 1.0 to a file it cannot find, so the
    demonstration would inject nothing and would say nothing about it."""
    from ml4fd import config

    declared = Path(config.load().drift_switch)
    assert not declared.is_absolute(), declared
    assert declared.parts[0] == "runtime", declared

    compose = yaml.safe_load(
        (ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    producer = compose["services"]["producer"]["volumes"]
    assert any(str(mount).startswith("./runtime:") for mount in producer), (
        "the producer does not mount runtime/: %s" % producer)
    scheduler = compose["x-airflow-common"]["volumes"]
    writable = [m for m in scheduler
                if str(m).startswith("./runtime:") and not str(m).endswith(":ro")]
    assert writable, "the scheduler cannot write runtime/: %s" % scheduler


def test_the_tool_names_the_managers_spelling_of_its_options():
    """An operator reads this file's usage block and may then type --off at the
    manager, where PowerShell would take it for a positional argument. The
    block says which spelling belongs where."""
    source = MODULE.read_text(encoding="utf-8")
    usage = source.split('"""')[1]
    assert "-Factor" in usage and "-Off" in usage
    assert "fd-manager drift" in usage

# --- the history of the flips (B4-L4, 20/11/2026) -----------------------------
#
# WHY A HISTORY AT ALL. The switch is overwritten at every flip, which was
# enough while the stream fed nothing: a drifted transaction was served,
# scored, and that was the end of it. Since datasets.include_stream the stream
# is a TRAINING source, so a drifted transaction enters the corpus and stays --
# and "which rows carry an amount this demonstration multiplied" became a
# question the system could not answer, the only record having been overwritten
# by the next flip.

def test_each_flip_appends_one_line_and_rewrites_none(tmp_path, monkeypatch):
    path = tmp_path / "drift.json"
    monkeypatch.setattr(tool, "switch_path", lambda settings, root: path)

    assert tool.main(["--factor", "3"]) == 0
    assert tool.main(["--off"]) == 0
    assert tool.main(["--factor", "2.5"]) == 0

    lines = [json.loads(log_entry) for log_entry
             in tool.journal_path(path).read_text(encoding="utf-8").splitlines()
             if log_entry.strip()]
    assert [entry["factor"] for entry in lines] == [3.0, 1.0, 2.5]
    assert all(entry["at"] for entry in lines)
    assert all(entry["note"] for entry in lines)


def test_the_journal_sits_beside_the_switch():
    """So it is under runtime/ with it, which is the only path the scheduler
    writes and the producer reads -- and so a reader looking for the history
    finds it next to the thing it records."""
    switch = Path("/opt/project/runtime/producer/drift.json")
    assert tool.journal_path(switch).parent == switch.parent
    assert tool.journal_path(switch).name.endswith(".jsonl")


def test_an_injection_already_in_force_is_recovered_from_the_switch(tmp_path,
                                                                    monkeypatch):
    """THE ONE CASE WHERE THE PAST CAN BE RECOVERED. The journal was added
    after a drift had been switched on; the switch carries the moment it was
    set, so the first entry can state the true start of the contaminated window
    instead of the moment the journal came into being. Nothing is invented --
    the date is read from the file."""
    path = tmp_path / "drift.json"
    path.write_text(json.dumps({"factor": 3.0,
                                "set_at": "2026-11-20T11:49:52+00:00",
                                "note": "derive injectee"}), encoding="utf-8")
    monkeypatch.setattr(tool, "switch_path", lambda settings, root: path)

    assert tool.main(["--off"]) == 0
    lines = [json.loads(log_entry) for log_entry
             in tool.journal_path(path).read_text(encoding="utf-8").splitlines()
             if log_entry.strip()]
    assert len(lines) == 2, lines
    assert lines[0]["at"] == "2026-11-20T11:49:52+00:00"
    assert lines[0]["reconstructed"] is True
    assert lines[0]["factor"] == 3.0
    assert lines[1]["factor"] == 1.0


def test_nothing_is_recovered_when_no_drift_was_in_force(tmp_path, monkeypatch):
    """A switch at 1.0 means no injection, so seeding would claim a
    contaminated window that never existed -- and a reader would then exclude
    rows that are perfectly good. The understatement is the safe direction."""
    path = tmp_path / "drift.json"
    path.write_text(json.dumps({"factor": 1.0,
                                "set_at": "2026-11-20T11:50:29+00:00",
                                "note": "derive levee"}), encoding="utf-8")
    monkeypatch.setattr(tool, "switch_path", lambda settings, root: path)

    assert tool.main(["--factor", "3"]) == 0
    lines = [json.loads(log_entry) for log_entry
             in tool.journal_path(path).read_text(encoding="utf-8").splitlines()
             if log_entry.strip()]
    assert len(lines) == 1 and lines[0]["factor"] == 3.0


def test_a_refused_factor_records_nothing(tmp_path, monkeypatch):
    """The journal records flips, not attempts. A line for a change that did
    not happen would overstate the contaminated window."""
    path = tmp_path / "drift.json"
    monkeypatch.setattr(tool, "switch_path", lambda settings, root: path)
    assert tool.main(["--factor", "0"]) == 1
    assert not tool.journal_path(path).exists()
