"""A column that is not there yet says so, and says what to run.

Block: B3, B4.
Lot: B3-Q2, B4-L0.
Competency: B3-C4, B3-C5.
Covers: B3-EC23, B3-EI12.

WHAT THIS CAME FROM. The first real run of `fd-manager purge` ended in a
driver traceback: `psycopg.errors.UndefinedColumn: column p.digests does not
exist`. Nothing was wrong with the purge -- the DDL of Q1 had simply not been
applied to that database. The message sent the reader to the SQL, which is
correct and useless; the remedy was one command, and nothing said it.

So the condition is checked BEFORE the query that would fail, in one place
that every reader of the fingerprints goes through, and the message carries
the command. A missing migration is an ordinary state of a project under
construction, not an incident.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

import os

# config.load() refuses to run without them. Set here rather than
# inherited from whichever test module happens to import first.
os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import purge, runs  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


class Conn:
    """A connection whose information_schema answer we choose."""

    def __init__(self, present: bool):
        self.present = present
        self.executed = []

    def cursor(self):
        conn = self

        class Cursor:
            def __enter__(self): return self
            def __exit__(self, *exc): return False

            def execute(self, sql, params=None):
                conn.executed.append(sql)

            def fetchone(self):
                return (1,) if conn.present else None

        return Cursor()


def test_the_column_being_there_passes_silently():
    runs.assert_digests(Conn(present=True))


def test_the_column_being_absent_is_named_with_its_remedy():
    with pytest.raises(runs.MigrationMissing) as raised:
        runs.assert_digests(Conn(present=False))
    message = str(raised.value)
    assert "fd_pipeline_runs.digests" in message
    assert "Q1" in message
    assert "fd-manager.ps1 init" in message        # the command, in the message
    assert "digests -Apply" in message             # and the step after it
    assert "additif" in message                    # applying it loses nothing


def test_the_question_is_asked_of_the_catalogue_and_not_of_the_table():
    """`SELECT digests FROM fd_pipeline_runs LIMIT 1` would answer the same
    question by provoking the very error we are avoiding -- and on a table of
    millions of rows it would not be free either."""
    conn = Conn(present=True)
    runs.assert_digests(conn)
    ((sql,)) = conn.executed
    assert "information_schema.columns" in sql
    assert "fd_pipeline_runs" in sql and "digests" in sql


def test_the_planner_asks_before_it_reads_anything(monkeypatch):
    """THE ORDER IS THE WHOLE POINT. Asked after the candidates query, the
    guard would never be reached: the driver would have raised first."""
    asked = []
    monkeypatch.setattr(runs, "assert_digests",
                        lambda conn: asked.append("first"))
    monkeypatch.setattr(purge, "store_plan",
                        lambda *a, **k: asked.append("read") or (_ for _ in ()).throw(
                            RuntimeError("stop here")))
    import datetime as dt
    import types
    settings = types.SimpleNamespace(
        raw_retention_days=90, quarantine_retention_days=90,
        purge_max_share=0.5)
    with pytest.raises(RuntimeError, match="stop here"):
        purge.plan(object(), settings,
                   as_of=dt.datetime(2026, 11, 17, tzinfo=dt.timezone.utc))
    assert asked[0] == "first"


@pytest.mark.parametrize("tool", ["purge_plan.py", "digest_batches.py"])
def test_every_tool_that_reads_the_fingerprints_catches_it(tool):
    """A tool that lets it through prints a traceback, and a traceback is not
    a diagnosis. Read from the source: both tools must name the exception."""
    source = (ROOT / "tools" / tool).read_text(encoding="utf-8")
    tree = ast.parse(source)
    caught = {ast.unparse(handler.type)
              for node in ast.walk(tree)
              if isinstance(node, ast.Try)
              for handler in node.handlers if handler.type is not None}
    caught = " ".join(sorted(caught))
    assert "runs.MigrationMissing" in caught, (
        "%s does not catch the missing migration: its user gets a traceback "
        "instead of the command that fixes it" % tool)


@pytest.mark.parametrize("tool", ["purge_plan.py", "digest_batches.py"])
def test_the_tools_exit_non_zero_on_it(tool):
    """fd-manager reads the exit code. A readable message with code 0 would be
    a failure the chain walks past -- the defect of the acceptance files, in
    another costume."""
    source = (ROOT / "tools" / tool).read_text(encoding="utf-8")
    block = source.split("MigrationMissing")[1].split("\n\n")[0]
    assert "return 1" in block
