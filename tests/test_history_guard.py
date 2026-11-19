"""The historical load is idempotent at the RUN level, not only the task level.

Covers: EC23, EC24
Lot: L1
Competency: C2, C4

WHY THIS TEST EXISTS. raw.batch_id_for() derives the batch_id from the run_id,
so a retried TASK writes nothing -- and that was read as "the load is
idempotent". It is not: a NEW run has a new run_id, hence a new batch_id, hence
a second copy of the same 555,719 lines. That is what an operator starts after
a failure, and four copies reached this project's raw zone that way. No purge
removes them: the cutoff is a date, and the four copies share it.

The guard therefore lives in preflight, BEFORE the CSV is fetched, and it reads
ONE definition (sql/checks/history_already_loaded.sql). The two branches matter
as much as the refusal itself: a FAILED run's rows are replayable, a SUCCEEDED
run's rows are the history itself.
"""
from __future__ import annotations

import ast
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import db, raw  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DAG_FILE = ROOT / "airflow" / "dags" / "dag_load_history.py"
GUARD_SQL = ROOT / "sql" / "checks" / "history_already_loaded.sql"
DAG_ID = "dag_load_history"
BATCH = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
ENDED = datetime(2026, 11, 15, 20, 12, tzinfo=timezone.utc)


@pytest.fixture
def rows(monkeypatch):
    """db.query_file answered from a list, and the calls recorded."""
    state = {"answer": [], "calls": []}

    def fake(conn, relative, **params):
        state["calls"].append((relative, params))
        return state["answer"]

    monkeypatch.setattr(db, "query_file", fake)
    return state


class Forbidden:
    """A connection that fails the test if it is used at all."""

    def cursor(self):                               # pragma: no cover
        raise AssertionError("the database was queried")


# --- the refusal itself --------------------------------------------------------

def test_a_replay_is_never_refused(rows):
    """A replay READS the raw zone and never writes it, so holding rows is
    precisely its reason to run -- the guard must not even look."""
    raw.refuse_second_load(Forbidden(), DAG_ID, replay=str(BATCH))
    assert rows["calls"] == []


def test_an_empty_raw_zone_lets_the_load_through(rows):
    rows["answer"] = []
    raw.refuse_second_load(object(), DAG_ID)
    assert rows["calls"] == [("checks/history_already_loaded.sql",
                              {"dag_id": DAG_ID})]


def test_rows_left_by_a_failed_run_send_the_operator_to_replay(rows):
    """The case that made four copies: `load` relaunched after a failure."""
    rows["answer"] = [("failed", "manual__2026-11-15", BATCH, 555_719, ENDED)]
    with pytest.raises(raw.HistoryAlreadyLoaded) as refused:
        raw.refuse_second_load(object(), DAG_ID)
    message = str(refused.value)
    assert "replay -Batch %s" % BATCH in message
    assert str(555_719) in message or "555719" in message
    assert "reset" not in message          # the data is there: do not wipe it


def test_a_finished_load_is_not_offered_a_replay(rows):
    """Replaying a successful batch would redo the whole aval for nothing; the
    only honest remedy is an empty database, and it must be spelled out."""
    rows["answer"] = [("success", "manual__2026-11-10", BATCH, 555_719, ENDED)]
    with pytest.raises(raw.HistoryAlreadyLoaded) as refused:
        raw.refuse_second_load(object(), DAG_ID)
    message = str(refused.value)
    assert "ALREADY LOADED" in message
    assert "reset" in message
    assert "replay -Batch" not in message


def test_a_success_decides_even_when_a_failure_is_listed_too(rows):
    """Four copies means several rows at once. A single successful batch means
    the history IS loaded, whatever else failed around it."""
    rows["answer"] = [("success", "r1", BATCH, 555_719, ENDED),
                      ("failed", "r2", uuid.uuid4(), 120_000, ENDED)]
    with pytest.raises(raw.HistoryAlreadyLoaded) as refused:
        raw.refuse_second_load(object(), DAG_ID)
    assert "ALREADY LOADED" in str(refused.value)


def test_every_held_batch_is_named(rows):
    """An operator cannot clean up what the refusal does not show."""
    second = uuid.uuid4()
    rows["answer"] = [("failed", "r1", BATCH, 555_719, ENDED),
                      ("failed", "r2", second, 555_719, None)]
    with pytest.raises(raw.HistoryAlreadyLoaded) as refused:
        raw.refuse_second_load(object(), DAG_ID)
    message = str(refused.value)
    assert str(BATCH) in message and str(second) in message


def test_a_run_still_open_is_not_silently_ignored(rows):
    """A 'running' row of an interrupted load holds rows all the same."""
    rows["answer"] = [("running", "r1", BATCH, 300_000, None)]
    with pytest.raises(raw.HistoryAlreadyLoaded):
        raw.refuse_second_load(object(), DAG_ID)


# --- one definition, and it is read before anything is fetched -----------------

def test_the_question_is_asked_in_sql_not_in_python():
    text = GUARD_SQL.read_text(encoding="utf-8")
    assert "%(dag_id)s" in text, "the dag_id must be a parameter, not a literal"
    assert "fd_raw_events" in text and "fd_pipeline_runs" in text
    assert "count(*)" in text, "the refusal names a row count"


def test_a_purged_batch_stops_blocking_the_load():
    """Proven on PostgreSQL 16: a run that succeeded but whose rows the purge
    has since removed does NOT appear. With a LEFT JOIN it would, and a database
    purged down to nothing could never be loaded again -- the refusal would
    outlive the data it protects."""
    text = GUARD_SQL.read_text(encoding="utf-8").upper()
    assert "LEFT JOIN" not in text
    assert "JOIN   FD_PIPELINE_RUNS" in text or "JOIN FD_PIPELINE_RUNS" in text


def test_the_dag_does_not_ask_the_question_its_own_way():
    """The third reproduction of a rule written twice is what this guards."""
    text = DAG_FILE.read_text(encoding="utf-8")
    assert "refuse_second_load" in text
    for spelled in ("state = 'success'", 'state = "success"',
                    "JOIN fd_pipeline_runs", "history_already_loaded.sql"):
        assert spelled not in text, "the DAG spells the guard itself: %s" % spelled


def test_the_guard_runs_in_preflight_before_the_csv_is_fetched():
    """Refusing after the download would still have cost twenty minutes and a
    fifth copy of the file on disk."""
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    bodies = {node.name: node for node in ast.walk(tree)
              if isinstance(node, ast.FunctionDef)}
    assert "preflight" in bodies and "write_raw" in bodies

    def calls(node):
        return {ast.unparse(n.func) for n in ast.walk(node)
                if isinstance(n, ast.Call) and not isinstance(n.func, ast.Constant)}

    assert "raw.refuse_second_load" in calls(bodies["preflight"])
    assert "raw.refuse_second_load" not in calls(bodies["write_raw"])
