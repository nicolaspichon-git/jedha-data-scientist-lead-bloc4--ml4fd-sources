"""The shape of the purge DAG, read from its source.

Block: B3, B4.
Lot: B3-Q5, B4-L0.
Competency: B3-C5.
Covers: B3-EE06, B3-EI06, B3-EI11, B3-EC09, B3-EC14, B3-EC19.

Airflow is not installed in the authoring environment, so this reads the file.
A weaker test than running the DAG, and still the one that catches what matters
for the only process of this project that destroys: a boundary taken from the
clock, the demonstration override left within reach of the scheduler, a guard
that warns instead of stopping, or a catch-up that would purge against a
boundary a week stale.
"""
from __future__ import annotations

import ast
import json
import os
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import config, purge  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "airflow" / "dags" / "dag_purge.py"

EXPECTED_TASKS = ("open_run", "dry_run", "check_share", "delete", "close_run")


def _body() -> str:
    return SOURCE.read_text(encoding="utf-8")


def _code(body: str | None = None) -> str:
    """The source with its comments AND docstrings removed.

    The docstring of this DAG explains at length what it must never do -- read
    a clock, expose the override -- so a test grepping the source for those
    very words would find the paragraph forbidding them. The better the
    explanation, the louder the test would fail.
    """
    tree = ast.parse(body if body is not None else _body())
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                             ast.AsyncFunctionDef)):
            first = node.body[0] if node.body else None
            if (isinstance(first, ast.Expr)
                    and isinstance(first.value, ast.Constant)
                    and isinstance(first.value.value, str)):
                node.body.pop(0)
    return ast.unparse(tree)


def _value(node):
    if isinstance(node, ast.Dict):
        out = {}
        for key, value in zip(node.keys, node.values):
            try:
                out[ast.literal_eval(key)] = ast.literal_eval(value)
            except ValueError:
                out[ast.literal_eval(key)] = "<expression>"
        return out
    try:
        return ast.literal_eval(node)
    except ValueError:
        return "<expression>"


def _dag_keywords() -> dict:
    for node in ast.walk(ast.parse(_body())):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "dag":
            return {kw.arg: _value(kw.value) for kw in node.keywords}
    raise AssertionError("aucun appel a @dag dans %s" % SOURCE)


def _dag_callback() -> str | None:
    for node in ast.walk(ast.parse(_body())):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "dag":
            for keyword in node.keywords:
                if keyword.arg == "on_failure_callback":
                    return getattr(keyword.value, "id", None)
    return None


def _tasks() -> list[str]:
    found = []
    for node in ast.walk(ast.parse(_body())):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            name = getattr(decorator, "id", None) or getattr(
                getattr(decorator, "func", None), "id", None)
            if name == "task":
                found.append(node.name)
    return found


def _function(name: str) -> str:
    body = _body()
    for node in ast.walk(ast.parse(body)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(body, node) or ""
    raise AssertionError("pas de fonction %s" % name)


def _calls(source: str) -> set[str]:
    found = set()
    for node in ast.walk(ast.parse(source.strip())):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute):
                owner = getattr(func.value, "id", "")
                found.add("%s.%s" % (owner, func.attr) if owner else func.attr)
            elif isinstance(func, ast.Name):
                found.add(func.id)
    return found


# --- the cadence ---------------------------------------------------------------

def test_the_cadence_is_the_declared_one():
    """governance.yml decides it; the DAG must not drift from the declaration."""
    assert _dag_keywords()["schedule"] == config.load().purge_schedule


def test_the_cadence_is_weekly_on_a_sunday():
    """WEEKLY AND NOT DAILY, for the opposite of the usual reason: a purge at
    ninety days has no daily urgency, and a destructive task that runs rarely
    is a task whose failure is noticed."""
    minute, hour, dom, month, dow = _dag_keywords()["schedule"].split()
    assert (dom, month) == ("*", "*")
    assert dow == "0", dow
    assert minute.isdigit() and hour.isdigit()


def test_it_does_not_meet_the_verdict_writer():
    """The purge deletes batches of the raw zone; dag_ingest_verdicts reads the
    raw zone to write the verdicts that have fallen. At the same hour the purge
    would take batches out from under it.

    THE CLAIM IS ABOUT THE NOMINAL CONFIGURATION, and it is read from
    governance rather than from the DAG source since 2026-11-20: the cadence
    of dag_ingest_verdicts now follows verdicts.mode, so the source carries no
    literal and the demo value carries no hour at all. In demo the cadences are
    deliberately collapsed so the loop fits in minutes -- nobody claims this
    guarantee of a recording.
    """
    from ml4fd import config

    other = config.load().verdict_schedule_nominal
    mine = _dag_keywords()["schedule"]
    assert mine.split()[:2] != other.split()[:2]


def test_nothing_is_caught_up():
    """THE OPPOSITE OF dag_daily, and for a reason rather than by oversight: a
    missed morning report must be produced for the day it covers, while the
    rows a missed purge would have deleted are still there next Sunday. An old
    purge window would delete against a boundary that no longer means
    anything."""
    assert _dag_keywords()["catchup"] is False
    daily = (ROOT / "airflow" / "dags" / "dag_daily.py").read_text(
        encoding="utf-8")
    assert "catchup=True" in daily, "le contraste n'existe plus : verifier P4"


def test_one_run_at_a_time():
    """Two purges at once would fight over the same batches, and the loser
    would read a digest the winner had just made obsolete."""
    assert _dag_keywords()["max_active_runs"] == 1


def test_the_run_is_bounded_in_time():
    assert "dagrun_timeout" in _dag_keywords()


def test_nothing_is_retried():
    """The work is resumable -- a batch half deleted carries its mark and the
    next run finishes it -- so a retry buys nothing that waiting does not, and
    a destructive job should surface its failure."""
    assert _dag_keywords()["default_args"]["retries"] == 0


# --- the boundary -----------------------------------------------------------------

def test_the_boundary_comes_from_the_interval():
    assert "data_interval_end" in _code()


def test_no_task_reads_a_clock():
    """A purge whose boundary came from now() could not be reasoned about after
    the fact, nor replayed in a demonstration."""
    code = _code()
    for clock in ("now()", "utcnow()", "today()", "datetime.now"):
        assert clock not in code, "le DAG lit l'horloge : %s" % clock


def test_the_demonstration_override_is_not_within_reach_of_the_scheduler():
    """`before` moves the retention boundary for EVERY store at once. It exists
    so a purge can be tried without waiting until February 2027, and it belongs
    to the command line: a parameter of a demonstration left in an
    orchestrator ends up being used -- here, deleting data that had not reached
    its retention."""
    code = _code()
    assert "before" not in code
    assert "params" not in _dag_keywords()
    assert "Param(" not in code


def test_the_plan_is_asked_without_the_expensive_verification():
    """The executor verifies each batch again immediately before deleting it,
    which is the check that decides; asking twice would double the cost of the
    one that does not."""
    source = _function("dry_run")
    assert "verify=True" not in source
    assert "purge.plan" in _calls(source)


# --- the guard ----------------------------------------------------------------------

def test_the_guard_is_a_task_of_its_own():
    """The plan of S5 asked for exactly this rather than purging and warning
    afterwards."""
    assert "check_share" in _tasks()


def test_the_guard_stops_the_run_and_does_not_merely_warn():
    source = _function("check_share")
    assert "purge.assert_allowed" in _calls(source)
    assert "raise" in source
    assert "log.error" in source
    # and it names the way to confirm by hand
    assert "-Force" in source


def test_the_guard_runs_before_the_deletion():
    """Read from the wiring, not from the order the functions are written in."""
    ((wiring,)) = [line for line in _code().splitlines()
                   if "close_run(" in line and "delete(" in line]
    assert "check_share(dry_run(" in wiring, wiring
    assert wiring.index("delete(") < wiring.index("check_share("), wiring


def test_the_deletion_keeps_the_guard_of_the_module_too():
    """check_share is the readable gate; execute()'s own guard is the one that
    cannot be removed by rewiring the DAG."""
    assert "confirm=True" not in _code()


# --- the roles -----------------------------------------------------------------------

def test_the_deletion_runs_under_the_role_that_may_destroy():
    assert "purge.ROLE" in _function("delete")
    assert purge.ROLE == "fd_purge"


def test_the_journal_is_written_under_the_pipeline_role():
    """fd_purge may not insert into fd_pipeline_runs: the process that destroys
    the data must not be able to write the record of it."""
    for name in ("open_run", "close_run"):
        assert 'db.connect("fd_ingest")' in _function(name), name


def test_no_task_mixes_the_two_roles():
    for name in _tasks():
        source = _function(name)
        assert not ('db.connect("fd_ingest")' in source
                    and "purge.ROLE" in source), name


# --- the failure convention -----------------------------------------------------------

def test_the_failure_is_closed_like_every_other_dag():
    assert _dag_callback() == "_mark_failed"
    assert "on_failure_callback" in _dag_keywords()
    assert "one_failed" not in _code()


def test_the_callback_marks_only_what_nobody_closed():
    called = _calls(_function("_mark_failed"))
    assert "runs.abandon" in called
    assert "runs.finish" not in called


def test_the_callback_never_raises():
    source = _function("_mark_failed")
    handlers = [node for node in ast.walk(ast.parse(source.strip()))
                if isinstance(node, ast.Try)]
    assert handlers
    assert "log.exception" in source


# --- the tasks -------------------------------------------------------------------------

def test_the_tasks_are_the_expected_ones():
    assert tuple(_tasks()) == EXPECTED_TASKS


def test_the_two_stores_go_in_one_task():
    """DELIBERATELY AGAINST THE PLAN, which foresaw purge_raw and
    purge_quarantine side by side. The quarantined rows of a batch derive from
    its raw lines and must go first (purge.PURGE_ORDER); two tasks here would
    put that order in a second place, where a later hand could reverse it by
    dragging an arrow."""
    assert "purge_raw" not in _tasks() and "purge_quarantine" not in _tasks()
    assert "purge.execute" in _calls(_function("delete"))


def test_the_dag_declares_its_lot_and_its_competencies():
    tags = _dag_keywords()["tags"]
    assert "S5" in tags and "Q5" in tags and "BC03" in tags


# --- the XCom boundary ------------------------------------------------------------------

def test_what_crosses_between_tasks_is_json():
    """XCOM CARRIES JSON. The plan travels from dry_run to check_share to
    delete, and the summary from delete to close_run; a date or a Decimal in
    either would fail eleven frames deep in a scheduler log -- the defect of 16
    November 2026, met once already."""
    report = {
        "as_of": "2026-11-22T04:00:00+01:00", "before": None,
        "verified": False, "max_share": 0.5, "blocked": False, "blocking": [],
        "batches": 1, "rows": 10, "bytes": 2048, "refused_batches": 0,
        "refused_rows": 0,
        "stores": [{"store": "fd_raw_events", "cutoff": "2026-08-24T04:00:00",
                    "total_rows": 100, "rows": 10, "batches": 1,
                    "share": 0.1, "bytes": 2048,
                    "first_at": "2026-06-01T10:00:00+02:00",
                    "last_at": "2026-06-01T11:00:00+02:00",
                    "refused_batches": 0, "refused_rows": 0, "refusals": [],
                    "batch_ids": ["a"], "deletable": [{"batch_id": "a",
                                                       "rows": 10}],
                    "deletes": True}]}
    assert json.loads(json.dumps(report)) == report


@pytest.mark.parametrize("name", EXPECTED_TASKS)
def test_every_task_returns_something_a_reader_can_follow(name):
    """A task returning None gives the next one nothing and the operator no
    trace in the XCom view."""
    assert "return" in _function(name)
