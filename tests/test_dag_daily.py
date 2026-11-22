"""The shape of the daily report DAG, read from its source.

Block: B3, B4.
Lot: B3-P4, B4-L0.
Competency: B3-C5.
Covers: B3-EE06, B3-EI06, B3-EC09, B3-EC14.

Airflow is not installed in the authoring environment, so this reads the file.
A weaker test than running the DAG, and still the one that catches what matters
here: a window taken from the clock instead of from the interval, a covered day
confused with the interval itself, a catch-up switched off, or a date crossing
an XCom boundary where only JSON travels.

THE DEFECT THIS FILE GUARDS ABOVE ALL OTHERS. Airflow's interval for "0 6 * * *"
runs from 06:00 to 06:00, which is not a calendar day. A report built on the
interval would split every night's fraud peak -- 84 of every 100 frauds between
22h and 03h -- across two documents, and nothing would raise.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from ml4fd import config

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "airflow" / "dags" / "dag_daily.py"

EXPECTED_TASKS = ("open_run", "produce", "close_run")


def _body() -> str:
    return SOURCE.read_text(encoding="utf-8")


def _code(body: str | None = None) -> str:
    """The source with its COMMENTS AND DOCSTRINGS removed.

    THE FIFTH TIME THIS PROJECT PAYS FOR THE SAME MISTAKE, and the first time
    the prose was a docstring rather than a comment. A test that greps the
    source for a forbidden construct finds the paragraph explaining why it is
    forbidden -- and the better that paragraph is, the louder the test fails.
    Dropping lines that start with "#" was enough in SQL; in Python the
    explanation lives in triple quotes.

    ast.unparse drops the comments on its own, so removing the docstring of
    every module, class and function leaves the code and nothing else.
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


def _tasks() -> dict:
    found = {}
    for node in ast.walk(ast.parse(_body())):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            call = decorator if isinstance(decorator, ast.Call) else None
            name = getattr(call.func if call else decorator, "id", None)
            if name != "task":
                continue
            found[node.name] = ({kw.arg: _value(kw.value) for kw in call.keywords}
                                if call else {})
    return found


def _task_source(name: str) -> str:
    body = _body()
    for node in ast.walk(ast.parse(body)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(body, node) or ""
    raise AssertionError("pas de tache %s" % name)


# --- the cadence -------------------------------------------------------------

def test_the_cadence_is_the_declared_one():
    """governance.yml declares it. The DAG must not drift from the
    declaration, the way dag_ingest once drifted from D1 on its name."""
    assert _dag_keywords()["schedule"] == config.load().report_schedule


def test_the_report_passes_after_the_verdicts_are_written():
    """A DEPENDENCY AND NOT A PREFERENCE: section 3 reads the verdicts that
    fell, and dag_ingest_verdicts writes them at 02:00. Produced before, the
    section would be empty every morning and nothing would say why.

    THE CLAIM IS ABOUT THE NOMINAL CONFIGURATION, and it is read from
    governance rather than from the DAG source since 2026-11-20: the cadence
    of dag_ingest_verdicts now follows verdicts.mode, so the source carries no
    literal and the demo value carries no hour at all. In demo the cadences are
    deliberately collapsed so the loop fits in minutes -- nobody claims this
    guarantee of a recording.
    """
    report_hour = int(_dag_keywords()["schedule"].split()[1])
    verdict_hour = int(config.load().verdict_schedule_nominal.split()[1])
    assert report_hour > verdict_hour, (report_hour, verdict_hour)


def test_one_run_at_a_time():
    """A catch-up of several days runs in order, and two reports never write
    while the other records."""
    assert _dag_keywords()["max_active_runs"] == 1


def test_a_run_that_overruns_is_cut_off():
    assert "dagrun_timeout" in _dag_keywords()


# --- the only catch-up of the project ----------------------------------------

def test_this_dag_catches_up():
    """A missing morning must be catchable, and a catch-up produces the
    document OF ITS DAY with the KNOWLEDGE OF ITS DAY."""
    assert _dag_keywords()["catchup"] is True


def test_it_is_the_only_dag_that_catches_up():
    """THE EXCEPTION IS DELIBERATE AND HELD AS SUCH. The stream returns the
    transaction of the instant, so a backfill of dag_ingest would replay a past
    it cannot collect; a backfill of dag_train would retrain on data that has
    moved. Only a report can be rebuilt from stored facts."""
    others = []
    for path in sorted((ROOT / "airflow" / "dags").glob("dag_*.py")):
        if path == SOURCE:
            continue
        body = path.read_text(encoding="utf-8")
        found = re.search(r"catchup=(\w+)", body)
        if found:
            others.append((path.name, found.group(1)))
    assert others, "aucun autre DAG ne declare catchup"
    for name, value in others:
        assert value == "False", "%s : catchup=%s" % (name, value)


# --- the window, which is the whole lot --------------------------------------

def test_the_window_comes_from_the_interval_and_never_from_the_clock():
    """A catch-up of 3 November must produce the document of 3 November, not
    3 November seen with today's knowledge."""
    task = _task_source("produce")
    assert 'context["data_interval_start"]' in task
    assert 'context["data_interval_end"]' in task
    code = _code("def _wrapper():\n" + "\n".join(
        "    " + line for line in task.splitlines()))
    for forbidden in ("now()", "datetime.now", "utcnow", "today()"):
        assert forbidden not in code, forbidden


def test_the_covered_day_is_derived_and_not_the_interval():
    """THE DEFECT THIS LOT HAD TO AVOID. The interval of "0 6 * * *" runs from
    06:00 to 06:00. Used as the window, it would split the night fraud peak --
    84 of every 100 frauds between 22h and 03h -- across two documents, and
    nothing would raise. The covered day is the calendar day of the interval's
    START, read in the declared zone, and the window runs midnight to
    midnight."""
    task = _task_source("produce")
    assert 'context["data_interval_start"].astimezone(zone).date()' in task
    # The midnight-to-midnight rule lives in report.window_for() since S6/R1:
    # it was written three times, and the dashboard would have been the
    # fourth. The DAG hands over the DAY, never the interval.
    assert "report.window_for(settings, day" in task
    # And the interval itself is NOT handed over as the window.
    assert "start=context[" not in task
    assert "end=context[" not in task
    # The one definition does run midnight to midnight, in the declared zone.
    import inspect
    from ml4fd import report
    # window_for() is span_for() over one day since S6/R8; the rule is there.
    assert "span_for(settings, day, day" in inspect.getsource(report.window_for)
    rule = inspect.getsource(report.span_for)
    assert "dt.time.min" in rule and "dt.timedelta(days=1)" in rule
    assert "settings.report_timezone" in rule


def test_the_knowledge_date_is_the_end_of_the_interval():
    """The instant the run is for. A catch-up months later still asks "what was
    known at six on the 4th of November"."""
    assert 'knowledge_at=context["data_interval_end"]' in _task_source("produce")


def test_the_zone_comes_from_governance():
    task = _task_source("produce")
    assert "settings.report_timezone" in task
    assert "'Europe/Paris'" not in task


# --- XCom carries JSON -------------------------------------------------------

def test_nothing_but_json_crosses_a_task_boundary():
    """THE DEFECT OF 16 NOVEMBER 2026, in another DAG: a UUID crossed an XCom
    and failed eleven frames deep on "'str' object has no attribute 'hex'".
    Here a date, a datetime and a Decimal would all travel badly, so they are
    converted AT THE BOUNDARY rather than discovered beyond it."""
    task = _task_source("produce")
    assert '"report_date": recorded["report_date"].isoformat()' in task
    assert '"knowledge_date": recorded["knowledge_date"].isoformat()' in task
    assert 'float(recorded["threshold"])' in task
    # And the null case of a day with no alert is handled, not assumed.
    assert 'if recorded["threshold"] is not None else None' in task


# --- the two roles -----------------------------------------------------------

def test_the_tasks_are_the_expected_ones():
    assert sorted(_tasks()) == sorted(EXPECTED_TASKS)


def test_the_report_runs_under_the_reporting_role():
    """fd_report has no access to the raw zone. If the chain needed more than
    the exploited layer, this is where it would fail -- and that failure is the
    guarantee."""
    assert 'db.connect("fd_report")' in _task_source("produce")
    assert 'db.connect("fd_ingest")' not in _task_source("produce")


@pytest.mark.parametrize("name", ["open_run", "close_run"])
def test_the_pipeline_journal_is_written_under_the_ingestion_role(name):
    """The journal is the pipeline's, not the report's: fd_report may write
    only fd_reports."""
    assert 'db.connect("fd_ingest")' in _task_source(name)


def test_the_journal_records_no_invented_counts():
    """"Lines read, valid, rejected" has no meaning for a report, and
    n_read = n_payments would be a figure invented to fill a column. What the
    report covered is in fd_reports, which was built for it."""
    assert "record_counts" not in _code()


# --- the failure convention --------------------------------------------------

def test_a_failure_closes_the_run_and_only_if_nobody_closed_it():
    """The convention consolidated in N9: a callback on the DAG, and
    runs.abandon so a failure after close_run cannot overwrite a success."""
    keywords = _dag_keywords()
    assert "on_failure_callback" in keywords
    called = {ast.unparse(node.func)
              for node in ast.walk(ast.parse(_body()))
              if isinstance(node, ast.Call)}
    assert "runs.abandon" in called
    assert "runs.finish" in called          # the success path
    assert "one_failed" not in _code()


def test_every_dag_of_the_project_uses_the_same_failure_convention():
    """ONE CONVENTION FOR ONE JOB, held across the directory rather than per
    file: N7 invented a second mechanism and nothing noticed for a lot."""
    checked = 0
    for path in sorted((ROOT / "airflow" / "dags").glob("dag_*.py")):
        code = _code(path.read_text(encoding="utf-8"))
        if "on_failure_callback" in code:
            checked += 1
            assert "one_failed" not in code, path.name
    assert checked >= 3, checked


def test_the_dag_declares_its_lot_and_its_competencies():
    tags = _dag_keywords()["tags"]
    assert "S4" in tags and "P4" in tags
