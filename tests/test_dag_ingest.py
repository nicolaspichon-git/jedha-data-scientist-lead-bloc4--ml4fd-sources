"""The shape of the real-time DAG, read from its source.

Block: B3, B4.
Lot: B3-N7, B4-L0.
Competency: B3-C5.
Covers: B3-EI06, B3-EC09, B3-EC14, B3-EC23.

Airflow is not installed in the authoring environment, so this reads the file.
A weaker test than running the DAG, and still the one that catches what matters
here: a cadence that no longer matches the declared capacity, a retry on the
only task that must not be retried, or a failure that leaves the run counted as
neither a success nor a failure.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

from ml4fd import config

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "airflow" / "dags" / "dag_ingest.py"

EXPECTED_TASKS = ("poll", "validate", "type_and_feature", "score_batch",
                  "raise_and_notify_fraud", "close_run",
                  # N9: stage (3c) of diagram 3.3, which the plan of S3 never
                  # listed and nobody missed for a whole process.
                  "check", "raise_and_notify_ops")


def _body() -> str:
    return SOURCE.read_text(encoding="utf-8")


def _tasks() -> dict:
    """Every @task function of the DAG, with its decorator keywords."""
    tree = ast.parse(_body())
    found = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            name = getattr(decorator, "id", None)
            call = decorator if isinstance(decorator, ast.Call) else None
            if call is not None:
                name = getattr(call.func, "id", None)
            if name != "task":
                continue
            keywords = {}
            if call is not None:
                for keyword in call.keywords:
                    try:
                        keywords[keyword.arg] = ast.literal_eval(keyword.value)
                    except ValueError:
                        keywords[keyword.arg] = "<expression>"
            found[node.name] = keywords
    return found


def _value(node):
    """A literal where possible, and a dict evaluated KEY BY KEY otherwise.

    default_args holds a timedelta, which literal_eval cannot evaluate: an
    all-or-nothing reading would turn the whole dict into a placeholder and the
    retry count with it. That is how the first version of this file failed --
    on itself, not on the DAG.
    """
    if isinstance(node, ast.Dict):
        out = {}
        for key, value in zip(node.keys, node.values):
            try:
                out[ast.literal_eval(key)] = ast.literal_eval(value)
            except ValueError:
                try:
                    out[ast.literal_eval(key)] = "<expression>"
                except ValueError:
                    pass
        return out
    try:
        return ast.literal_eval(node)
    except ValueError:
        return "<expression>"


def _function(name: str) -> str:
    """The source of a module-level function of the DAG."""
    body = _body()
    for node in ast.parse(body).body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(body, node) or ""
    raise AssertionError("pas de fonction %s dans %s" % (name, SOURCE))


def _task_source(name: str) -> str:
    """The source of a @task function of the DAG, which is NESTED in the dag
    function and so invisible to _function, which reads the module level only.
    That is what the first version of the L5 tests called: _function raised on
    score_batch, a task that is very much there."""
    body = _body()
    for node in ast.walk(ast.parse(body)):
        if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == name):
            return ast.get_source_segment(body, node) or ""
    raise AssertionError("pas de tache %s dans %s" % (name, SOURCE))


def _calls(source: str) -> set:
    """Every function CALLED in a piece of source, by name.

    Read from the syntax and not from the text: the third time in this project
    that a test grepping for a call matched the docstring explaining why that
    call is not used.
    """
    return {ast.unparse(node.func)
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.Call)}


def _dag_callback() -> str:
    """on_failure_callback as written -- a NAME, which no literal_eval reads."""
    body = _body()
    for node in ast.walk(ast.parse(body)):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "dag":
            for keyword in node.keywords:
                if keyword.arg == "on_failure_callback":
                    return ast.unparse(keyword.value)
    return ""


def _dag_keywords() -> dict:
    tree = ast.parse(_body())
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call)
                and getattr(node.func, "id", None) == "dag"):
            return {keyword.arg: _value(keyword.value)
                    for keyword in node.keywords}
    raise AssertionError("aucun appel a @dag dans %s" % SOURCE)


# --- the cadence, and what depends on it --------------------------------------

def test_the_cadence_is_the_declared_one():
    """governance.yml declares it, and config checks the product against
    threshold.API_TXN_PER_DAY. The DAG must not drift from the declaration."""
    assert _dag_keywords()["schedule"] == config.load().realtime_schedule


def test_only_one_run_at_a_time():
    """One 502 costs 25 s and brings the worst realistic run to about 47 s,
    still inside the minute; two would overrun. A slow run must DELAY the next,
    never run beside it, or two collectors would write the same batch."""
    assert _dag_keywords()["max_active_runs"] == 1


def test_a_run_that_overruns_is_cut_off():
    """Without a timeout, a stuck run at this cadence builds a queue of
    minute-runs behind it."""
    assert "dagrun_timeout" in _dag_keywords()


def test_it_never_backfills():
    """1,440 runs a day: a backfill would try to replay a past it cannot
    collect -- the API returns the transaction of the instant and nothing
    else."""
    assert _dag_keywords()["catchup"] is False


# --- the tasks ----------------------------------------------------------------

def test_the_tasks_are_the_expected_ones():
    assert sorted(_tasks()) == sorted(EXPECTED_TASKS)


def test_the_network_task_is_never_retried():
    """THE asymmetry of this DAG. A retry would make five fresh calls and throw
    the first five away: the API returns the transaction of the instant, so a
    reading not written is a transaction nobody will see again."""
    assert _tasks()["poll"].get("retries") == 0


def test_everything_after_the_poll_may_be_retried():
    """It is replayable from the raw zone, which is why the hand-off goes
    through the table."""
    defaults = _dag_keywords()["default_args"]
    assert defaults["retries"] >= 1


def test_a_failure_closes_the_run_as_failed():
    """WITHOUT THIS THE GUARD OF M1 COUNTS WRONG. It counts state = 'success'
    and state = 'failed'; a run abandoned in 'running' is in neither, so a
    window of 1,440 runs a day could be full of silent failures and look like a
    window with none."""
    assert _dag_callback() == "_mark_failed"
    assert "runs.abandon" in _calls(_function("_mark_failed"))


def test_the_marking_is_not_a_task_with_a_trigger_rule():
    """N9 REPLACED N7's TASK, and this is the regression to guard.

    trigger_rule="one_failed" needs an upstream task in the failed state and
    the scheduler to place one more task. dagrun_timeout fails the RUN and kills
    what is running, so the task could be skipped in exactly the case the
    two-minute timeout exists to catch -- leaving the row in 'running', the hole
    the marking exists to close. The callback fires on the run's failure.
    """
    for name, keywords in _tasks().items():
        assert "trigger_rule" not in keywords, name
    # And nowhere else either -- read from the syntax, not from the text, or
    # the paragraph above would match itself.
    for node in ast.walk(ast.parse(_body())):
        if isinstance(node, ast.Call):
            for keyword in node.keywords:
                assert keyword.arg != "trigger_rule", ast.unparse(node.func)


def test_the_callback_never_raises():
    """A callback that throws loses the run it was closing AND the traceback of
    the failure that called it. Best effort, logged, never propagated."""
    source = _function("_mark_failed")
    tree = ast.parse(source)
    handlers = [handler for node in ast.walk(tree)
                if isinstance(node, ast.Try) for handler in node.handlers]
    assert handlers, "aucun try/except dans _mark_failed"
    assert any(getattr(handler.type, "id", None) == "Exception"
               for handler in handlers)
    assert "log.exception" in source


def test_both_dags_close_a_failure_the_same_way():
    """ONE CONVENTION FOR ONE JOB. dag_load_history has used the callback since
    L2; N7 invented a second mechanism for the same thing, and nothing held the
    two files to the same answer. This does."""
    other = (ROOT / "airflow" / "dags" / "dag_load_history.py").read_text(
        encoding="utf-8")
    assert "on_failure_callback=_mark_failed" in other
    assert "on_failure_callback=_mark_failed" in _body()


def test_the_callback_marks_only_what_nobody_closed():
    """N9 HUNG TWO TASKS AFTER close_run, which records the success. With
    runs.finish the callback would overwrite that success with a failure, and
    the journal would report a failed ingestion while the transactions sit in
    the table -- which M1's guard reads as a reason to refuse training."""
    called = _calls(_function("_mark_failed"))
    assert "runs.abandon" in called
    assert "runs.finish" not in called


# --- N9: the controls and the operations channel -------------------------------

def test_the_controls_run_after_the_run_was_closed():
    """They read fd_pipeline_runs, and close_run writes the counters and the
    final state. Placed before it, the control would read its own run as still
    'running' with a null n_read and report an empty run every minute -- a
    control firing on its own execution."""
    body = _body()
    wiring = body[body.index("    polled = poll()"):]
    assert "closed = close_run(polled, counts, alerted)" in wiring
    assert "raise_and_notify_ops(check(closed))" in wiring


def test_the_two_audiences_are_two_tasks():
    """The analyst is told a TRANSACTION was flagged; the pipeline owner is told
    a STATE OF THE CHAIN changed. One task each, named for its reader."""
    tasks = _tasks()
    assert "raise_and_notify_fraud" in tasks
    assert "raise_and_notify_ops" in tasks
    assert "raise_and_notify" not in tasks


def test_a_red_control_does_not_fail_the_run():
    """Read from the syntax and not from the text: a control that failed the run
    would stop the ingestion it exists to watch."""
    for node in ast.walk(ast.parse(_body())):
        if isinstance(node, ast.FunctionDef) and node.name == "check":
            assert not [inner for inner in ast.walk(node)
                        if isinstance(inner, ast.Raise)]
            break
    else:
        raise AssertionError("aucune tache check dans %s" % SOURCE)


def test_a_broken_operations_channel_shows_as_a_red_task():
    """The last warning available when the channel for warning is broken. The
    state is not recorded either, so the next run retries."""
    body = _body()
    task = body[body.index("def raise_and_notify_ops("):
                body.index("    polled = poll()")]
    assert 'sent["failed"]' in task
    assert "raise AssertionError" in task


def test_the_controls_task_is_the_only_reader_of_the_health_module():
    """The judgement lives in ml4fd/health.py, which is testable without a
    database; the DAG only calls it."""
    body = _body()
    assert body.count("health.check(") == 1
    assert body.count("health.notify_ops(") == 1


def test_the_callback_announces_only_a_closure_that_happened():
    """runs.finish logged "run ... closed" even when the UPDATE matched no row
    -- the case where the failure precedes runs.start(). It now returns the
    count, and the callback reports on that count."""
    import inspect

    from ml4fd import runs

    closing = inspect.getsource(runs.finish)
    assert "closed = cur.rowcount" in closing
    assert "return closed" in closing
    assert "if closed:" in _function("_mark_failed")


# --- the hand-off goes through the raw zone -----------------------------------

def test_every_stage_after_the_poll_rereads_the_raw_zone():
    """Airflow passes JSON between tasks, not Python objects -- and re-polling
    is not an option, since five calls cost about 27 s and the API will never
    return the same five transactions again."""
    body = _body()

    # ONE task touches the network. That is the property; the rest follows.
    assert body.count("stream.poll(") == 1

    # And each stage that needs the records goes back to the table for them --
    # by whichever route fits: validate needs the ENVELOPES, because the clock
    # bound reads our_ms and latency_ms, while type_and_feature needs the rows.
    validate_task = body[body.index("def validate("):
                         body.index("def type_and_feature(")]
    typing_task = body[body.index("def type_and_feature("):
                       body.index("def score_batch(")]
    assert "raw.read_batch(conn, batch_id)" in validate_task
    assert "stream.records_from_raw(conn, batch_id)" in typing_task
    for task_body in (validate_task, typing_task):
        assert "stream.poll(" not in task_body


def test_the_validation_task_keeps_the_clock_bound():
    """N7's first draft called checks.validate_batch on the rows alone, which
    silently dropped CLOCK_IMPLAUSIBLE from the DAG while the manual runner kept
    it -- the rule N0 earned by measuring one reading 818 s out, our own process
    suspended. The envelope carries our_ms and latency_ms; the row does not."""
    body = _body()
    validate_task = body[body.index("def validate("):
                         body.index("def type_and_feature(")]
    assert "stream.validate_raw(" in validate_task
    assert "settings.clock_tolerance_s" in validate_task
    # The rule lives in ONE place, reachable from both doors.
    assert "checks.validate_batch(" not in validate_task


def test_the_quarantined_lines_are_read_back_and_not_carried():
    """A set of line numbers crossing the task boundary as JSON would be a
    second record of something the quarantine table already holds."""
    assert "quarantine.rejected_lines(conn, batch_id)" in _body()


def test_the_counters_close_on_what_reached_the_raw_zone():
    """A FAILED HTTP CALL IS NOT A READ LINE: fd_pipeline_runs carries
    CHECK (n_read = n_valid + n_rejected), and a failed call produced no
    record."""
    body = _body()
    assert "runs.record_counts(" in body
    assert '"n_valid": n_read - rejected_lines' in body


# --- N6's fourth guarantee still holds ----------------------------------------

def test_the_realtime_dag_writes_no_verdict():
    """N6: the write lives in dag_ingest_verdicts, daily, sharing no task with
    the scoring."""
    tree = ast.parse(_body())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imported.add(alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name.split(".")[0])
    assert "truth" not in imported
    assert "verdicts" not in imported
    assert "load_ground_truth" not in _body()


def test_the_scoring_is_a_task_of_its_own():
    """A tracking server that is down must show as a RED TASK and not as a
    warning line inside a green run: the operator who reads `fd-manager
    progress` has to see which stage stopped."""
    assert "score_batch" in _tasks()
    body = _body()
    scoring = body[body.index("def score_batch"):body.index("def raise_and_notify")]
    # No swallowing of the failure inside the task.
    assert "except Exception" not in scoring


def test_the_dag_declares_the_lot_and_the_competencies():
    keywords = _dag_keywords()
    assert "N7" in keywords["tags"]
    assert "S3" in keywords["tags"]


def test_the_module_header_states_the_measured_cadence():
    """The reason for 5 calls a minute is a measurement, not a preference: 5 x
    1,440 = 7,200, the rate the threshold and the 60-alert capacity rest on."""
    header = _body()[:_body().index('"""', 10)]
    assert "7,200" in header or "7200" in header
    assert re.search(r"13 calls a minute|13 appels", header)


# --- XCom ne transporte que du JSON -------------------------------------------

def test_every_task_receiving_the_batch_converts_it_back_to_a_uuid():
    """THE defect of 16 November 2026. XCom carries JSON, so the identifier
    arrives as a string, and raw.read_batch touches .hex first thing: it failed
    on "'str' object has no attribute 'hex'", eleven frames deep, naming neither
    the parameter nor the fix. dag_load_history has converted at the top of
    three tasks since L2; this DAG did not.
    """
    body = _body()
    bounds = {"validate": "def type_and_feature(",
              "type_and_feature": "def score_batch(",
              "score_batch": "def raise_and_notify_fraud("}
    for name, following in bounds.items():
        task_body = body[body.index("def %s(" % name):body.index(following)]
        assert 'polled["batch_id"]' in task_body, name
        assert '_uuid.UUID(polled["batch_id"])' in task_body, name
        # And never the raw string handed to a function.
        assert 'batch_id = polled["batch_id"]' not in task_body, name


def test_the_poll_serialises_the_identifier_as_a_string():
    """It must, since XCom is JSON -- which is exactly why the others
    convert."""
    body = _body()
    poll_body = body[body.index("def poll("):body.index("def validate(")]
    assert '"batch_id": str(batch_id)' in poll_body


def test_the_raw_reader_refuses_a_string_and_names_the_fix():
    """A guard where the failure happened, so the next one is a named error
    instead of an AttributeError eleven frames down."""
    import pytest as _pytest

    from ml4fd import raw

    with _pytest.raises(TypeError, match="uuid.UUID"):
        list(raw.read_batch(None, "4d4fb164-3734-57ff-a04c-01ea278876b8"))


# --- L5: the pipeline no longer loads the model -----------------------------

def test_the_scoring_task_no_longer_loads_a_model():
    """THE DECOUPLING, HELD IN PLACE. Until L5 this task called score.load()
    in the scheduler, so the model lived in two processes -- this one and the
    service -- each resolving the champion alias on its own. A service nobody
    calls is not a decoupling, it is a second path. Without this test the
    coupling comes back at the first convenient edit."""
    called = _calls(_task_source("score_batch"))
    assert "score.remote_rows" in called
    assert "score.load" not in called
    assert "MLFLOW_TRACKING_URI" not in _task_source("score_batch")


def test_the_endpoint_is_read_from_governance_and_not_written_here():
    """The same rule as realtime.endpoint: an address in a DAG is an address
    nobody can change without editing a DAG."""
    body = _task_source("score_batch")
    assert "settings.scoring_endpoint" in body
    assert "settings.scoring_endpoint_timeout_seconds" in body
    assert "http://" not in body


def test_the_rows_are_still_read_and_written_here():
    """Only the middle moved. fd_scores keeps its foreign key to
    fd_transactions, and that guarantee belongs to whoever owns the data:
    fd_scoring_service has no write on it at all."""
    called = _calls(_task_source("score_batch"))
    assert "score.rows_to_score" in called
    assert "score.write" in called


def test_an_empty_batch_reports_no_model_version():
    """Nothing was scored, so no model answered. Asking the service which
    version it WOULD have used would put a name on a decision never taken."""
    body = _task_source("score_batch")
    empty = body[body.index("if not len(frame)"):]
    assert '"model_version": None' in empty


def test_the_whole_dag_holds_no_model_load():
    """Belt and braces across the file: another task could reintroduce it."""
    assert "score.load" not in _calls(_body())
