"""B4-L4: the monitoring of the model in service, and the retraining it asks for.

Block: B4.
Lot: B4-L4.
Competency: B4-C5, B4-C6.
Covers: B4-EC33, B4-EC38, B4-EC39, B4-EC40, B4-EC42, B4-EC44, B4-EC45.

B4-C5 AND NOT ONLY B4-C6, corrected 2026-11-20. The requirements document
files EC33 -- the trigger policy -- under B4-C5, and this file exercises
retraining_due, which IS that policy: the alert that must not trigger, the
cooldown, the unanswered alert. Declaring B4-C6 alone made the matrix flag the
mark, and the matrix was right. Monitoring raises the alert, retraining
answers it; the file sits on both sides of that line and now says so.

WHAT THESE TESTS ARE FOR. The whole policy of B4-C6 is a set of comparisons, and
a live run almost never produces the interesting ones: a stream that has not
drifted gives an ok on every control, every hour, for ever. So the cases worth
guarding -- the floor crossed, the window too thin to judge, the alert that
must NOT trigger a retraining, the cooldown that suppresses the second one --
would never be exercised at all if they were not exercised here.

AND THE POLICY MUST BE EXERCISABLE WITHOUT EVIDENTLY. evidently is installed in
the Airflow image and nowhere else: the suite and the integration chain install
psycopg, pandas, requests and PyYAML. ml4fd/monitor.py therefore keeps every
judgement in pure functions over dictionaries -- the snapshot dict goes in,
measurements come out -- and the import of evidently sits inside the two
functions that call it. One test below holds that property in place.

THE SNAPSHOT FIXTURE IS A REAL ONE. SNAPSHOT below is the dict Evidently 0.7.23
actually returned, copied from a run against the container rather than written
from the documentation: the keys are config.type, value.share and value.count,
and a fixture invented from memory would pass while the real reader failed.
"""
from __future__ import annotations

import ast
import datetime as dt
import os
import re
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import config, monitor  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]

NOW = dt.datetime(2026, 11, 19, 18, 0, tzinfo=dt.timezone.utc)
WINDOW = monitor.window_back(NOW, dt.timedelta(hours=24))

# Evidently 0.7.23, verbatim: three numeric columns, one of them drifted.
SNAPSHOT = {
    "metrics": [
        {"metric_name": "DriftedColumnsCount(drift_share=0.4)",
         "config": {"type": "evidently:metric_v2:DriftedColumnsCount",
                    "drift_share": 0.4},
         "value": {"count": 1.0, "share": 0.3333333333333333}},
        {"metric_name": "ValueDrift(column=amt,method=psi,threshold=0.1)",
         "config": {"type": "evidently:metric_v2:ValueDrift",
                    "column": "amt", "method": "psi", "threshold": 0.1},
         "value": 0.84},
        {"metric_name": "ValueDrift(column=f_tx_count,method=psi,threshold=0.1)",
         "config": {"type": "evidently:metric_v2:ValueDrift",
                    "column": "f_tx_count", "method": "psi",
                    "threshold": 0.1},
         "value": 0.02},
        {"metric_name": "ValueDrift(column=category,method=psi,threshold=0.1)",
         "config": {"type": "evidently:metric_v2:ValueDrift",
                    "column": "category", "method": "psi", "threshold": 0.1},
         "value": 0.01},
    ],
    "tests": [],
}

PERF = {"verdicts": 4800, "frauds": 210, "true_positives": 120,
        "recall_flagged": 0.5714, "recall_alerted": 0.4190,
        "precision_flagged": 0.52, "base_rate": 0.004375}


def stored(control: str, state: str, minutes_ago: int) -> dict:
    """A recorded control state, as fd_ops_state holds it.

    transition() and report() lived here too, and went with the rule they
    served: the trigger read report["transitions"] until 2026-11-20. Keeping
    two dead builders would have left the next reader looking for the rule that
    used them.
    """
    return {"control": control, "state": state,
            "since": NOW - dt.timedelta(minutes=minutes_ago)}


# --- the window ----------------------------------------------------------------

def test_a_window_runs_forwards():
    """Reversed bounds would make every comparison over time wrong in one
    direction, and silently. fd_monitoring carries the same CHECK."""
    with pytest.raises(ValueError) as raised:
        monitor.Window(start=NOW, end=NOW - dt.timedelta(hours=1))
    assert "forwards" in str(raised.value)


def test_a_window_of_no_width_is_refused():
    with pytest.raises(ValueError):
        monitor.Window(start=NOW, end=NOW)


# --- the drift of the inputs (EC39) --------------------------------------------

def test_the_share_below_the_limit_is_not_an_alert():
    judged = monitor.drift_measurements(SNAPSHOT, 0.40, n_rows=7200)[0]
    assert judged.metric == "drifted_columns_share"
    assert judged.state == monitor.OK
    assert judged.threshold == 0.40


def test_the_share_at_the_limit_is_an_alert():
    """THE BOUNDARY IS INCLUSIVE, and it is stated here rather than left to be
    read off an operator. Two columns of five is exactly 0.40, which is the
    declared limit: a limit that the value may equal without consequence is a
    limit nobody can reason about."""
    payload = {"metrics": [
        {"config": {"type": monitor.DRIFTED_COUNT},
         "value": {"count": 2.0, "share": 0.40}}]}
    assert monitor.drift_measurements(payload, 0.40, 5)[0].state == monitor.ALERT


def test_the_share_above_the_limit_is_an_alert():
    judged = monitor.drift_measurements(SNAPSHOT, 0.30, n_rows=7200)[0]
    assert judged.state == monitor.ALERT
    assert "1 of 3 columns drifted" in judged.detail


def test_every_column_is_recorded_whatever_the_verdict():
    """The column that moved first must stay readable afterwards: EC45 is about
    the series, and a snapshot that kept only the aggregate could not say which
    column started it."""
    out = monitor.drift_measurements(SNAPSHOT, 0.40, n_rows=7200)
    columns = [m.metric for m in out if m.metric.startswith("column_drift:")]
    assert columns == ["column_drift:amt", "column_drift:f_tx_count",
                       "column_drift:category"]


def test_a_column_carries_the_tool_s_own_method_and_limit():
    """Recorded, not applied. A p-value falls as evidence of drift grows while
    a distance rises, so a direction decided here would be wrong for half the
    methods the tool can pick -- the share is what carries a verdict."""
    out = monitor.drift_measurements(SNAPSHOT, 0.40, n_rows=7200)
    amt = next(m for m in out if m.metric == "column_drift:amt")
    assert amt.value == 0.84
    assert amt.threshold == 0.1
    assert amt.state == monitor.OK
    assert "psi" in amt.detail and "judged by the tool" in amt.detail


def test_a_snapshot_without_the_count_is_refused_not_guessed():
    """Deriving the share from the per-column entries would hide a change in
    the tool's output behind a figure that still looked plausible."""
    payload = {"metrics": SNAPSHOT["metrics"][1:]}
    with pytest.raises(LookupError) as raised:
        monitor.drift_measurements(payload, 0.40, 7200)
    assert monitor.DRIFTED_COUNT in str(raised.value)


def test_the_count_is_matched_on_the_type_and_not_on_the_name():
    """metric_name carries the arguments -- DriftedColumnsCount(drift_share=0.4)
    -- so it changes when a threshold changes. The type does not."""
    payload = {"metrics": [
        {"metric_name": "DriftedColumnsCount(drift_share=0.9)",
         "config": {"type": monitor.DRIFTED_COUNT},
         "value": {"count": 0.0, "share": 0.0}}]}
    assert monitor.drift_measurements(payload, 0.40, 10)[0].value == 0.0


# --- the drift of the predictions (EC39) ---------------------------------------

def test_a_flag_rate_within_the_ratio_is_not_an_alert():
    out = monitor.prediction_drift_measurements(
        {"scored": 7200, "alerts_per_day": 60.0}, 41.6, 2.0)
    assert out[0].state == monitor.OK


def test_a_flag_rate_over_the_ratio_is_an_alert():
    out = monitor.prediction_drift_measurements(
        {"scored": 7200, "alerts_per_day": 95.0}, 41.6, 2.0)
    assert out[0].state == monitor.ALERT
    assert "2.28 times" in out[0].detail


def test_nothing_scored_is_unmeasurable_and_not_an_alert():
    """An empty window is a fact about the window. Reporting a flag rate of
    zero as a collapse would raise on a quiet night."""
    out = monitor.prediction_drift_measurements({"scored": 0}, 41.6, 2.0)
    assert out[0].state == monitor.UNMEASURABLE


def test_without_a_published_rate_no_ratio_is_invented():
    out = monitor.prediction_drift_measurements(
        {"scored": 7200, "alerts_per_day": 95.0}, 0.0, 2.0)
    assert out[0].state == monitor.UNMEASURABLE
    assert out[0].threshold is None


def test_the_score_quantiles_are_recorded_and_not_judged():
    out = monitor.prediction_drift_measurements(
        {"scored": 7200, "alerts_per_day": 40.0, "score_median": 0.02,
         "score_p99": 0.97}, 41.6, 2.0)
    quantiles = [m for m in out if m.metric.startswith("score_")]
    assert [m.state for m in quantiles] == [monitor.OK, monitor.OK]
    assert all(m.threshold is None for m in quantiles)


# --- the performance against the specification (EC40) --------------------------

def test_a_recall_under_the_floor_is_an_alert():
    out = monitor.performance_measurements(PERF, min_recall=0.60,
                                           min_frauds=30)
    assert out[0].metric == "recall_flagged"
    assert out[0].state == monitor.ALERT
    assert out[0].n_labelled == 210


def test_a_recall_on_the_floor_is_not_an_alert():
    figures = dict(PERF, recall_flagged=0.60)
    out = monitor.performance_measurements(figures, 0.60, 30)
    assert out[0].state == monitor.OK


def test_too_few_frauds_is_unmeasurable_and_keeps_its_denominator():
    """A floor crossed by sampling noise costs a retraining. The count is kept
    on the row so the gap is auditable rather than merely absent."""
    out = monitor.performance_measurements(
        {"verdicts": 300, "frauds": 12, "recall_flagged": 0.1}, 0.60, 30)
    assert out[0].state == monitor.UNMEASURABLE
    assert out[0].n_labelled == 12
    assert out[0].threshold is None


def test_the_service_recall_is_measured_beside_the_model_s():
    """THE DIAGNOSTIC THAT KEEPS THE TRIGGER HONEST. recall_alerted below
    recall_flagged means the alert capacity is binding, and no retraining can
    refit its way out of a ceiling."""
    out = monitor.performance_measurements(PERF, 0.60, 30)
    service = next(m for m in out if m.metric == "recall_alerted")
    assert service.state == monitor.OK
    assert "capacity binding" in service.detail


def test_a_service_recall_equal_to_the_model_s_says_capacity_is_not_binding():
    figures = dict(PERF, recall_alerted=PERF["recall_flagged"])
    out = monitor.performance_measurements(figures, 0.60, 30)
    service = next(m for m in out if m.metric == "recall_alerted")
    assert "not binding" in service.detail


def test_the_precision_is_recorded_with_the_base_rate_of_its_window():
    """A precision read without the base rate of its own window compares
    nothing: the rate runs from 0.185 % to 0.554 % across months."""
    out = monitor.performance_measurements(PERF, 0.60, 30)
    names = [m.metric for m in out]
    assert "precision_flagged" in names and "base_rate" in names


def test_only_the_model_s_recall_carries_a_threshold():
    out = monitor.performance_measurements(PERF, 0.60, 30)
    judged = [m for m in out if m.threshold is not None]
    assert [m.metric for m in judged] == ["recall_flagged"]


# --- one state per family (EC44) -----------------------------------------------

def test_a_family_with_one_alert_is_in_alert():
    out = monitor.performance_measurements(PERF, 0.60, 30)
    controls = monitor.controls_from(out)
    assert [c.control for c in controls] == [monitor.PERFORMANCE]
    assert controls[0].state == monitor.ALERT


def test_a_family_within_its_limits_is_ok():
    out = monitor.performance_measurements(dict(PERF, recall_flagged=0.8),
                                           0.60, 30)
    assert monitor.controls_from(out)[0].state == monitor.OK


def test_a_family_that_could_not_be_measured_is_not_in_alert():
    """"Could not measure" is said in the measurement's own state. Raising the
    control would call a gap in the data a fault in the model."""
    out = monitor.performance_measurements({"verdicts": 0, "frauds": 0},
                                           0.60, 30)
    assert monitor.controls_from(out)[0].state == monitor.OK


def test_a_family_with_no_measurement_yields_no_control():
    assert monitor.controls_from([]) == []


# --- the trigger (EC33) --------------------------------------------------------
#
# THE RULE MOVED ON 2026-11-20 AND THESE TESTS MOVED WITH IT. It used to read
# report["transitions"]; it reads the recorded state and the last recorded
# request. The reason is in monitor.retraining_due and in
# sql/ddl/180_retrain_requests.sql: the transition was computed by one task and
# committed by the next, so a crash in the second consumed it and the request
# was lost for good. Each test below carries the intent of the one it replaces,
# and the two that are new are the recovery the change exists for.

def ago(minutes: int) -> dt.datetime:
    return NOW - dt.timedelta(minutes=minutes)


POLICY = (monitor.INPUT_DRIFT, monitor.PERFORMANCE)
COOLDOWN = dt.timedelta(hours=6)


def test_an_alert_nobody_answered_asks_for_a_retraining():
    reason = monitor.retraining_due(
        [stored(monitor.PERFORMANCE, monitor.ALERT, 5)], None,
        POLICY, COOLDOWN, NOW)
    assert reason is not None and monitor.PERFORMANCE in reason


def test_an_alert_on_a_control_the_policy_excludes_asks_for_nothing():
    """Prediction drift calls for a threshold to be rechosen, not a model to be
    refitted; a data quality alert calls for the stream to be fixed, and
    refitting on a broken stream would learn the breakage."""
    for control in (monitor.PREDICTION_DRIFT, monitor.DATA_QUALITY):
        assert monitor.retraining_due(
            [stored(control, monitor.ALERT, 5)], None,
            POLICY, COOLDOWN, NOW) is None


def test_a_control_at_ok_asks_for_nothing():
    assert monitor.retraining_due(
        [stored(monitor.PERFORMANCE, monitor.OK, 5)], None,
        POLICY, COOLDOWN, NOW) is None


def test_an_alert_already_answered_asks_for_nothing():
    """THE INTENT OF the old test_a_standing_alert_without_a_transition: a drift
    that lasts three days raises the same alert every window and must ask ONCE,
    not once per window. What changed is the evidence -- a recorded request
    instead of the absence of a transition -- and that is exactly what makes it
    survive a crash."""
    assert monitor.retraining_due(
        [stored(monitor.INPUT_DRIFT, monitor.ALERT, 2000)], ago(1999),
        POLICY, COOLDOWN, NOW) is None


def test_an_alert_that_began_after_the_last_request_asks_again():
    """A control that recovers and fails again is a NEW incident. The request
    is older than the alert, so the trigger is re-armed."""
    assert monitor.retraining_due(
        [stored(monitor.INPUT_DRIFT, monitor.ALERT, 10)], ago(500),
        POLICY, COOLDOWN, NOW) is not None


def test_the_cooldown_suppresses_a_request_made_minutes_ago():
    """THE COOLDOWN KEYS ON THE REQUEST, which is what it was always about. The
    old rule keyed on another triggering control's alert age -- a proxy for "a
    training was asked for recently" that held only while a request followed
    every transition."""
    assert monitor.retraining_due(
        [stored(monitor.PERFORMANCE, monitor.ALERT, 1)], ago(30),
        POLICY, COOLDOWN, NOW) is None


def test_a_request_older_than_the_cooldown_does_not_suppress():
    assert monitor.retraining_due(
        [stored(monitor.PERFORMANCE, monitor.ALERT, 1)], ago(7 * 60),
        POLICY, COOLDOWN, NOW) is not None


def test_a_control_in_alert_outside_the_policy_does_not_suppress():
    """Only a control that could have asked for a training bears on the next
    request. A data quality alert never asked for one."""
    assert monitor.retraining_due(
        [stored(monitor.PERFORMANCE, monitor.ALERT, 1),
         stored(monitor.DATA_QUALITY, monitor.ALERT, 5)], None,
        POLICY, COOLDOWN, NOW) is not None


def test_an_alert_with_no_since_is_not_acted_on():
    """A row with no since cannot be compared to a request, and guessing would
    mean asking for ever or never. fd_ops_state.since is NOT NULL, so the
    database cannot produce this shape -- and a pure function must not depend
    on that."""
    assert monitor.retraining_due(
        [{"control": monitor.INPUT_DRIFT, "state": monitor.ALERT,
          "since": None}], None, POLICY, COOLDOWN, NOW) is None


def test_a_request_the_trigger_never_made_is_asked_for_again():
    """THE INCIDENT OF 2026-11-20 09:34 UTC, AS A TEST, and the reason the rule
    changed. The alert was raised, the state was committed, and the task that
    was to ask for the training crashed -- so no request was recorded. Every
    later run must ask again. The transition-based rule could not: the
    transition had been consumed by the run that failed, and the system then
    believed for ever that it had already asked."""
    alert = [stored(monitor.INPUT_DRIFT, monitor.ALERT, 90)]
    assert monitor.retraining_due(alert, None, POLICY, COOLDOWN, NOW) is not None
    later = NOW + dt.timedelta(minutes=2)
    assert monitor.retraining_due(alert, None, POLICY, COOLDOWN,
                                  later) is not None


def test_the_reason_carries_the_detail_an_operator_reads():
    """"3 of 5 columns drifted" is the sentence that tells an operator what
    happened. It lives in the recorded state's detail, and stored_from() is
    what carries it across XCom -- a second parser of that shape is how the
    field would be dropped."""
    row = stored(monitor.INPUT_DRIFT, monitor.ALERT, 5)
    row["detail"] = "3 of 5 columns drifted"
    reason = monitor.retraining_due([row], None, POLICY, COOLDOWN, NOW)
    assert "3 of 5 columns drifted" in reason


def test_the_report_is_read_back_without_losing_a_field():
    """stored_from() parses what measure() put through XCom: since arrives as an
    ISO string because a report travels as JSON."""
    rows = monitor.stored_from({"stored": [
        {"control": monitor.INPUT_DRIFT, "state": monitor.ALERT,
         "since": NOW.isoformat(), "detail": "parce que"},
        {"control": monitor.PERFORMANCE, "state": monitor.OK,
         "since": None, "detail": None}]})
    assert rows[0]["since"] == NOW
    assert rows[0]["detail"] == "parce que"
    assert rows[1]["since"] is None
    assert monitor.alerting_controls(rows, POLICY) == [monitor.INPUT_DRIFT]


# --- the row that reaches the database -----------------------------------------

def test_a_row_carries_the_window_the_tool_and_the_version():
    row = monitor.Measurement(
        control=monitor.INPUT_DRIFT, metric="drifted_columns_share",
        value=0.5, threshold=0.4, state=monitor.ALERT, n_rows=7200
    ).row(WINDOW, model_version="8", tool="evidently 0.7.23", run_id="r-1")
    assert row["window_start"] == WINDOW.start
    assert row["window_end"] == WINDOW.end
    assert row["tool"] == "evidently 0.7.23"
    assert row["model_version"] == "8" and row["run_id"] == "r-1"
    assert set(row) == {
        "run_id", "window_start", "window_end", "model_version", "control",
        "metric", "value", "threshold", "state", "n_rows", "n_labelled",
        "tool", "detail"}


def test_the_row_names_match_the_insert_file():
    """The parameters of the INSERT and the keys of row() must be the same set,
    or psycopg raises on a missing key at the first real run -- which is the
    class of defect that cost the gate of B4-L3 a KeyError on is_fraud."""
    text = (ROOT / "sql" / "monitoring" / "record_measurement.sql").read_text(
        encoding="utf-8")
    named = set(re.findall(r"%\((\w+)\)s", text))
    row = monitor.Measurement(control=monitor.INPUT_DRIFT, metric="m",
                              value=0.0, state=monitor.OK, n_rows=0
                              ).row(WINDOW, "8", "evidently 0.0")
    assert named == set(row)


# --- the declarations agree ----------------------------------------------------

def test_the_control_names_agree_everywhere():
    """Four names, three places: this module, the CHECK on fd_monitoring, and
    the list config validates trigger.retrain_on against. A name that agreed in
    two of the three would disarm the trigger or make the measurement
    unrecordable."""
    names = {monitor.INPUT_DRIFT, monitor.PREDICTION_DRIFT,
             monitor.PERFORMANCE, monitor.DATA_QUALITY}
    assert set(config.CONTROLS) == names
    ddl = (ROOT / "sql" / "ddl" / "170_monitoring.sql").read_text(
        encoding="utf-8")
    clause = ddl[ddl.index("fd_monitoring_control_is_known"):]
    assert names == set(re.findall(r"'(\w+)'", clause[:clause.index(")")]))


def test_the_states_agree_with_the_check():
    ddl = (ROOT / "sql" / "ddl" / "170_monitoring.sql").read_text(
        encoding="utf-8")
    clause = ddl[ddl.index("fd_monitoring_state_is_known"):]
    assert {monitor.OK, monitor.ALERT, monitor.UNMEASURABLE} == set(
        re.findall(r"'(\w+)'", clause[:clause.index(")")]))


def test_the_drift_window_reads_what_the_model_reads():
    """THE CLAIM THE DRIFT MEASUREMENT RESTS ON. features_window.sql must
    select the same columns as the scoring query, derivations included: a
    column computed differently there would report a drift of the query rather
    than of the stream."""
    def selected(path):
        # THE COMMENTS ARE STRIPPED FIRST, and that is not fussiness: both
        # files discuss their own select list in prose, so an index of the
        # word SELECT lands in a comment. The same short anchor cost a
        # rewrite of features_window.sql on 2026-11-19.
        text = (ROOT / "sql" / path).read_text(encoding="utf-8")
        code = "\n".join(line for line in text.splitlines()
                         if not line.lstrip().startswith("--"))
        body = code[code.index("SELECT"):code.index("FROM")]
        return re.findall(r"(?:AS\s+(\w+)|t\.(\w+))", body)

    assert selected("monitoring/features_window.sql") == selected(
        "scoring/rows_to_score.sql")


@pytest.mark.parametrize("module", ["ml4fd/monitor.py"])
def test_the_policy_imports_no_evidently_at_load_time(module):
    """evidently lives in the Airflow image and nowhere else. A module-level
    import here would make the suite -- and the integration chain -- unable to
    even collect."""
    tree = ast.parse((ROOT / module).read_text(encoding="utf-8"))
    for node in tree.body:
        names = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        assert not any(n.startswith(("evidently", "pandas", "airflow"))
                       for n in names), "%s imports %s at load time" % (
                           module, names)

def test_a_window_under_the_floor_is_not_a_verdict():
    """THE SYMMETRY WITH performance, which refuses a recall under
    min_labelled_frauds. Until 2026-11-20 any non-empty window produced a drift
    verdict, so three rows carried the authority of seven thousand."""
    out = monitor.thin_window(3, 50)
    assert len(out) == 1
    assert out[0].state == monitor.UNMEASURABLE
    assert out[0].control == monitor.INPUT_DRIFT
    assert out[0].n_rows == 3
    # both numbers in the detail: a reader must not have to look up the floor
    assert "3 row(s)" in out[0].detail and "50" in out[0].detail


def test_an_empty_window_records_its_reason_instead_of_a_hole():
    """The DAG used to log the empty window and record nothing, so the series
    EC45 asks for carried a hole where it should have carried a reason -- and a
    hole cannot be told apart from a measurement nobody took."""
    out = monitor.thin_window(0, 50)
    assert len(out) == 1 and out[0].state == monitor.UNMEASURABLE
    assert out[0].n_rows == 0


def test_a_window_at_the_floor_is_measured():
    """At the floor and above, nothing is returned: the caller runs the tool."""
    assert monitor.thin_window(50, 50) == []
    assert monitor.thin_window(7200, 500) == []


def test_the_floor_and_the_verdict_share_one_metric_name():
    """One query over fd_monitoring must return the series with its gaps in
    place, rather than two series for a reader to join."""
    floored = monitor.thin_window(1, 50)[0]
    measured = monitor.drift_measurements(SNAPSHOT, 0.40, 7200)
    assert floored.metric == measured[0].metric == "drifted_columns_share"

def test_a_postgresql_numeric_reaches_the_tool_as_a_float():
    """THE DEFECT OF 2026-11-20 08:48 UTC, on the first real run of dag_monitor:
    "ufunc 'isinf' not supported for the input types". amt, f_avg_amount and
    f_home_distance_km are numeric(12,2) in the DDL, so psycopg returns
    decimal.Decimal, the column's dtype is object, and Evidently -- told by our
    own DataDefinition that the column is numerical -- calls np.isinf on it.

    REPLAYING A DICT CANNOT SEE THIS. Every other drift test here replays the
    snapshot Evidently returned, which is the right way to judge a verdict
    without the tool and is blind to everything happening before the tool is
    called. This test is that blind spot.
    """
    import decimal

    import pandas as pd

    frame = pd.DataFrame({
        "amt": [decimal.Decimal("12.34"), decimal.Decimal("5.00")],
        "f_tx_count": [3, 7],
        "category": ["gas_transport", "shopping_net"]})
    out = monitor.comparable(frame, ("amt", "f_tx_count", "category"))

    assert out["amt"].dtype == float
    # and nothing else is touched: an integer stays one, text stays text
    assert out["f_tx_count"].dtype == frame["f_tx_count"].dtype
    assert out["category"].dtype == object
    # the caller's frame is left alone, since len(current) is read after this
    assert isinstance(frame["amt"].iloc[0], decimal.Decimal)


def test_a_frame_with_nothing_to_cast_is_returned_as_it_is():
    """No copy when there is nothing to cast. The reference side is 1.16 M rows
    and a copy per hourly run would be paid for nothing."""
    import pandas as pd

    frame = pd.DataFrame({"amt": [1.5, 2.5], "category": ["a", "b"]})
    assert monitor.comparable(frame, ("amt", "category")) is frame


def test_a_declared_feature_the_window_does_not_carry_is_skipped():
    """comparable() walks the CHAMPION's declared features, which can name a
    column the window does not carry -- a recipe changed while a champion from
    before it is still in service. Skipping is right: whether the column sets
    agree is Evidently's verdict to give, and it gives it with a message of its
    own. Raising here would replace that message with a KeyError."""
    import pandas as pd

    frame = pd.DataFrame({"amt": [1.5]})
    out = monitor.comparable(frame, ("amt", "f_absent"))
    assert list(out.columns) == ["amt"]


# --- B4-L4: which columns the drift is measured on, and why not all of them ---

SERVED = ("amt", "category", "f_tx_count", "f_avg_amount",
          "f_home_distance_km")


def test_the_declared_exclusions_are_subtracted_in_the_served_order():
    kept = monitor.drift_columns(SERVED, ("f_tx_count", "f_avg_amount"))
    assert kept == ["amt", "category", "f_home_distance_km"]


def test_excluding_nothing_measures_every_served_feature():
    """Governance may legitimately exclude nothing, and then the control must
    behave exactly as it did before this setting existed."""
    assert monitor.drift_columns(SERVED, ()) == list(SERVED)


def test_a_stale_exclusion_is_refused_and_not_silently_dropped():
    """The failure this guards is invisible by construction: a feature renamed
    in the recipe makes the exclusion match nothing, the column returns under
    measurement, and the only symptom is a drift share that climbs for reasons
    nobody declared."""
    with pytest.raises(LookupError, match="does not use"):
        monitor.drift_columns(SERVED, ("f_tx_count", "f_txn_count"))


def test_the_refusal_names_the_governance_key_and_the_served_features():
    """A reader who meets this has to change governance.yml, so the message
    carries the line to change and the list to choose from."""
    with pytest.raises(LookupError) as raised:
        monitor.drift_columns(SERVED, ("f_nope",))
    message = str(raised.value)
    assert "monitoring.input_drift.not_comparable" in message
    assert "'f_home_distance_km'" in message


def test_excluding_every_feature_is_refused():
    """Evidently would report a share over zero columns, drift_measurements
    would read 0.0, and input_drift would publish a serene ok for ever."""
    with pytest.raises(ValueError, match="no column left"):
        monitor.drift_columns(SERVED, SERVED)


def test_governance_excludes_the_two_features_of_fd_client_features():
    """THE CRITERION IS THE DEFINITION, NOT THE FIGURE, and this test is what
    keeps it that way. fd_client_features returns tx_count and avg_amount over
    "the whole fd_transactions table, all batches together" (ruling 3), so both
    are incomparable between a corpus row and a stream row. f_avg_amount was
    measured at PSI 0.027 -- inside its limit -- and is excluded all the same;
    a list chosen by the figures would have kept it."""
    declared = set(config.load().monitoring_drift_not_comparable)
    assert declared == {"f_tx_count", "f_avg_amount"}


def test_the_per_row_features_stay_under_measurement():
    """amt, category and f_home_distance_km are computed from one row's own
    fields -- transform.home_distance_km reads only that row's coordinates --
    so both sides are computed identically and the comparison is meaningful.
    Two of them do drift, and that drift is real: the producer replays a narrow
    slice of another file. Excluding them would leave a detector that can only
    see what we inject ourselves."""
    declared = set(config.load().monitoring_drift_not_comparable)
    for name in ("amt", "category", "f_home_distance_km"):
        assert name not in declared


def test_the_exclusion_is_declared_in_governance_and_not_inferred_in_code():
    """No rule in monitor.py may decide that a name looks like a history
    feature. A prefix test would silence the next feature that happens to be
    called f_something, without a line of governance to answer for it."""
    body = (ROOT / "ml4fd" / "monitor.py").read_text(encoding="utf-8")
    tree = ast.parse(body)
    function = next(node for node in ast.walk(tree)
                    if isinstance(node, ast.FunctionDef)
                    and node.name == "drift_columns")
    literals = [node.value for node in ast.walk(function)
                if isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and node is not function.body[0].value]
    assert not [text for text in literals if text.startswith("f_")]


def test_the_dag_measures_the_subset_and_not_the_served_list():
    """The wiring, read from the source: drift_snapshot must be handed the
    filtered list. Passing served["features"] here would leave every test above
    passing and the control unchanged in production."""
    body = (ROOT / "airflow" / "dags" / "dag_monitor.py").read_text(
        encoding="utf-8")
    assert "monitor.drift_columns(" in body
    call = body[body.index("snapshot = monitor.drift_snapshot("):]
    call = call[:call.index(")")]
    assert "columns" in call and 'served["features"]' not in call


# A KNOWN OFFSET THE CONTROL MUST SIT ABOVE. Measured 2026-11-20 on the
# fifteen-minute window, at rest and with no injection: of the three
# comparable columns, category (PSI 0.230) and f_home_distance_km (0.557) are
# outside their limits because the producer replays a one-month slice of
# fraudTest against a corpus of fraudTrain over eighteen months.
RESTING_DRIFTED_COLUMNS = 2


def test_the_limit_sits_above_the_resting_offset_of_the_stream():
    """THE REASON THE LOOP FIRED ONLY ONCE, pinned so it cannot come back. A
    limit at or below the resting share makes input_drift read alert for ever:
    it never falls back to ok, so it never re-arms, and retraining_due answers
    it once and stays silent. The alert is then a lamp left on rather than a
    trigger."""
    settings = config.load()
    kept = monitor.drift_columns(SERVED,
                                 settings.monitoring_drift_not_comparable)
    resting = RESTING_DRIFTED_COLUMNS / len(kept)
    assert settings.monitoring_max_drifted_share > resting, (
        "%d of %d columns drift at rest (%.3f), and the limit is %.3f: the "
        "control can never return to ok"
        % (RESTING_DRIFTED_COLUMNS, len(kept), resting,
           settings.monitoring_max_drifted_share))


def test_one_more_column_moving_is_still_an_alert():
    """And the other side of the same pair: a limit raised far enough to clear
    the offset must not be raised so far that nothing can reach it. With three
    comparable columns the only share above the offset is 1.0, so the limit has
    to leave it reachable."""
    settings = config.load()
    kept = monitor.drift_columns(SERVED,
                                 settings.monitoring_drift_not_comparable)
    reached = (RESTING_DRIFTED_COLUMNS + 1) / len(kept)
    payload = {"metrics": [
        {"config": {"type": monitor.DRIFTED_COUNT},
         "value": {"count": float(RESTING_DRIFTED_COLUMNS + 1),
                   "share": reached}}]}
    judged = monitor.drift_measurements(
        payload, settings.monitoring_max_drifted_share, n_rows=75)[0]
    assert judged.state == monitor.ALERT
