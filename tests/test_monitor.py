"""L4: the monitoring of the model in service, and the retraining it asks for.

Covers: EC33, EC38, EC39, EC40, EC42, EC44, EC45   Lot: L4   Competency: C6

WHAT THESE TESTS ARE FOR. The whole policy of C6 is a set of comparisons, and
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


def transition(control: str, state: str, detail: str = "because") -> dict:
    return {"control": control, "state": state, "detail": detail}


def stored(control: str, state: str, minutes_ago: int) -> dict:
    return {"control": control, "state": state,
            "since": NOW - dt.timedelta(minutes=minutes_ago)}


def report(*changes, **extra) -> dict:
    out = {"transitions": list(changes)}
    out.update(extra)
    return out


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

def test_a_transition_into_alert_asks_for_a_retraining():
    reason = monitor.retraining_due(
        report(transition(monitor.PERFORMANCE, monitor.ALERT)),
        [], (monitor.INPUT_DRIFT, monitor.PERFORMANCE),
        dt.timedelta(hours=6), NOW)
    assert reason is not None and monitor.PERFORMANCE in reason


def test_an_alert_on_a_control_the_policy_excludes_asks_for_nothing():
    """Prediction drift calls for a threshold to be rechosen, not a model to be
    refitted; a data quality alert calls for the stream to be fixed, and
    refitting on a broken stream would learn the breakage."""
    for control in (monitor.PREDICTION_DRIFT, monitor.DATA_QUALITY):
        assert monitor.retraining_due(
            report(transition(control, monitor.ALERT)), [],
            (monitor.INPUT_DRIFT, monitor.PERFORMANCE),
            dt.timedelta(hours=6), NOW) is None


def test_a_transition_back_to_ok_asks_for_nothing():
    assert monitor.retraining_due(
        report(transition(monitor.PERFORMANCE, monitor.OK)), [],
        (monitor.INPUT_DRIFT, monitor.PERFORMANCE),
        dt.timedelta(hours=6), NOW) is None


def test_a_standing_alert_without_a_transition_asks_for_nothing():
    """The whole reason the trigger reads transitions: a drift that lasts three
    days raises the same alert every window, and firing on the state would
    queue one training per window."""
    assert monitor.retraining_due(
        report(), [stored(monitor.INPUT_DRIFT, monitor.ALERT, 2000)],
        (monitor.INPUT_DRIFT, monitor.PERFORMANCE),
        dt.timedelta(hours=6), NOW) is None


def test_the_cooldown_suppresses_a_second_control_flapping_in():
    """A control that flaps transitions every time. The training the first one
    asked for is still recent, and a second would learn from the same data."""
    assert monitor.retraining_due(
        report(transition(monitor.PERFORMANCE, monitor.ALERT)),
        [stored(monitor.INPUT_DRIFT, monitor.ALERT, 30)],
        (monitor.INPUT_DRIFT, monitor.PERFORMANCE),
        dt.timedelta(hours=6), NOW) is None


def test_an_older_alert_does_not_suppress_anything():
    reason = monitor.retraining_due(
        report(transition(monitor.PERFORMANCE, monitor.ALERT)),
        [stored(monitor.INPUT_DRIFT, monitor.ALERT, 7 * 60)],
        (monitor.INPUT_DRIFT, monitor.PERFORMANCE),
        dt.timedelta(hours=6), NOW)
    assert reason is not None


def test_a_control_in_alert_outside_the_policy_does_not_suppress():
    """Only a control that could have asked for a training suppresses the next
    request. A data quality alert never asked for one."""
    reason = monitor.retraining_due(
        report(transition(monitor.PERFORMANCE, monitor.ALERT)),
        [stored(monitor.DATA_QUALITY, monitor.ALERT, 5)],
        (monitor.INPUT_DRIFT, monitor.PERFORMANCE),
        dt.timedelta(hours=6), NOW)
    assert reason is not None


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
    class of defect that cost the gate of L3 a KeyError on is_fraud."""
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
