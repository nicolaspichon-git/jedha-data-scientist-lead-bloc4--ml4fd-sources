"""Monitoring the model in service: drift, performance, and what follows.

Covers: EC38, EC39, EC40, EC42, EC45   Lot: L4   Competency: C6

WHAT THIS WATCHES THAT S3 DID NOT. ml4fd/health.py watches the CHAIN -- is it
running, is it rejecting, is it fresh. This watches the MODEL: are the
transactions still shaped like the ones it was fitted on, is it still flagging
at the rate it was tuned for, is it still catching what the specification
requires. A chain in perfect health can serve a model that has quietly stopped
working, and no control of S3 would say so.

THE TOOL MEASURES, THE POLICY JUDGES, and the split is the same one L3 made
between paired.py and promote.py. Evidently computes the statistics; the
verdict is a comparison with a threshold from governance.yml, taken in the pure
functions below. Two consequences: the whole decision layer is testable without
Evidently installed -- which matters, because the suite installs psycopg,
pandas, requests and PyYAML and nothing more -- and the figure recorded in
fd_monitoring is the tool's while the verdict recorded beside it is ours.

EVIDENTLY 0.7, AND THE VERSION IS NOT AN INCIDENTAL DETAIL. The API was
rewritten at 0.7: up to 0.6.7 a report came from evidently.report with a
metric_preset and took two DataFrames. Here the frames are wrapped in Dataset
objects described by a DataDefinition, the presets live in evidently.presets,
and -- the trap -- Report.run takes the CURRENT data first and the reference
second. Reversed, the two give a drift computed backwards, and since a
distribution statistic is nearly symmetric the figure would be plausible. They
are therefore passed by keyword here, never positionally.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)

FEATURES_SQL = "monitoring/features_window.sql"
FLAGS_SQL = "monitoring/flag_rate_window.sql"
PERFORMANCE_SQL = "monitoring/performance_window.sql"
RECORD_SQL = "monitoring/record_measurement.sql"
LAST_REQUEST_SQL = "monitoring/last_retrain_request.sql"
RECORD_REQUEST_SQL = "monitoring/record_retrain_request.sql"

# The two metric types of a DataDriftPreset snapshot, as 0.7 names them in
# each entry's config.type. Matched on the type and never on metric_name:
# metric_name carries the arguments ("DriftedColumnsCount(drift_share=0.4)"),
# so it changes when a threshold changes, while the type does not.
DRIFTED_COUNT = "evidently:metric_v2:DriftedColumnsCount"
VALUE_DRIFT = "evidently:metric_v2:ValueDrift"

OK = "ok"
ALERT = "alert"
UNMEASURABLE = "unmeasurable"

INPUT_DRIFT = "input_drift"
PREDICTION_DRIFT = "prediction_drift"
PERFORMANCE = "performance"
DATA_QUALITY = "data_quality"


@dataclass(frozen=True)
class Window:
    """The span a measurement covers. Both bounds, never a duration alone."""
    start: dt.datetime
    end: dt.datetime

    def __post_init__(self):
        if self.end <= self.start:
            raise ValueError(
                "a window runs forwards: %s is not after %s"
                % (self.end, self.start))

    def params(self) -> dict:
        return {"window_start": self.start, "window_end": self.end}


def window_back(end: dt.datetime, span: dt.timedelta) -> Window:
    """The window of width span ending at end."""
    return Window(start=end - span, end=end)


@dataclass(frozen=True)
class Measurement:
    """One figure, its limit, and the verdict that follows.

    state is OK, ALERT or UNMEASURABLE. The third is not a failure: it is how a
    window that could not support a comparison gets recorded instead of hidden,
    and fd_monitoring's CHECK on performance requires it when no verdict was
    available.
    """
    control: str
    metric: str
    value: float
    state: str
    n_rows: int
    threshold: float | None = None
    n_labelled: int | None = None
    detail: str | None = None

    def row(self, window: Window, model_version: str, tool: str,
            run_id: str | None = None) -> dict:
        """The parameters of sql/monitoring/record_measurement.sql."""
        row = {"run_id": run_id, "model_version": model_version,
               "control": self.control, "metric": self.metric,
               "value": self.value, "threshold": self.threshold,
               "state": self.state, "n_rows": self.n_rows,
               "n_labelled": self.n_labelled, "tool": tool,
               "detail": self.detail}
        row.update(window.params())
        return row


# --------------------------------------------------------------- the tool (EC38)

def tool_version() -> str:
    """The tool and its version, as every row of fd_monitoring records it.

    Read from the package rather than from governance.yml: a version written by
    hand is a version that goes stale at the next rebuild, and the whole point
    of recording it is that a drift statistic is comparable only with another
    computed the same way.
    """
    import evidently

    return "evidently %s" % getattr(evidently, "__version__", "unknown")


def drift_columns(features, not_comparable) -> list[str]:
    """The served features minus the ones governance declares incomparable.

    WHY A SUBTRACTION AND NOT A LIST OF COLUMNS TO WATCH. A list of columns to
    watch stops watching a feature the day the recipe gains one, and says
    nothing when it does: the drift of the new feature is simply never
    measured. Subtracting makes the default "watched", which is the direction a
    silence should fall in, and leaves the exclusion the thing that has to be
    argued for in governance.yml.

    AN EXCLUSION THAT NAMES NOTHING IS AN ERROR, not an exclusion of nothing.
    The names are checked against the features of the model ACTUALLY IN
    SERVICE, read from its manifest, so a rename in the recipe makes this
    refuse at the next run instead of quietly reverting to measuring all five
    columns -- the failure a reader would never think to look for.

    AN EMPTY RESULT IS AN ERROR TOO. Evidently would report a share over zero
    columns, drift_measurements would read 0.0, and the control would publish a
    serene ok forever.

    Covers: EC39, EC45
    Lot: L4
    Competency: C6
    """
    names = list(features)
    excluded = list(not_comparable)
    unknown = [name for name in excluded if name not in names]
    if unknown:
        raise LookupError(
            "monitoring.input_drift.not_comparable names %s, which the model "
            "in service does not use. Its features are %s. A stale exclusion "
            "would put the column back under measurement without saying so."
            % (", ".join(repr(name) for name in unknown),
               ", ".join(repr(name) for name in names)))
    kept = [name for name in names if name not in excluded]
    if not kept:
        raise ValueError(
            "monitoring.input_drift.not_comparable excludes every feature of "
            "the model in service (%s), so there would be no column left to "
            "measure and the drift share would read 0.0 forever."
            % ", ".join(repr(name) for name in names))
    return kept


def definition(features):
    """Which columns hold numbers and which hold categories.

    0.7 ASKS BEFORE IT WILL COMPARE ANYTHING, because the per-column test
    depends on the answer. The split comes from train.CATEGORICAL, the same
    declaration the model is fitted with -- a list written here could drift
    from what the model actually sees, and then the drift measured would be of
    two different column types.
    """
    from evidently import DataDefinition

    from ml4fd import train

    names = list(features)
    categorical = [name for name in names if name in train.CATEGORICAL]
    numerical = [name for name in names if name not in train.CATEGORICAL]
    return DataDefinition(numerical_columns=numerical,
                          categorical_columns=categorical)


def comparable(frame, features):
    """The frame Evidently can read: PostgreSQL numerics turned into floats.

    MEASURED ON THE FIRST REAL RUN of dag_monitor, 2026-11-20 08:48 UTC, which
    failed with "ufunc 'isinf' not supported for the input types". amt,
    f_avg_amount and f_home_distance_km are numeric(12,2) in the DDL, so
    psycopg returns decimal.Decimal and the column's dtype is object. Our
    DataDefinition then declares that column numerical, and Evidently collects
    numerical stats by calling np.isinf on it, which cannot work on Decimals.

    BOTH SIDES, AND THAT IS WHY THIS LIVES HERE. The window comes from psycopg;
    the reference is a Parquet that ml4fd/build.py wrote from a frame built by
    psycopg too, so pyarrow stored decimal128 and hands Decimals back. The two
    sides were equally affected and only the first one evaluated raised. Fixing
    one of them would have turned a crash into a comparison between two types,
    which is worse: it would have produced a number.

    FLOAT AND NOT DECIMAL, because float is what the model was fitted on --
    train.py casts with .astype(float) before it fits -- so the drift is then
    measured on the values the model actually saw.

    NOT IN THE SQL. scoring/rows_to_score.sql selects the same columns for the
    scoring path, a cross-declaration test ties the two select lists together,
    and the scoring path needs no cast. One boundary, one cast, and no second
    declaration of what the columns are.

    WHY NO UNIT TEST CAUGHT IT. Every drift test in this suite replays the
    snapshot Evidently returned, as a dict -- which is the right design for
    judging a verdict without the tool, and is also blind to everything that
    happens before the tool is called. The test added beside this function is
    the one that closes that gap.

    Covers: EC39
    Lot: L4
    Competency: C6
    """
    import decimal

    casts = {}
    for name in features:
        if name not in frame.columns:
            continue
        column = frame[name]
        if column.dtype != object:
            continue
        known = column.dropna()
        if len(known) and isinstance(known.iloc[0], decimal.Decimal):
            casts[name] = column.astype(float)
    return frame.assign(**casts) if casts else frame


def drift_snapshot(current, reference, features, max_share: float,
                   method: str) -> dict:
    """Run the drift report and return its dictionary.

    max_share IS HANDED TO THE TOOL, as DataDriftPreset's own drift_share, so
    the threshold declared in governance.yml is literally the argument of the
    call. The count and the share come back computed against it; the comparison
    itself is still made in drift_measurements, because a verdict belongs to
    the policy and has to be testable without this import.
    """
    from evidently import Dataset, Report
    from evidently.presets import DataDriftPreset

    shape = definition(features)
    # BOTH SIDES, BEFORE THE TOOL SEES EITHER. See comparable().
    current = comparable(current, features)
    reference = comparable(reference, features)
    report = Report([DataDriftPreset(columns=list(features),
                                     drift_share=max_share,
                                     method=method)])
    # BY KEYWORD. run(current_data, reference_data) puts the current first,
    # which is the reverse of what the sentence "compare against the
    # reference" suggests. See this module's header.
    snapshot = report.run(
        current_data=Dataset.from_pandas(current, data_definition=shape),
        reference_data=Dataset.from_pandas(reference, data_definition=shape))
    return snapshot.dict()


# ------------------------------------------------- the verdicts, without the tool

def thin_window(n_rows: int, min_rows: int) -> list[Measurement]:
    """One unmeasurable measurement when the window is too small to speak
    about the stream, and nothing at all when it is big enough. Pure.

    ASKED BEFORE THE SNAPSHOT, AND NOT INSIDE drift_measurements. That function
    reads a snapshot, so a floor applied there would have let the tool run on a
    sample it cannot speak about: the figure would be computed, recorded, and
    only then thrown away. Here the thin window costs nothing and still leaves
    a row saying why there is no verdict.

    AN EMPTY WINDOW IS A THIN WINDOW, which is a repair of its own. The DAG
    used to log "aucune transaction dans la fenetre de derive" and record
    NOTHING, so the series EC45 asks for carried a hole exactly where it should
    have carried a reason. A hole cannot be told apart from a measurement
    nobody took.

    THE METRIC NAME IS THE SAME ONE the verdict uses, drifted_columns_share, so
    one query over fd_monitoring returns the series with its gaps in place
    rather than two series to be joined by a reader.

    Covers: EC39, EC45
    Lot: L4
    Competency: C6
    """
    if n_rows >= min_rows:
        return []
    return [Measurement(
        control=INPUT_DRIFT, metric="drifted_columns_share", value=0.0,
        state=UNMEASURABLE, n_rows=n_rows,
        detail="%d row(s) in the window, under the floor of %d: a distribution "
               "test over so few says more about the sample than about the "
               "stream" % (n_rows, min_rows))]


def drift_measurements(payload: dict, max_share: float,
                       n_rows: int) -> list[Measurement]:
    """The measurements of one drift snapshot. Pure: a dict in, rows out.

    THE SHARE IS JUDGED, THE COLUMNS ARE NOT. Evidently has already decided
    per column, with its own method and its own threshold, and counted how many
    drifted; re-deciding here with a direction of our own would be a second
    opinion on the same data. Which direction, besides, is not a constant: a
    p-value falls as evidence of drift grows while a distance rises, so a rule
    written here would be wrong for half the methods the tool can pick. The
    per-column figures are therefore recorded as observations, each carrying
    the method and the limit the tool used, and only the share carries a
    verdict.
    """
    metrics = payload.get("metrics") or []
    counted = [m for m in metrics
               if (m.get("config") or {}).get("type") == DRIFTED_COUNT]
    if not counted:
        raise LookupError(
            "the drift snapshot carries no %s. Either the preset was built "
            "without it or this version of Evidently names it otherwise; "
            "either way the share cannot be read, and guessing it from the "
            "per-column entries would hide the change." % DRIFTED_COUNT)

    value = counted[0].get("value") or {}
    share = float(value.get("share"))
    count = int(value.get("count"))
    out = [Measurement(
        control=INPUT_DRIFT, metric="drifted_columns_share", value=share,
        threshold=max_share, state=ALERT if share >= max_share else OK,
        n_rows=n_rows,
        detail="%d of %d columns drifted" % (count, len(_columns(metrics))))]

    for entry in metrics:
        config = entry.get("config") or {}
        if config.get("type") != VALUE_DRIFT:
            continue
        out.append(Measurement(
            control=INPUT_DRIFT,
            metric="column_drift:%s" % config.get("column"),
            value=float(entry.get("value")),
            # The tool's own limit for the statistic it chose -- recorded, not
            # applied here.
            threshold=_as_float(config.get("threshold")),
            state=OK, n_rows=n_rows,
            detail="method %s, judged by the tool" % config.get("method")))
    return out


def prediction_drift_measurements(figures: dict, published_rate: float,
                                  max_ratio: float) -> list[Measurement]:
    """The flag rate against the rate the model was published at. Pure.

    A RATIO AND NOT AN ABSOLUTE RATE, because the reference moves with every
    promotion: a figure written in governance.yml would go stale at the first
    retraining. published_rate is what M5 measured for the version in service,
    read from its manifest.
    """
    scored = int(figures.get("scored") or 0)
    if not scored:
        return [Measurement(
            control=PREDICTION_DRIFT, metric="alerts_per_day", value=0.0,
            state=UNMEASURABLE, n_rows=0,
            detail="nothing scored in the window")]

    observed = float(figures["alerts_per_day"])
    ratio = observed / published_rate if published_rate else None
    out = [Measurement(
        control=PREDICTION_DRIFT, metric="alerts_per_day", value=observed,
        threshold=published_rate * max_ratio if published_rate else None,
        state=(UNMEASURABLE if ratio is None
               else ALERT if ratio > max_ratio else OK),
        n_rows=scored,
        detail=("%.2f times the %.1f a day the version was published at"
                % (ratio, published_rate)) if ratio is not None else
               "the manifest of the version in service carries no rate")]
    for name in ("score_median", "score_p99"):
        if figures.get(name) is not None:
            out.append(Measurement(
                control=PREDICTION_DRIFT, metric=name,
                value=float(figures[name]), state=OK, n_rows=scored,
                detail="recorded to explain the rate, not judged"))
    return out


def performance_measurements(figures: dict, min_recall: float,
                             min_frauds: int) -> list[Measurement]:
    """The real performance against the specification. Pure.

    THE FLOOR IS READ ON recall_flagged, the model's own decision, and never on
    recall_alerted. The two diverge whenever the alert capacity binds, and no
    retraining can refit its way out of a ceiling -- so a trigger reading the
    second would answer a question about the queue with a question about the
    model. sql/monitoring/performance_window.sql carries the same warning.

    BELOW min_frauds NOTHING IS COMPARED. A recall over twenty frauds swings
    more than the gap being watched, so the measurement is recorded
    UNMEASURABLE: the floor was not crossed, it was not asked.
    """
    verdicts = int(figures.get("verdicts") or 0)
    frauds = int(figures.get("frauds") or 0)
    if frauds < min_frauds:
        return [Measurement(
            control=PERFORMANCE, metric="recall_flagged", value=0.0,
            state=UNMEASURABLE, n_rows=verdicts, n_labelled=frauds,
            detail="%d labelled frauds, under the floor of %d: a recall over "
                   "so few carries more noise than the gap watched"
                   % (frauds, min_frauds))]

    recall = float(figures["recall_flagged"])
    out = [Measurement(
        control=PERFORMANCE, metric="recall_flagged", value=recall,
        threshold=min_recall, state=ALERT if recall < min_recall else OK,
        n_rows=verdicts, n_labelled=frauds,
        detail="%d of %d frauds flagged" % (
            int(figures.get("true_positives") or 0), frauds))]

    alerted = figures.get("recall_alerted")
    if alerted is not None:
        gap = recall - float(alerted)
        out.append(Measurement(
            control=PERFORMANCE, metric="recall_alerted",
            value=float(alerted), state=OK, n_rows=verdicts,
            n_labelled=frauds,
            detail=("the service, not the model: %.3f below what the model "
                    "flagged, which is the alert capacity binding"
                    % gap) if gap > 0 else
                   "the service matched the model: capacity is not binding"))
    for name in ("precision_flagged", "base_rate"):
        if figures.get(name) is not None:
            out.append(Measurement(
                control=PERFORMANCE, metric=name, value=float(figures[name]),
                state=OK, n_rows=verdicts, n_labelled=frauds,
                detail="recorded beside the recall: a precision cannot be "
                       "read without the base rate of its own window"))
    return out


def alerting(measurements) -> list[Measurement]:
    """The measurements in alert, in the order they were taken."""
    return [m for m in measurements if m.state == ALERT]


def retrain_reason(measurements, allowed) -> str | None:
    """Why a retraining is due, or None.

    THE POLICY DECIDES WHICH ALERTS COUNT. Input drift and a performance floor
    crossed are the two a retraining can answer. Prediction drift usually calls
    for the threshold to be rechosen rather than the model refitted, and a data
    quality alert calls for the stream to be fixed -- refitting on a broken
    stream would learn the breakage. The list lives in governance.yml.
    """
    due = [m for m in alerting(measurements) if m.control in allowed]
    if not due:
        return None
    return "; ".join("%s: %s = %.4f against %s"
                     % (m.control, m.metric, m.value, m.threshold)
                     for m in due)


# ----------------------------------------------------------------- reading, writing

def features_window(conn, window: Window):
    """The transactions of the window, as the model reads them."""
    import pandas as pd

    from ml4fd import db

    rows, names = db.query_file_columns(conn, FEATURES_SQL, **window.params())
    return pd.DataFrame(rows, columns=names)


def flag_rate(conn, window: Window, model_version: str,
              per_day: float) -> dict:
    """The flag rate of the window, scaled to the announced daily volume.

    SCALED, because a share means nothing to an operator and a count depends on
    how long the window is. per_day is threshold.API_TXN_PER_DAY, the rate the
    API announces -- the same denominator M5 published the model's figure with,
    which is what makes the two comparable at all.
    """
    from ml4fd import db

    rows, names = db.query_file_columns(
        conn, FLAGS_SQL, model_version=model_version, **window.params())
    figures = dict(zip(names, rows[0])) if rows else {}
    share = figures.get("flag_share")
    figures["alerts_per_day"] = float(share) * per_day if share else 0.0
    return figures


def performance(conn, window: Window, model_version: str) -> dict:
    """The confusion matrix of the window, on the knowledge axis."""
    from ml4fd import db

    rows, names = db.query_file_columns(
        conn, PERFORMANCE_SQL, model_version=model_version, **window.params())
    return dict(zip(names, rows[0])) if rows else {}


def record(conn, measurements, window: Window, model_version: str, tool: str,
           run_id: str | None = None) -> int:
    """Append the measurements to fd_monitoring. Returns how many."""
    from ml4fd import db

    for measurement in measurements:
        db.execute_file(conn, RECORD_SQL,
                        **measurement.row(window, model_version, tool, run_id))
    log.info("%d measurement(s) recorded, %d in alert",
             len(measurements), len(alerting(measurements)))
    return len(measurements)


def reference_split(settings, dataset_version: str):
    """The training split the current window is compared against.

    THE REFERENCE MOVES WITH THE CHAMPION, which is why it is derived from the
    version in service rather than named in governance.yml. A fixed path would
    keep measuring the drift away from a model nobody is running, and after
    two promotions the figure would be about history.

    Through score.manifest_path so there is one place that knows a recipe
    directory is named by its VERSION and never by its digest.
    """
    from ml4fd import score

    return score.manifest_path(settings, dataset_version).parent / "train.parquet"


def data_quality_measurements(conn, settings, window: Window
                              ) -> list[Measurement]:
    """The quality of what the stream delivered, copied and not re-measured.

    NOTHING NEW IS COMPUTED HERE, and governance.yml says why: schema, missing
    values, out-of-range values and freshness are already decided and already
    measured -- validation rejects to fd_quarantine with a reason, and
    health.py watches freshness and the rejection rate. A second measurement
    of the same thing is a second figure that can disagree with the first.
    So the existing facts are read through health.facts_of and COPIED here,
    judged against the limits ops already declares.
    What this buys is EC45: the quality figures land on the same timeline as
    the drift and the performance. Three stores with three retentions cannot
    be read together, and "constater une evolution" needs them read together.
    THE SAME EVENT CAN REACH TWO CHANNELS, and that is intended rather than
    overlooked. The pipeline owner is told the chain is rejecting; the model
    owner is told the inputs are no longer trustworthy. Same fact, two
    decisions to take, and data_quality is deliberately absent from
    trigger.retrain_on: refitting on a broken stream would learn the breakage.
    """
    from ml4fd import health

    facts = health.facts_of(conn, "dag_ingest", window.start, window.end)
    read = int(facts.lines_read)
    out = [Measurement(
        control=DATA_QUALITY, metric="rejection_share",
        value=round(facts.rejection_share, 6),
        threshold=settings.ops_max_rejection_share,
        state=(ALERT if facts.rejection_share > settings.ops_max_rejection_share
               else OK),
        n_rows=read,
        detail="%d of %d lines quarantined" % (facts.lines_rejected, read))]

    if facts.seconds_since_success is not None:
        limit = settings.ops_freshness_minutes * 60.0
        out.append(Measurement(
            control=DATA_QUALITY, metric="seconds_since_success",
            value=round(facts.seconds_since_success, 1), threshold=limit,
            state=ALERT if facts.seconds_since_success > limit else OK,
            n_rows=read,
            detail="the stream last delivered %.0f s ago"
                   % facts.seconds_since_success))
    else:
        out.append(Measurement(
            control=DATA_QUALITY, metric="seconds_since_success", value=0.0,
            state=UNMEASURABLE, n_rows=read,
            detail="no successful ingestion run on record: there is no "
                   "freshness to judge yet"))
    return out


# --------------------------------------------- transitions, channel, trigger

def controls_from(measurements) -> list:
    """One verdict per family of control, for health.py's state machine.

    FOUR CONTROLS AND NOT TWENTY MEASUREMENTS. fd_monitoring keeps every
    figure, because EC45 asks for a series; fd_ops_state keeps one state per
    control, because a message is worth sending about a family and not about
    each column of it. A family is in alert when any of its judged figures is.

    A family with nothing measurable is reported ok and not alert: "could not
    measure" is said in the measurement's own state, and raising the control
    would call a gap in the data a fault in the model.
    """
    from ml4fd import health

    out = []
    for control in (INPUT_DRIFT, PREDICTION_DRIFT, PERFORMANCE, DATA_QUALITY):
        family = [m for m in measurements if m.control == control]
        if not family:
            continue
        bad = [m for m in family if m.state == ALERT]
        out.append(health.Control(
            control=control,
            state=ALERT if bad else OK,
            detail=("; ".join(m.detail or m.metric for m in bad) if bad
                    else "%d figure(s) within their limits" % len(family))))
    return out


def send_file(payload: dict, settings) -> None:
    """One JSON line appended to the DECLARED MONITORING PATH.

    A SIBLING OF health.send_file AND NOT A PARAMETER OF IT. The destination is
    the one thing that must not be passed in: three audiences, three files, and
    config refuses any two of them equal. A shared function taking a path would
    make the separation a caller's choice, which is how one channel ends up
    writing into another's file.
    """
    from ml4fd import notify

    notify.append_json_line(settings.monitoring_file, payload)


def send_log(payload: dict, settings) -> None:
    """A structured line in the service journal, under its own prefix."""
    import json

    log.error("SURVEILLANCE %s", json.dumps(payload, sort_keys=True,
                                            ensure_ascii=False))


CHANNELS = {"log": send_log, "file": send_file}


def channel_of(name: str):
    """The monitoring channel a name designates, refusing an unknown one."""
    if name not in CHANNELS:
        raise ValueError(
            "monitoring.channel is %r; known channels are %s. A real channel "
            "plugs into the same interface -- a function taking (payload, "
            "settings) -- and is declared in ml4fd/monitor.py."
            % (name, ", ".join(sorted(CHANNELS))))
    return CHANNELS[name]


def notify(conn, settings, report: dict) -> dict:
    """Send each transition, then record it and journal it. SEND, THEN RECORD.

    The order is health.notify_ops's, and so is the reason: recording first
    would mark the control announced while nobody was told, and no later run
    would announce it -- there would no longer be a transition to see. That
    failure is silent and permanent; a duplicate is neither.

    health.payload_of is reused rather than copied, so the one check that no
    transaction field reaches a channel lives in one place.

    Covers: EC44
    Lot: L4
    Competency: C6
    """
    from ml4fd import health

    channel_name = settings.monitoring_channel
    send = channel_of(channel_name)
    sent, failed = 0, 0

    for change in report.get("transitions", []):
        transition = health.Transition(change["control"], change["state"],
                                       change["detail"])
        payload = health.payload_of(transition, report)
        try:
            send(payload, settings)
        except Exception as problem:            # noqa: BLE001
            failed += 1
            log.exception("canal de surveillance %r en echec sur %s : %s",
                          channel_name, transition.control, problem)
            continue
        health.record(conn, transition, report.get("run_id"))
        health.journal(conn, payload, channel_name)
        conn.commit()
        sent += 1

    return {"channel": channel_name, "sent": sent, "failed": failed,
            "recipient": settings.monitoring_recipient,
            "transitions": len(report.get("transitions", [])),
            "in_alert": report.get("in_alert", 0)}


def last_request(conn):
    """When the monitoring last asked for a retraining, or None if never.

    None AND NOT A NULL FROM max(). An aggregate over an empty table returns
    NULL, and the caller would then have to tell "never asked" from "asked at
    an unknown time" -- which are the same thing here, but would not read as
    such at the call site.
    """
    from ml4fd import db

    rows = db.query_file(conn, LAST_REQUEST_SQL)
    return rows[0][0] if rows else None


def record_request(conn, controls, reason: str, run_id=None) -> None:
    """Record a retraining request, AFTER the trigger fired.

    TRIGGER, THEN RECORD -- the same order and the same reason as fd_frauds and
    fd_ops_state, which sql/ddl/100_ops_state.sql states for the channel: the
    record is written only after the thing it records has happened, so a crash
    between the two repeats one request on the next run rather than losing it.
    At-least-once, as governance.yml declares for the alerts.
    """
    from ml4fd import db

    db.execute_file(conn, RECORD_REQUEST_SQL, run_id=run_id,
                    controls=",".join(sorted(controls)), reason=reason)


def stored_from(report: dict) -> list[dict]:
    """The recorded control states, read back out of a report. Pure.

    ONE PARSER AND NOT TWO. decide() and the task that records the request both
    need these rows, and a report carries `since` as an ISO string because it
    travels through XCom as JSON. Two parsers of one shape is how one of them
    comes to forget a field -- `detail`, here, which is what puts the drifted
    column count into the reason an operator reads.
    """
    return [{"control": row["control"], "state": row["state"],
             "since": (dt.datetime.fromisoformat(row["since"])
                       if row.get("since") else None),
             "detail": row.get("detail")}
            for row in report.get("stored", [])]


def alerting_controls(stored, allowed) -> list[str]:
    """The triggering controls currently in alert, by name."""
    return sorted(row["control"] for row in stored
                  if row["control"] in allowed and row["state"] == ALERT)


def retraining_due(stored, answered, allowed, cooldown: dt.timedelta,
                   now: dt.datetime) -> str | None:
    """Why a retraining is due, or None. Pure.

    ON AN ALERT NOT YET ANSWERED, and no longer on a transition. The old rule
    read report["transitions"], and it was right about one thing -- a drift
    that lasts three days must ask once and not once per window -- and wrong
    about where the evidence lived. The transition was computed by one task and
    COMMITTED BY THE NEXT, so a failure in this one consumed it: on 2026-11-20
    at 09:34 UTC the drift was seen, input_drift went into alert, tell() wrote
    the state, decide() crashed, and every later run found a standing alert
    with nothing left to report. One crash, and the request was lost for good.

    SO THE QUESTION MOVED, from "did a transition happen in the run I am part
    of" to "has this alert been answered". The second is a question about the
    database, so it survives a crash, a retry and a scheduler restart --
    nothing about it is held in a task's memory.

    ANSWERED MEANS a request recorded at or after the alert began. An alert
    that starts AFTER the last request therefore re-arms the trigger, which is
    what lets a control that recovers and fails again ask a second time.

    THE COOLDOWN NOW KEYS ON THE REQUEST, which is what it was always about.
    The old rule suppressed when ANOTHER triggering control had been in alert
    for less than the cooldown -- a proxy for "a training was asked for
    recently" that held only while a request followed every transition.

    WHY IT IS ALLOWED TO BE WRONG. It may fire on a drift that turns out
    harmless; the cost is a training nobody needed. It cannot put a bad model
    in service, because registering is not promoting: the gate of L3 judges the
    candidate against the champion and refuses it. So this is tuned to miss
    little rather than to be right -- the expensive mistake is downstream, and
    it is already guarded.

    Covers: EC33
    Lot: L4
    Competency: C5, C6
    """
    alerting = [row for row in stored
                if row["control"] in allowed
                and row["state"] == ALERT
                and row["since"] is not None]
    if not alerting:
        return None

    began = max(row["since"] for row in alerting)
    if answered is not None and answered >= began:
        log.info("reentrainement deja demande : alerte ouverte le %s, "
                 "demande enregistree le %s", began, answered)
        return None
    if answered is not None and now - answered < cooldown:
        log.info("reentrainement supprime : demande il y a moins de %s",
                 cooldown)
        return None

    return "; ".join(
        "%s in alert since %s%s"
        % (row["control"], row["since"],
           " (%s)" % row["detail"] if row.get("detail") else "")
        for row in sorted(alerting, key=lambda row: row["control"]))


def _columns(metrics) -> list[str]:
    return [(m.get("config") or {}).get("column") for m in metrics
            if (m.get("config") or {}).get("type") == VALUE_DRIFT]


def _as_float(value):
    return None if value is None else float(value)
