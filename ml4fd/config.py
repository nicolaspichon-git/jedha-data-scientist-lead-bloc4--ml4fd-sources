"""Configuration: governance.yml and environment variables.

Block: B3, B4.
Lot: B3-L0, B4-L0.
Competency: B3-C4.
Covers: B3-EI12, B3-EC19.

Durations that outlast the demonstration itself (verdict delay, retention) live
here and nowhere else. Secrets never live in governance.yml: they come from the
environment.
"""
from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

# Inside the container the file is mounted under /opt/project; outside it sits at
# the root of step-2. Look in both places.
CANDIDATES = (
    Path(os.environ.get("FRAUD_GOVERNANCE", "")),
    Path("/opt/project/governance.yml"),
    Path(__file__).resolve().parent.parent / "governance.yml",
)

# WHERE THE DAG FILES CAN BE, and the repo layout is NOT the container's.
# docker-compose.yml mounts ./airflow/dags at /opt/airflow/dags -- NOT under
# /opt/project -- so a tool running inside the scheduler with root=/opt/project
# finds no airflow/ directory at all.
#
# ONE DEFINITION, HERE, BECAUSE IT WAS WRITTEN TWICE AND WRONG TWICE. The S3
# checker derived the path under the root and read both DAGs ABSENT for a whole
# lot; it was fixed there, with a test on ITS constant -- and the S4 checker,
# written afterwards with its own derivation, read dag_daily ABSENT the same
# way. A test guarding one copy says nothing about the next copy. So there is
# no copy: tools ask dags_dir(), and tests/test_dag_paths.py refuses any tool
# that builds the path itself.
DAG_DIRS = ("airflow/dags", "/opt/airflow/dags")

REQUIRED_ENV = ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB")


@dataclass(frozen=True)
class Settings:
    verdict_mode: str
    verdict_delay_days: int
    demo_delay_seconds: int
    verdict_schedule_nominal: str
    verdict_schedule_demo: str
    raw_retention_days: int
    quarantine_retention_days: int
    airflow_logs_retention_days: int
    retention_kept: tuple[str, ...]
    purge_max_share: float
    purge_schedule: str
    decision_threshold: float
    rule_min_amount: float
    rule_night_hours: tuple[int, ...]
    drop_features: tuple[str, ...]
    alert_channel: str
    alert_file: str
    alert_delivery: str
    enforce_capacity: bool
    ops_channel: str
    ops_file: str
    ops_window_minutes: int
    ops_freshness_minutes: int
    ops_max_empty_share: float
    ops_max_rejection_share: float
    monitoring_mode: str
    monitoring_schedule_nominal: str
    monitoring_schedule_demo: str
    monitoring_tool: str
    monitoring_channel: str
    monitoring_file: str
    monitoring_recipient: str
    monitoring_drift_window_minutes: int
    monitoring_demo_drift_window_minutes: int
    monitoring_drift_min_rows: int
    monitoring_demo_drift_min_rows: int
    monitoring_max_drifted_share: float
    monitoring_drift_method: str
    monitoring_drift_not_comparable: tuple[str, ...]
    monitoring_max_alert_rate_ratio: float
    monitoring_min_recall: float
    monitoring_performance_window_days: int
    monitoring_demo_performance_window_minutes: int
    monitoring_min_labelled_frauds: int
    monitoring_retrain_on: tuple[str, ...]
    monitoring_cooldown_minutes: int
    monitoring_demo_cooldown_minutes: int
    report_timezone: str
    report_root: str
    report_schedule: str
    api_endpoint: str
    api_timeout_s: float
    realtime_schedule: str
    calls_per_run: int
    call_spacing_s: float
    clock_tolerance_s: float
    threshold_policy: str
    alert_capacity_per_day: int
    cost_false_alert: float
    validation_share: float
    # B4-L3, the promotion gate. Read here like every other decision
    # of governance.yml, so a caller asks a field and not a file.
    champion_alias: str
    promotion_significance: float
    promotion_respects_capacity: bool
    promote_first_unopposed: bool
    max_amount: float
    scoring_endpoint: str
    scoring_endpoint_timeout_seconds: int
    systematic_share: float
    # NOT A URL ANY MORE, which is why it is not called one. Since the
    # corpora moved to a private bucket this carries whatever governance.yml
    # declares -- an http(s) URL, an S3 object key, or a bare file name -- and
    # ml4fd/fetch.py decides which of the three it is. A field named csv_url
    # holding "kaggle-datasets/fraudTrain.csv" would be a name that lies, and
    # the next reader would spend a minute finding out.
    csv_source: str
    # THE SWITCH THE DEMONSTRATION TURNS. Declared here and read on both
    # sides of it since 2026-11-20: tools/drift.py writes it and
    # producer/app.py reads it, through this one field. Before that the
    # declaration was decorative -- the producer used an environment
    # default of its own, so the line in governance.yml and the path in
    # service could have disagreed without anyone finding out.
    drift_switch: str
    api_url: str
    label_horizon: dt.datetime
    min_positives: int
    datasets_root: str
    dataset_family: str
    dataset_schema_version: str
    split_rule: str
    include_stream: bool
    identifying_fields: tuple[str, ...]
    pepper: str                 # never logged, never in governance.yml
    pg_host: str
    pg_db: str
    pg_user: str
    pg_password: str

    def verdict_delay(self) -> dt.timedelta:
        """The delay in force, from the single verdicts.mode switch.

        S1 and S3 both call this rather than reading a figure of their own: the
        historical labels and the real-time ones must sit on the same delay, or
        the blind band differs between training and scoring.

        Block: B3, B4.
        Lot: B3-L5, B4-L0.
        Competency: B3-C2.
        Covers: B3-EI02, B3-EI12.
        """
        if self.verdict_mode == "nominal":
            return dt.timedelta(days=self.verdict_delay_days)
        if self.verdict_mode == "demo":
            return dt.timedelta(seconds=self.demo_delay_seconds)
        raise ValueError(
            "verdicts.mode is %r; expected 'nominal' or 'demo'" % self.verdict_mode)

    def drift_window(self) -> dt.timedelta:
        """The window the input distribution is compared over.

        RESOLVED BY THE SWITCH, exactly as verdict_delay() is, and for the
        same reason: a loop whose windows are measured in days is a loop
        nobody can show. The two modes are declared side by side in
        governance.yml so the demonstration value is never a figure somebody
        edited over the real one and forgot to put back.

        Block: B4.
        Lot: B4-L4.
        Competency: B4-C6.
        Covers: B4-EC39, B4-EC45.
        """
        return self._by_mode(
            dt.timedelta(minutes=self.monitoring_drift_window_minutes),
            dt.timedelta(minutes=self.monitoring_demo_drift_window_minutes))

    def performance_window(self) -> dt.timedelta:
        """The window the real performance is measured over.

        LONGER THAN THE DRIFT WINDOW, and that is a measurement and not a
        preference. At a base rate of 0.414 % the stream yields about 30
        frauds a day, and a recall over 30 frauds carries a standard error of
        0.079 -- so one day swings +/-0.16 at two sigma, wider than the gap
        between the published 0.751 and the floor of 0.60. Seven days is about
        210 frauds and a standard error of 0.030.

        Block: B4.
        Lot: B4-L4.
        Competency: B4-C6.
        Covers: B4-EC40.
        """
        return self._by_mode(
            dt.timedelta(days=self.monitoring_performance_window_days),
            dt.timedelta(
                minutes=self.monitoring_demo_performance_window_minutes))

    def monitoring_schedule(self) -> str:
        """How often the model is watched, by the switch.

        READ AT DAG PARSE TIME, which departs from the five other DAGs: each of
        those repeats its cron as a literal and a test ties it to governance.
        That convention keeps a DAG parseable without governance, and it is
        right for a cadence nobody changes. It is wrong for a DEMONSTRATION
        knob: flipping monitoring.mode would then need a second edit inside the
        DAG, and between the two edits the suite would be red. One switch has
        to move one thing.

        Block: B4.
        Lot: B4-L4.
        Competency: B4-C5, B4-C6.
        Covers: B4-EC32, B4-EC33.
        """
        return self._by_mode(self.monitoring_schedule_nominal,
                             self.monitoring_schedule_demo)

    def verdict_schedule(self) -> str:
        """How often the due verdicts are written, by the VERDICT switch.

        BY verdicts.mode AND NOT BY monitoring.mode, because the two are
        independent and must stay so: shortening the windows a recording is
        read over is not the same decision as shortening the delay before a
        verdict is known. A cadence resolved by the wrong switch would write
        verdicts once at 02:00 while they fall due every 300 s -- the delay
        short, the labels never arriving, and nothing saying why.

        Block: B4.
        Lot: B4-L4.
        Competency: B4-C4, B4-C6.
        Covers: B4-EC41.
        """
        return self._by_mode(self.verdict_schedule_nominal,
                             self.verdict_schedule_demo,
                             mode=self.verdict_mode, name="verdicts.mode")

    def drift_min_rows(self) -> int:
        """The smallest window the drift measurement will speak about.

        THE SYMMETRY WITH performance, which refuses to read a recall under
        min_labelled_frauds. Until 2026-11-20 the drift measurement had no such
        floor -- `if len(current):` was the whole guard -- so a window of three
        rows produced a verdict with the authority of one of seven thousand.
        The floor counts ROWS and not frauds, because a distribution test
        counts observations and not positives.

        Block: B4.
        Lot: B4-L4.
        Competency: B4-C6.
        Covers: B4-EC39, B4-EC45.
        """
        return self._by_mode(self.monitoring_drift_min_rows,
                             self.monitoring_demo_drift_min_rows)

    def retrain_cooldown(self) -> dt.timedelta:
        """How long a retraining suppresses the next one (EC33)."""
        return self._by_mode(
            dt.timedelta(minutes=self.monitoring_cooldown_minutes),
            dt.timedelta(minutes=self.monitoring_demo_cooldown_minutes))

    def _by_mode(self, nominal, demo, mode=None, name="monitoring.mode"):
        """One place where a demonstration switch is read.

        Untyped on purpose: it resolves durations, a row count and two cron
        expressions, and a second copy of the rule per type would be the same
        rule written four times.

        TWO SWITCHES, ONE VALIDATOR. monitoring.mode governs the windows, the
        cooldown, the drift floor and the monitoring cadence; verdicts.mode
        governs the verdict delay and the cadence that writes them. They are
        independent by design, so the mode is a parameter here and the key is
        named in the refusal -- otherwise a bad value in verdicts.mode would
        send a reader to the wrong line of governance.yml."""
        chosen = self.monitoring_mode if mode is None else mode
        if chosen == "nominal":
            return nominal
        if chosen == "demo":
            return demo
        raise ValueError("%s is %r; expected 'nominal' or 'demo'"
                         % (name, chosen))

    def observed_per_day(self) -> int:
        """Transactions B3-N7 will actually see in a day, from its own settings.

        WHY THIS IS COMPUTED AND NOT NOTED IN A COMMENT. One call returns one
        transaction and there is no queue behind it (B3-N0), so the cadence times
        the calls per run IS the share of the stream that gets scored -- and
        ml4fd/threshold.py chose the threshold and the 60-alert capacity at
        API_TXN_PER_DAY = 7200. Lower this product and every published figure
        stays plausible while being wrong by the same factor.

        tests/test_config.py compares the result against that constant, so a
        later edit to the cadence or to calls_per_run fails the suite instead of
        quietly invalidating the calibration.

        Block: B3, B4.
        Lot: B3-N4, B4-L0.
        Competency: B3-C5.
        Covers: B3-EI04, B3-EC24.
        """
        return self.calls_per_run * runs_per_day(self.realtime_schedule)

    def rule_baseline(self) -> tuple[float, tuple[int, ...]]:
        """The analyst's rule the model must beat: (min_amount, night_hours).

        Settings is a FLAT frozen dataclass: it keeps no copy of the parsed
        governance file, so everything a caller needs is a field, read in
        load(). Reaching for an attribute that does not exist is how this
        method failed the first time -- after I had read the dataclass and
        written down that it was flat.

        Block: B3, B4.
        Lot: B3-M5, B4-L0.
        Competency: B3-C5.
        Covers: B3-EI04.
        """
        return self.rule_min_amount, self.rule_night_hours

    def pg_params(self) -> dict[str, str]:
        """Connection parameters, passed to psycopg as keywords.

        A dict rather than a hand-assembled DSN string: a password holding a
        space, a quote or an equals sign would break the string silently, and
        libpq's error would not name the cause.
        """
        return {"host": self.pg_host, "dbname": self.pg_db,
                "user": self.pg_user, "password": self.pg_password}

    def redacted(self) -> dict:
        """A loggable view: no pepper, no password."""
        return {
            "verdict_mode": self.verdict_mode,
            "label_horizon": self.label_horizon.isoformat(),
            "verdict_delay": str(self.verdict_delay()),
            "raw_retention_days": self.raw_retention_days,
            "quarantine_retention_days": self.quarantine_retention_days,
            "airflow_logs_retention_days": self.airflow_logs_retention_days,
            "purge_max_share": self.purge_max_share,
            "purge_schedule": self.purge_schedule,
            "decision_threshold": self.decision_threshold,
            "postgres": "%s@%s/%s" % (self.pg_user, self.pg_host, self.pg_db),
            "pepper": "<%d bytes>" % len(self.pepper) if self.pepper else "<MISSING>",
        }


def _as_utc(value) -> dt.datetime:
    """Read a governance date into an aware UTC datetime.

    PyYAML already parses an ISO date with a zone into an aware datetime, but a
    date written without one comes back naive. Refusing a naive value rather
    than assuming a zone: a horizon off by a few hours would silently let
    unlabelled rows into a dataset.
    """
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            raise ValueError(
                "labels.horizon has no time zone (%s): write it as "
                "2020-12-22T23:13:39Z" % value)
        return value.astimezone(dt.timezone.utc)
    raise ValueError("labels.horizon is not a date: %r" % value)


# The two cron forms this project declares, and no others. A general parser
# would be a second implementation of something Airflow already does, and a
# wrong one would answer a question about the calibration with a guess.
EVERY_N_MINUTES = re.compile(r"^\*(?:/(\d+))? \* \* \* \*$")


def runs_per_day(schedule: str) -> int:
    """Runs a day for a minute-based cron expression.

    Deliberately narrow: it accepts "* * * * *" and "*/N * * * *" and refuses
    everything else by name rather than returning a number it cannot justify.
    """
    match = EVERY_N_MINUTES.match((schedule or "").strip())
    if not match:
        raise ValueError(
            "realtime.schedule is %r. This function reads only minute-based "
            "cron (\"* * * * *\" or \"*/N * * * *\"), because the number it "
            "returns is what the threshold calibration is checked against -- a "
            "guess there would be worse than a refusal. Either declare a "
            "minute-based cadence or extend runs_per_day deliberately."
            % schedule)
    step = int(match.group(1) or 1)
    if not 1 <= step <= 1440:
        raise ValueError("realtime.schedule steps every %d minutes, which is "
                         "not a cadence" % step)
    return 1440 // step


def _hours(value) -> tuple[int, ...]:
    """Read a list of hours, refusing anything outside 0-23.

    An hour of 24 would silently match nothing, and the rule would then look
    merely weak instead of broken.
    """
    hours = tuple(int(h) for h in value or ())
    if not hours or not all(0 <= h <= 23 for h in hours):
        raise ValueError(
            "scoring.rule_baseline.night_hours must list hours between 0 and "
            "23, read %r" % (value,))
    return hours


POLICIES = ("cost", "capacity")


def _columns(value, where: str) -> tuple[str, ...]:
    """A list of column names, checked for SHAPE and not for membership.

    Whether a name is a feature of the recipe is ml4fd/datasets.without()'s
    business, and it refuses an unknown one there -- one place deciding, called
    by the DAG. This module stays free of any fraud import, so a governance file
    cannot be read differently depending on what else is loaded.

    where IS PASSED AND NOT DEFAULTED. It used to name scoring.drop_features in
    the message because that was the only caller; the second caller arrived
    2026-11-20 and would have been told to look at the wrong line of
    governance.yml. A default that names one caller is a default that lies as
    soon as there are two.
    """
    if value is None:
        return ()
    if isinstance(value, str):
        value = value.split(",")
    names = tuple(str(name).strip() for name in value if str(name).strip())
    bad = [name for name in names if not name.replace("_", "").isalnum()]
    if bad:
        raise ValueError(
            "%s holds %s, which is not a column name"
            % (where, ", ".join(repr(name) for name in bad)))
    return names


def _delivery(value) -> str:
    """The declared delivery semantics.

    Refused if unknown, because the two differ in which failure they accept: at
    least once can duplicate a notification, at most once can lose one. A value
    nobody recognises would fall through to whatever the code happens to do.
    """
    declared = str(value or "").strip()
    if declared not in ("at-least-once", "at-most-once"):
        raise ValueError(
            "notifications.delivery must be at-least-once or at-most-once, "
            "read %r. The project declares at-least-once: a fraud notified "
            "twice is better than a fraud never notified." % value)
    return declared


def _share_under_one(value, key: str) -> float:
    """A declared share, refused outside ]0, 1[.

    Zero would put a control in permanent alert -- any rejection at all, any
    empty minute -- and one would put it permanently asleep. Both are ways of
    having no control while governance.yml claims one.
    """
    share = float(value)
    if not 0 < share < 1:
        raise ValueError(
            "%s must sit strictly between 0 and 1, read %r. At 0 the control "
            "is always in alert and at 1 it never is; either way the "
            "declaration would describe a control that does not exist."
            % (key, value))
    return share


def _days(value, key: str) -> int:
    """A declared duration or count: a whole number, at least 1.

    Zero would purge everything on the first run and a negative number would
    reach into the future; a boolean is an integer to Python and a mistake here.
    """
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("%s must be a whole number of at least 1, read %r"
                         % (key, value))
    return value


def _retention(section: dict) -> dict:
    """The retention section, with the two orderings S5 depends on.

    A store that may quote the raw data cannot outlive it: fd_quarantine holds
    the raw payload of a rejected record and Airflow's logs can hold a line of
    it in a stack trace. A longer duration for either would keep personal data
    after the zone that was supposed to limit it had been purged -- so it is
    refused here, where the declaration is read, and not found at audit.

    Every `kept` store needs a written reason: "not purged" without one is a
    store nobody decided about.
    """
    out = {key: _days(section[key], "retention.%s" % key)
           for key in ("raw_events_days", "quarantine_days",
                       "airflow_logs_days")}
    for key in ("quarantine_days", "airflow_logs_days"):
        if out[key] > out["raw_events_days"]:
            raise ValueError(
                "retention.%s (%d) is longer than retention.raw_events_days "
                "(%d). It holds data quoted from the raw zone, so it would "
                "keep personal data after the raw zone had been purged."
                % (key, out[key], out["raw_events_days"]))
    kept = section.get("kept") or {}
    if not isinstance(kept, dict) or not kept:
        raise ValueError("retention.kept must name each store that is not "
                         "purged, with the reason")
    for store, reason in kept.items():
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError(
                "retention.kept.%s has no reason. A store that is not purged "
                "without a written reason is one nobody decided about." % store)
    out["kept"] = tuple(kept)
    return out


CONTROLS = ("input_drift", "prediction_drift", "performance",
            "data_quality")


def _retrain_on(value) -> tuple[str, ...]:
    """The controls allowed to trigger a retraining, checked against the four.

    AN UNKNOWN NAME IS REFUSED AND NOT IGNORED. A typo in this list would
    silently disarm the trigger: the loop would run, measure, alert, and never
    retrain, and nothing would say why. This is the same refusal as
    datasets.without() applies to a feature name.

    The names must also match the CHECK on fd_monitoring.control, or a
    measurement the policy acts on could not be recorded.
    """
    names = tuple(str(name) for name in (value or ()))
    unknown = [name for name in names if name not in CONTROLS]
    if unknown:
        raise ValueError(
            "monitoring.trigger.retrain_on names %s, which is not a control. "
            "The controls are %s. (In YAML 1.1 the bare word on is a boolean, "
            "which is why this key is retrain_on.)"
            % (", ".join(unknown), ", ".join(CONTROLS)))
    return names


def _third_destination(alert_file: str, ops_file: str,
                       monitoring_file: str) -> str:
    """The monitoring channel must not write where the other two write.

    THREE AUDIENCES, THREE DESTINATIONS. The analyst is told about a
    transaction, the pipeline owner about the chain, the model owner that what
    is in service no longer matches the world. _distinct_destinations below
    refuses the first two being equal; this refuses the third colliding with
    either, which is the same separation extended rather than a new rule.
    """
    for other, name in ((alert_file, "notifications.file"),
                        (ops_file, "ops.file")):
        if Path(monitoring_file) == Path(other):
            raise ValueError(
                "monitoring.file and %s are the same path (%s). The model "
                "owner's channel and that one have different readers; one "
                "would bury the other." % (name, monitoring_file))
    return monitoring_file


def _distinct_destinations(alert_file: str, ops_file: str) -> str:
    """The two channels must not write to the same place.

    THE ONE THING THEY MUST NOT SHARE. The analyst's queue exists to be read
    transaction by transaction under a capacity of 60 a day; the operations
    channel carries states of the chain. Merged into one file, pipeline noise
    would sit in the queue the capacity policy exists to protect, and the
    separation this lot was built for would be undone by two equal lines of
    configuration.
    """
    if Path(alert_file) == Path(ops_file):
        raise ValueError(
            "notifications.file and ops.file both resolve to %s. The analyst's "
            "queue and the operations channel must have distinct destinations: "
            "an analyst who receives pipeline messages stops reading alerts."
            % alert_file)
    return ops_file


def _policy(value) -> str:
    """Read the threshold policy, refusing an unknown one.

    A policy nobody recognises would otherwise fall through to a default, and
    the threshold would then be chosen by a rule nobody declared.
    """
    policy = str(value or "").strip()
    if policy not in POLICIES:
        raise ValueError(
            "scoring.threshold_policy must be one of %s, read %r"
            % (", ".join(POLICIES), value))
    return policy


def _share(value) -> float:
    """Read the validation share, refusing a slice that cannot hold a threshold."""
    share = float(value)
    if not 0.05 <= share <= 0.5:
        raise ValueError(
            "scoring.validation_share must sit between 0.05 and 0.5: below "
            "that the slice is too small to choose a threshold on, above it the "
            "model is fitted on too little. Read %r" % value)
    return share


def _under_project(value) -> str:
    """Resolve a declared path to an ABSOLUTE one, against the PROJECT.

    Never against the process's working directory. An Airflow task runs from
    /opt/airflow, so a bare "datasets" landed in /opt/airflow/datasets --
    inside the container, outside the mounted volume. The rows were registered
    in fd_datasets, the acceptance criteria on those rows all passed, and the
    Parquet, the model and the manifest vanished with the container. Only the
    artefact check found it.

    Used by every declared path, not just the dataset root: an alert file
    written to /opt/airflow/runtime/channels/ would disappear exactly the same
    way, and a notification nobody can find has not been delivered.
    """
    path = Path(value)
    if path.is_absolute():
        return str(path)
    return str(Path(__file__).resolve().parent.parent / path)


def _datasets_root(value) -> str:
    """Kept as the name load() has always used for the dataset root."""
    return _under_project(value)


def _cron(value, where: str) -> str:
    """A five-field cron expression, or a refusal that names the line.

    Airflow refuses a malformed one too, but at DAG parse time and with a
    message about croniter. Named here, the reader is sent to the line of
    governance.yml that is wrong rather than to a stack trace.
    """
    text = str(value).strip()
    if len(text.split()) != 5:
        raise ValueError("%s is %r; a cron expression has five fields"
                         % (where, text))
    return text


def governance_path() -> Path:
    for p in CANDIDATES:
        if p and p.is_file():
            return p
    raise FileNotFoundError(
        "governance.yml not found; looked in: %s"
        % ", ".join(str(p) for p in CANDIDATES if p)
    )


def dags_dir(root: Path, candidates=None) -> Path | None:
    """The first candidate that exists, or None. Relative ones against root.

    The candidates default to DAG_DIRS, read at CALL time so that a test can
    put the container's layout in its place.

    None rather than an exception: the callers are checkers, and "not found,
    looked in ..." is a criterion they report, naming DAG_DIRS.
    """
    for candidate in (DAG_DIRS if candidates is None else candidates):
        path = Path(candidate)
        path = path if path.is_absolute() else Path(root) / path
        if path.is_dir():
            return path
    return None


def load(path: Path | None = None) -> Settings:
    """Read governance.yml, then complete it from the environment."""
    g = yaml.safe_load((path or governance_path()).read_text(encoding="utf-8"))

    # Fail early and name the variable: an empty string here yields
    # "fe_sendauth: no password supplied", which does not say where to look.
    missing = [k for k in REQUIRED_ENV if not os.environ.get(k)]
    if missing:
        raise EnvironmentError(
            "connection variables missing from the environment: %s. Inside a "
            "container, check they appear in the service's environment section "
            "(docker-compose.yml) and not only in .env; outside one, export "
            "them before running." % ", ".join(missing)
        )

    retention = _retention(g["retention"])

    return Settings(
        verdict_mode=str(g["verdicts"]["mode"]),
        verdict_delay_days=int(g["verdicts"]["delay_days"]),
        demo_delay_seconds=int(g["verdicts"]["demo_delay_seconds"]),
        verdict_schedule_nominal=_cron(
            g["verdicts"]["schedule"], "verdicts.schedule"),
        verdict_schedule_demo=_cron(
            g["verdicts"]["demo_schedule"], "verdicts.demo_schedule"),
        raw_retention_days=retention["raw_events_days"],
        quarantine_retention_days=retention["quarantine_days"],
        airflow_logs_retention_days=retention["airflow_logs_days"],
        retention_kept=retention["kept"],
        purge_max_share=_share_under_one(
            g["purge"]["max_share"], "purge.max_share"),
        purge_schedule=str(g["purge"]["schedule"]),
        decision_threshold=float(g["scoring"]["decision_threshold"]),
        rule_min_amount=float(g["scoring"]["rule_baseline"]["min_amount"]),
        rule_night_hours=_hours(g["scoring"]["rule_baseline"]["night_hours"]),
        drop_features=_columns(g["scoring"].get("drop_features"),
                               "scoring.drop_features"),
        alert_channel=str(g["notifications"]["channel"]),
        alert_file=_under_project(g["notifications"]["file"]),
        alert_delivery=_delivery(g["notifications"]["delivery"]),
        enforce_capacity=bool(g["notifications"]["enforce_capacity"]),
        ops_channel=str(g["ops"]["channel"]),
        ops_file=_distinct_destinations(
            _under_project(g["notifications"]["file"]),
            _under_project(g["ops"]["file"])),
        ops_window_minutes=int(g["ops"]["window_minutes"]),
        ops_freshness_minutes=int(g["ops"]["freshness_minutes"]),
        ops_max_empty_share=_share_under_one(
            g["ops"]["max_empty_run_share"], "ops.max_empty_run_share"),
        ops_max_rejection_share=_share_under_one(
            g["ops"]["max_rejection_rate"], "ops.max_rejection_rate"),
        monitoring_mode=str(g["monitoring"]["mode"]),
        monitoring_tool=str(g["monitoring"]["tool"]),
        monitoring_channel=str(g["monitoring"]["channel"]),
        monitoring_file=_third_destination(
            _under_project(g["notifications"]["file"]),
            _under_project(g["ops"]["file"]),
            _under_project(g["monitoring"]["file"])),
        # EC44 asks the alert to designate its recipient. A role and never
        # a person: the holder changes, the responsibility does not.
        monitoring_recipient=str(g["monitoring"]["recipient"]),
        monitoring_schedule_nominal=_cron(
            g["monitoring"]["schedule"], "monitoring.schedule"),
        monitoring_schedule_demo=_cron(
            g["monitoring"]["demo_schedule"], "monitoring.demo_schedule"),
        monitoring_drift_window_minutes=int(
            g["monitoring"]["input_drift"]["window_minutes"]),
        monitoring_demo_drift_window_minutes=int(
            g["monitoring"]["input_drift"]["demo_window_minutes"]),
        monitoring_drift_min_rows=int(
            g["monitoring"]["input_drift"]["min_rows"]),
        monitoring_demo_drift_min_rows=int(
            g["monitoring"]["input_drift"]["demo_min_rows"]),
        monitoring_max_drifted_share=_share_under_one(
            g["monitoring"]["input_drift"]["max_drifted_share"],
            "monitoring.input_drift.max_drifted_share"),
        monitoring_drift_method=str(g["monitoring"]["input_drift"]["method"]),
        monitoring_drift_not_comparable=_columns(
            g["monitoring"]["input_drift"].get("not_comparable"),
            "monitoring.input_drift.not_comparable"),
        monitoring_max_alert_rate_ratio=float(
            g["monitoring"]["prediction_drift"]["max_alert_rate_ratio"]),
        monitoring_min_recall=_share_under_one(
            g["monitoring"]["performance"]["min_recall"],
            "monitoring.performance.min_recall"),
        monitoring_performance_window_days=int(
            g["monitoring"]["performance"]["window_days"]),
        monitoring_demo_performance_window_minutes=int(
            g["monitoring"]["performance"]["demo_window_minutes"]),
        monitoring_min_labelled_frauds=int(
            g["monitoring"]["performance"]["min_labelled_frauds"]),
        monitoring_retrain_on=_retrain_on(
            g["monitoring"]["trigger"]["retrain_on"]),
        monitoring_cooldown_minutes=int(
            g["monitoring"]["trigger"]["cooldown_minutes"]),
        monitoring_demo_cooldown_minutes=int(
            g["monitoring"]["trigger"]["demo_cooldown_minutes"]),
        report_timezone=str(g["report"]["timezone"]),
        report_root=_under_project(g["report"]["root"]),
        report_schedule=str(g["report"]["schedule"]),
        api_endpoint=str(g["realtime"]["endpoint"]),
        api_timeout_s=float(g["realtime"]["timeout_seconds"]),
        realtime_schedule=str(g["realtime"]["schedule"]),
        calls_per_run=int(g["realtime"]["calls_per_run"]),
        call_spacing_s=float(g["realtime"]["spacing_seconds"]),
        clock_tolerance_s=float(g["realtime"]["clock_tolerance_seconds"]),
        threshold_policy=_policy(g["scoring"]["threshold_policy"]),
        alert_capacity_per_day=int(g["scoring"]["alert_capacity_per_day"]),
        champion_alias=str(g["promotion"]["champion_alias"]),
        promotion_significance=_share_under_one(
            g["promotion"]["significance"], "promotion.significance"),
        promotion_respects_capacity=bool(
            g["promotion"]["respect_alert_capacity"]),
        promote_first_unopposed=bool(
            g["promotion"]["promote_first_unopposed"]),
        cost_false_alert=float(g["scoring"]["cost_false_alert"]),
        validation_share=_share(g["scoring"]["validation_share"]),
        max_amount=float(g["validation"]["max_amount"]),
        scoring_endpoint=str(g["scoring"]["endpoint"]),
        scoring_endpoint_timeout_seconds=int(
            g["scoring"]["endpoint_timeout_seconds"]),
        systematic_share=float(g["validation"]["systematic_share"]),
        csv_source=g["sources"]["historical_csv"],
        drift_switch=str(g["sources"]["drift_switch"]),
        label_horizon=_as_utc(g["labels"]["horizon"]),
        min_positives=int(g["labels"]["min_positives"]),
        datasets_root=_datasets_root(g["datasets"]["root"]),
        dataset_family=str(g["datasets"]["family"]),
        dataset_schema_version=str(g["datasets"]["schema_version"]),
        split_rule=str(g["datasets"]["split_rule"]),
        include_stream=bool(g["datasets"]["include_stream"]),
        api_url=g["sources"]["realtime_api"],
        identifying_fields=tuple(g["personal_data"]["identifying_fields"]),
        pepper=os.environ.get("ML4FD_PEPPER", ""),
        pg_host=os.environ.get("POSTGRES_HOST", "postgres"),
        pg_db=os.environ["POSTGRES_DB"],
        pg_user=os.environ["POSTGRES_USER"],
        pg_password=os.environ["POSTGRES_PASSWORD"],
    )
