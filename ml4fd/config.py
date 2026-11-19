"""Configuration: governance.yml and environment variables.

Covers: EI12, EC19
Lot: L0
Competency: C4

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
    max_amount: float
    systematic_share: float
    csv_url: str
    api_url: str
    label_horizon: dt.datetime
    min_positives: int
    datasets_root: str
    dataset_family: str
    dataset_schema_version: str
    split_rule: str
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

        Covers: EI02, EI12
        Lot: L5
        Competency: C2
        """
        if self.verdict_mode == "nominal":
            return dt.timedelta(days=self.verdict_delay_days)
        if self.verdict_mode == "demo":
            return dt.timedelta(seconds=self.demo_delay_seconds)
        raise ValueError(
            "verdicts.mode is %r; expected 'nominal' or 'demo'" % self.verdict_mode)

    def observed_per_day(self) -> int:
        """Transactions N7 will actually see in a day, from its own settings.

        WHY THIS IS COMPUTED AND NOT NOTED IN A COMMENT. One call returns one
        transaction and there is no queue behind it (N0), so the cadence times
        the calls per run IS the share of the stream that gets scored -- and
        ml4fd/threshold.py chose the threshold and the 60-alert capacity at
        API_TXN_PER_DAY = 7200. Lower this product and every published figure
        stays plausible while being wrong by the same factor.

        tests/test_config.py compares the result against that constant, so a
        later edit to the cadence or to calls_per_run fails the suite instead of
        quietly invalidating the calibration.

        Covers: EI04, EC24
        Lot: N4
        Competency: C5
        """
        return self.calls_per_run * runs_per_day(self.realtime_schedule)

    def rule_baseline(self) -> tuple[float, tuple[int, ...]]:
        """The analyst's rule the model must beat: (min_amount, night_hours).

        Settings is a FLAT frozen dataclass: it keeps no copy of the parsed
        governance file, so everything a caller needs is a field, read in
        load(). Reaching for an attribute that does not exist is how this
        method failed the first time -- after I had read the dataclass and
        written down that it was flat.

        Covers: EI04
        Lot: M5
        Competency: C5
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


def _columns(value) -> tuple[str, ...]:
    """A list of column names, checked for SHAPE and not for membership.

    Whether a name is a feature of the recipe is ml4fd/datasets.without()'s
    business, and it refuses an unknown one there -- one place deciding, called
    by the DAG. This module stays free of any fraud import, so a governance file
    cannot be read differently depending on what else is loaded.
    """
    if value is None:
        return ()
    if isinstance(value, str):
        value = value.split(",")
    names = tuple(str(name).strip() for name in value if str(name).strip())
    bad = [name for name in names if not name.replace("_", "").isalnum()]
    if bad:
        raise ValueError(
            "scoring.drop_features holds %s, which is not a column name"
            % ", ".join(repr(name) for name in bad))
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
        drop_features=_columns(g["scoring"].get("drop_features")),
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
        cost_false_alert=float(g["scoring"]["cost_false_alert"]),
        validation_share=_share(g["scoring"]["validation_share"]),
        max_amount=float(g["validation"]["max_amount"]),
        systematic_share=float(g["validation"]["systematic_share"]),
        csv_url=g["sources"]["historical_csv"],
        label_horizon=_as_utc(g["labels"]["horizon"]),
        min_positives=int(g["labels"]["min_positives"]),
        datasets_root=_datasets_root(g["datasets"]["root"]),
        dataset_family=str(g["datasets"]["family"]),
        dataset_schema_version=str(g["datasets"]["schema_version"]),
        split_rule=str(g["datasets"]["split_rule"]),
        api_url=g["sources"]["realtime_api"],
        identifying_fields=tuple(g["personal_data"]["identifying_fields"]),
        pepper=os.environ.get("ML4FD_PEPPER", ""),
        pg_host=os.environ.get("POSTGRES_HOST", "postgres"),
        pg_db=os.environ["POSTGRES_DB"],
        pg_user=os.environ["POSTGRES_USER"],
        pg_password=os.environ["POSTGRES_PASSWORD"],
    )
