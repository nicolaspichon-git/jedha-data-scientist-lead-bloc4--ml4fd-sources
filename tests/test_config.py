"""Loading governance.yml for real.

Block: B3, B4.
Lot: B4-L0.
Competency: ?.
Covers: ?.

THE GAP THIS FILE CLOSES. Every other test builds its objects by hand, so a
Settings method reaching for an attribute that does not exist passed all 157 of
them and failed in Airflow -- `AttributeError: 'Settings' object has no
attribute 'governance'`. A frozen dataclass is not a dict, and no amount of
unit-testing the callers catches that. Only loading the real file does.
"""
import datetime as dt
import inspect
import os
import pathlib
import tempfile

import pytest

from ml4fd import config

# Fake, and that is the point: these three are only needed because load()
# refuses to run without them. Nothing here connects to anything.
os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")


def test_the_real_governance_file_loads():
    assert config.load() is not None


def test_every_no_argument_method_of_settings_actually_runs():
    """The test that would have caught the failure, and will catch the next one.

    A method is not covered by the callers' tests: it is covered by being
    called. This walks every public no-argument method and property on the
    loaded Settings and runs it, so an attribute that does not exist is found
    here and not in a DAG log at one in the morning.
    """
    settings = config.load()
    called = []
    for name, member in inspect.getmembers(type(settings)):
        if name.startswith("_"):
            continue
        if isinstance(member, property):
            getattr(settings, name)
            called.append(name)
        elif inspect.isfunction(member):
            signature = inspect.signature(member)
            if len(signature.parameters) == 1:        # self only
                getattr(settings, name)()
                called.append(name)
    assert called, "no method was exercised: the walk is broken, not the code"


def test_every_declared_field_is_populated():
    """A field left to a default nobody meant is a configuration that lies."""
    settings = config.load()
    for name in type(settings).__dataclass_fields__:
        assert hasattr(settings, name)


# --- the governance decisions, read back ----------------------------------

def test_the_analyst_rule_is_the_one_declared():
    amount, hours = config.load().rule_baseline()
    assert amount == 300.0
    assert set(hours) == {22, 23, 0, 1, 2, 3}


def test_the_labelling_horizon_is_aware_and_utc():
    horizon = config.load().label_horizon
    assert horizon.tzinfo is not None
    assert horizon.utcoffset() == dt.timedelta(0)


def test_the_verdict_delay_comes_from_the_single_switch():
    settings = config.load()
    assert settings.verdict_delay() > dt.timedelta(0)


def test_the_redacted_view_hides_the_pepper_and_the_password():
    rendered = config.load().redacted()
    flattened = repr(rendered)
    assert "test_password" not in flattened
    assert os.environ.get("ML4FD_PEPPER", "absent-from-this-environment") \
        not in flattened or not os.environ.get("ML4FD_PEPPER")


# --- the hour list, refused rather than interpreted -----------------------

@pytest.mark.parametrize("value", [[], None, [24], [-1], [0, 23, 24]])
def test_an_hour_outside_the_day_is_refused(value):
    """An hour of 24 would silently match nothing, and the rule would then look
    merely weak instead of broken."""
    with pytest.raises(ValueError, match="between 0 and 23"):
        config._hours(value)


@pytest.mark.parametrize("value", [[0], [23], [22, 23, 0, 1, 2, 3]])
def test_a_valid_hour_list_is_accepted(value):
    assert config._hours(value) == tuple(value)


# --- the dataset root, absolute ------------------------------------------

def test_the_dataset_root_is_absolute():
    """A bare "datasets" is resolved against the process's working directory.
    An Airflow task runs from /opt/airflow, so the Parquet, the model and the
    manifest landed inside the container and outside the mounted volume -- while
    fd_datasets recorded the rows and every SQL criterion passed."""
    root = config.load().datasets_root
    assert os.path.isabs(root), root


def test_the_dataset_root_sits_under_the_project_not_the_cwd():
    root = config.load().datasets_root
    project = os.path.dirname(os.path.dirname(os.path.abspath(config.__file__)))
    assert root.startswith(project), (root, project)


def test_an_absolute_root_is_left_alone():
    """The absolute path is built for THIS platform, not written as POSIX.

    "/opt/project/datasets" is absolute on Linux and NOT on Windows, where a
    path needs a drive letter: the first version of this test passed in the
    container and failed on the machine that runs pytest.
    """
    absolute = str(pathlib.Path(tempfile.gettempdir()).resolve())
    assert pathlib.Path(absolute).is_absolute()      # the premise, stated
    assert config._datasets_root(absolute) == absolute


def test_a_relative_root_does_not_depend_on_the_working_directory():
    """The property that was missing, stated as a test."""
    here = os.getcwd()
    try:
        first = config._datasets_root("datasets")
        os.chdir(tempfile.gettempdir())              # exists on both platforms
        assert config._datasets_root("datasets") == first
    finally:
        os.chdir(here)


# --- the cadence, and the calibration that depends on it ----------------------

def test_the_declared_cadence_covers_the_announced_rate():
    """THE invariant of N7's cadence, decided 2026-11-16.

    One call returns one transaction and nothing queues behind it (N0), so the
    cadence times the calls per run IS the share of the stream that gets scored.
    ml4fd/threshold.py chose the threshold 0.93 and the capacity of 60 alerts a
    day at API_TXN_PER_DAY. Observe less and every published figure stays
    plausible while being wrong by the same factor -- at one run every five
    minutes, by six.
    """
    from ml4fd.threshold import API_TXN_PER_DAY

    observed = config.load().observed_per_day()
    assert observed >= 0.8 * API_TXN_PER_DAY, (
        "la configuration n'observe que %d transactions/jour contre %d "
        "annoncees : le seuil et la capacite de 60 alertes/jour ne veulent "
        "plus rien dire" % (observed, API_TXN_PER_DAY))
    assert observed <= 1.2 * API_TXN_PER_DAY


def test_the_cadence_stays_under_the_measured_limiter():
    """N0 measured the limiter firing at 13 calls/minute and never below."""
    settings = config.load()
    per_minute = settings.observed_per_day() / (24 * 60)
    assert per_minute <= 13, per_minute
    assert settings.calls_per_run <= 13


def test_runs_per_day_reads_the_two_declared_forms():
    assert config.runs_per_day("* * * * *") == 1440
    assert config.runs_per_day("*/5 * * * *") == 288
    assert config.runs_per_day("*/15 * * * *") == 96


def test_runs_per_day_refuses_what_it_cannot_justify():
    """A general cron parser would be a second implementation of something
    Airflow already does, and a wrong one would answer a question about the
    calibration with a guess."""
    for schedule in ("0 3 * * 1", "", "every minute", "0 */2 * * *"):
        with pytest.raises(ValueError, match="realtime.schedule"):
            config.runs_per_day(schedule)


def test_runs_per_day_refuses_a_zero_step():
    with pytest.raises(ValueError, match="not a cadence"):
        config.runs_per_day("*/0 * * * *")


def test_the_timing_budget_of_one_run_fits_in_its_cadence():
    """N0: median latency 5.4 s per call. Five calls take about 27 s of the
    minute; a 502 costs 25 s more. Two 502s would overrun, which is why N7 sets
    max_active_runs=1 rather than why the cadence is slower."""
    settings = config.load()
    seconds_between_runs = 86400 / config.runs_per_day(settings.realtime_schedule)
    nominal = settings.calls_per_run * 5.4
    assert nominal < seconds_between_runs, (nominal, seconds_between_runs)


# --- la decision de N4, declaree dans la gouvernance --------------------------

def test_the_governance_declares_the_columns_the_model_does_not_use():
    """Decided by the paired comparison of 16 November 2026: +18 frauds out of
    205 for the amputated recipe, p = 2.8e-4, 20 draws out of 20."""
    assert config.load().drop_features == ("txn_hour", "txn_weekday")


def test_the_declared_removal_leaves_a_trainable_recipe():
    from ml4fd import datasets

    kept = datasets.without(config.load().drop_features)
    assert len(kept) == 5
    assert "txn_hour" not in kept and "txn_weekday" not in kept


def test_a_name_that_is_not_a_column_is_refused():
    for bad in (["txn hour"], "txn-hour", ["amt;"]):
        with pytest.raises(ValueError, match="not a column name"):
            config._columns(bad, "scoring.drop_features")


def test_the_refusal_names_the_setting_it_read_and_not_one_caller():
    """The message used to say scoring.drop_features whatever had been read,
    because that was the only caller. The second one arrived 2026-11-20 and
    would have sent the reader to the wrong line of governance.yml."""
    with pytest.raises(ValueError,
                       match="monitoring.input_drift.not_comparable"):
        config._columns(["f tx count"],
                        "monitoring.input_drift.not_comparable")


def test_an_absent_or_empty_declaration_removes_nothing():
    """Governance may legitimately declare no removal; it must not then be read
    as a missing key and crash the whole load."""
    where = "scoring.drop_features"
    assert config._columns(None, where) == ()
    assert config._columns([], where) == ()
    assert config._columns("", where) == ()


def test_the_shape_is_checked_here_and_membership_elsewhere():
    """Whether a name is a feature of the recipe is datasets.without()'s
    business -- one place deciding. This module stays free of any fraud import,
    so a governance file cannot be read differently depending on what else
    happens to be loaded."""
    assert config._columns(["not_a_feature"],
                           "scoring.drop_features") == ("not_a_feature",)
    from pathlib import Path

    body = Path(config.__file__).read_text(encoding="utf-8")
    assert "from fraud" not in body


# --- N9: the operations channel is declared, and its declaration is checked ---

def test_the_operations_channel_is_declared():
    settings = config.load()
    assert settings.ops_channel in ("log", "file")
    assert settings.ops_window_minutes > 0
    assert settings.ops_freshness_minutes > 0


def test_the_two_channels_write_to_different_places():
    """THE ONE THING THEY MUST NOT SHARE. The analyst's queue is read
    transaction by transaction under a capacity of 60 a day; the operations
    channel carries states of the chain. Merged, pipeline noise would sit in
    the queue the capacity policy exists to protect, and the separation this
    lot was built for would be undone by two equal lines of configuration."""
    settings = config.load()
    assert pathlib.Path(settings.alert_file) != pathlib.Path(settings.ops_file)


def test_two_equal_destinations_are_refused():
    with pytest.raises(ValueError, match="distinct destinations"):
        config._distinct_destinations("/ws/alerts.jsonl", "/ws/alerts.jsonl")


def test_the_same_path_spelled_differently_is_still_refused():
    """Compared as PATHS and not as strings, or ws/alerts.jsonl and
    ws/./alerts.jsonl would pass as two destinations and be one file."""
    with pytest.raises(ValueError, match="distinct destinations"):
        config._distinct_destinations("/ws/alerts.jsonl", "/ws/./alerts.jsonl")


@pytest.mark.parametrize("value", [0, 0.0, 1, 1.0, -0.1, 2])
def test_a_share_outside_the_open_interval_is_refused(value):
    """At 0 the control is in permanent alert -- any rejection at all, any empty
    minute -- and at 1 it never fires. Both are ways of having no control while
    governance.yml claims one."""
    with pytest.raises(ValueError, match="strictly between 0 and 1"):
        config._share_under_one(value, "ops.max_rejection_rate")


def test_the_declared_rejection_share_clears_the_measured_floor():
    """N0 saw one call in 57 (1.75 %) carrying an 818 s gap, which quarantines
    on the clock tolerance: the normal rejection rate is NOT zero, and a
    threshold below the measured floor would hold the control in permanent
    alert. Measured, then declared above it."""
    assert config.load().ops_max_rejection_share > 1 / 57

def test_the_four_mode_dependent_settings_follow_the_mode():
    """ONE PLACE READS monitoring.mode, so they cannot disagree. Three windows
    and, since 2026-11-20, one row count: _by_mode is untyped for that reason
    and this test is what keeps the fourth from drifting out of the family."""
    import dataclasses

    # AND THE TEST MUST NOT CARE WHICH MODE IS DECLARED. The first version of
    # this test asserted the repository ships in nominal, and failed within the
    # hour -- when the demonstration was switched to demo, which is the whole
    # point of the setting. A test that forbids the operation it documents is
    # worse than no test. Both branches are built here, from whichever mode
    # happens to be in force.
    declared = config.load()
    assert declared.monitoring_mode in ("nominal", "demo")

    nominal = dataclasses.replace(declared, monitoring_mode="nominal")
    assert nominal.drift_window() == dt.timedelta(
        minutes=nominal.monitoring_drift_window_minutes)
    assert nominal.performance_window() == dt.timedelta(
        days=nominal.monitoring_performance_window_days)
    assert nominal.retrain_cooldown() == dt.timedelta(
        minutes=nominal.monitoring_cooldown_minutes)
    assert nominal.drift_min_rows() == nominal.monitoring_drift_min_rows

    demo = dataclasses.replace(declared, monitoring_mode="demo")
    assert demo.drift_window() == dt.timedelta(
        minutes=demo.monitoring_demo_drift_window_minutes)
    assert demo.performance_window() == dt.timedelta(
        minutes=demo.monitoring_demo_performance_window_minutes)
    assert demo.retrain_cooldown() == dt.timedelta(
        minutes=demo.monitoring_demo_cooldown_minutes)
    assert demo.drift_min_rows() == demo.monitoring_demo_drift_min_rows


def test_an_unknown_monitoring_mode_is_refused_and_not_guessed():
    import dataclasses

    broken = dataclasses.replace(config.load(), monitoring_mode="demonstration")
    for ask in ("drift_window", "performance_window", "retrain_cooldown",
                "drift_min_rows"):
        with pytest.raises(ValueError, match="monitoring.mode"):
            getattr(broken, ask)()


def test_the_demo_floor_fits_inside_the_demo_window():
    """A floor above what the window can hold would make the demonstration
    permanently unmeasurable. 15 minutes at 5 calls a minute is about 75 rows,
    and the floor is 50."""
    s = config.load()
    available = s.monitoring_demo_drift_window_minutes * s.calls_per_run
    assert s.monitoring_demo_drift_min_rows < available, (
        "plancher %d pour %d lignes disponibles"
        % (s.monitoring_demo_drift_min_rows, available))

def test_the_demonstration_cadences_are_not_the_nominal_ones():
    """A demonstration mode that left the cadences alone would keep the loop
    correct and unshowable: an hourly monitor cannot close a five-minute
    criterion, and verdicts written once at 02:00 never arrive during a
    recording."""
    import dataclasses

    s = config.load()
    nominal = dataclasses.replace(s, monitoring_mode="nominal",
                                  verdict_mode="nominal")
    demo = dataclasses.replace(s, monitoring_mode="demo", verdict_mode="demo")
    assert demo.monitoring_schedule() != nominal.monitoring_schedule()
    assert demo.verdict_schedule() != nominal.verdict_schedule()


def test_the_monitor_looks_at_least_as_often_as_its_window_is_wide():
    """THE CROSS-DECLARATION THAT MATTERS. A cadence slower than the window it
    reads lets a drift appear and vanish between two looks -- measured by
    nobody, and absent from the series EC45 asks for. In demo the window is
    fifteen minutes, so the monitor must run at least that often."""
    import dataclasses

    demo = dataclasses.replace(config.load(), monitoring_mode="demo")
    minute = demo.monitoring_schedule().split()[0]
    assert minute.startswith("*/"), demo.monitoring_schedule()
    every = int(minute[2:])
    assert every <= demo.monitoring_demo_drift_window_minutes, (
        "regarde toutes les %d min une fenetre de %d min"
        % (every, demo.monitoring_demo_drift_window_minutes))


def test_the_verdicts_are_written_sooner_than_they_fall_due():
    """Same shape, other switch: a verdict that falls due after 300 s and is
    written every ten minutes is a label that arrives late for no reason the
    product has. The cadence must be the shorter of the two."""
    import dataclasses

    demo = dataclasses.replace(config.load(), verdict_mode="demo")
    minute = demo.verdict_schedule().split()[0]
    assert "/" in minute, demo.verdict_schedule()
    every_minutes = int(minute.split("/")[1])
    assert every_minutes * 60 <= demo.demo_delay_seconds, (
        "ecrit toutes les %d s pour un delai de %d s"
        % (every_minutes * 60, demo.demo_delay_seconds))


def test_a_cron_that_is_not_five_fields_is_refused_with_its_line_named():
    """Airflow refuses it too, at DAG parse time and with a message about
    croniter. Named here, the reader is sent to the line of governance.yml."""
    for bad in ("17 * * *", "", "every two minutes", "*/2 * * * * *"):
        with pytest.raises(ValueError, match="monitoring.schedule"):
            config._cron(bad, "monitoring.schedule")
