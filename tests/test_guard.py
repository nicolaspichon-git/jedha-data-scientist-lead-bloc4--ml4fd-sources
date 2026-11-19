"""The gate S2 passes through. The decision is a pure function, so it is tested
without a database -- which is the point of separating inspect from verdict."""
import datetime as dt

import pytest

from ml4fd.guard import WindowReport, verdict

UTC = dt.timezone.utc
HORIZON = dt.datetime(2020, 12, 22, 23, 13, 39, tzinfo=UTC)
CUT_OFF = dt.datetime(2020, 11, 30, tzinfo=UTC)


def report(**changes):
    base = dict(
        runs_total=1, runs_success=1, runs_failed=0,
        last_success=dt.datetime(2026, 11, 15, tzinfo=UTC), last_failure=None,
        rows_in_window=100_000, labelled_rows=100_000, labelled_positives=400,
        cut_off=CUT_OFF, label_horizon=HORIZON,
        verdict_delay=dt.timedelta(days=30))
    base.update(changes)
    return WindowReport(**base)


def test_a_healthy_window_is_accepted():
    assert verdict(report(), min_positives=1) == []


# --- ingestion health -----------------------------------------------------

def test_no_run_at_all_is_refused():
    """A dataset built over data nobody confirmed arriving is not a dataset."""
    reasons = verdict(report(runs_total=0, runs_success=0, last_success=None),
                      min_positives=1)
    assert any("no ingestion run" in r for r in reasons)


def test_runs_that_all_failed_are_refused():
    reasons = verdict(report(runs_total=3, runs_success=0, runs_failed=3,
                             last_success=None,
                             last_failure=dt.datetime(2026, 11, 15, tzinfo=UTC)),
                      min_positives=1)
    assert any("none successful" in r for r in reasons)


def test_a_failure_after_the_last_success_is_refused():
    """The window may be truncated, and NO training metric would show it."""
    reasons = verdict(
        report(runs_total=2, runs_success=1, runs_failed=1,
               last_success=dt.datetime(2026, 11, 15, tzinfo=UTC),
               last_failure=dt.datetime(2026, 11, 16, tzinfo=UTC)),
        min_positives=1)
    assert any("most recent ingestion run FAILED" in r for r in reasons)


def test_a_failure_before_the_last_success_is_tolerated():
    """A failure that was retried and succeeded is history, not a defect."""
    assert verdict(
        report(runs_total=2, runs_success=1, runs_failed=1,
               last_success=dt.datetime(2026, 11, 16, tzinfo=UTC),
               last_failure=dt.datetime(2026, 11, 15, tzinfo=UTC)),
        min_positives=1) == []


# --- labelling health -----------------------------------------------------

def test_a_cut_off_past_the_horizon_is_refused():
    reasons = verdict(report(cut_off=dt.datetime(2020, 12, 31, tzinfo=UTC)),
                      min_positives=1)
    assert any("beyond the labelling horizon" in r for r in reasons)
    assert any("false negatives" in r for r in reasons)


def test_a_window_without_a_single_positive_is_refused():
    """The last nine days of this source are exactly that case."""
    reasons = verdict(report(labelled_positives=0), min_positives=1)
    assert any("teaches the model that this period is never fraudulent" in r
               for r in reasons)


def test_too_few_positives_is_refused():
    reasons = verdict(report(labelled_positives=3), min_positives=10)
    assert any("3 labelled positive(s)" in r for r in reasons)


def test_a_window_with_no_verdict_known_is_refused():
    reasons = verdict(report(labelled_rows=0, labelled_positives=0),
                      min_positives=1)
    assert any("no verdict known at the cut-off" in r for r in reasons)


def test_the_blind_band_refusal_names_the_date_verdicts_stop_at():
    """The real failure of 2026-11-15: a cut-off of 2020-11-30 with a 30-day
    delay knows verdicts only to 2020-10-31, so a test window opening on
    2020-11-01 is entirely blind. The message must say so, with the date."""
    reasons = verdict(report(labelled_rows=0, labelled_positives=0),
                      min_positives=1)
    blind = next(r for r in reasons if "blind band" in r)
    assert "2020-10-31" in blind          # verdicts known only to here
    assert "2020-11-30" in blind          # the cut-off asked for
    assert "30 day" in blind


def test_verdicts_known_until_is_the_cut_off_less_the_delay():
    assert report().verdicts_known_until == dt.datetime(
        2020, 10, 31, tzinfo=UTC)


def test_an_empty_window_is_refused():
    reasons = verdict(report(rows_in_window=0, labelled_rows=0,
                             labelled_positives=0), min_positives=1)
    assert any("no transaction in this window" in r for r in reasons)


# --- several faults at once -----------------------------------------------

def test_every_reason_is_reported_not_only_the_first():
    """Knowing a window is both unlabelled AND incomplete beats learning it
    twice, one run apart."""
    reasons = verdict(
        report(runs_total=0, runs_success=0, last_success=None,
               labelled_positives=0,
               cut_off=dt.datetime(2020, 12, 31, tzinfo=UTC)),
        min_positives=1)
    assert len(reasons) == 3


def test_the_log_view_carries_the_base_rate():
    """A precision figure cannot be read without it."""
    rendered = report().as_log()
    assert "base_rate" in rendered
    assert rendered["positives"] == 400


def test_the_log_view_survives_an_empty_window():
    assert report(labelled_rows=0).as_log()["base_rate"] == "n/a"
