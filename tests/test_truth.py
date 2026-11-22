"""Ground truth: the verdict, its knowledge date, and the blind band.

Block: B3, B4.
Lot: B4-L0.
Competency: ?.
Covers: ?.
"""
import datetime as dt

import pytest

from ml4fd import checks, truth

TS = checks.parse_ts("2020-06-21 12:14:53")
NOMINAL = dt.timedelta(days=30)
DEMO = dt.timedelta(seconds=300)

GOOD = {
    "trans_date_trans_time": "2020-06-21 12:14:53",
    "trans_num": "a1b2c3d4e5f6",
    "is_fraud": "1",
}


def altered(**changes):
    record = dict(GOOD)
    record.update(changes)
    return record


# --- EI02: the verdict is dated by when it became known -------------------

def test_the_verdict_is_dated_after_the_transaction():
    assert truth.label_ts_for(TS, NOMINAL) == TS + NOMINAL
    assert truth.label_ts_for(TS, NOMINAL) > TS


def test_a_zero_delay_dates_the_verdict_with_the_transaction():
    """Allowed: the CHECK constraint asks for >=, not >."""
    assert truth.label_ts_for(TS, dt.timedelta(0)) == TS


def test_a_negative_delay_is_refused():
    """It would date the verdict before the transaction -- the CHECK would fail
    on insert, far from the mistake."""
    with pytest.raises(ValueError, match="negative delay"):
        truth.label_ts_for(TS, dt.timedelta(days=-1))


def test_a_naive_timestamp_is_refused():
    """Adding a delay to a wall clock of unknown zone yields an unknown date."""
    with pytest.raises(ValueError, match="timezone-aware"):
        truth.label_ts_for(dt.datetime(2020, 6, 21, 12, 14, 53), NOMINAL)


def test_the_delay_is_the_width_of_the_blind_band():
    """A consumer filtering on label_ts <= cut_off sees nothing in that band."""
    cut_off = TS + NOMINAL - dt.timedelta(seconds=1)
    assert truth.label_ts_for(TS, NOMINAL) > cut_off


# --- the label itself -----------------------------------------------------

def test_the_two_values_of_the_source_are_read():
    """Measured on the source: is_fraud holds exactly '0' and '1'."""
    assert truth.to_bool("1") is True
    assert truth.to_bool("0") is False


def test_other_renderings_are_accepted():
    for value in ("true", "True", " t ", "YES"):
        assert truth.to_bool(value) is True
    for value in ("false", "F", "no", " 0 "):
        assert truth.to_bool(value) is False


def test_an_unreadable_label_is_refused_not_guessed():
    """A label read wrongly poisons the training set without leaving a trace."""
    for value in ("2", "", "maybe", None):
        with pytest.raises(ValueError, match="unreadable label"):
            truth.to_bool(value)


# --- the row handed to the database ---------------------------------------

def test_the_row_holds_exactly_the_schema_columns():
    row = truth.to_ground_truth(GOOD, NOMINAL, "batch-1")
    assert set(row) == set(truth.OUTPUT_COLUMNS)


def test_the_row_carries_the_transaction_date_as_well():
    """Copied on purpose: a CHECK cannot reference another table, so without it
    "label_ts is later than txn_ts" would only hold by code."""
    row = truth.to_ground_truth(GOOD, NOMINAL, "batch-1")
    assert row["txn_ts"] == TS
    assert row["label_ts"] == TS + NOMINAL


def test_the_check_constraint_would_hold():
    row = truth.to_ground_truth(GOOD, NOMINAL, "batch-1")
    assert row["label_ts"] >= row["txn_ts"]


def test_no_identity_reaches_the_verdict_table():
    row = truth.to_ground_truth(
        altered(cc_num="2291163933867244", first="Jeff", dob="1968-03-19"),
        NOMINAL, "batch-1")
    for field in ("cc_num", "first", "dob", "client_key"):
        assert field not in row


def test_an_unreadable_timestamp_is_refused():
    with pytest.raises(ValueError, match="unreadable timestamp"):
        truth.to_ground_truth(altered(trans_date_trans_time="21/06/2020"),
                              NOMINAL, "batch-1")


def test_the_demo_delay_makes_a_verdict_fall_due_during_a_recording():
    """F13.1: the durations that outlast the demonstration are parameterised."""
    row = truth.to_ground_truth(GOOD, DEMO, "batch-1")
    assert row["label_ts"] - row["txn_ts"] == dt.timedelta(minutes=5)
