"""The four queries of the morning report, read as text.

Block: B3, B4.
Lot: B3-P1, B4-L0.
Competency: B3-C5.
Covers: B3-EE04, B3-EI04, B3-EI08, B3-EC17.

PostgreSQL is not in the authoring environment, so these read the SQL. A
weaker test than running it, and still the one that catches what matters here:
a window taken from the clock instead of from the DAG's interval, a query that
reaches into the raw zone, an order that is not total, or an alert losing the
sort P0 decided on.

The queries themselves were executed against a real PostgreSQL 16 while P1 was
written -- all four under the fd_report role, with the aggregates of
daily_volumes checked to close against a direct count on the same window. What
cannot be re-run here is checked here.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "sql" / "reports"

QUERIES = ("daily_volumes.sql", "daily_frauds.sql", "daily_verdicts.sql",
           "daily_pending.sql")

# Every parameter each query may take, and no other: a name that drifts between
# the SQL and ml4fd/report.py fails with "no value found for parameter", which
# names the parameter but not the file.
PARAMETERS = {
    "daily_volumes.sql":  {"window_start", "window_end", "zone"},
    "daily_frauds.sql":   {"window_start", "window_end"},
    "daily_verdicts.sql": {"window_start", "window_end"},
    "daily_pending.sql":  {"window_start", "window_end", "knowledge_at"},
}


def _text(name: str) -> str:
    return (REPORTS / name).read_text(encoding="utf-8")


def _code(name: str) -> str:
    """The SQL without its comment lines.

    EVERY TEST BELOW READS THIS AND NOT THE FILE. The comments of these queries
    explain at length why now() must never appear, so a test grepping the whole
    file for "now()" finds the explanation and fails. That mistake was made
    three times in S3 before this helper existed: a test that searches text
    cannot tell code from the prose about it.
    """
    return "\n".join(line for line in _text(name).splitlines()
                     if not line.lstrip().startswith("--"))


@pytest.mark.parametrize("name", QUERIES)
def test_the_query_exists(name):
    assert (REPORTS / name).is_file()


@pytest.mark.parametrize("name", QUERIES)
def test_the_window_never_comes_from_the_clock(name):
    """A report replayed next month must produce the document of the day it
    covers. now() would make two runs of the same day differ, and comparing two
    mornings is precisely what an analyst does."""
    code = _code(name)
    for forbidden in ("now()", "current_timestamp", "current_date", "localtimestamp"):
        assert forbidden not in code.lower(), forbidden


@pytest.mark.parametrize("name", QUERIES)
def test_the_window_is_a_parameter(name):
    found = set(re.findall(r"%\((\w+)\)s", _code(name)))
    assert found == PARAMETERS[name]
    assert {"window_start", "window_end"} <= found


@pytest.mark.parametrize("name", QUERIES)
def test_the_window_is_half_open(name):
    """>= start AND < end. A closed upper bound would count a transaction at
    midnight sharp in two consecutive reports, and the two would not add up to
    the month."""
    code = _code(name)
    assert ">= %(window_start)s" in code
    assert "<  %(window_end)s" in code or "< %(window_end)s" in code
    assert "<= %(window_end)s" not in code


@pytest.mark.parametrize("name", QUERIES)
def test_the_query_never_touches_the_raw_zone(name):
    """fd_report has no access to it, so a query that needed the raw zone would
    fail at run time -- but it would also mean the exploited layer was not
    enough, and the guarantee of EC17 rests on it being enough. The raw payload
    still carries names, addresses and card numbers."""
    assert "fd_raw_events" not in _code(name)


@pytest.mark.parametrize("name", QUERIES)
def test_the_query_is_one_statement(name):
    """ml4fd.db.query_file runs a single statement. Two would silently execute
    only the first."""
    code = _code(name).strip()
    assert code.count(";") == 1, code.count(";")
    assert code.endswith(";")


@pytest.mark.parametrize("name", QUERIES)
def test_no_identifying_column_is_named(name):
    """fd_transactions holds none, so this cannot happen by accident -- and
    that is an argument. This is a measurement."""
    code = _code(name)
    for column in ("first", "last", "street", "city", "zip", "lat", "long",
                   "job", "dob", "gender", "cc_num", "city_pop"):
        assert not re.search(r"\b%s\b" % column, code), column


# --- what P0 decided, and the queries must carry -------------------------------

def test_the_alerts_are_ordered_by_expected_loss():
    """THE DECISION OF P0. An analyst who reviews the first 38 of a list of 60
    must know what he sorted on, so the order is written in the document -- and
    it has to be the order the query actually returns."""
    code = _code("daily_frauds.sql")
    assert "round(a.score * t.amt, 2)" in code
    assert "expected_loss" in code
    assert "ORDER BY expected_loss DESC" in code


@pytest.mark.parametrize("name", QUERIES)
def test_the_order_is_total(name):
    """Two runs on the same window must produce the same document to the
    character (criterion of P2). A query returning several rows without a
    tie-break can reorder them between two runs and break it on ties alone."""
    code = _code(name)
    if "ORDER BY" not in code:
        # A single-row aggregate has nothing to order. Check that is what it is.
        assert "GROUP BY" not in code, name
        return
    order = code[code.index("ORDER BY"):]
    assert order.count(",") >= 1, name


def test_the_alert_carries_the_cardholder_habit_and_its_ratio():
    """The decision of P0: "this payment is 16.9 times the cardholder's habit"
    reads, where two raw columns ask the reader to divide forty times a
    morning."""
    code = _code("daily_frauds.sql")
    for column in ("f_tx_count", "f_avg_amount", "times_usual"):
        assert column in code


def test_an_unknown_habit_reads_as_unknown_and_not_as_zero():
    """f_avg_amount is null for a cardholder's FIRST transaction. A coalesce to
    zero would divide by it, or report a ratio of zero as if it were measured."""
    code = _code("daily_frauds.sql")
    assert "nullif(t.f_avg_amount, 0)" in code
    assert "coalesce(t.f_avg_amount" not in code


# --- the two axes of time, which is the whole process -------------------------

def test_the_verdicts_are_filtered_on_the_knowledge_axis():
    """THE CENTRAL TRAP OF S4. A section titled "yesterday's verdicts" that
    filtered on txn_ts would show zero for ever, since no transaction of
    yesterday has a verdict. It filters on label_ts, and returns the payment
    dates it covers so the reader cannot mistake one axis for the other."""
    code = _code("daily_verdicts.sql")
    assert "g.label_ts >= %(window_start)s" in code
    assert "g.label_ts <  %(window_end)s" in code
    assert "t.txn_ts >= %(window_start)s" not in code
    assert "payments_from" in code and "payments_to" in code


def test_the_other_sections_are_filtered_on_the_payment_axis():
    for name in ("daily_volumes.sql", "daily_frauds.sql", "daily_pending.sql"):
        code = _code(name)
        assert re.search(r"\w*\.?txn_ts >= %\(window_start\)s", code), name
        assert "label_ts >= %(window_start)s" not in code, name


def test_a_day_without_fraud_gives_an_unknown_recall_and_not_a_zero():
    """"No fraud to catch" and "caught none of the frauds" are opposite
    findings and must not render alike."""
    code = _code("daily_verdicts.sql")
    assert "nullif(count(*) FILTER (WHERE label), 0)" in code
    assert "nullif(count(*) FILTER (WHERE alerted), 0)" in code


def test_the_base_rate_travels_with_the_ratios():
    """A recall compared across two different base rates compares nothing: the
    report puts the observed base rate beside the one M5 measured."""
    assert "base_rate_observed" in _code("daily_verdicts.sql")


def test_a_verdict_is_known_by_its_due_date_and_not_by_a_row_existing():
    """The row is written the night of the payment with a label_ts in the
    FUTURE, so its existence proves nothing -- the filter is the withholding."""
    code = _code("daily_pending.sql")
    assert "g.label_ts > %(knowledge_at)s" in code
    assert "g.txn_id IS NULL" in code          # and the row may be missing too


def test_a_missing_verdict_row_is_reported_as_a_finding():
    """dag_ingest_verdicts runs nightly, so by report time every transaction of
    yesterday should carry its row with a due date. A non-zero count means that
    DAG did not run, and the report cannot then say when the verdict falls."""
    assert "without_verdict_row" in _code("daily_pending.sql")


# --- the aggregates of section 1 cannot disagree with their own total ---------

def test_the_volumes_come_from_one_scan():
    """Three separate queries would let a category sum differ from the headline
    figure -- a defect a reader finds before a test does. GROUPING SETS makes
    the total and the breakdowns come from the same scan, so they cannot."""
    code = _code("daily_volumes.sql")
    assert "GROUPING SETS" in code
    assert "grouping(category)" in code
    assert "grouping(local_hour)" in code


def test_the_hour_bucket_is_local_and_the_zone_is_declared():
    """A report whose hours were UTC would place the night fraud peak -- 84 of
    every 100 frauds between 22h and 03h, measured in M5 -- two hours off in
    summer and one in winter."""
    code = _code("daily_volumes.sql")
    assert "AT TIME ZONE %(zone)s" in code
    assert "'Europe/Paris'" not in code         # declared, not written here


def test_the_zone_is_declared_in_governance():
    from ml4fd import config
    assert config.load().report_timezone == "Europe/Paris"
