"""The six rulings that make both feature modes agree -- EE07.

The proof that the two SQL expressions agree needs a database and lives in
sql/checks/reconcile_features.sql, run by `.\\fd-manager.ps1 check`. What runs here,
anywhere and in milliseconds, is the layer underneath: the semantics the SQL
encodes, and the wording of the two files that encode it.

Two reference implementations are written in Python on purpose: the strict-bound
one the rulings prescribe, and the ROWS-style one they forbid. A test then shows
the two differ ON TIES AND NOWHERE ELSE -- which is why the reconciliation has to
be stratified, and why a random sample proves nothing.
"""
import re
from decimal import Decimal
from pathlib import Path

SQL = Path(__file__).resolve().parent.parent / "sql"

# A cardholder with two transactions sharing a timestamp. Measured on the
# source: 24 such pairs, 48 rows out of 555,719.
TIED = [
    {"txn_id": "a", "client_key": "k1", "txn_ts": 100, "amt": Decimal("10.00")},
    {"txn_id": "b", "client_key": "k1", "txn_ts": 200, "amt": Decimal("20.00")},
    {"txn_id": "c", "client_key": "k1", "txn_ts": 200, "amt": Decimal("30.00")},
    {"txn_id": "d", "client_key": "k1", "txn_ts": 300, "amt": Decimal("40.00")},
]
UNTIED = [dict(r, txn_ts=t) for r, t in zip(TIED, (100, 200, 250, 300))]


def _sql(relative: str) -> str:
    """The text of a .sql file, read from the repository."""
    return (SQL / relative).read_text(encoding="utf-8")


def _without_comments(text: str) -> str:
    """The statements alone: a `;` inside a comment is not a statement."""
    return "\n".join(line for line in text.split("\n")
                      if not line.strip().startswith("--"))


def strict_bound(rows, row):
    """What the rulings prescribe: txn_ts STRICTLY earlier (rulings 1 and 2)."""
    earlier = [r["amt"] for r in rows
               if r["client_key"] == row["client_key"] and r["txn_ts"] < row["txn_ts"]]
    if not earlier:
        return 0, None                      # ruling 6
    total = sum(earlier)
    return len(earlier), (total / len(earlier)).quantize(Decimal("0.01"))


def rows_style(rows, row):
    """What ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING does instead.

    Sort order decides, so a tie that happens to sort earlier is counted.
    """
    ordered = sorted((r for r in rows if r["client_key"] == row["client_key"]),
                     key=lambda r: (r["txn_ts"], r["txn_id"]))
    index = next(i for i, r in enumerate(ordered) if r["txn_id"] == row["txn_id"])
    earlier = [r["amt"] for r in ordered[:index]]
    if not earlier:
        return 0, None
    total = sum(earlier)
    return len(earlier), (total / len(earlier)).quantize(Decimal("0.01"))


# --- ruling 1: a transaction never counts itself --------------------------

def test_a_transaction_never_counts_itself():
    assert strict_bound(TIED, TIED[0]) == (0, None)
    assert strict_bound(TIED, TIED[3])[0] == 3


# --- ruling 6: the first transaction of a cardholder ----------------------

def test_a_first_transaction_counts_zero_and_averages_nothing():
    """Zero and NULL, not zero and zero: an average over nothing is not 0.00."""
    count, average = strict_bound(TIED, TIED[0])
    assert count == 0
    assert average is None


# --- ruling 2: ties, and why the sample must be stratified ----------------

def test_the_two_readings_differ_on_a_tie():
    """THE divergence the reconciliation exists to catch."""
    tied_row = TIED[2]                      # shares txn_ts=200 with TIED[1]
    assert strict_bound(TIED, tied_row) == (1, Decimal("10.00"))
    assert rows_style(TIED, tied_row) == (2, Decimal("15.00"))
    assert strict_bound(TIED, tied_row) != rows_style(TIED, tied_row)


def test_the_two_readings_agree_everywhere_else():
    """Which is exactly why a random sample proves nothing: on this source only
    48 rows out of 555,719 sit on a tie, so a 1,000-row random sample misses
    them about 92 % of the time."""
    for row in UNTIED:
        assert strict_bound(UNTIED, row) == rows_style(UNTIED, row), row["txn_id"]


def test_both_tied_rows_see_the_same_history():
    """Two transactions sharing a timestamp exclude each other, symmetrically."""
    assert strict_bound(TIED, TIED[1]) == strict_bound(TIED, TIED[2])


# --- ruling 3: the population is the cardholder's, not everyone's ---------

def test_another_cardholder_does_not_enter_the_count():
    mixed = TIED + [{"txn_id": "z", "client_key": "k2", "txn_ts": 150,
                     "amt": Decimal("999.00")}]
    assert strict_bound(mixed, TIED[3]) == strict_bound(TIED, TIED[3])


# --- ruling 4: the type is exact, never a float ---------------------------

def test_the_average_is_an_exact_decimal():
    count, average = strict_bound(TIED, TIED[3])
    assert count == 3
    assert average == Decimal("20.00")          # (10 + 20 + 30) / 3
    assert isinstance(average, Decimal)


# --- the rulings as written in the SQL ------------------------------------
# Crude but effective: these guard the exact wording that carries rulings 1
# and 2. Replacing the window frame with ROWS, or the strict bound with <=,
# would pass every test above and break the agreement on ties.

def test_the_backfill_uses_range_and_excludes_the_peer_group():
    sql = (SQL / "backfill" / "features_window.sql").read_text(encoding="utf-8")
    body = "\n".join(line for line in sql.split("\n") if not line.strip().startswith("--"))
    assert "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in body
    assert "EXCLUDE GROUP" in body
    assert "ROWS BETWEEN" not in body


def test_the_real_time_function_uses_a_strict_bound():
    sql = (SQL / "functions" / "fd_client_features.sql").read_text(encoding="utf-8")
    body = "\n".join(line for line in sql.split("\n") if not line.strip().startswith("--"))
    assert "txn_ts < p_ts" in body
    assert "txn_ts <= p_ts" not in body


def test_both_expressions_read_the_same_table():
    """One definition expressed twice: a divergence of table would be silent."""
    for relative in (("backfill", "features_window.sql"),
                     ("functions", "fd_client_features.sql")):
        body = (SQL / relative[0] / relative[1]).read_text(encoding="utf-8")
        assert "fd_transactions" in body


def test_the_reconciliation_is_stratified_not_random():
    sql = (SQL / "checks" / "reconcile_features.sql").read_text(encoding="utf-8")
    body = "\n".join(line for line in sql.split("\n") if not line.strip().startswith("--"))
    assert "tied" in body and "firsts" in body
    assert "IS DISTINCT FROM" in body


# --- the batch-scoped update, and the condition it rests on --------------

def test_the_batch_update_is_a_separate_function_from_the_backfill():
    """Not a parameter on backfill, deliberately. The backfill recomputes the
    whole table because a new batch MOVES the features of rows already stored;
    the batch update is only sound when the batch is later than everything
    else. Two different soundness conditions, two functions."""
    from ml4fd import features

    assert hasattr(features, "apply_batch")
    assert hasattr(features, "backfill")
    assert features.apply_batch is not features.backfill


def test_the_batch_update_calls_the_declared_function_not_its_own_expression():
    """THE definition of the features in real-time mode is
    sql/functions/fd_client_features.sql. A second expression in apply_batch.sql
    would be a third mode to keep in step with the other two."""
    text = _sql("features/apply_batch.sql")
    assert "fd_client_features(" in text
    assert "LATERAL" in text
    # no reimplementation of the aggregate
    assert "avg(" not in text.lower() and "count(" not in text.lower()


def test_the_batch_update_is_a_single_statement():
    """db.execute_file runs one statement: psycopg3 refuses several."""
    body = _without_comments(_sql("features/apply_batch.sql"))
    assert body.count(";") == 1


def test_the_latest_batch_check_counts_rows_outside_the_batch():
    """It must ignore the batch's own rows, or it would always find offenders."""
    body = _without_comments(_sql("checks/batch_is_latest.sql"))
    assert "IS DISTINCT FROM" in body
    assert "min(b.txn_ts)" in body


def test_the_condition_is_checked_not_documented():
    """A comment would not stop a wrong update. apply_batch raises."""
    import inspect

    from ml4fd import features

    source = inspect.getsource(features.apply_batch)
    assert "raise ValueError" in source
    assert "batch_is_latest" in source


def test_the_refusal_names_the_remedy():
    import inspect

    from ml4fd import features

    assert "features.backfill()" in inspect.getsource(features.apply_batch)


def test_the_reconciliation_has_a_stratum_for_rows_without_features():
    """7 rows carried no features on 2026-11-16 -- stream transactions ingested
    before N3 existed -- and a 1,000-row random sample out of 555,730 had a
    1.25 % chance of catching any of them. Same reasoning as the tie stratum:
    a sample nearly blind to the rows most likely to be wrong proves nothing."""
    body = _without_comments(_sql("checks/reconcile_features.sql"))
    assert "stale AS (" in body
    assert "f_tx_count IS NULL" in body


def test_the_four_strata_are_all_in_the_sample():
    body = _without_comments(_sql("checks/reconcile_features.sql"))
    union = body[body.index("t.txn_id IN (SELECT"):]
    for stratum in ("tied", "stale", "firsts", "rest"):
        assert stratum in union, stratum


def test_the_strata_do_not_overlap():
    """rest must exclude all three named strata, or a row would be compared
    twice and a divergence counted twice.

    THE SPELLING CHANGED ON 2026-11-19 AND THE REASON IS THE POINT. This test
    required `NOT IN (SELECT txn_id FROM <stratum>)`. On the 1.2 M-row corpus
    that form stopped being an anti-join at all: the hash outgrew work_mem and
    PostgreSQL fell back to rescanning the SubPlan once per row, for a plan
    cost of 4.6e11. NOT EXISTS is a reliable anti-join, and it is safe here
    because txn_id is the primary key and therefore never NULL -- which is the
    single case where the two forms disagree.

    So the test now requires the EXCLUSION, keyed on txn_id, and no longer a
    spelling. A test that pins the shape of a query forbids its optimisation,
    and this one did: it survived the rewrite by failing on it.
    """
    body = _without_comments(_sql("checks/reconcile_features.sql"))
    rest = body[body.index("rest AS ("):body.index("sample AS MATERIALIZED (")]
    for stratum in ("tied", "firsts", "stale"):
        tied_on_txn_id = re.search(
            r"NOT\s+EXISTS\s*\(\s*SELECT\s+1\s+FROM\s+" + stratum
            + r"\s+(\w+)\s+WHERE\s+\1\.txn_id\s*=\s*t\.txn_id\s*\)",
            rest)
        assert tied_on_txn_id, stratum


def test_a_first_transaction_is_not_a_stale_one():
    """Ruling 6 gives a first transaction f_tx_count = 0, never NULL. Confusing
    the two would put 924 legitimate rows in the stale stratum."""
    body = _without_comments(_sql("checks/reconcile_features.sql"))
    stale = body[body.index("stale AS ("):body.index("firsts AS (")]
    assert "f_tx_count IS NULL" in stale
    assert "f_tx_count = 0" not in stale
