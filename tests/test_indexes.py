"""The indexes fd_transactions is read by, held against the queries that read it.

Covers: EC23
Lot: L5
Competency: C3, C5

WHY THIS FILE EXISTS. On 21/11/2026 `fd-manager accept` took two minutes, and
the cause was not a check: fd_transactions carried no index on batch_id, while
sql/scoring/rows_to_score.sql and sql/features/apply_batch.sql both filter on
it. PostgreSQL does not index a foreign key on its own, so every dag_ingest run
scanned 1,300,406 rows twice to find the five of its own batch -- 60 ms against
0.07 ms, measured -- and s4_acceptance.sql's planner chose a nested loop that
rescanned the table once per batch: 161 seconds.

A LITERAL LIST WOULD NOT HAVE CAUGHT IT. The index was missing for months while
three queries needed it, and no list said so. So the rule is read from the
queries themselves: a column that the SQL filters fd_transactions on must be
the leading column of a declared index. A query added tomorrow with no index
fails here rather than on a stopwatch.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DDL = (ROOT / "sql" / "ddl" / "030_transactions.sql").read_text(encoding="utf-8")
SQL_DIRS = ("scoring", "features", "frauds", "dashboard", "checks", "datasets",
            "monitoring", "reports", "scoring_service", "verdicts", "purge")

# Columns a query may filter fd_transactions on without an index of its own.
# ONE ENTRY, AND NOT A LIST WRITTEN IN ADVANCE: measured across sql/, the rule
# below finds exactly two filtered columns -- batch_id and txn_ts -- and both
# are indexed. A longer tolerance list would be configuration nobody uses,
# reading as though every entry had been weighed.
NO_INDEX_NEEDED = {
    # The primary key, which has its own index by definition.
    "txn_id",
}


def _code(text: str) -> str:
    return "\n".join(line.split("--")[0] for line in text.splitlines())


def _declared_leading_columns() -> set:
    """The first column of every index declared on fd_transactions.

    THE LEADING COLUMN AND NOT ANY COLUMN: an index on (client_key, txn_ts)
    serves a filter on client_key and does nothing for one on txn_ts alone.
    """
    leading = set()
    for columns in re.findall(
            r"CREATE\s+INDEX[^;]*?ON\s+fd_transactions\s*\(([^)]*)\)",
            _code(DDL), re.I | re.S):
        leading.add(columns.split(",")[0].strip())
    return leading


def _files():
    for folder in SQL_DIRS:
        directory = ROOT / "sql" / folder
        if directory.is_dir():
            for path in sorted(directory.glob("*.sql")):
                # Files prefixed with _ are diagnostics, outside every chain.
                if not path.name.startswith("_"):
                    yield path


def _filtered_columns(code: str) -> set:
    """Columns compared to a PARAMETER on an alias of fd_transactions.

    A parameter is what makes a filter selective: `t.batch_id = %(batch_id)s`
    asks for a few rows out of a million, where `t.txn_ts < l.test_end` inside a
    join is a scan either way. Only the first needs an index.
    """
    aliases = set(re.findall(r"fd_transactions\s+(?:AS\s+)?(\w+)", code, re.I))
    found = set()
    for alias in aliases:
        for column in re.findall(
                r"\b%s\.(\w+)\s*(?:=|<|>|<=|>=)\s*%%\(\w+\)s" % re.escape(alias),
                code):
            found.add(column)
    return found


def test_the_batch_is_indexed():
    """The one the measurement of 21/11 is about, named explicitly: the generic
    rule below would also pass if every query stopped filtering by batch, and
    the index would then be dropped for the wrong reason."""
    assert "batch_id" in _declared_leading_columns()


def test_every_selective_filter_on_the_transactions_has_an_index():
    declared = _declared_leading_columns()
    missing = {}
    for path in _files():
        for column in _filtered_columns(_code(path.read_text(encoding="utf-8"))):
            if column not in declared and column not in NO_INDEX_NEEDED:
                missing.setdefault(column, []).append(path.name)
    assert not missing, (
        "colonne(s) filtrees sur fd_transactions sans index en tete : %s"
        % {k: sorted(v) for k, v in missing.items()})


def test_the_rule_would_have_caught_the_defect_of_21_11():
    """The test that tests this test. With batch_id removed from the declared
    indexes, rows_to_score.sql must be reported -- otherwise the rule above
    passes for a reason that has nothing to do with the queries."""
    code = _code((ROOT / "sql" / "scoring" / "rows_to_score.sql").read_text(
        encoding="utf-8"))
    assert "batch_id" in _filtered_columns(code)


def test_the_features_path_is_covered_too():
    """apply_batch.sql filters the same way, in an UPDATE: the scan happened
    twice per run, not once."""
    code = _code((ROOT / "sql" / "features" / "apply_batch.sql").read_text(
        encoding="utf-8"))
    assert "batch_id" in _filtered_columns(code)


def test_the_existing_indexes_are_still_declared():
    """Without them the features scan the whole table per cardholder, which is
    what 030_transactions.sql says they exist for."""
    declared = _declared_leading_columns()
    assert "client_key" in declared
    assert "txn_ts" in declared
