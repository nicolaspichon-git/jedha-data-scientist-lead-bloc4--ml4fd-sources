"""The raw-zone listing counts lines, not lines times runs.

Covers: EC12, EC23
Lot: L7
Competency: C4

WHAT HAPPENED. `fd-manager batches` joined fd_raw_events to fd_pipeline_runs on
batch_id and counted afterwards. batch_id is not a key of fd_pipeline_runs: a
batch loaded once and replayed twice carries three runs, the GROUP BY folded
them into one group, and count(*) returned 1,667,157 for a batch holding
555,719. The figure was a join multiplication, and it was about to be read as
three copies of the historical CSV sitting in the raw zone.

Proven on PostgreSQL 16 before and after: 3,000 then 1,000 for 1,000 rows and
three runs.

The invariant below is blunt on purpose: no aggregate may stand after a join in
this file. A count taken over joined rows is not a count of anything.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LISTING = ROOT / "sql" / "checks" / "raw_batches.sql"
MANAGER = ROOT / "fd-manager.ps1"


def _code(path: Path) -> str:
    """The file without its comments: a rule must hold in the SQL, and the
    comments here talk about counts and joins at length."""
    return "\n".join(line.split("--")[0]
                     for line in path.read_text(encoding="utf-8").splitlines())


def test_every_count_is_taken_before_any_join():
    code = _code(LISTING).upper()
    first_join = code.find("JOIN")
    assert first_join > 0, "the listing joins nothing: has it been rewritten?"
    for m in re.finditer(r"\bCOUNT\s*\(", code):
        assert m.start() < first_join, (
            "an aggregate at offset %d stands after the first JOIN (offset %d): "
            "it counts rows of the join, not of the table" % (m.start(), first_join))


def test_each_counting_scope_reads_a_single_table():
    """The guard above holds only because the CTEs are single-table. A join
    moved up inside one of them would pass it and lie again."""
    code = _code(LISTING)
    head = code[:code.upper().find("JOIN")]
    tables = set(re.findall(r"FROM\s+(fd_[a-z_]+)", head))
    assert tables == {"fd_raw_events", "fd_pipeline_runs",
                      "fd_transactions"}, tables
    assert "JOIN" not in head.upper()


def test_the_one_to_many_is_shown_and_not_hidden():
    """A batch with three runs must say three, since that is the fact that made
    the count wrong in the first place."""
    assert re.search(r"count\(\*\)\s*AS executions", _code(LISTING))
    assert "AS execs" in _code(LISTING)


def test_the_exploited_layer_is_shown_per_batch():
    """The fact that decides whether a batch may be forgotten, and the one that
    was missing on 2026-11-17: a FAILED load held all 555,719 transactions,
    while the SUCCEEDED one held none of its own -- so the listing's `state`
    column pointed at the wrong batch."""
    code = _code(LISTING)
    assert "fd_transactions" in code
    assert "AS exploitee" in code


def test_a_batch_without_any_run_is_still_listed():
    """Orphan raw rows are exactly what someone runs this listing to find, so
    the runs must be joined to the batches and never the reverse."""
    code = _code(LISTING).upper()
    assert "LEFT JOIN" in code
    assert code.index("FROM") < code.index("LEFT JOIN")
    assert re.search(r"FROM\s+lots\s+l", _code(LISTING), re.I)


def test_the_manager_runs_the_file_and_does_not_spell_the_query():
    """Inline in the PowerShell script, this query was unversioned and untested
    -- which is how it stayed wrong."""
    text = MANAGER.read_text(encoding="utf-8")
    assert "/work/sql/checks/raw_batches.sql" in text
    assert "LEFT JOIN fd_pipeline_runs p ON p.batch_id = r.batch_id" not in text
