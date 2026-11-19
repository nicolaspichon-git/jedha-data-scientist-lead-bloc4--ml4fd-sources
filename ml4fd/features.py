"""History features: drives both computation modes and proves they agree.

Covers: EE07
Lot: L4
Competency: C2, C5

One definition, expressed twice, and a test that proves the equality:
  - real time : sql/functions/fd_client_features.sql   (per row)
  - batch     : sql/backfill/features_window.sql    (window)
  - proof     : sql/checks/reconcile_features.sql   (stratified)

WHY TWO EXPRESSIONS RATHER THAN ONE. D1 and the process guide propose a single
SQL expression invoked both ways. That does not hold at scale: with 924
cardholders and 601 transactions each on average, a correlated call costs about
601^2 / 2 ~ 180,600 row visits per cardholder, so ~167 million in all, and as
many times over as there are features. The window sorts once: ~11 million,
shared by every feature. The guarantee therefore comes from the reconciliation,
not from shared code.

WHY THE RECONCILIATION IS STRATIFIED. Measured on the source: 48 rows out of
555,719 sit on a timestamp tie, and ties are exactly what tells ROWS from
RANGE ... EXCLUDE GROUP. A random sample of 1,000 rows would contain one with
probability 0.083 -- so a random test would pass a wrong backfill nine times out
of ten. The sample is built to hold every tied row and every cardholder's first
transaction.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ml4fd import db

if TYPE_CHECKING:
    import psycopg

log = logging.getLogger(__name__)

BACKFILL_SQL = "backfill/features_window.sql"
RECONCILE_SQL = "checks/reconcile_features.sql"
COVERAGE_SQL = "checks/reconcile_coverage.sql"
APPLY_BATCH_SQL = "features/apply_batch.sql"
IS_LATEST_SQL = "checks/batch_is_latest.sql"


def backfill(conn: "psycopg.Connection") -> int:
    """Backfill the features by window function. Returns the number of rows.

    Recomputes the whole table and not just the current batch: a history feature
    depends on every earlier transaction of the cardholder, so a new batch moves
    the values of rows that were already stored. Deterministic given the data,
    therefore replayable (EC23).

    Covers: EE07
    Lot: L4
    Competency: C2
    """
    rows = db.execute_file(conn, BACKFILL_SQL)
    log.info("backfill: %d row(s) updated", rows)
    return rows


def batch_is_latest(conn: "psycopg.Connection", batch_id) -> tuple[int, object]:
    """How many stored transactions are later than this batch's earliest row.

    Must be 0 for a batch-scoped feature update to be sound. The check exists
    because the alternative is an assumption: every stream transaction carries
    current_time and is therefore later than everything stored -- true today,
    and nothing in the schema enforces it.

    Covers: EE07
    Lot: N3
    Competency: C5
    """
    rows = db.query_file(conn, IS_LATEST_SQL, batch_id=batch_id)
    return (rows[0][0], rows[0][1]) if rows else (0, None)


def apply_batch(conn: "psycopg.Connection", batch_id) -> int:
    """Compute the features of one batch by the per-row function.

    REFUSES when the batch is not the latest. A batch-scoped update would then
    leave stale features on the rows that follow it, and no error would say so
    -- the values would simply be wrong, which is the failure mode this whole
    lot exists to avoid.

    Covers: EE07, EI03
    Lot: N3
    Competency: C2, C5
    """
    later, offender = batch_is_latest(conn, batch_id)
    if later:
        raise ValueError(
            "%d stored transaction(s) are later than this batch, the earliest "
            "at %s. A batch-scoped feature update would leave their features "
            "stale: run features.backfill() instead, which recomputes the "
            "whole table." % (later, offender))

    rows = db.execute_file(conn, APPLY_BATCH_SQL, batch_id=batch_id)
    log.info("batch %s: features applied to %d row(s) (per-row mode)",
             batch_id, rows)
    return rows


def compute_one(conn: "psycopg.Connection", client_key: str, txn_ts
                ) -> tuple[int, object]:
    """Compute one transaction's features by calling the SQL function.

    This is the real-time path, used here to confront the two modes row by row.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT tx_count, avg_amount FROM fd_client_features(%s, %s)",
                    (client_key, txn_ts))
        return cur.fetchone()


def coverage(conn: "psycopg.Connection") -> dict:
    """Size of each reconciliation stratum."""
    total, ties, firsts, without = db.query_file(conn, COVERAGE_SQL)[0]
    return {"total_rows": total, "tie_rows": ties, "first_rows": firsts,
            "rows_without_features": without}


def reconcile(conn: "psycopg.Connection") -> list[tuple]:
    """Confront both modes over the stratified sample. MUST return [].

    Covers: EE07
    Lot: L4
    Competency: C5
    """
    rows = db.query_file(conn, RECONCILE_SQL)
    if rows:
        log.error("%d divergence(s) between the two modes", len(rows))
        for row in rows[:10]:
            log.error("  %s", row)
        # The stale stratum does not report a disagreement between the two
        # modes: those rows hold no stored value at all, so naming it a mode
        # divergence would send the reader looking for a bug in fd_client_features
        # instead of at a batch that never went through apply_batch.
        stale = sum(1 for row in rows if row[0] == "stale")
        if stale:
            log.error("dont %d ligne(s) sans features stockees : "
                      "un lot a echappe a features.apply_batch, "
                      "remede features.backfill()", stale)
    else:
        log.info("the two computation modes agree over the whole sample")
    return rows
