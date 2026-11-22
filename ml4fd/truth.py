"""Ground truth: the verdict and the date it became known.

Block: B3, B4.
Lot: B3-L5, B4-L0.
Competency: B3-C2.
Covers: B3-EI01, B3-EI02.

The verdict does not live on the transaction. That separation is structural --
fd_transactions has no column for a label -- and not a convention, because a
model trained on a verdict it could not have known at scoring time reports a
recall it will never reach in production (F02.3, EI01).

label_ts = txn_ts + the delay in force, and the delay comes from the single
verdicts.mode switch in governance.yml. For the historical load the choice has no
observable effect -- the transactions date from 2020, every verdict is long
past due -- but it must be THE SAME constant S3 uses, which is why neither
module holds a figure of its own.

A BLIND BAND follows: in the delay before any cut-off, the verdicts are not yet
known. A consumer filters on label_ts <= cut_off and therefore sees nothing in
that band. That is the point, not a defect.

HYPOTHESIS, stated because the schema settles it: txn_id is the PRIMARY KEY of
fd_ground_truth, so a transaction has exactly ONE verdict, for ever. A reversed
chargeback would need an UPDATE, which fd_ingest cannot perform. Should
corrections become necessary, this table has to become a log -- several rows per
transaction, the latest label_ts winning -- and that is a schema change, not a
configuration one.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import TYPE_CHECKING, Iterable

from ml4fd import checks, db

if TYPE_CHECKING:
    import psycopg

log = logging.getLogger(__name__)

OUTPUT_COLUMNS = ("txn_id", "txn_ts", "label", "label_ts", "source")
TRUE_VALUES = ("1", "true", "t", "yes", "y")
FALSE_VALUES = ("0", "false", "f", "no", "n")


def to_bool(value: str) -> bool:
    """Read the source's label.

    Measured on the source: is_fraud holds exactly '0' and '1'. The other
    spellings are accepted so that a differently-rendered export does not fail
    silently, but anything else is refused rather than guessed -- a label read
    wrongly poisons the training set without leaving a trace.
    """
    text = (value or "").strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    raise ValueError("unreadable label: %r" % value)


def label_ts_for(txn_ts: dt.datetime, delay: dt.timedelta) -> dt.datetime:
    """The date the verdict becomes known: the transaction's date plus the delay.

    Block: B3, B4.
    Lot: B3-L5.
    Competency: B3-C2.
    Covers: B3-EI02.
    """
    if txn_ts.tzinfo is None:
        raise ValueError("txn_ts must be timezone-aware; see checks.SOURCE_TZ")
    if delay < dt.timedelta(0):
        raise ValueError("a negative delay would date the verdict before the "
                         "transaction, which the CHECK constraint forbids")
    return txn_ts + delay


def to_ground_truth(record: dict[str, str], delay: dt.timedelta,
                    source: str,
                    profile: checks.SourceProfile = checks.CSV_PROFILE
                    ) -> dict:
    """Build the verdict row for one valid record.

    parse_ts comes from ml4fd.checks: how a source timestamp is read, time zone
    included, has a single definition.

    Block: B3, B4.
    Lot: B3-L5.
    Competency: B3-C2.
    Covers: B3-EI01, B3-EI02.
    """
    # Through the profile, and it must agree with to_transaction's: the FK from
    # fd_ground_truth to fd_transactions is on txn_id, so two derivations would
    # break every verdict of the stream.
    txn_ts = profile.timestamp(record)
    if txn_ts is None:
        raise ValueError("unreadable timestamp: %r in %s"
                         % (record.get(profile.time_field), profile.time_field))
    return {
        "txn_id": profile.event_id(record),
        "txn_ts": txn_ts,
        "label": to_bool(record.get("is_fraud", "")),
        "label_ts": label_ts_for(txn_ts, delay),
        "source": source,
    }


def load_ground_truth(write_conn: "psycopg.Connection",
                      records: Iterable[tuple[int, dict]], delay: dt.timedelta,
                      source: str, skip: set[int],
                      profile: checks.SourceProfile = checks.CSV_PROFILE
                      ) -> int:
    """Write the verdicts. Returns the number inserted.

    COPY into a TEMP table, then INSERT ... ON CONFLICT DO NOTHING, as in L3:
    speed and the idempotency EC23 requires, together.

    CAUTION: `records` must NOT come from a server-side cursor on write_conn --
    a COPY and a FETCH cannot share one connection without deadlocking. See
    transform.load_transactions for the detail.

    Quarantined lines are skipped: fd_ground_truth references
    fd_transactions(txn_id), and a rejected record never reached that table.

    Block: B3, B4.
    Lot: B3-L5.
    Competency: B3-C2, B3-C5.
    Covers: B3-EC23.
    """
    columns = ", ".join(OUTPUT_COLUMNS)
    with write_conn.cursor() as cur:
        # Callable any number of times in one transaction: write_due() calls
        # it once per batch (db.staging_table, the defect of 2026-11-17).
        db.staging_table(cur, "tmp_fd_ground_truth", "fd_ground_truth")

        written = 0
        with cur.copy("COPY tmp_fd_ground_truth (%s) FROM STDIN" % columns) as copy:
            copy.set_types(["text", "timestamptz", "boolean", "timestamptz", "text"])
            for line_no, record in records:
                if line_no in skip:
                    continue
                row = to_ground_truth(record, delay, source, profile)
                copy.write_row(tuple(row[c] for c in OUTPUT_COLUMNS))
                written += 1
                if written % 100_000 == 0:
                    log.info("  %d verdicts copied", written)

        cur.execute(
            "INSERT INTO fd_ground_truth (%s) SELECT %s FROM tmp_fd_ground_truth "
            "ON CONFLICT (txn_id) DO NOTHING" % (columns, columns))
        inserted = cur.rowcount

    log.info("%d verdicts prepared, %d inserted (%d already present)",
             written, inserted, written - inserted)
    return inserted
