"""Writing the real verdict, and withholding it. No simulation.

Block: B3, B4.
Lot: B3-N6, B4-L0.
Competency: B3-C3, B3-C4, B3-C5.
Covers: B3-EE03, B3-EI03, B3-EI12, B3-EC14, B3-EC17, B3-EC23.

WHY THIS LOT IS THE MOST SENSITIVE OF S3. The API delivers is_fraud in the SAME
payload as the features -- N0 measured it: 21 columns, is_fraud present as a
str. So the label sits one line of code away from the scoring, and no amount of
care in a code review proves it did not get there.

WHAT IS NOT DONE HERE: nothing is simulated. The verdict is real. It is written
immediately with label_ts = txn_ts + delay, exactly as S1 does, and what holds
it back is that every consumer filters on label_ts <= cut_off. That filter is
the mechanism M3's training set and L4's reconciliation already rest on, so the
stream inherits a guarantee that has been exercised on 555,719 rows rather than
getting a new one written for it.

THE FOUR GUARANTEES, and which of them this module provides:

  1. to_transaction does not know is_fraud. Already true -- ml4fd/transform.py
     contains neither the word is_fraud nor the word label. Not provided here;
     ASSERTED by tests/test_leak.py, which reads the module.
  2. fd_transactions has no column to hold it. True since S1, and the strongest
     of the four: a leak would need a schema migration, so a visible decision.
  3. The scoring does not import this module. ml4fd/score.py imports db and
     registry and nothing else. Asserted by import analysis, not by re-reading.
  4. The verdict write shares no task with the scoring. THIS is what the module
     provides: it is called by its own daily DAG, dag_ingest_verdicts, and
     tools/ingest_once.py no longer writes a verdict at all.

The fourth is defence in depth and not the primary mechanism -- the filter is.
It earns its place because a change to the scoring then cannot reach the verdict
code even by accident, which a shared task cannot promise.
"""
from __future__ import annotations

import datetime as dt
import logging

from ml4fd import checks, db, raw, stream, truth

log = logging.getLogger(__name__)

AWAITING_SQL = "verdicts/batches_awaiting_verdict.sql"
KNOWN_SQL = "verdicts/known_transactions.sql"
WITHHELD_SQL = "verdicts/withheld.sql"


def batches_awaiting(conn, source: str = "current-transactions") -> list[tuple]:
    """Stream batches holding transactions with no verdict row at all.

    A REPAIR question and not a due-date question: the delay is carried by
    label_ts, not by postponing the write. A batch appears here because its
    verdict write failed or because it was ingested before the write was
    separated out -- so the daily run is self-healing, like N5's alert raising.
    """
    return db.query_file(conn, AWAITING_SQL, source=source)


def withheld(conn, source: str = "current-transactions") -> dict:
    """The blind band, counted. A guarantee nobody can count is a promise."""
    row = db.query_file(conn, WITHHELD_SQL, source=source)
    if not row or row[0][0] is None:
        return {"verdicts": 0, "not_yet_due": 0, "due": 0, "min_delay": None,
                "max_delay": None, "distinct_delays": 0}
    verdicts, not_due, due, low, high, distinct = row[0]
    return {"verdicts": verdicts, "not_yet_due": not_due, "due": due,
            "min_delay": low, "max_delay": high, "distinct_delays": distinct}


def lines_without_transaction(conn, records,
                              profile=checks.STREAM_PROFILE) -> set[int]:
    """Line numbers whose transaction is absent, so the FK would refuse them.

    records are (line_no, RECORD) pairs -- rows, not raw-zone payloads. The
    envelope is unwrapped once by write_for_batch, by stream.row_of, and
    everything downstream sees the shape the ingestion saw.

    fd_ground_truth.txn_id REFERENCES fd_transactions(txn_id). A line that went
    to the quarantine never became a transaction, and writing its verdict would
    be refused -- taking the whole batch's write with it.

    The ingestion knew which lines it had rejected and skipped them. This daily
    run starts again from the raw zone and knows nothing of that, which is the
    price of the separation. Filtering on EXISTENCE rather than on a remembered
    reject list pays that price once and covers every other reason a transaction
    may be absent.

    The identifiers come from profile.event_id, the SAME derivation
    transform.to_transaction used: two derivations would make every verdict of
    the stream miss its transaction.
    """
    # RECORDS, already unwrapped by the caller. Unwrapping again here would
    # work -- row_of is tolerant -- and would leave this function accepting
    # either shape, which is the ambiguity that produced the defect in the
    # first place: one place unwrapped, the other did not.
    pairs = [(profile.event_id(record), line_no)
             for line_no, record in records]
    if not pairs:
        return set()
    rows = db.query_file(conn, KNOWN_SQL,
                         txn_ids=[txn_id for txn_id, _ in pairs])
    known = {row[0] for row in rows}
    return {line_no for txn_id, line_no in pairs if txn_id not in known}


def write_for_batch(conn, batch_id, delay: dt.timedelta,
                    source: str = "current-transactions") -> int:
    """Write the verdicts of one raw batch. Returns the number inserted.

    THE RECORDS ARE RE-READ FROM THE RAW ZONE, not carried over from the
    ingestion. That is what makes the separation real: this function shares no
    object, no task and no transaction with the scoring -- it starts again from
    the only durable, append-only copy of what the API said.

    The reader is materialised into a list before the write. raw.read_batch is
    a SERVER-SIDE cursor, and a COPY cannot share a connection with a FETCH
    without deadlocking -- the caution truth.load_ground_truth states, and the
    deadlock L3 already paid for once.
    """
    reader = raw.read_batch(conn, batch_id)
    try:
        # Unwrapped HERE, once, so everything downstream -- the existence
        # filter and truth.load_ground_truth alike -- sees a record of the
        # shape the ingestion saw. Unwrapping in one place and not the other is
        # exactly the defect this line replaces.
        records = [(line_no, stream.row_of(payload))
                   for line_no, payload in reader]
    finally:
        reader.close()

    if not records:
        return 0

    skip = lines_without_transaction(conn, records)
    if skip:
        log.info("lot %s : %d ligne(s) sans transaction, ecartee(s) (mises en "
                 "quarantaine a l'ingestion)", batch_id, len(skip))

    written = truth.load_ground_truth(
        conn, records, delay, source, skip=skip,
        profile=checks.STREAM_PROFILE)
    log.info("lot %s : %d verdict(s) ecrit(s) sur %d ligne(s) brutes",
             batch_id, written, len(records))
    return written


def write_due(conn, delay: dt.timedelta,
              source: str = "current-transactions") -> dict:
    """Write every missing verdict of the stream. Returns a report.

    Block: B3, B4.
    Lot: B3-N6.
    Competency: B3-C3.
    Covers: B3-EI03, B3-EC23.
    """
    awaiting = batches_awaiting(conn, source)
    report = {"batches": len(awaiting), "written": 0, "rows": 0}
    for batch_id, transactions, without, earliest, latest in awaiting:
        report["rows"] += without
        report["written"] += write_for_batch(conn, batch_id, delay, source)
    return report


def render(report: dict, band: dict, delay: dt.timedelta) -> str:
    """One line per run, and the band stated rather than assumed."""
    return ("%d lot(s) sans verdict, %d ligne(s) concernee(s), %d verdict(s) "
            "ecrit(s) ; bande aveugle : %d verdict(s) non encore echus sur %d, "
            "delai declare %s"
            % (report["batches"], report["rows"], report["written"],
               band["not_yet_due"], band["verdicts"], delay))
