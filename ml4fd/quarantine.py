"""What becomes of invalid records.

Covers: EC22
Lot: L2
Competency: C5

A rejected record is neither dropped nor corrected: it is kept with its reason,
in a store that F07.3 can report a breakdown over. One line can hold several
rows here -- one per reason code -- which is why sql/checks/counters_close.sql
counts DISTINCT line_no rather than rows.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterable

from ml4fd.checks import Reject

if TYPE_CHECKING:
    import psycopg

log = logging.getLogger(__name__)

CHUNK = 5_000

INSERT = """
INSERT INTO fd_quarantine
    (batch_id, line_no, reason_class, reason_code, reason_detail, payload)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (batch_id, line_no, reason_code) DO NOTHING
"""


def write(conn: "psycopg.Connection", batch_id,
          items: Iterable[tuple[int, dict, list[Reject]]]) -> tuple[int, int]:
    """Write the rejects with their coded reason.

    Returns (rejected_lines, rejected_rows). The two differ as soon as one line
    fails several checks, and it is the first that enters the counter closure.

    Consumed in chunks: a wholly invalid batch would otherwise hold 555,719
    payloads in memory at once. ON CONFLICT DO NOTHING keeps a replay idempotent
    (EC23).
    """
    from psycopg.types.json import Jsonb

    lines = rows = 0
    buffer: list[tuple] = []

    def flush() -> None:
        nonlocal buffer
        if not buffer:
            return
        with conn.cursor() as cur:
            cur.executemany(INSERT, buffer)
        buffer = []

    for line_no, payload, rejects in items:
        if not rejects:
            continue
        lines += 1
        for r in rejects:
            buffer.append((batch_id, line_no, r.reason_class, r.reason_code,
                           r.reason_detail, Jsonb(payload)))
            rows += 1
        if len(buffer) >= CHUNK:
            flush()
    flush()

    if lines:
        log.info("quarantine: %d line(s) rejected, %d reason row(s)", lines, rows)
    else:
        log.info("quarantine: no rejected line in this batch")
    return lines, rows


def rejected_lines(conn: "psycopg.Connection", batch_id) -> set[int]:
    """The line numbers of a batch that went to the quarantine.

    Read back from the table rather than carried across a task boundary: the
    DAG of N7 hands work from one task to the next through the database, and a
    set of line numbers travelling as JSON would be a second record of
    something the quarantine already holds.

    DISTINCT in the query, because the quarantine holds one row per line AND
    per reason -- UNIQUE (batch_id, line_no, reason_code) -- so a line with two
    defects appears twice.

    Covers: EC14, EC23
    Lot: N7
    Competency: C3
    """
    from ml4fd import db

    rows = db.query_file(conn, "checks/quarantined_lines.sql",
                         batch_id=batch_id)
    return {line_no for (line_no,) in rows}


def breakdown(conn: "psycopg.Connection", batch_id) -> list[tuple]:
    """Rejects per class and per reason. Feeds F07.3.

    Covers: EC15
    Lot: L2
    Competency: C4
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT reason_class, reason_code, count(*) AS rows, "
            "       count(DISTINCT line_no) AS lines "
            "FROM   fd_quarantine WHERE batch_id = %s "
            "GROUP BY reason_class, reason_code ORDER BY rows DESC",
            (batch_id,))
        return cur.fetchall()
