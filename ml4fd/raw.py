"""Writing into and reading back from the raw zone.

Block: B3, B4.
Lot: B3-L1, B4-L0.
Competency: B3-C2, B3-C5.
Covers: B3-EC23, B3-EC24.

The raw zone is the seam of the whole system: everything downstream can be
rebuilt from it without querying the source again (EC24), and it is append-only
by revocation, not by convention (see sql/ddl/060_roles_grants.sql).
"""
from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Iterable, Iterator

if TYPE_CHECKING:          # the annotations are strings, so this stays type-only
    import psycopg

log = logging.getLogger(__name__)

NAMESPACE = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")   # uuid.NAMESPACE_URL
PROGRESS_EVERY = 100_000
HISTORY_LOADED_SQL = "checks/history_already_loaded.sql"


class HistoryAlreadyLoaded(RuntimeError):
    """The raw zone already holds the historical CSV. Named, with the command
    that takes an existing batch further, rather than a fifth copy."""


def batch_id_for(run_id: str) -> uuid.UUID:
    """Derive a batch_id from the Airflow run_id, deterministically.

    A random uuid4 would hand a retried task a fresh batch_id, and the same
    lines would be ingested twice. Derived from the run_id, a retry reuses the
    same identifier, so the pre-check below and the UNIQUE (batch_id, line_no)
    constraint both make the retry a no-op. That is EC23 in practice.
    """
    return uuid.uuid5(NAMESPACE, "fraud-detection:raw:" + run_id)


def history_in_raw(conn: "psycopg.Connection", dag_id: str) -> list[tuple]:
    """The historical batches the raw zone holds, whatever their run's state.

    Reads sql/checks/history_already_loaded.sql -- THE definition. Returns
    (state, run_id, batch_id, n_rows, ended_at), successes first.

    Block: B3, B4.
    Lot: B4-L0.
    Competency: ?.
    Covers: B3-EC23, B3-EC24.
    """
    from ml4fd import db
    return db.query_file(conn, HISTORY_LOADED_SQL, dag_id=dag_id)


def refuse_second_load(conn: "psycopg.Connection", dag_id: str,
                       replay: str | None = None) -> None:
    """Raise unless this run has any business writing the raw zone.

    A retried TASK is already harmless: batch_id_for() derives the identifier
    from the run_id, so write_batch() finds the rows and writes nothing. A new
    RUN is not: a new run_id gives a new batch_id, and the whole CSV lands a
    second time. The idempotence was at the task level only; this puts it at
    the run level, which is where an operator relaunching after a failure
    actually stands.

    Block: B3, B4.
    Lot: ?, B4-L0.
    Competency: ?.
    Covers: B3-EC23, B3-EC24.
    """
    if replay:                         # a replay reads the raw zone, never writes it
        return
    held = history_in_raw(conn, dag_id)
    if not held:
        return

    listing = "\n".join(
        "  %s  %s  %d rows  (run %s%s)"
        % (state, batch_id, n_rows, run_id,
           ", closed " + ended_at.isoformat(timespec="seconds") if ended_at else "")
        for state, run_id, batch_id, n_rows, ended_at in held)
    done = [row for row in held if row[0] == "success"]
    if done:
        remedy = (
            "The history is ALREADY LOADED (batch %s). Loading it again would "
            "add a second copy of the same lines under a new batch_id, which "
            "no purge removes -- the cutoff is a date, and both copies share "
            "it.\n"
            "  Nothing to do, unless you mean to start from an empty database:"
            "  .\\fd-manager.ps1 reset, then .\\fd-manager.ps1 load"
            % done[0][2])
    else:
        remedy = (
            "A previous load left its rows in the raw zone WITHOUT finishing. "
            "They are the data: take THEM further rather than fetching the "
            "source again.\n"
            "  .\\fd-manager.ps1 replay -Batch %s\n"
            "  List what the raw zone holds:  .\\fd-manager.ps1 batches"
            % held[0][2])
    raise HistoryAlreadyLoaded(
        "the raw zone already holds this history:\n%s\n\n%s" % (listing, remedy))


def batch_size(conn: "psycopg.Connection", batch_id: uuid.UUID) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fd_raw_events WHERE batch_id = %s",
                    (batch_id,))
        return cur.fetchone()[0]


def write_batch(conn: "psycopg.Connection", batch_id: uuid.UUID, source: str,
                rows: Iterable[dict[str, str]]) -> int:
    """Write one row per source line. Returns the number of rows in the batch.

    COPY rather than one INSERT per row: 555,719 round trips would take minutes
    where a single stream takes seconds. The price is that COPY cannot carry
    ON CONFLICT DO NOTHING, hence the pre-check -- which is in any case the
    better behaviour, since it reports an already-loaded batch instead of
    silently doing nothing.
    """
    already = batch_size(conn, batch_id)
    if already:
        log.info("batch %s already holds %d rows: nothing written (idempotent)",
                 batch_id, already)
        return already

    # Imported here, not at module level: batch_id_for() needs no driver, and a
    # test of the identifier derivation must run without one.
    from psycopg.types.json import Jsonb

    written = 0
    with conn.cursor() as cur:
        with cur.copy("COPY fd_raw_events (batch_id, source, line_no, payload) "
                      "FROM STDIN") as copy:
            # Declared rather than inferred: COPY has no statement to infer the
            # column types from.
            copy.set_types(["uuid", "text", "integer", "jsonb"])
            for line_no, row in enumerate(rows, start=1):
                copy.write_row((batch_id, source, line_no, Jsonb(row)))
                written += 1
                if written % PROGRESS_EVERY == 0:
                    log.info("  %d rows copied", written)

    log.info("batch %s: %d rows written from %s", batch_id, written, source)
    return written


def read_batch(conn: "psycopg.Connection", batch_id: uuid.UUID
               ) -> Iterator[tuple[int, dict]]:
    """Yield (line_no, payload) for a batch, in source order.

    A server-side cursor: 555,719 payloads will not fit comfortably in memory,
    and replay has no reason to hold them all at once.
    """
    # REFUSED HERE, with the remedy named. XCom carries JSON, so a DAG hands
    # this identifier over as a STRING -- and the first thing this function
    # touches is .hex, which a str does not have. On 2026-11-16 the real-time
    # DAG failed on "'str' object has no attribute 'hex'", eleven frames deep,
    # naming neither the parameter nor the fix.
    if not isinstance(batch_id, uuid.UUID):
        raise TypeError(
            "batch_id must be a uuid.UUID, not %s (%r). XCom carries JSON, so "
            "a task that received this identifier from another one has a "
            "string: convert it with uuid.UUID(...) first, as the tasks of "
            "dag_load_history do."
            % (type(batch_id).__name__, batch_id))

    # A name unique to each call: two successive reads inside one connection
    # would otherwise collide, and a caller that stops early (profile_gap does)
    # leaves its cursor open until the generator is closed.
    name = "raw_replay_%s_%s" % (batch_id.hex[:8], uuid.uuid4().hex[:8])
    with conn.cursor(name=name) as cur:
        cur.itersize = 10_000
        cur.execute("SELECT line_no, payload FROM fd_raw_events "
                    "WHERE batch_id = %s ORDER BY line_no", (batch_id,))
        for line_no, payload in cur:
            yield line_no, payload
