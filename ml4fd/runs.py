"""Run journal.

Covers: EC12
Lot: L7
Competency: C4

One row per run in fd_pipeline_runs, written as the facts become known rather
than all at once at the end: a run that dies mid-chain still leaves a row saying
where it got to, which is the whole point of a journal.

The counter closure (n_read = n_valid + n_rejected) is a CHECK constraint on the
table, so a wrong count is refused at write time rather than discovered later in
a report.
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import psycopg

log = logging.getLogger(__name__)

RUNNING, SUCCESS, FAILED = "running", "success", "failed"


def start(conn: "psycopg.Connection", run_id: str, dag_id: str) -> None:
    """Open the run record. Idempotent, so a retried task does not fail here."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO fd_pipeline_runs (run_id, dag_id, started_at, state) "
            "VALUES (%s, %s, now(), %s) "
            "ON CONFLICT (run_id) DO UPDATE SET state = %s",
            (run_id, dag_id, RUNNING, RUNNING))
    log.info("run %s opened", run_id)


def set_batch(conn: "psycopg.Connection", run_id: str, batch_id) -> None:
    """Attach the batch to the run, once it is known."""
    with conn.cursor() as cur:
        cur.execute("UPDATE fd_pipeline_runs SET batch_id = %s WHERE run_id = %s",
                    (batch_id, run_id))


def record_counts(conn: "psycopg.Connection", run_id: str, n_read: int,
                  n_valid: int, n_rejected: int) -> None:
    """Record the volumes. The CHECK constraint refuses them if they do not close."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE fd_pipeline_runs "
            "SET n_read = %s, n_valid = %s, n_rejected = %s WHERE run_id = %s",
            (n_read, n_valid, n_rejected, run_id))
    log.info("run %s: n_read=%d n_valid=%d n_rejected=%d",
             run_id, n_read, n_valid, n_rejected)


def finish(conn: "psycopg.Connection", run_id: str, state: str) -> int:
    """Close the run and compute its duration from the recorded timestamps.

    Returns THE NUMBER OF ROWS CLOSED, and zero means the run was never
    started -- which happens when the failure precedes runs.start(). An UPDATE
    matching nothing is not an error, but a caller that announces a closure
    must not announce one that did not take place: the old version logged
    "run ... closed" either way.
    """
    if state not in (SUCCESS, FAILED):
        raise ValueError("state must be %r or %r, not %r" % (SUCCESS, FAILED, state))
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE fd_pipeline_runs "
            "SET ended_at = now(), state = %s, "
            "    duration_s = round(extract(epoch FROM now() - started_at)::numeric, 3) "
            "WHERE run_id = %s", (state, run_id))
        closed = cur.rowcount
    if closed:
        log.info("run %s closed: %s", run_id, state)
    else:
        log.warning("no run %s to close: it was never started", run_id)
    return closed


def abandon(conn: "psycopg.Connection", run_id: str) -> int:
    """Close a run as FAILED, AND ONLY IF NOBODY CLOSED IT.

    THE REASON THIS IS NOT finish(). N9 hung two control tasks after close_run,
    which records the success. A control failing downstream would then make the
    DAG-level failure callback overwrite that success with a failure, and the
    journal would say the ingestion failed when the transactions are in the
    table. The guard of M1 reads that journal: a window of sound ingestions
    could come to refuse training because a watchdog broke.

    So the callback marks what is still RUNNING -- i.e. what nobody closed --
    and a run already closed either way is left alone. Returns the number of
    rows closed; zero means it was already closed, or never started.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE fd_pipeline_runs "
            "SET ended_at = now(), state = %s, "
            "    duration_s = round(extract(epoch FROM now() - started_at)::numeric, 3) "
            "WHERE run_id = %s AND state = %s", (FAILED, run_id, RUNNING))
        closed = cur.rowcount
    if closed:
        log.info("run %s closed: %s", run_id, FAILED)
    else:
        log.info("run %s was already closed, or never started: left as it is",
                 run_id)
    return closed


def digest(conn: "psycopg.Connection") -> dict[str, tuple[int, str]]:
    """Digest of each store, to compare a replay against its original.

    Covers: EC23, EC24
    Lot: L6
    Competency: C5
    """
    from ml4fd import db
    return {store: (rows, dig)
            for store, rows, dig in db.query_file(conn, "checks/table_digest.sql")}


# --- S5/Q1: the fingerprint of what a run wrote, STORED ----------------------
#
# runs.digest() above fingerprints whole TABLES, once, at the end of the
# historical load, and only logs it. That is not what a purge needs: the purge
# works BATCH BY BATCH, the minute DAG never computes a store digest (it would
# hash a table of 555 thousand rows sixty times an hour), and a log is neither
# kept nor protected. So the unit of the fingerprint is the batch, it is taken
# by whoever WRITES the batch, and it is stored in fd_pipeline_runs.digests.

INGEST, BACKFILL = "ingest", "backfill"

# The two reasons a purge gives for refusing a batch. Named, because "refused"
# without a reason sends the reader looking in the wrong place.
NO_DIGEST = "no_digest"
DIGEST_MISMATCH = "digest_mismatch"

BACKFILL_DAG = "digest_backfill"


class MigrationMissing(RuntimeError):
    """A column this code needs is not in the database yet.

    RAISED BEFORE THE QUERY THAT WOULD FAIL, so the operator reads what to do
    instead of a driver's traceback ending in `column p.digests does not
    exist`. The DDL is additive and replayable: the remedy is one command, and
    the message carries it.
    """


def assert_digests(conn: "psycopg.Connection") -> None:
    """Refuse to go on if the column of Q1 is absent. Cheap, and asked first.

    Every reader of the fingerprints goes through here -- the purge planner and
    the backfill tool -- because the same missing column would otherwise raise
    three different obscure errors in three different places.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'fd_pipeline_runs' AND column_name = 'digests'")
        if cur.fetchone() is None:
            raise MigrationMissing(
                "la colonne fd_pipeline_runs.digests est absente de cette "
                "base : la migration du lot Q1 n'y a pas ete appliquee. "
                "Sans elle, aucune empreinte de lot n'est stockee, et la "
                "purge refuserait tout.\n"
                "  Appliquer le DDL :  .\\fd-manager.ps1 init\n"
                "Il est additif et rejouable : rien n'est perdu. Puis "
                "relever l'empreinte des lots deja charges :\n"
                "  .\\fd-manager.ps1 digests -Apply")


def _uuid(batch_id) -> uuid.UUID:
    """A UUID object, whatever XCom or a CLI handed over."""
    return batch_id if isinstance(batch_id, uuid.UUID) else uuid.UUID(str(batch_id))


def batch_digest(conn: "psycopg.Connection", batch_id) -> dict:
    """The fingerprint of one batch of the raw zone, as it stands now.

    {"rows": n, "digest": md5}. One SQL file, shared by the writers, the
    backfill and the purge, so they cannot mean different things by it.
    """
    from ml4fd import db
    ((rows, digest),) = db.query_file(
        conn, "checks/batch_digest.sql", batch_id=_uuid(batch_id))
    return {"rows": int(rows), "digest": digest}


def _merge(conn: "psycopg.Connection", run_id: str, payload: dict) -> int:
    """Add keys to the run's digests without losing the ones already there."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE fd_pipeline_runs "
            "SET digests = coalesce(digests, '{}'::jsonb) || %s::jsonb "
            "WHERE run_id = %s", (json.dumps(payload), run_id))
        return cur.rowcount


def _set_nested(conn: "psycopg.Connection", run_id: str, top: str, key: str,
                value: dict) -> int:
    """Set digests -> top -> key, leaving every sibling of `key` alone.

    TWO TRAPS, BOTH MET FOR REAL ON PostgreSQL 16.

    The `||` of _merge() cannot do this: a shallow merge of
    {"purge": {"fd_quarantine": ...}} REPLACES the whole purge object and
    loses what was recorded for fd_raw_events -- which is the state a
    resumption depends on.

    And jsonb_set with a two-element path cannot either: create_missing only
    creates the LAST element, so on a run whose digests hold no `purge` key at
    all the statement returns the document UNCHANGED, reports a row updated,
    and writes nothing. A purge would then start with no mark, and an
    interruption would leave a batch nothing could explain.

    So the path has ONE element -- a top-level key, always creatable -- and
    what is written into it is the existing object merged with this one entry.

    A THIRD TRAP, met the first time this ran for real (2026-11-17):
    `could not determine data type of parameter $3`. jsonb_build_object is
    variadic "any", and psycopg sends a Python str WITHOUT a type so the server
    can infer it -- which is what makes `batch_id = %s` work against a uuid
    column. Inside a variadic "any" there is nothing to infer from, so the key
    must say its type. Every parameter here is cast for that reason.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE fd_pipeline_runs "
            "SET digests = jsonb_set("
            "        coalesce(digests, '{}'::jsonb), ARRAY[%s::text], "
            "        coalesce(digests -> %s::text, '{}'::jsonb) "
            "            || jsonb_build_object(%s::text, %s::jsonb), true) "
            "WHERE run_id = %s",
            (top, top, key, json.dumps(value), run_id))
        return cur.rowcount


def mark_purging(conn: "psycopg.Connection", run_id: str, store: str,
                 started_at: str, expected: int) -> None:
    """Say that a purge of this batch has STARTED, before the first slice.

    THE REASON THIS EXISTS. A chunked purge commits between slices, so a
    container killed half-way leaves a batch whose rows no longer match its
    recorded fingerprint. Without this mark, the next run could not tell that
    remainder from a batch someone had tampered with -- and would refuse it for
    ever, which is the one outcome Q1 was built to prevent.
    """
    if not _set_nested(conn, run_id, "purge", store,
                       {"started_at": started_at, "expected": expected,
                        "done_at": None, "deleted": None}):
        raise RuntimeError(
            "run %s is not in fd_pipeline_runs: a purge cannot announce "
            "itself, so it does not start" % run_id)


def mark_purged(conn: "psycopg.Connection", run_id: str, store: str,
                done_at: str, deleted: int, expected: int,
                started_at: str) -> None:
    """Say that it finished, and how much it took."""
    _set_nested(conn, run_id, "purge", store,
                {"started_at": started_at, "expected": expected,
                 "done_at": done_at, "deleted": deleted})


def purge_state(conn: "psycopg.Connection", batch_id) -> dict:
    """What a purge has already done to this batch, by store. {} if none."""
    held = recorded_digest(conn, batch_id)
    return (held or {}).get("purge") or {}


def record_batch_digest(conn: "psycopg.Connection", run_id: str, batch_id,
                        origin: str = INGEST) -> dict:
    """Fingerprint the batch and write it on the run. RAISES if the run is absent.

    Called by every writer of the raw zone, right after it writes -- inside the
    same transaction where it can, so the fingerprint and the rows it describes
    commit together. An UPDATE that matches no run is an error here: recording
    nothing without saying so is exactly how a batch becomes impurgeable, or
    worse, how someone is tempted to loosen the purge.

    origin says WHEN the fingerprint was taken: "ingest" at write time, or
    "backfill" for a batch that predates the column. A backfilled fingerprint
    proves the batch is unchanged since the backfill, not since it was written,
    and an audit must be able to tell the two apart.
    """
    if origin not in (INGEST, BACKFILL):
        raise ValueError("origin must be %r or %r, not %r"
                         % (INGEST, BACKFILL, origin))
    batch_id = _uuid(batch_id)
    entry = batch_digest(conn, batch_id)
    entry.update(batch_id=str(batch_id), origin=origin)
    if not _merge(conn, run_id, {"raw_batch": entry}):
        raise RuntimeError(
            "run %s is not in fd_pipeline_runs, so the fingerprint of batch %s "
            "has nowhere to go. runs.start() must precede the write."
            % (run_id, batch_id))
    log.info("run %s: batch %s fingerprinted (%d rows, %s, %s)",
             run_id, batch_id, entry["rows"], entry["digest"], origin)
    return entry


def record_store_digests(conn: "psycopg.Connection", run_id: str,
                         digests: dict[str, tuple[int, str]]) -> None:
    """Store the digest of the exploited layer on the run, beside the log line.

    This is what lets the purge PROVE it did not overflow: fd_transactions has
    the same digest before and after. It was logged only; a log rotates.
    """
    stores = {name: {"rows": int(rows), "digest": dig}
              for name, (rows, dig) in digests.items()}
    if not _merge(conn, run_id, {"stores": stores}):
        raise RuntimeError("run %s is not in fd_pipeline_runs: the digests of "
                           "the stores have nowhere to go" % run_id)


def recorded_digest(conn: "psycopg.Connection", batch_id) -> dict | None:
    """The fingerprint recorded for a batch, or None if nobody took one."""
    from ml4fd import db
    rows = db.query_file(conn, "checks/batch_digest_recorded.sql",
                         batch_id=str(_uuid(batch_id)))
    if not rows:
        return None
    n_rows, digest, origin, run_id, purge = rows[0]
    return {"rows": int(n_rows), "digest": digest, "origin": origin,
            "run_id": run_id, "purge": purge or {}}


def undigested_batches(conn: "psycopg.Connection") -> list[dict]:
    """The batches of the raw zone with no recorded fingerprint."""
    from ml4fd import db
    return [{"batch_id": b, "source": src, "rows": int(n), "first_at": first,
             "last_at": last}
            for b, src, n, first, last
            in db.query_file(conn, "checks/undigested_batches.sql")]


def refusal(conn: "psycopg.Connection", batch_id) -> tuple[str, str] | None:
    """Why a batch may NOT be purged, or None if it may.

    THE STRUCTURAL GUARANTEE OF S5: evidence is not destroyed before it has
    been taken. Two refusals, each named:

      no_digest        nobody recorded what the batch held. Deleting it would
                       leave no trace of its content.
      digest_mismatch  the batch is not what was recorded. Something changed
                       the raw zone, which is append-only; deleting it now
                       would destroy the only evidence of the change.
    """
    batch_id = _uuid(batch_id)
    held = recorded_digest(conn, batch_id)
    if held is None:
        return (NO_DIGEST,
                "batch %s has no recorded fingerprint: `fd-manager digests "
                "-Apply` records it" % batch_id)
    now = batch_digest(conn, batch_id)
    if (now["rows"], now["digest"]) != (held["rows"], held["digest"]):
        why = ("batch %s holds %d rows with digest %s, but run %s recorded "
               "%d rows with digest %s"
               % (batch_id, now["rows"], now["digest"], held["run_id"],
                  held["rows"], held["digest"]))
        # A PURGE LEFT HALF-WAY EXPLAINS IT, and must not look like tampering:
        # the slices already deleted are the difference. Said here so that the
        # dry run of Q2, which reports this, names the real cause -- and
        # purge_batch() resumes rather than refusing (ml4fd/purge.py).
        marks = held.get("purge") or {}
        unfinished = sorted(store for store, mark in marks.items()
                            if not mark.get("done_at"))
        finished = sorted(store for store, mark in marks.items()
                          if mark.get("done_at"))
        if unfinished:
            why += (" -- une purge de ce lot a ete interrompue (%s) : la "
                    "relancer la termine" % ", ".join(unfinished))
        elif finished:
            # A FINISHED PURGE EXPLAINS IT FOR GOOD. Nothing is wrong and
            # nothing is pending: the rows are gone on purpose, and what the
            # batch held is what the recorded fingerprint says. Said here
            # because this message is what the dry run of Q2 and the
            # acceptance of Q6 show, and "mismatch" alone reads as tampering.
            why += (" -- ce lot a ete purge (%s) : l'empreinte enregistree est "
                    "ce qui subsiste de son contenu"
                    % ", ".join(finished))
        return (DIGEST_MISMATCH, why)
    return None


def backfill_batch(conn: "psycopg.Connection", batch_id) -> str:
    """Record the fingerprint of a batch that predates the column. Returns the run.

    The fingerprint goes on the run that wrote the batch (the earliest run
    attached to it). A batch with no run at all -- the load died before
    runs.start() -- gets a run of its own under dag_id "digest_backfill", so
    the record exists and says what it is; it is never attached to an unrelated
    run. Either way origin="backfill".
    """
    batch_id = _uuid(batch_id)
    with conn.cursor() as cur:
        cur.execute("SELECT run_id FROM fd_pipeline_runs WHERE batch_id = %s "
                    "ORDER BY started_at LIMIT 1", (batch_id,))
        row = cur.fetchone()
    if row:
        run_id = row[0]
    else:
        run_id = "%s__%s" % (BACKFILL_DAG, batch_id)
        start(conn, run_id, BACKFILL_DAG)
        set_batch(conn, run_id, batch_id)
        finish(conn, run_id, SUCCESS)
    record_batch_digest(conn, run_id, batch_id, origin=BACKFILL)
    return run_id
