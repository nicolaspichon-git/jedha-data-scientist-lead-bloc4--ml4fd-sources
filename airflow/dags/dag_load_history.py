"""S1 -- historical data load. A thin DAG: it orders, it does not compute.

Covers: EI06, EC08, EC12
Lot: L7
Competency: C3

All the logic sits in the ml4fd/ package, so it can be exercised without
starting Airflow -- that is what makes the plan's acceptance criteria runnable.

TWO ENTRY POINTS, one DAG:

  normal     -- trigger with no configuration: download, raw zone, then the rest
  replay     -- trigger with {"batch_id": "<uuid>"}: resume at validate() from
                what fd_raw_events already holds, WITHOUT querying the source
                again (EC24). fetch_csv() and write_raw() then do nothing and
                say so in the log.

The run is journalled as the facts become known rather than all at once at the
end, so a run that dies mid-chain still leaves a row saying where it got to.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

DAG_ID = "dag_load_history"


def _mark_failed(context) -> None:
    """Close the run record on failure. A callback must never raise."""
    try:
        from ml4fd import db, runs
        with db.connect("fd_ingest") as conn:
            runs.finish(conn, context["run_id"], runs.FAILED)
    except Exception:                       # noqa: BLE001 -- best effort only
        log.exception("could not mark run %s as failed",
                      context.get("run_id", "?"))


def _replay_batch() -> str | None:
    """The batch_id given in --conf, if this run is a replay."""
    from airflow.operators.python import get_current_context
    conf = get_current_context()["dag_run"].conf or {}
    return conf.get("batch_id")


@dag(
    dag_id=DAG_ID,
    description="S1: load the 555,719 historical transactions",
    schedule=None,                      # manual bootstrap, not a stream (EI06)
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,                  # one bootstrap at a time
    tags=["S1", "BC03", "C2", "C5"],
    default_args={"retries": 0},        # the retry policy belongs to S3
    on_failure_callback=_mark_failed,
)
def load_history():

    @task
    def preflight() -> dict:
        """Check the plumbing before any processing.

        Covers: EC12
        Lot: L0
        Competency: C4
        """
        from airflow.operators.python import get_current_context

        from ml4fd import config, db, raw, runs

        context = get_current_context()
        s = config.load()
        log.info("governance.yml read from %s", config.governance_path())
        log.info("settings: %s", s.redacted())
        if not s.pepper:
            raise ValueError(
                "ML4FD_PEPPER is empty: pseudonymisation would be reversible. "
                "Run .\\fd-manager.ps1 setup, then recreate the containers."
            )

        with db.connect("fd_ingest") as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_user, session_user, current_database()")
                cur_user, sess_user, database = cur.fetchone()
            log.info("connected to %s; session_user=%s current_user=%s",
                     database, sess_user, cur_user)

            runs.start(conn, context["run_id"], DAG_ID)

            counts = db.table_counts(conn)
            log.info("store row counts: %s", counts)

            # BEFORE anything is fetched or written: does this history already
            # sit in the raw zone? The task-level idempotence (batch_id derived
            # from the run_id) does not cover a NEW run, which is exactly what
            # an operator starts after a failure. Four copies of the same
            # 555,719 lines reached this project's raw zone that way.
            raw.refuse_second_load(conn, DAG_ID, _replay_batch())

            # The append-only property is not a promise: verify it.
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute("UPDATE fd_raw_events SET source = source")
                raise AssertionError(
                    "UPDATE on fd_raw_events SUCCEEDED: the append-only property "
                    "is not in place. Check sql/ddl/060_roles_grants.sql and the "
                    "SET ROLE."
                )
            except Exception as exc:
                if isinstance(exc, AssertionError):
                    raise
                log.info("append-only confirmed: UPDATE refused (%s)",
                         type(exc).__name__)

            # Same check one level finer: the features may be recomputed, the
            # business facts of a transaction may not. The grant is on the
            # feature columns alone, so this attempt must fail.
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute("UPDATE fd_transactions SET amt = amt")
                raise AssertionError(
                    "UPDATE of fd_transactions.amt SUCCEEDED: the business "
                    "columns are rewritable. Check the column-level grant in "
                    "sql/ddl/060_roles_grants.sql -- UPDATE must be granted on "
                    "the feature columns only.")
            except Exception as exc:
                if isinstance(exc, AssertionError):
                    raise
                log.info("business columns immutable: UPDATE of amt refused (%s)",
                         type(exc).__name__)

        return counts

    @task
    def fetch_csv() -> str:
        """Download the historical CSV, with cache and checksum. Returns its path.

        Covers: EI04, EI05
        Lot: L1
        Competency: C2
        """
        replay = _replay_batch()
        if replay:
            log.info("replay of batch %s: the source is NOT queried again (EC24)",
                     replay)
            return ""

        from ml4fd import config, fetch
        s = config.load()
        return str(fetch.fetch_csv(s.csv_source,
                                   Path("/opt/project/.local_cache")))

    @task
    def write_raw(csv_path: str) -> str:
        """Write the payload as received. Returns the batch_id.

        Covers: EC23, EC24
        Lot: L1
        Competency: C2
        """
        import uuid as _uuid
        from pathlib import Path as _Path

        from airflow.operators.python import get_current_context

        from ml4fd import db, fetch, raw, runs

        run_id = get_current_context()["run_id"]

        replay = _replay_batch()
        if replay:
            batch_id = _uuid.UUID(replay)
            with db.connect("fd_ingest") as conn:
                held = raw.batch_size(conn, batch_id)
                runs.set_batch(conn, run_id, batch_id)
            if not held:
                raise ValueError(
                    "batch %s holds no row in fd_raw_events: nothing to replay.\n"
                    "  List what the raw zone actually holds:  .\\fd-manager.ps1 batches\n"
                    "  If it is empty, the raw zone was wiped (a reset does "
                    "that) and a plain load is what you want: .\\fd-manager.ps1 load "
                    "-- the CSV is cached on disk, so nothing is downloaded "
                    "again." % batch_id)
            log.info("replay of batch %s (%d rows): fd_raw_events is NOT written "
                     "to", batch_id, held)
            return str(batch_id)

        batch_id = raw.batch_id_for(run_id)
        path = _Path(csv_path)
        log.info("run_id=%s -> batch_id=%s (derived, so a retry is a no-op)",
                 run_id, batch_id)

        with db.connect("fd_ingest") as conn:
            n_read = raw.write_batch(conn, batch_id, path.name,
                                     fetch.read_rows(path))
            runs.set_batch(conn, run_id, batch_id)
            # S5/Q1: fingerprint what was just written, before anything else
            # reads it. A purge refuses a batch without this.
            runs.record_batch_digest(conn, run_id, batch_id)
        log.info("n_read=%d", n_read)
        return str(batch_id)

    @task
    def validate(batch_id: str) -> str:
        """Validate in two classes, quarantine the rejects. Returns the batch_id.

        Covers: EC20, EC21, EC22
        Lot: L2
        Competency: C5
        """
        import uuid as _uuid

        from airflow.operators.python import get_current_context

        from ml4fd import checks, config, db, quarantine, raw, runs

        s = config.load()
        bid = _uuid.UUID(batch_id)
        run_id = get_current_context()["run_id"]

        with db.connect("fd_ingest") as conn:
            # Profile BEFORE ruling: a systematic gap is a source defect, a
            # sporadic one is a record defect. Getting this backwards would
            # quarantine the whole batch (see ml4fd/checks.py).
            reader = raw.read_batch(conn, bid)
            try:
                profile = checks.profile_gap((p for _, p in reader),
                                             s.systematic_share)
            finally:
                reader.close()
            log.info("time-column gap: %s", profile)
            reject_gap = profile["verdict"] == "sporadic"
            if profile["verdict"] == "systematic":
                log.warning(
                    "gap of %d days over %.3f%% of rows: SOURCE defect, ruled on "
                    "once as hypothesis H5, unix_time dropped, NO per-record "
                    "rejection", profile["dominant_gap_days"],
                    100 * profile["dominant_share"])

            n_read = 0

            def counted():
                nonlocal n_read
                for line_no, record, rejects in checks.validate_batch(
                        raw.read_batch(conn, bid), s.max_amount, reject_gap):
                    n_read += 1
                    yield line_no, record, rejects

            rejected_lines, reason_rows = quarantine.write(conn, bid, counted())
            # Written here, where the figures are known. The CHECK constraint on
            # fd_pipeline_runs refuses them if they do not close.
            runs.record_counts(conn, run_id, n_read, n_read - rejected_lines,
                               rejected_lines)

        n_valid = n_read - rejected_lines
        log.info("n_read=%d n_valid=%d n_rejected=%d (%d reason rows)",
                 n_read, n_valid, rejected_lines, reason_rows)
        # The closure that fd_pipeline_runs also enforces as a CHECK constraint.
        if n_read != n_valid + rejected_lines:
            raise AssertionError(
                "counters do not close: %d != %d + %d"
                % (n_read, n_valid, rejected_lines))
        if not n_read:
            raise ValueError("batch %s holds no row: nothing to validate" % bid)
        return batch_id

    @task
    def type_and_pseudonymize(batch_id: str) -> str:
        """Type the valid records, strip identity. Returns the batch_id.

        Covers: EC17, EC23
        Lot: L3
        Competency: C2, C4
        """
        import uuid as _uuid

        from ml4fd import config, db, raw, transform

        s = config.load()
        bid = _uuid.UUID(batch_id)

        # TWO connections, and this is not a detail. Reading a batch uses a
        # server-side cursor; writing uses COPY. psycopg serialises the
        # operations of one connection behind a lock, so a COPY that waits for
        # rows from a generator that waits for a FETCH on the same connection
        # deadlocks -- silently, with no exception, for as long as you let it.
        with db.connect("fd_ingest") as write_conn, \
             db.connect("fd_ingest") as read_conn:
            skip = transform.rejected_lines(write_conn, bid)
            log.info("%d quarantined line(s) to skip", len(skip))
            inserted = transform.load_transactions(
                write_conn, bid, raw.read_batch(read_conn, bid), s.pepper, skip)
            log.info("%d transaction(s) inserted", inserted)

            # The acceptance criteria of L3, verified rather than assumed.
            with write_conn.cursor() as cur:
                cur.execute("SELECT count(*), count(DISTINCT client_key), "
                            "       coalesce(sum(amt), 0) "
                            "FROM fd_transactions WHERE batch_id = %s", (bid,))
                rows, cardholders, total = cur.fetchone()
        log.info("stored=%d distinct client_key=%d sum(amt)=%s",
                 rows, cardholders, total)
        return batch_id

    @task
    def backfill_features(batch_id: str) -> int:
        """Backfill the features, then prove both modes agree.

        Covers: EE07
        Lot: L4
        Competency: C5
        """
        from ml4fd import db, features

        with db.connect("fd_ingest") as conn:
            rows = features.backfill(conn)

            # A reconciliation that probed no tie proves nothing about ruling 2.
            # Refuse a green result that tested nothing.
            cover = features.coverage(conn)
            log.info("reconciliation sample: %s", cover)
            if not cover["total_rows"]:
                raise ValueError("fd_transactions is empty: nothing to reconcile")
            if not cover["tie_rows"]:
                log.warning(
                    "no timestamp tie in the data: the reconciliation cannot "
                    "tell ROWS from RANGE ... EXCLUDE GROUP, so ruling 2 stays "
                    "unproven by this run")
            if cover["rows_without_features"]:
                raise AssertionError(
                    "%d row(s) left without features after the backfill"
                    % cover["rows_without_features"])

            divergences = features.reconcile(conn)

        if divergences:
            raise AssertionError(
                "the two feature computation modes disagree on %d row(s) of the "
                "stratified sample; first: %s" % (len(divergences), divergences[0]))
        log.info("backfill done over %d row(s), both modes in agreement", rows)
        return rows

    @task
    def write_ground_truth(batch_id: str) -> int:
        """Write the verdict and the date it became known.

        Covers: EI01, EI02, EC23
        Lot: L5
        Competency: C2
        """
        import uuid as _uuid

        from ml4fd import config, db, raw, transform, truth

        s = config.load()
        bid = _uuid.UUID(batch_id)
        delay = s.verdict_delay()
        log.info("verdict delay in force: %s (mode=%s)", delay, s.verdict_mode)

        # Two connections, for the same reason as in type_and_pseudonymize: a
        # server-side cursor and a COPY cannot share one connection.
        with db.connect("fd_ingest") as write_conn, \
             db.connect("fd_ingest") as read_conn:
            skip = transform.rejected_lines(write_conn, bid)
            inserted = truth.load_ground_truth(
                write_conn, raw.read_batch(read_conn, bid), delay, str(bid), skip)

            # The acceptance criteria of L5, verified rather than assumed.
            #
            # SCOPED TO THIS BATCH, and it was not until N9. The query read the
            # WHOLE of fd_ground_truth, which held only S1's verdicts when this
            # was written -- S3 now writes the stream's there too. The delay
            # happens to be 30 days on both sides today, so this passes; the
            # day the stream carried a different delay, a REPLAY OF S1 would
            # fail on "the delay is not uniform" and blame the historical load
            # for a property of the stream. The same defect as in
            # s1_acceptance.sql, in a place no acceptance check looks.
            with write_conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*), count(*) FILTER (WHERE label), "
                    "       min(label_ts - txn_ts), max(label_ts - txn_ts) "
                    "FROM   fd_ground_truth WHERE source = %s", (str(bid),))
                rows, frauds, min_gap, max_gap = cur.fetchone()

                # The dissociation is structural: fd_transactions must hold no
                # column that looks like a label.
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'fd_transactions' "
                    "  AND column_name IN ('label', 'is_fraud', 'fraud')")
                leaked = [r[0] for r in cur]

        if leaked:
            raise AssertionError(
                "fd_transactions carries a label column (%s): the verdict is no "
                "longer dissociated from the transaction" % ", ".join(leaked))
        if not rows:
            # Named, because the scoped query above makes this reachable and
            # the next test would report it as "min=None max=None", which names
            # neither the batch nor the cause.
            raise AssertionError(
                "no verdict carries source=%s: the ground truth of this batch "
                "was not written, so there is nothing to check the delay on"
                % bid)
        if min_gap != max_gap or min_gap != delay:
            raise AssertionError(
                "the delay is not uniform within batch %s: min=%s max=%s, "
                "expected %s" % (bid, min_gap, max_gap, delay))

        rate = 100.0 * frauds / rows if rows else 0.0
        log.info("ground truth: %d verdicts, %d frauds (%.3f %%), delay %s",
                 rows, frauds, rate, min_gap)
        return inserted

    @task
    def close_run(backfilled: int, verdicts: int) -> dict:
        """Close the journal and publish the digests of the exploited layer.

        The digests are what makes a replay verifiable: run the DAG again on the
        same batch and compare. Identical digests mean the replay reproduced the
        data exactly, which is what EC23 and EC24 ask for and what no amount of
        row counting would establish.

        Covers: EC12, EC23, EC24
        Lot: L6, L7
        Competency: C4, C5
        """
        from airflow.operators.python import get_current_context

        from ml4fd import db, runs

        run_id = get_current_context()["run_id"]
        with db.connect("fd_ingest") as conn:
            digests = runs.digest(conn)
            # S5/Q1: STORED, not only logged. The log rotates; the purge needs
            # the digest of fd_transactions from before it ran to prove it did
            # not overflow.
            runs.record_store_digests(conn, run_id, digests)
            runs.finish(conn, run_id, runs.SUCCESS)

        for store, (rows, dig) in sorted(digests.items()):
            log.info("%-18s %8d rows  digest=%s", store, rows, dig)
        log.info("backfilled=%d verdicts=%d", backfilled, verdicts)
        return {store: dig for store, (_, dig) in digests.items()}

    # The chain. The batch_id travels from task to task, so the ordering reads
    # from the passing of values; the only explicit >> is the preflight gate,
    # which produces nothing the rest needs.
    #
    # Replay (EC24): trigger with {"batch_id": "<uuid>"} and the first two tasks
    # do nothing -- the source is not queried and fd_raw_events is not written.
    counts = preflight()
    path = fetch_csv()
    counts >> path

    batch = write_raw(path)
    checked = validate(batch)
    typed = type_and_pseudonymize(checked)
    close_run(backfill_features(typed), write_ground_truth(typed))


load_history()
