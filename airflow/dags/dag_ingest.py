"""S3 / N7: the real-time ingestion, one run a minute.

Covers: EE01, EE02, EE09, EI01, EI05, EI06, EC09, EC14, EC23, EC24
Lot: N7
Competency: C1, C2, C3, C4

THE RAW ZONE IS THE HAND-OFF, and that is the design rather than a convenience.
Airflow passes JSON between tasks, not Python objects, so the readings cannot
travel. They do not need to: the poll writes them to fd_raw_events -- durable
and append-only -- and every later task re-reads from there. Two consequences
that matter more than the plumbing:

  THE POLL IS THE ONLY TASK THAT TOUCHES THE NETWORK. Five calls cost about 27
  seconds (N0: median latency 5.4 s) and the API will never return the same five
  transactions again, so it is the one task with no retry. Everything after it
  is replayable from the table, and those tasks do retry.

  A FAILURE DOWNSTREAM LOSES NOTHING. The transactions are already in the raw
  zone; a replay types, scores and alerts them.

EIGHT TASKS, AND THE LAST TWO WERE MISSING FOR A WHOLE PROCESS. The plan named
five; scoring was separated from typing in N7, because a tracking server that is
down must show as a RED TASK and not as a warning line inside a green run. The
last two are N9's, and they are stage (3c) of diagram 3.3 -- the controls and the
operations channel -- which that diagram specified, the plan of S3 did not list,
and nobody missed until the diagram was read against the code. See ml4fd/health.py
for what the traceability table could not see.

THE TWO AUDIENCES ARE TWO TASKS, which is why raise_and_notify_fraud is named for
its reader. It tells the analyst a TRANSACTION was flagged, under a capacity of
60 a day. raise_and_notify_ops tells the pipeline owner that a STATE OF THE CHAIN
changed. Same three-line channel interface, deliberately different destinations:
governance.yml refuses two equal ones, because an analyst who receives pipeline
messages stops reading alerts.

THE CADENCE, decided 2026-11-16 and declared in governance.yml. One run a
minute, five calls -- 5 x 1,440 = 7,200 a day, which is exactly the rate
threshold.py chose the threshold and the 60-alert capacity at. The limiter fired
at 13 calls a minute in N0 and never below, so the margin is 2.6x. max_active_runs
is 1: one 502 costs 25 s and brings the worst realistic run to about 47 s, still
inside the minute, but two would overrun -- a slow run must DELAY the next, never
run beside it, or two collectors would write the same batch.

XCOM CARRIES JSON, SO batch_id TRAVELS AS A STRING and every task that
receives it converts back with uuid.UUID() before handing it to a function.
dag_load_history has done this at the top of three tasks since L2; I had read
that and not applied it, and raw.read_batch failed on
"'str' object has no attribute 'hex'" -- deep inside, naming nothing. It now
refuses a non-UUID with a message that names the fix.

A FAILURE IS CLOSED BY A CALLBACK ON THE DAG and not by a task carrying
trigger_rule="one_failed", which is what N7 first wrote. That rule needs an
upstream task in the failed state AND the scheduler to place one more task; the
dagrun_timeout below fails the RUN and kills what is running, so the marking
task could be skipped in exactly the case the two-minute timeout exists to
catch, leaving the row in 'running' -- the hole the marking exists to close.
dag_load_history has used the callback since L2; this DAG had invented a second
convention for the same job. See _mark_failed.

NO VERDICT IS WRITTEN HERE. That is N6's fourth guarantee: the write lives in
dag_ingest_verdicts, daily, sharing no task with the scoring. tests/test_leak.py
reads these imports to check it.
"""
from __future__ import annotations

import datetime as dt
import logging

import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

DAG_ID = "dag_ingest"


def _mark_failed(context) -> None:
    """Close the run as FAILED. A CALLBACK MUST NEVER RAISE.

    WITHOUT THIS THE GUARD OF M1 COUNTS WRONG. It reads fd_pipeline_runs and
    counts state = 'success' and state = 'failed'; a run abandoned in 'running'
    is in neither, so a window of 1,440 runs a day could be full of silent
    failures and still look like a window with no failure at all.

    Why a callback and not a task: see the module docstring. Why the run_id is
    the right key: the poll records the run under str(context["run_id"]), the
    same value Airflow hands this callback.

    AND WHY runs.abandon() AND NOT runs.finish(). N9 hung two control tasks
    after close_run, which records the success. One of them failing would make
    this callback overwrite that success with a failure, and the journal would
    report a failed ingestion while the transactions sit in the table -- which
    M1's guard would then read as a reason to refuse training. abandon() closes
    only what is still RUNNING, i.e. what nobody closed.

    Covers: EC14
    Lot: N9
    Competency: C4
    """
    run_id = str(context.get("run_id", "?"))
    try:
        from ml4fd import db, runs
        with db.connect("fd_ingest") as conn:
            closed = runs.abandon(conn, run_id)
            conn.commit()
        if closed:
            log.error("execution %s marquee en echec", run_id)
    except Exception:                       # noqa: BLE001 -- best effort only
        log.exception("could not mark run %s as failed", run_id)


@dag(
    dag_id=DAG_ID,
    description="real-time ingestion, scoring and alerting",
    schedule="* * * * *",
    start_date=pendulum.datetime(2026, 11, 16, tz="Europe/Paris"),
    catchup=False,                      # 1,440 runs a day: never backfill
    max_active_runs=1,
    # About 27 s nominally, about 47 s with one 502. Past two minutes something
    # is wrong and a queue of minute-runs would build behind it.
    dagrun_timeout=dt.timedelta(minutes=2),
    tags=["S3", "N7", "BC03", "C2", "C3", "C4"],
    default_args={"retries": 1, "retry_delay": dt.timedelta(seconds=10)},
    # On the DAG and not on a task: dagrun_timeout fails the run, and a
    # one_failed task may never be scheduled then.
    on_failure_callback=_mark_failed,
)
def ingest_realtime():

    @task(retries=0)                    # the network task is never retried
    def poll() -> dict:
        """Call the API and write the readings to the raw zone.

        NO RETRY, deliberately. A retry would make five fresh calls and throw
        the first five away: the API returns the transaction of the instant, so
        a reading not written is a transaction nobody will see again. Better to
        fail the minute and let the next run collect.

        Covers: EE01, EI01, EC23
        Lot: N7
        Competency: C2
        """
        from airflow.operators.python import get_current_context

        from ml4fd import config, db, raw, runs, stream

        context = get_current_context()
        run_id = str(context["run_id"])
        settings = config.load()
        batch_id = raw.batch_id_for(run_id)

        readings, failures = stream.poll(
            settings.api_endpoint, settings.calls_per_run,
            spacing_s=settings.call_spacing_s, timeout=settings.api_timeout_s)

        for attempt in failures:
            log.warning("appel en echec : HTTP %s, %.1f s, %s",
                        attempt.status or "reseau", attempt.latency_s,
                        attempt.error)

        with db.connect("fd_ingest") as conn:
            runs.start(conn, run_id, DAG_ID)
            runs.set_batch(conn, run_id, batch_id)
            n_read = stream.write_batch(conn, batch_id, readings) if readings \
                else 0
            if n_read:
                # S5/Q1: the fingerprint is taken where the raw zone is written,
                # in the same transaction, so what was written and what was
                # fingerprinted commit together. A purge refuses a batch
                # without one.
                runs.record_batch_digest(conn, run_id, batch_id)
            conn.commit()

        if not readings:
            # Not an error of this pipeline: the Space refused or timed out.
            # N0 measured 5 % of calls failing, so a minute with nothing is
            # expected and must not look like a defect.
            raise AssertionError(
                "aucune lecture : %d appel(s) en echec. Rien a ingerer. "
                "N0 a mesure 5 %% d'echecs, donc une minute vide arrive ; "
                "c'est une minute perdue, pas un defaut de la chaine."
                % len(failures))

        log.info("lot %s : %d lecture(s) ecrite(s), %d appel(s) en echec",
                 batch_id, n_read, len(failures))
        # A FAILED CALL IS NOT A READ LINE: n_read counts what reached the raw
        # zone, which is what fd_pipeline_runs' CHECK constraint closes on.
        return {"run_id": run_id, "batch_id": str(batch_id),
                "n_read": n_read, "failures": len(failures)}

    @task
    def validate(polled: dict) -> dict:
        """Validate the batch and write the quarantine.

        Covers: EE02, EC14, EC25
        Lot: N7
        Competency: C2, C5
        """
        import uuid as _uuid

        from ml4fd import config, db, quarantine, raw, stream

        settings = config.load()
        # XCom carries JSON: the identifier arrived as a string.
        batch_id = _uuid.UUID(polled["batch_id"])

        with db.connect("fd_ingest") as conn:
            # The ENVELOPES, not the rows: the clock bound needs our_ms and
            # latency_ms, which the envelope carries. Calling
            # checks.validate_batch on the rows alone was the first draft, and
            # it silently dropped CLOCK_IMPLAUSIBLE from the DAG while the
            # manual runner kept it -- the rule N0 earned by measuring one
            # reading 818 s out, our own process suspended.
            reader = raw.read_batch(conn, batch_id)
            try:
                envelopes = list(reader)
            finally:
                reader.close()
            accepted, items = stream.validate_raw(
                envelopes, settings.max_amount, settings.clock_tolerance_s)
            rejected_lines, reason_rows = quarantine.write(conn, batch_id,
                                                           items)
            conn.commit()

        n_read = polled["n_read"]
        log.info("lu %d, valide %d, rejete %d (%d motif(s))",
                 n_read, n_read - rejected_lines, rejected_lines, reason_rows)
        return {"n_read": n_read, "n_valid": n_read - rejected_lines,
                "n_rejected": rejected_lines, "reason_rows": reason_rows}

    @task
    def type_and_feature(polled: dict, counts: dict) -> int:
        """Type, pseudonymise, and compute the features of the batch.

        Covers: EI03, EE07, EC17
        Lot: N7
        Competency: C2
        """
        import uuid as _uuid

        from ml4fd import (checks, config, db, features, quarantine, stream,
                           transform)

        settings = config.load()
        batch_id = _uuid.UUID(polled["batch_id"])

        with db.connect("fd_ingest") as conn:
            records = stream.records_from_raw(conn, batch_id)
            # The quarantined lines, read back from the table rather than
            # carried across the task boundary: the one durable record of what
            # was rejected. Passed as `skip`, which is load_transactions' own
            # contract -- filtering the list here would be a second
            # implementation of the same exclusion.
            skip = quarantine.rejected_lines(conn, batch_id)
            n_typed = transform.load_transactions(
                conn, batch_id, records, settings.pepper, skip,
                profile=checks.STREAM_PROFILE)
            n_features = features.apply_batch(conn, batch_id)
            conn.commit()

        log.info("%d transaction(s), %d features", n_typed, n_features)
        return n_typed

    @task
    def score_batch(polled: dict, n_typed: int) -> dict:
        """Score the batch with the registered model version.

        A RED TASK IF THE SERVICE IS UNREACHABLE, and that is the point of
        separating it: the transactions are already stored, so nothing is lost,
        but the operator must see which stage stopped rather than read a warning
        inside a green run.

        THE SERVICE DECIDES, SINCE L5 (20/11/2026). This task used to call
        score.load() here, so the model lived in TWO processes -- this
        scheduler and the scoring service -- each resolving the champion alias
        on its own. A service nobody calls is not a decoupling, it is a second
        path. The middle of this task moved to an HTTP call; the reading of the
        rows and the writing of fd_scores did not.

        THE FAILURE IS EXCHANGED, NOT ADDED. This task was already red when the
        registry was unreachable; it is now red when the service is. One
        external dependency replaces another, and the remedy is named in the
        message rather than left in a traceback.

        fd_scores IS STILL WRITTEN HERE, deliberately: its foreign key to
        fd_transactions guarantees the row exists, and that guarantee belongs
        to whoever owns the data. fd_scoring_service records only the calls it
        answers for third parties -- a decision is recorded once, by whoever
        holds what makes it re-derivable. sql/ddl/195_scoring_decisions.sql is
        the view that reads the two journals as one.

        Covers: EE09, EI04, EC16, EC24
        Lot: N7 (decouple en L5, 20/11/2026)
        Competency: C3
        """
        import uuid as _uuid

        from ml4fd import config, db, score

        settings = config.load()
        batch_id = _uuid.UUID(polled["batch_id"])

        with db.connect("fd_ingest") as conn:
            frame = score.rows_to_score(conn, batch_id)
            if not len(frame):
                # NO MODEL VERSION TO REPORT, and None rather than a lookup:
                # nothing was scored, so no model answered. Asking the service
                # which version it WOULD have used would put a name on a
                # decision that was never taken.
                log.info("aucune ligne a scorer dans ce lot")
                conn.commit()
                return {"scored": 0, "flagged": 0, "model_version": None}
            rows = score.remote_rows(
                frame, settings.scoring_endpoint,
                settings.scoring_endpoint_timeout_seconds)
            written = score.write(conn, rows)
            conn.commit()

        version = rows[0]["model_version"]
        threshold = rows[0]["threshold"]
        flagged = sum(1 for row in rows if row["flagged"])
        log.info("score %d transaction(s) par %s avec la version %s, seuil "
                 "%.2f : %d au-dessus", written, settings.scoring_endpoint,
                 version, threshold, flagged)
        return {"scored": written, "flagged": flagged,
                "model_version": version}

    @task
    def raise_and_notify_fraud(scored: dict) -> dict:
        """Raise the alerts and notify THE ANALYST, within the declared capacity.

        Named for its reader since N9, which added the second audience: this
        task notifies a flagged TRANSACTION and raise_and_notify_ops notifies a
        state of the CHAIN. The queue this one feeds is the one the capacity of
        60 a day exists to protect.

        Covers: EI05, EC09, EC14
        Lot: N7
        Competency: C3, C4
        """
        from ml4fd import config, db, notify

        settings = config.load()
        with db.connect("fd_ingest") as conn:
            raised = notify.raise_alerts(conn)
            conn.commit()
            report = notify.notify(conn, settings)

        log.info("notification : %s", notify.render(report))
        return {"raised": raised, "sent": report["sent"],
                "withheld": report["withheld"], "failed": report["failed"]}

    @task
    def close_run(polled: dict, counts: dict, alerted: dict) -> dict:
        """Record the volumes and close the run as a success.

        Fed to fd_pipeline_runs exactly as S1 does, so M1's guard watches this
        process too -- a window whose runs all failed must refuse to train.

        Covers: EC14, EC23
        Lot: N7
        Competency: C4
        """
        from ml4fd import db, runs

        with db.connect("fd_ingest") as conn:
            runs.record_counts(conn, polled["run_id"], counts["n_read"],
                               counts["n_valid"], counts["n_rejected"])
            runs.finish(conn, polled["run_id"], runs.SUCCESS)
            conn.commit()
        return {"run_id": polled["run_id"], "alerts": alerted["raised"]}

    @task
    def check(closed: dict) -> dict:
        """The four controls of diagram 3.3, read AFTER the run was closed.

        AFTER, AND THAT IS NOT A PREFERENCE. The controls read
        fd_pipeline_runs, and the counters and the final state are written by
        close_run. Placed before it, this task would read its own run as still
        'running' with a null n_read and report an empty run every minute --
        a control that fires on its own execution.

        IT RAISES ON NOTHING A CONTROL FOUND. A red control is this task's
        normal output, not an error: failing the run on one would stop the
        ingestion the control exists to watch. It does fail on its own
        infrastructure -- an unreachable database -- because that is a red task
        and not a verdict.

        Covers: EC14
        Lot: N9
        Competency: C4
        """
        from ml4fd import config, db, health

        settings = config.load()
        with db.connect("fd_ingest") as conn:
            report = health.check(conn, settings, DAG_ID, closed["run_id"])

        log.info("controles : %s", health.render(report))
        return report

    @task
    def raise_and_notify_ops(report: dict) -> dict:
        """Tell the pipeline owner what CHANGED, and record that it went out.

        ON TRANSITION, NEVER ON CONDITION: 1,440 runs a day would send 1,440
        identical messages while a control stayed red. fd_ops_state holds the
        last state and health.notify_ops sends only a change -- the return to
        green included.

        A SEND THAT FAILS RAISES HERE, deliberately. The state is NOT recorded,
        so the next run sees the same transition and tries again; and the red
        task is the only thing left that can tell anybody the channel for
        telling anybody is broken. The journal keeps its success either way:
        the ingestion did succeed, and runs.abandon() will not take that back.

        Covers: EC14, EC17
        Lot: N9
        Competency: C3, C4
        """
        from ml4fd import config, db, health

        settings = config.load()
        with db.connect("fd_ingest") as conn:
            sent = health.notify_ops(conn, settings, report)

        if sent["failed"]:
            raise AssertionError(
                "canal d'exploitation %r : %d transition(s) non transmise(s). "
                "L'etat n'est PAS enregistre, donc la prochaine execution "
                "reessaiera ; cette tache rouge est le seul avertissement "
                "restant quand le canal d'avertissement est casse."
                % (sent["channel"], sent["failed"]))
        if sent["sent"]:
            log.warning("exploitation : %d message(s) envoye(s) sur %s",
                        sent["sent"], sent["channel"])
        return sent

    polled = poll()
    counts = validate(polled)
    n_typed = type_and_feature(polled, counts)
    scored = score_batch(polled, n_typed)
    alerted = raise_and_notify_fraud(scored)
    closed = close_run(polled, counts, alerted)
    # (3c) of diagram 3.3: the controls, then the operations channel. After
    # close_run, because the controls read the counters it writes.
    raise_and_notify_ops(check(closed))


ingest_realtime()
