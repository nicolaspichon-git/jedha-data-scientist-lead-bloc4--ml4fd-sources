"""One real-time ingestion cycle, run by hand.

Covers: EE01, EE02, EI01, EC14, EC25
Lot: N2
Competency: C2, C5

WHY THIS EXISTS RATHER THAN A DAG. The DAG is lot N7. N2's acceptance criterion
is that a deliberately corrupted response reaches the quarantine with its coded
reason and that the counters close -- which is about validation, not about
orchestration. This runner exercises exactly that chain, and N7 will wrap the
same functions without rewriting any of them.

THE COUNTER CLOSURE, and a decision it forces. fd_pipeline_runs carries
CHECK (n_read = n_valid + n_rejected). For a stream, what is "read"? The
readings written to the raw zone -- a FAILED HTTP CALL IS NOT A READ LINE, since
it produced no record. The failed calls are counted and logged separately, and
they belong to N8's acceptance rather than to this closure.

Usage, from the scheduler container:
    python3 /opt/project/tools/ingest_once.py [--calls 4] [--corrupt]
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("ingest_once")


def corrupt(reading):
    """Break one reading on purpose, for EC25.

    Two defects at once, of both classes: an amount in letters (a field defect)
    and a missing mandatory column (a schema defect). One line, several reasons
    -- which is what UNIQUE (batch_id, line_no, reason_code) was built for.
    """
    broken = dict(reading.row)
    broken["amt"] = "beaucoup"
    broken.pop("merchant", None)
    object.__setattr__(reading, "row", broken)
    return reading


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="One real-time ingestion cycle: poll, raw zone, validate, "
                    "quarantine, type, verdict, counters.",
        epilog="--corrupt breaks the last reading on purpose, to show that a "
               "bad response reaches the quarantine with its coded reason "
               "instead of entering fd_transactions.")
    parser.add_argument("--calls", type=int, default=None,
                        help="calls to make (default: realtime.calls_per_run)")
    parser.add_argument("--corrupt", action="store_true",
                        help="break the last reading, for EC25")
    args = parser.parse_args(argv)

    # NO truth, NO verdicts: tests/test_leak.py reads these imports, so the
    # separation is checked and not merely described.
    from ml4fd import (checks, config, db, features, notify, quarantine, raw,
                       runs, score, stream, transform)

    settings = config.load()
    calls = args.calls if args.calls is not None else settings.calls_per_run
    run_id = "manual__%s" % dt.datetime.now(dt.timezone.utc).strftime(
        "%Y%m%dT%H%M%S")
    batch_id = raw.batch_id_for(run_id)

    log.info("run %s, batch %s, %d appel(s)", run_id, batch_id, calls)

    # --- collect (N1)
    readings, failures = stream.poll(settings.api_endpoint, calls,
                                     spacing_s=settings.call_spacing_s,
                                     timeout=settings.api_timeout_s)
    if args.corrupt and readings:
        corrupt(readings[-1])
        log.warning("la derniere lecture a ete corrompue volontairement (EC25)")
    if not readings:
        log.error("aucune lecture : %d appel(s) en echec. Rien a ingerer.",
                  len(failures))
        return 1

    with db.connect("fd_ingest") as conn:
        runs.start(conn, run_id, "ingest_once")
        runs.set_batch(conn, run_id, batch_id)

        # --- raw zone (N1)
        n_read = stream.write_batch(conn, batch_id, readings)
        runs.record_batch_digest(conn, run_id, batch_id)       # S5/Q1

        # --- validate and quarantine (N2)
        accepted, items = stream.validate(
            readings, settings.max_amount, settings.clock_tolerance_s)
        rejected_lines, reason_rows = quarantine.write(conn, batch_id, items)

        # --- type, pseudonymise, verdict (N3, reusing L3 and L5 unchanged)
        skip = {rank for rank, _, _ in items}
        records = stream.valid_records(readings)
        # STREAM_PROFILE, and the same one for both: the FK from
        # fd_ground_truth to fd_transactions is on txn_id, so two derivations
        # would break every verdict.
        n_typed = transform.load_transactions(
            conn, batch_id, records, settings.pepper, skip,
            profile=checks.STREAM_PROFILE)
        # N3: the per-row mode, in anger for the first time. Scoped to the
        # batch, which is sound only because every stream transaction is later
        # than everything stored -- and apply_batch CHECKS that rather than
        # assuming it.
        n_features = features.apply_batch(conn, batch_id)

        # --- score (N4). No verdict exists for these transactions yet: N6
        # moved the write into its own daily DAG, so the scoring could not
        # reach a label even by changing this file. It reads fd_transactions,
        # which has no column to hold one.
        #
        # A tracking server that is down loses the scoring, not the ingestion:
        # the transactions, their features and their verdicts are already
        # written, and a replay of the batch scores them. So the failure is
        # reported and the cycle continues.
        n_scored = 0
        try:
            # From the environment, as dag_train reads it: the URI
            # is infrastructure, so it lives in docker-compose.yml and
            # not in governance.yml, which holds decisions.
            scorer = score.load(
                settings,
                tracking_uri=os.environ.get("MLFLOW_TRACKING_URI"))
            frame = score.rows_to_score(conn, batch_id)
            if len(frame):
                rows = score.as_rows(frame, scorer.scores(frame), scorer)
                n_scored = score.write(conn, rows)
                flagged = sum(1 for row in rows if row["flagged"])
                log.info("score %d transaction(s) avec %s v%s, seuil %.2f : "
                         "%d au-dessus", n_scored, score.registry.REGISTERED_NAME,
                         scorer.model_version, scorer.threshold, flagged)
            else:
                log.info("aucune ligne a scorer dans ce lot")
        except Exception as problem:            # noqa: BLE001
            log.warning("scoring non effectue : %s", problem)
            log.warning("l'ingestion se poursuit ; rejouer le lot scorera "
                        "ces transactions")

        # NO VERDICT IS WRITTEN HERE, and its absence is the fourth guarantee
        # of N6. The API delivers is_fraud in the SAME payload as the features,
        # so the label sits one line of code away from the scoring; moving the
        # write into its own daily DAG (dag_ingest_verdicts) means a change to
        # the scoring cannot reach the verdict code even by accident, which a
        # shared task cannot promise.
        #
        # Nothing is lost by waiting: the verdict is not due for the declared
        # delay anyway, the raw zone keeps the payload append-only, and the
        # daily run repairs every batch that has none -- self-healing like N5's
        # alert raising.
        #
        # To see it now:  .\fd-manager.ps1 verdicts

        # --- the closure
        runs.record_counts(conn, run_id, n_read=n_read,
                           n_valid=len(accepted), n_rejected=rejected_lines)
        runs.finish(conn, run_id, "success")
        conn.commit()

        # --- alert and notify (N5), AFTER the commit of the ingestion.
        #
        # Deliberately outside the ingestion's transaction. Notifying commits
        # per alert -- which is what bounds a crash to one duplicated
        # notification instead of re-sending everything already delivered -- and
        # a notifier that committed inside the cycle would publish a half-done
        # ingestion. Raising an alert is also self-healing (it scans every
        # flagged score, not this batch), so a failure here is repaired by the
        # next run rather than lost.
        try:
            n_raised = notify.raise_alerts(conn)
            conn.commit()
            report = notify.notify(conn, settings)
            log.info("notification : %s", notify.render(report))
        except Exception as problem:            # noqa: BLE001
            log.error("alerte ou notification en echec : %r", problem)
            log.error("les transactions, scores et verdicts sont deja "
                      "enregistres ; le prochain passage reprend les alertes.")
            n_raised = 0

        # AFTER the commit, and guarded. The breakdown is REPORTING, not work:
        # the first version displayed it inside the transaction and a wrong
        # tuple unpacking there rolled back a perfectly good ingestion --
        # quarantine included, which is why nothing was found in the table.
        if rejected_lines:
            log.info("")
            log.info("quarantaine : %d ligne(s), %d motif(s)",
                     rejected_lines, reason_rows)
            try:
                for reason_class, code, rows, lines in quarantine.breakdown(
                        conn, batch_id):
                    log.info("  %-7s %-24s %d motif(s) sur %d ligne(s)",
                             reason_class, code, rows, lines)
            except Exception as problem:            # noqa: BLE001
                log.warning("  detail indisponible : %s", problem)

        # L4's reconciliation, replayed. S3 is the first process that DEPENDS
        # on the two modes agreeing: the backfill wrote the history, the per-row
        # function wrote this batch, and a divergence would mean the model is
        # scored on values it was not trained on.
        log.info("...calcul de reconciliation en cours...")
        divergences = features.reconcile(conn)
        if divergences:
            log.error("RECONCILIATION L4 EN ECHEC : %d divergence(s)",
                      len(divergences))
            for row in divergences[:5]:
                log.error("  %s", row)
            return 1
        strata = features.coverage(conn)
        log.info("reconciliation L4 : aucune divergence, strates %s", strata)

    log.info("")
    log.info("lu %d, valide %d, rejete %d  ->  %d transaction(s), "
             "%d features, %d score(s), %d alerte(s)",
             n_read, len(accepted), rejected_lines, n_typed, n_features,
             n_scored, n_raised)
    log.info("aucun verdict ecrit : c'est le DAG dag_ingest_verdicts, "
             "quotidien, qui s'en charge (N6)")
    log.info("appels en echec : %d  (hors cloture : un appel rate n'est pas "
             "une ligne lue)", len(failures))
    for attempt in failures:
        log.info("  HTTP %-6s %.1f s  %s",
                 attempt.status or "reseau", attempt.latency_s, attempt.error)
    log.info("")
    log.info("batch %s", batch_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
