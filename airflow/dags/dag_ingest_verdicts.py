"""S3 / N6: write the withheld verdicts. A DAG of its own, deliberately.

Covers: EE03, EI03, EI06, EI12, EC14, EC23
Lot: N6
Competency: C1, C3, C4

WHY A SEPARATE DAG, when the ingestion already had the payload in hand. The API
delivers is_fraud in the SAME payload as the features -- so the label sits one
line of code away from the scoring, and no code review proves it did not get
there. Three of N6's four guarantees are structural and already hold; this DAG
is the fourth: the verdict write shares no task, no transaction and no object
with the scoring. It starts again from the raw zone, the only append-only copy
of what the API said.

It is DEFENCE IN DEPTH and not the primary mechanism. What actually withholds a
verdict is label_ts = txn_ts + delay together with every consumer filtering on
label_ts <= cut_off -- the mechanism M3's training set and L4's reconciliation
already rest on, exercised on 555,719 rows. This DAG adds that a change to the
scoring cannot reach the verdict code even by accident, which a shared task
cannot promise.

A DISTINCT CADENCE, which EI06 asks for: the ingestion runs every minute, this
runs once a day. Nothing is lost by waiting -- the verdict is not due for the
declared delay anyway -- and a daily run that repairs every batch missing a
verdict is self-healing, like N5's alert raising.
"""
from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

DAG_ID = "dag_ingest_verdicts"


@dag(
    dag_id=DAG_ID,
    description="S3/N6: write the withheld verdicts of the stream",
    schedule="0 2 * * *",               # daily, 02:00 -- a cadence of its own
    start_date=pendulum.datetime(2026, 1, 5, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    tags=["S3", "N6", "BC03", "C3", "C4"],
    default_args={"retries": 0},
)
def write_verdicts():

    @task
    def write() -> dict:
        """Write every missing verdict of the stream, and count the band.

        Covers: EI03, EI12, EC23
        Lot: N6
        Competency: C3, C4
        """
        from ml4fd import config, db, stream, verdicts

        settings = config.load()
        delay = settings.verdict_delay()

        with db.connect("fd_ingest") as conn:
            report = verdicts.write_due(conn, delay, stream.SOURCE)
            conn.commit()
            band = verdicts.withheld(conn, stream.SOURCE)

        log.info("%s", verdicts.render(report, band, delay))

        # THE BAND, CHECKED AND NOT ASSUMED. Every verdict must carry the same
        # declared delay: more than one distinct value would mean the delay
        # changed between two runs, and then the blind band of a training set
        # built across both is not the one its recipe claims.
        if band["distinct_delays"] > 1:
            log.warning(
                "%d delais distincts entre txn_ts et label_ts (%s a %s) : le "
                "delai a change entre deux passages, donc la bande aveugle "
                "n'est pas uniforme et un jeu d'entrainement a cheval sur les "
                "deux ne porte pas la bande que sa recette annonce.",
                band["distinct_delays"], band["min_delay"], band["max_delay"])

        if report["rows"] and not report["written"]:
            raise AssertionError(
                "%d ligne(s) attendaient un verdict et aucun n'a ete ecrit"
                % report["rows"])
        return {"written": report["written"],
                "not_yet_due": band["not_yet_due"],
                "verdicts": band["verdicts"]}

    write()


write_verdicts()
