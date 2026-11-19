"""S4 / P4: the morning report, one run a day.

Covers: EE04, EE06, EI04, EI06, EC09, EC14
Lot: P4
Competency: C1, C3, C4

THE ONLY DAG OF THIS PROJECT WHERE catchup=True, and that is a consequence of
the two axes of time rather than a convenience. A missing morning must be
catchable, and a catch-up has to produce the document OF THE DAY IT COVERS with
the KNOWLEDGE OF THAT DAY -- not that day seen with today's knowledge. Nothing
else here can be caught up: the stream returns the transaction of the instant,
so a backfill of dag_ingest would replay a past it cannot collect, and a
backfill of dag_train would retrain on data that has moved.

THE CADENCE IS NOT THE WINDOW, and confusing the two is the defect this
docstring exists to prevent. Airflow's interval for "0 6 * * *" runs from 06:00
to 06:00:

    the run that FIRES on 17 November at 06:00 carries
        data_interval_start = 16 November 06:00
        data_interval_end   = 17 November 06:00

A report built on that interval would cover from six in the morning to six in
the morning, which is not a day an analyst recognises and would split every
night's fraud peak -- measured at 84 of every 100 frauds between 22h and 03h --
across two documents. So the COVERED DAY IS DERIVED: it is the calendar day of
data_interval_start, read in the declared zone, and the window is that day from
midnight to midnight. The run above therefore covers 16 November entirely.

The derivation is deterministic, so the property that matters survives: a
catch-up of 3 November computes exactly the window the original run computed,
and ml4fd/report.py carries no production timestamp, so the document is
identical to the byte.

THE KNOWLEDGE DATE IS data_interval_end, never the clock. It is the instant the
run is for -- 06:00 the morning after -- so a catch-up months later still asks
"what was known at six on the 4th of November", and section 3 shows the verdicts
that had fallen by then and no others.

TWO ROLES, AND THE JOURNAL IS NOT THE REPORT'S. The report runs under fd_report,
which has no access to the raw zone and may write only its own journal
(fd_reports). The pipeline journal, fd_pipeline_runs, is the pipeline's and is
written under fd_ingest. Hence three tasks rather than one: an operator who
reads a red task must see which of the two failed.

NO COUNTS IN fd_pipeline_runs for this DAG, deliberately. "Lines read, valid,
rejected" has no meaning for a report, and n_read = n_payments would be a figure
invented to fill a column. What the report covered is in fd_reports, which was
built for it.
"""
from __future__ import annotations

import datetime as dt
import logging
import zoneinfo

import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

DAG_ID = "dag_daily"


def _mark_failed(context) -> None:
    """Close the run as FAILED. A CALLBACK MUST NEVER RAISE.

    The convention of this repository since N9: a callback on the DAG and not a
    task carrying trigger_rule="one_failed", because dagrun_timeout fails the
    RUN and a task could then never be scheduled. runs.abandon() marks only what
    is still RUNNING, so a failure after close_run cannot overwrite a recorded
    success.

    Covers: EC14
    Lot: P4
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
    description="S4/P4: the morning report of the previous calendar day",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 11, 16, tz="Europe/Paris"),
    # THE ONLY TRUE HERE. See the module docstring: a missing morning must be
    # catchable, and a catch-up produces the document of ITS day with the
    # knowledge of ITS day.
    catchup=True,
    # One at a time, so a catch-up of several days runs in order and two reports
    # never write while the other records.
    max_active_runs=1,
    dagrun_timeout=dt.timedelta(minutes=30),
    tags=["S4", "P4", "BC03", "C2", "C3", "C4"],
    default_args={"retries": 1, "retry_delay": dt.timedelta(minutes=2)},
    on_failure_callback=_mark_failed,
)
def daily():

    @task
    def open_run() -> dict:
        """Open the pipeline journal. Under fd_ingest: the journal is the
        pipeline's, not the report's.

        Covers: EC14
        Lot: P4
        Competency: C4
        """
        from airflow.operators.python import get_current_context

        from ml4fd import db, runs

        run_id = str(get_current_context()["run_id"])
        with db.connect("fd_ingest") as conn:
            runs.start(conn, run_id, DAG_ID)
            conn.commit()
        return {"run_id": run_id}

    @task
    def produce(opened: dict) -> dict:
        """Gather, render, write the document and record it in fd_reports.

        THE WINDOW IS DERIVED FROM THE INTERVAL AND NEVER FROM THE CLOCK, which
        is what makes a catch-up produce the document of its own day. See the
        module docstring for why the covered day is not the interval.

        Covers: EE04, EE06, EI04
        Lot: P4
        Competency: C2, C3
        """
        from airflow.operators.python import get_current_context

        from ml4fd import config, db, report

        context = get_current_context()
        settings = config.load()
        zone = zoneinfo.ZoneInfo(settings.report_timezone)

        # The calendar day of the interval's START, read in the declared zone.
        # Its window is report.window_for()'s -- the one definition the
        # dashboard and the checker read as well.
        day = context["data_interval_start"].astimezone(zone).date()
        window = report.window_for(settings, day,
                                   knowledge_at=context["data_interval_end"])

        log.info("journee couverte %s, connaissance au %s",
                 window.day, window.knowledge_at)

        with db.connect("fd_report") as conn:
            recorded = report.produce(conn, settings, window)

        # XCOM CARRIES JSON, so a date, a datetime and a Decimal cannot travel.
        # The defect of 16 November 2026, eleven frames deep in another DAG:
        # converted here, at the boundary, rather than discovered there.
        return {"report_date": recorded["report_date"].isoformat(),
                "knowledge_date": recorded["knowledge_date"].isoformat(),
                "path": recorded["path"],
                "content_digest": recorded["content_digest"],
                "n_payments": int(recorded["n_payments"]),
                "n_alerts": int(recorded["n_alerts"]),
                "n_verdicts": int(recorded["n_verdicts"]),
                "n_pending": int(recorded["n_pending"]),
                "model_version": recorded["model_version"],
                "threshold": (float(recorded["threshold"])
                              if recorded["threshold"] is not None else None)}

    @task
    def close_run(opened: dict, produced: dict) -> dict:
        """Close the journal as a success. No counts: see the module docstring.

        Covers: EC14
        Lot: P4
        Competency: C4
        """
        from ml4fd import db, runs

        with db.connect("fd_ingest") as conn:
            runs.finish(conn, opened["run_id"], runs.SUCCESS)
            conn.commit()
        log.info("rapport du %s : %d paiement(s), %d alerte(s), %d verdict(s), "
                 "%d en attente -> %s",
                 produced["report_date"], produced["n_payments"],
                 produced["n_alerts"], produced["n_verdicts"],
                 produced["n_pending"], produced["path"])
        return produced

    opened = open_run()
    close_run(opened, produce(opened))


daily()
