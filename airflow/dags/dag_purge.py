"""S5 / Q5: the purge, once a week, on Sunday.

Block: B3, B4.
Lot: B3-Q5, B4-L0.
Competency: B3-C3, B3-C4.
Covers: B3-EE06, B3-EI06, B3-EI11, B3-EC09, B3-EC14, B3-EC19.

THE ONLY DAG OF THIS PROJECT THAT DESTROYS. Everywhere else a mistake is
repaired by a replay; here it is permanent, and every choice below follows from
that asymmetry rather than from habit.

WEEKLY AND NOT DAILY, for the opposite of the usual reason. A purge at ninety
days has no daily urgency -- nothing is gained by removing on Monday what could
go on Sunday -- and A DESTRUCTIVE TASK THAT RUNS RARELY IS A TASK WHOSE FAILURE
IS NOTICED. A daily purge that breaks on a Tuesday is a line in a log; a weekly
one that breaks is a red run somebody sees.

04:00 ON SUNDAY, and the hour is a dependency rather than a preference: the
purge deletes batches of the raw zone, and dag_ingest_verdicts READS the raw
zone at 02:00 to write the verdicts that have fallen. At the same instant the
purge would take batches out from under it. dag_train holds Monday 03:00 and
dag_daily 06:00, so Sunday at four is the hour where nothing else runs.

NO catchup, AND THAT IS THE OPPOSITE OF dag_daily. A missed morning report must
be produced for the day it covers, with the knowledge of that day -- so that
DAG catches up. A missed purge needs nothing of the kind: the rows it would
have deleted are still there next Sunday, and the boundary will simply be a
week later. Replaying old purge windows would delete against a stale boundary
for no gain at all.

THE BOUNDARY IS data_interval_end, NEVER THE CLOCK. The same discipline as the
morning report, for a sharper reason: a purge whose boundary came from now()
could not be reasoned about after the fact ("what did the run of 22 November
delete?") and could not be replayed in a demonstration either.

AND THE OVERRIDE OF Q2 IS NOT EXPOSED HERE. ml4fd.purge.plan() takes a `before`
that moves the boundary for every store at once; it exists so that a purge can
be tried without waiting until February 2027, and it belongs to the command
line. A parameter of a demonstration left in an orchestrator ends up being
used -- which here means deleting data that had not reached its retention.

THE GUARD STOPS THE RUN BEFORE ANYTHING GOES. check_share is a task of its own
and it fails rather than warning afterwards: beyond the declared share
(purge.max_share) the run is red on Sunday morning and nothing has been
deleted. Confirming such a purge is a human act, and it is done by hand with
`fd-manager purge -Apply -Force`.

ONE TASK FOR THE TWO STORES, deliberately against the plan of S5, which
foresaw purge_raw and purge_quarantine side by side. The quarantined rows of a
batch derive from its raw lines and must go FIRST (ml4fd/purge.py,
PURGE_ORDER): wiring two tasks here would put that order in a second place,
where a later hand could reverse it by dragging an arrow. The log shows one
line per store and per batch, so nothing is lost in readability.

TWO ROLES. The deletion runs under fd_purge, the only role in the project that
may delete, and which cannot read the exploited layer at all
(sql/ddl/120_purge_role.sql). The pipeline journal is the pipeline's and is
written under fd_ingest, as in dag_daily -- fd_purge may not insert into it.
"""
from __future__ import annotations

import datetime as dt
import logging

import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

DAG_ID = "dag_purge"


def _mark_failed(context) -> None:
    """Close the run as FAILED. A CALLBACK MUST NEVER RAISE.

    The convention of this repository since N9: a callback on the DAG and not a
    task carrying trigger_rule="one_failed", because dagrun_timeout fails the
    RUN and such a task could then never be scheduled. runs.abandon() marks
    only what is still RUNNING, so a failure after close_run cannot overwrite a
    recorded success.

    Block: B3, B4.
    Lot: B3-Q5, B4-L0.
    Competency: B3-C4.
    Covers: B3-EC14.
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
    description="S5/Q5: purge beyond retention, weekly",
    schedule="0 4 * * 0",                   # Sunday 04:00 -- see the docstring
    start_date=pendulum.datetime(2026, 11, 16, tz="Europe/Paris"),
    # NOT caught up: a missed purge has nothing to replay, and an old boundary
    # would delete against a week that no longer means anything.
    catchup=False,
    # Two purges at once would fight over the same batches, and the loser would
    # read a digest the winner had just made obsolete.
    max_active_runs=1,
    dagrun_timeout=dt.timedelta(hours=2),
    tags=["S5", "Q5", "BC03", "C2", "C3", "C4"],
    # NO RETRY. The work is idempotent and resumable -- a batch half deleted
    # carries its mark and the next run finishes it -- so a retry buys nothing
    # that waiting does not, and a destructive job should surface its failure
    # rather than quietly try again.
    default_args={"retries": 0},
    on_failure_callback=_mark_failed,
)
def purge_beyond_retention():

    @task
    def open_run() -> dict:
        """Open the pipeline journal. Under fd_ingest: the journal is the
        pipeline's, and fd_purge may not write to it.

        Block: B3, B4.
        Lot: B3-Q5, B4-L0.
        Competency: B3-C4.
        Covers: B3-EC14.
        """
        from airflow.operators.python import get_current_context

        from ml4fd import db, runs

        run_id = str(get_current_context()["run_id"])
        with db.connect("fd_ingest") as conn:
            runs.start(conn, run_id, DAG_ID)
            conn.commit()
        return {"run_id": run_id}

    @task
    def dry_run(opened: dict) -> dict:
        """Count what would go, and name what is refused. DELETES NOTHING.

        verify=False here, and that is not a shortcut: recomputing every
        candidate batch's fingerprint costs an md5 over 555 thousand rows and
        some 200 MB for a historical batch, and the executor verifies each
        batch again immediately before deleting it -- which is the check that
        decides. Asking twice would double the cost of the one that does not.

        Block: B3, B4.
        Lot: B3-Q5, B4-L0.
        Competency: B3-C5.
        Covers: B3-EC26, B3-EI13.
        """
        from airflow.operators.python import get_current_context

        from ml4fd import config, db, purge

        context = get_current_context()
        settings = config.load()
        # THE BOUNDARY. data_interval_end is the instant this run is for; no
        # `before` is passed, and none can be: see the module docstring.
        as_of = context["data_interval_end"]

        with db.connect(purge.ROLE) as conn:
            report = purge.plan(conn, settings, as_of=as_of)

        log.info("%s", purge.render(report))
        return report

    @task
    def check_share(report: dict) -> dict:
        """Stop the run if the purge would exceed the declared share.

        A TASK OF ITS OWN, AND IT FAILS. The plan of S5 asked for exactly this
        rather than purging and warning afterwards: a purge of 90 % of a store
        may be legitimate -- a retention shortened on purpose -- but it is
        never a routine, and what tells the two apart is a human saying so.

        Block: B3, B4.
        Lot: B3-Q5, B4-L0.
        Competency: B3-C4.
        Covers: B3-EC26, B3-EC09.
        """
        from ml4fd import purge

        try:
            purge.assert_allowed(report)
        except purge.PurgeRefused as refused:
            log.error("%s", refused)
            log.error("Pour la confirmer en connaissance de cause : "
                      ".\\fd-manager.ps1 purge -Apply -Force")
            raise
        log.info("part declaree respectee : %d ligne(s) a supprimer sur "
                 "%d lot(s)", report["rows"], report["batches"])
        return report

    @task
    def delete(opened: dict, report: dict) -> dict:
        """Delete. ROWS LEAVE THE DATABASE HERE.

        confirm=False, so ml4fd.purge.execute() applies the guard a second
        time: check_share above is the readable gate, this is the one that
        cannot be removed by rewiring the DAG.

        The run_id travels from open_run so that fd_purges names the Airflow
        run that did it. A purge run by hand records a null there, which is a
        fact worth keeping rather than hiding behind an invented identifier.

        Block: B3, B4.
        Lot: B3-Q5, B4-L0.
        Competency: B3-C2, B3-C3.
        Covers: B3-EE05, B3-EI11, B3-EC19.
        """
        from airflow.operators.python import get_current_context

        from ml4fd import config, db, purge

        context = get_current_context()
        settings = config.load()

        with db.connect(purge.ROLE) as conn:
            summary = purge.execute(conn, settings, report,
                                    context["data_interval_end"],
                                    run_id=opened["run_id"])

        log.info("%s", purge.render_execution(summary))
        return summary

    @task
    def close_run(opened: dict, summary: dict) -> dict:
        """Close the journal as a success.

        NO COUNTS IN fd_pipeline_runs for this DAG, as for dag_daily: "lines
        read, valid, rejected" has no meaning for a purge, and n_read = rows
        deleted would be a figure invented to fill a column. What was deleted
        is in the purge marks of the batches themselves, and Q6 journals it.

        Block: B3, B4.
        Lot: B3-Q5, B4-L0.
        Competency: B3-C4.
        Covers: B3-EC14.
        """
        from ml4fd import db, runs

        with db.connect("fd_ingest") as conn:
            runs.finish(conn, opened["run_id"], runs.SUCCESS)
            conn.commit()
        log.info("purge : %d ligne(s) supprimee(s) en %d lot(s), %d refus",
                 summary["rows"], summary["batches"],
                 len(summary["refused"]))
        return summary

    opened = open_run()
    close_run(opened, delete(opened, check_share(dry_run(opened))))


purge_beyond_retention()
