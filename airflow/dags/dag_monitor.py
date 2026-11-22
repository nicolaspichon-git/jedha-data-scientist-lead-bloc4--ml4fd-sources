"""B4-L4 -- monitoring the model in service, and asking for a retraining.

Block: B4.
Lot: B4-L4.
Competency: B4-C6, B4-C5.
Covers: B4-EC38, B4-EC39, B4-EC40, B4-EC42, B4-EC44, B4-EC45, B4-EC33.

THE DAG THAT CLOSES THE LOOP, and the only one here that triggers another. S2
trains on a cadence; B4-L3 decides what gets served; this is what notices that
what is served no longer matches the world, and asks for a new one. Without it
the two loops of the project never meet: the code loop runs on commits, the
model loop would run on nothing but the calendar.

ALL THE LOGIC SITS IN ml4fd/monitor.py, as S1's does in the package: the
decision layer is a set of pure functions over dictionaries, so the whole of
B4-C6's policy is exercisable by the test suite without Evidently, without Airflow
and without a database. This file orders the steps and owns none of them.

IT RAISES ON NOTHING IT MEASURES. A drifted input and a crossed floor are this
DAG's normal output, not errors. Failing the run on a red control would stop
the monitoring that the control exists to provide, and the operator would lose
the series exactly when it became interesting -- the same decision health.check
took for S3, and for the same reason.

WHAT IT DOES NOT DO. It does not retrain, it does not promote, and it does not
decide whether the candidate is any good. It asks dag_train for a candidate;
the gate of B4-L3 judges it. That separation is what lets the trigger be tuned to
miss little: the expensive mistake -- serving a bad model -- is downstream and
already guarded.
"""
from __future__ import annotations

import datetime as dt
import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from ml4fd import config

log = logging.getLogger(__name__)

DAG_ID = "dag_monitor"
TRAIN_DAG_ID = "dag_train"

# THE TWO BRANCH TARGETS, NAMED ONCE. @task.branch chooses by returning a
# task_id AS A STRING, so the branch and the tasks it picks between are two
# declarations of one name -- and on 2026-11-20 they disagreed. The branch
# returned "monitor_model.ask_for_retraining", prefixed with the name of the
# @dag function, while the task was plainly "ask_for_retraining": Airflow
# refused the run with "branch_task_ids must contain only valid task_ids".
# A @dag function is not a TaskGroup and adds no prefix; only a TaskGroup does.
#
# The refusal came at the worst moment -- after the drift had been injected,
# measured, and the alert raised -- because nothing before execution compares
# a returned string to a declared task_id. tests/test_dag_monitor.py now does.
ASK_FOR_RETRAINING = "ask_for_retraining"
NO_RETRAINING = "no_retraining"
RECORD_REQUEST = "record_request"


@dag(
    dag_id=DAG_ID,
    description="B4-L4: drift, performance, and the retraining they ask for",
    # THE CADENCE IS NEITHER WINDOW. The windows say what is measured; this
    # says how often it is looked at. Hourly in nominal -- short enough that a
    # drift is seen the same morning, long enough that the series does not fill
    # with near-identical rows, since fd_monitoring is kept for the long run.
    # Every two minutes in demo, which is what makes the five-minute criterion
    # reachable.
    #
    # READ FROM GOVERNANCE AT PARSE TIME, which departs from the five other
    # DAGs: each of those repeats its cron as a literal and a test ties it to
    # governance.yml. That convention keeps a DAG parseable without governance
    # and is right for a cadence nobody changes; it is wrong for a
    # demonstration knob, because flipping monitoring.mode would then need a
    # second edit in this file. config.monitoring_schedule() carries the full
    # argument. Every airflow service of the compose holds POSTGRES_*, so this
    # call cannot make the file parse in one container and fail in another.
    schedule=config.load().monitoring_schedule(),
    start_date=pendulum.datetime(2026, 11, 16, tz="Europe/Paris"),
    catchup=False,                      # a past window cannot be re-measured
    max_active_runs=1,
    tags=["B4-L4", "BC04", "B4-C5", "B4-C6"],
    default_args={"retries": 0},
)
def monitor_model():

    @task
    def champion() -> dict:
        """The version in service, and the figures it was published with.

        FROM THE ALIAS AND NEVER FROM THE NEWEST VERSION, which is B4-L3's whole
        point: the drift of the inputs is a property of the stream, but the
        performance and the flag rate are properties of the model that is
        actually serving. Measuring the newest registered version would report
        on a model nobody is running.
        """
        import os

        import mlflow

        from ml4fd import config, registry, score

        settings = config.load()
        uri = os.environ.get("MLFLOW_TRACKING_URI")
        if uri:
            mlflow.set_tracking_uri(uri)
        client = mlflow.MlflowClient()

        version = registry.champion(client, alias=settings.champion_alias)
        if version is None:
            raise LookupError(
                "no version carries the alias %r, so nothing is in service "
                "and there is nothing to monitor. Run .\\fd-manager.ps1 "
                "promote." % settings.champion_alias)

        params = dict(client.get_run(version.run_id).data.params)
        dataset_version = params.get("dataset_version")
        payload = score.read_manifest(
            score.manifest_path(settings, str(dataset_version)))
        return {"model_version": str(version.version),
                "dataset_version": str(dataset_version),
                "features": list(payload["dataset"]["features"]),
                "published_alerts_per_day": float(
                    payload["metrics"]["alerts_per_day"])}

    @task
    def measure(served: dict) -> dict:
        """Measure the four families, record every figure, say what changed.

        ONE TRANSACTION FOR THE READS AND THE WRITES. A measurement recorded
        against a window that moved under it would be a row nobody can
        re-derive, and fd_monitoring is append-only: there is no correcting it
        afterwards.
        """
        from airflow.operators.python import get_current_context

        from ml4fd import config, db, health, monitor, threshold, train

        settings = config.load()
        context = get_current_context()
        run_id = context["run_id"]
        # THE WINDOW ENDS AT data_interval_end AND NEVER AT now(), the same
        # decision dag_train takes for its cut-off: a re-run must measure the
        # window it was scheduled for, or two runs of the same interval would
        # disagree and the series would depend on when it was replayed.
        end = context["data_interval_end"]
        drift_window = monitor.window_back(end, settings.drift_window())
        perf_window = monitor.window_back(end, settings.performance_window())

        tool = monitor.tool_version()
        measurements = []
        with db.connect("fd_ingest", settings) as conn:
            current = monitor.features_window(conn, drift_window)
            reference = train.load_split(
                monitor.reference_split(settings, served["dataset_version"]))
            # WHICH COLUMNS, BEFORE THE FLOOR AND NOT INSIDE THE else. The
            # declaration is valid or stale whatever the window holds, so a
            # thin window must not be what hides a name that no longer matches
            # the model in service.
            columns = monitor.drift_columns(
                served["features"], settings.monitoring_drift_not_comparable)
            if settings.monitoring_drift_not_comparable:
                log.info("derive mesuree sur %d colonne(s) : %s ; exclues par "
                         "governance : %s", len(columns), ", ".join(columns),
                         ", ".join(settings.monitoring_drift_not_comparable))
            # THE FLOOR IS ASKED FIRST, so Evidently is never run on a sample
            # it cannot speak about. An empty window takes this branch too, and
            # records its reason instead of only logging it.
            thin = monitor.thin_window(len(current), settings.drift_min_rows())
            if thin:
                measurements += thin
                log.info("fenetre de derive trop mince : %d ligne(s) pour un "
                         "plancher de %d", len(current),
                         settings.drift_min_rows())
            else:
                snapshot = monitor.drift_snapshot(
                    current, reference, columns,
                    settings.monitoring_max_drifted_share,
                    settings.monitoring_drift_method)
                measurements += monitor.drift_measurements(
                    snapshot, settings.monitoring_max_drifted_share,
                    len(current))

            measurements += monitor.prediction_drift_measurements(
                monitor.flag_rate(conn, drift_window, served["model_version"],
                                  threshold.API_TXN_PER_DAY),
                served["published_alerts_per_day"],
                settings.monitoring_max_alert_rate_ratio)

            measurements += monitor.performance_measurements(
                monitor.performance(conn, perf_window,
                                    served["model_version"]),
                settings.monitoring_min_recall,
                settings.monitoring_min_labelled_frauds)

            measurements += monitor.data_quality_measurements(
                conn, settings, drift_window)

            monitor.record(conn, measurements, drift_window,
                           served["model_version"], tool, run_id)
            controls = monitor.controls_from(measurements)
            stored = health.stored_rows(conn)
            changed = health.transitions(controls, {row["control"]: row["state"]
                                                    for row in stored})
            conn.commit()

        report = {
            "dag_id": DAG_ID, "run_id": run_id,
            "window_minutes": int(
                settings.drift_window().total_seconds() // 60),
            "facts": {m.metric: float(m.value) for m in measurements},
            "controls": [control.as_dict() for control in controls],
            "transitions": [change.as_dict() for change in changed],
            "in_alert": sum(1 for control in controls if control.alert),
            "stored": [{"control": row["control"], "state": row["state"],
                        "since": row["since"].isoformat()
                                 if row["since"] else None,
                        "detail": row.get("detail")}
                       for row in stored],
            "tool": tool,
        }
        log.info("%d mesure(s), %d controle(s) en alerte, %d changement(s)",
                 len(measurements), report["in_alert"], len(changed))
        return report

    @task
    def tell(report: dict) -> dict:
        """Send the transitions on the model owner's own channel (EC44)."""
        from ml4fd import config, db, monitor

        settings = config.load()
        with db.connect("fd_ingest", settings) as conn:
            sent = monitor.notify(conn, settings, report)
        log.info("canal %s : %d envoi(s), destinataire %s",
                 sent["channel"], sent["sent"], sent["recipient"])
        return sent

    @task.branch
    def decide(report: dict) -> str:
        """Whether this run asks for a retraining (EC33).

        A BRANCH AND NOT A FLAG, so the trigger is a task that visibly ran or
        visibly did not. "Did the monitoring ask for a retraining on the 12th"
        is then answerable from the Airflow graph as well as from the channel,
        which matters for a jury reading one screen.
        """
        from ml4fd import config, db, monitor

        settings = config.load()
        stored = monitor.stored_from(report)
        # READ FROM THE DATABASE AND NOT FROM THIS RUN. The decision is "has
        # this alert been answered", which survives a crash of this very task;
        # the old rule asked "did a transition happen in the run I am part of",
        # and lost the request when the task after the transition failed.
        with db.connect("fd_ingest", settings) as conn:
            answered = monitor.last_request(conn)
        reason = monitor.retraining_due(
            stored, answered, settings.monitoring_retrain_on,
            settings.retrain_cooldown(),
            dt.datetime.now(dt.timezone.utc))
        if reason is None:
            log.info("aucun reentrainement demande")
            return NO_RETRAINING
        log.warning("reentrainement demande : %s", reason)
        return ASK_FOR_RETRAINING

    @task(task_id=NO_RETRAINING)
    def no_retraining() -> None:
        """The branch that does nothing, named so that it shows."""
        log.info("rien a demander : les controles qui declenchent sont au vert")

    # THE TRIGGER ITSELF. conf carries why, so the training run can be read
    # back to the measurement that asked for it -- the B4-C5/C6 junction, made
    # auditable rather than inferred from two timestamps.
    #
    # wait_for_completion is FALSE on purpose: a monitoring run must not hold
    # a worker for the length of a training, and what happens to the candidate
    # is the gate's business, not this DAG's.
    ask_for_retraining = TriggerDagRunOperator(
        task_id=ASK_FOR_RETRAINING,
        trigger_dag_id=TRAIN_DAG_ID,
        conf={"requested_by": DAG_ID,
              "reason": "{{ ti.xcom_pull(task_ids='measure')['transitions'] }}"},
        wait_for_completion=False,
        reset_dag_run=False,
    )

    @task(task_id=RECORD_REQUEST)
    def record_request(report: dict) -> None:
        """Record the request, AFTER the trigger fired.

        DOWNSTREAM OF THE TRIGGER, which is the whole repair. The default
        trigger rule is all_success, so nothing is recorded unless the
        TriggerDagRunOperator actually ran: a failure there leaves the alert
        unanswered and the next monitoring run asks again. The reverse order --
        record, then trigger -- is what lost the request of 2026-11-20.

        answered=None ON PURPOSE. The decision was taken by decide(); what is
        computed here is its justification, through the same sentence, so the
        reason in fd_retrain_requests is the one the operator read in the log.
        """
        from airflow.operators.python import get_current_context

        from ml4fd import config, db, monitor

        settings = config.load()
        stored = monitor.stored_from(report)
        controls = monitor.alerting_controls(stored,
                                             settings.monitoring_retrain_on)
        reason = monitor.retraining_due(
            stored, None, settings.monitoring_retrain_on,
            settings.retrain_cooldown(), dt.datetime.now(dt.timezone.utc))
        run_id = str(get_current_context()["run_id"])
        with db.connect("fd_ingest", settings) as conn:
            monitor.record_request(conn, controls or ["input_drift"],
                                   reason or "declenche par dag_monitor",
                                   run_id)
            conn.commit()
        log.info("demande de reentrainement enregistree : %s", reason)

    served = champion()
    report = measure(served)
    tell(report)
    branch = decide(report)
    branch >> [ask_for_retraining, no_retraining()]
    ask_for_retraining >> record_request(report)


monitor_model()
