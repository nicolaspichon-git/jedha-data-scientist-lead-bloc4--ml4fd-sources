"""S2 -- training the fraud scoring model. Weekly.

Block: B3, B4.
Lot: B3-M7, B4-L0.
Competency: B4-C3, B4-C5.
Covers: B3-EE09, B3-EI06, B3-EC08, B3-EC09, B4-EC31.

B4-EC31 ASKS FOR A RETRAINING THAT RUNS WITHOUT A HAND ON IT, and since
2026-11-20 this DAG is that: guard, dataset, fit, registration, and -- as of
challenge_and_promote -- the gate and the alias. Nothing in the chain waits for
a command to be typed. The mark was missing rather than the implementation.

B4-C5 JOINS THE LINE FOR THE SAME REASON. B4-C3 beside it belongs to B4-EC08 and
B4-EC09, which are marks of the referential this project grew out of and which the
requirements document has since renumbered -- see TRACEABILITY.md, section
"Marques en desaccord". They are left as they are rather than guessed at: the
re-pointing is one pass over 303 marks and does not belong in a lot about B4-EC31.

B3-M1 to B3-M8: the guard, the cut-off and recipe, the dataset with its temporal
split, the fit with its honest report, the threshold chosen off the test set,
the registration in MLflow, and the manifest that keeps the chain readable even
if the tracking server is gone.

THE CUT-OFF COMES FROM data_interval_end, NEVER now(). With now(), re-running
this DAG for a past week would build a different dataset from the one it had
produced, and the model-to-dataset traceability would become a lie.

It is then CLAMPED to the labelling horizon. On this source the labels stop on
2020-12-22 while the schedule runs in 2026, so an unclamped cut-off would be
refused by the guard every single week. Clamping is not a silent correction: it
is logged, and two intervals that clamp to the same instant produce the same
recipe and therefore reuse the same dataset -- which is exactly right, since
they describe the same data.
"""
from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

DAG_ID = "dag_train"


@dag(
    dag_id=DAG_ID,
    description="B3-S2: weekly training of the fraud scoring model",
    schedule="0 3 * * 1",               # Monday, 03:00 -- a distinct cadence (EI06)
    start_date=pendulum.datetime(2026, 1, 5, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    tags=["BC03", "B3-S2", "B3-C2", "B3-C3", "B3-C5"],
    default_args={"retries": 0},
)
def train_model():

    # THE TWO KINDS OF RUN, and they do not read the same instant. A SCHEDULED
    # run replays the interval it was scheduled for, so its cut-off is
    # data_interval_end and a re-run rebuilds the identical dataset -- that is
    # EI02. A TRIGGERED run exists because something happened just now, so its
    # cut-off is the instant the trigger fired.
    #
    # MEASURED 2026-11-20. This DAG is scheduled weekly, Monday 03:00 Paris.
    # Airflow infers a manual run's data interval from the timetable, so EVERY
    # run triggered between two Mondays received data_interval_end =
    # 2026-11-16T02:00:00+00:00 -- up to seven days in the past, and always
    # BEFORE the data that caused the trigger. The recipe digest is computed
    # from the cut-off, so three retrainings in one afternoon produced the same
    # digest, build_or_reuse handed back the same Parquet, and the models were
    # byte-identical: v4 published exactly v2's metrics and the gate correctly
    # refused to promote a model that was not different.
    #
    # So a drift-triggered retraining could never see the drift that triggered
    # it. The streamed rows carry a 2026-11-20 label_ts and
    # sql/datasets/training_set.sql keeps only label_ts <= cut_off: at a cut-off
    # of 2026-11-16 the stream contributed zero rows, however much of it had
    # arrived. The loop measured, alerted, triggered, trained and compared --
    # correctly at every link -- on last Monday's data.
    #
    # STILL NEVER THE CLOCK. logical_date is fixed when the run is created and
    # stored on the dag_run, so clearing a task and re-running it reads the
    # same instant; utcnow() inside the task would not. EI02 holds.
    INTERVAL_RUN_TYPES = ("scheduled", "backfill")

    @task
    def resolve_cut_off() -> str:
        """The cut-off of this run, from the run and never from the clock.

        Block: B3, B4.
        Lot: B3-M2, B4-L0.
        Competency: B3-C5.
        Covers: B3-EI02, B3-EC23.
        """
        from airflow.operators.python import get_current_context

        from ml4fd import config

        context = get_current_context()
        settings = config.load()

        run = context["dag_run"]
        override = (run.conf or {}).get("cut_off")
        if override:
            cut_off = pendulum.parse(override)
            log.info("cut-off given in --conf: %s", cut_off)
        elif run.run_type in INTERVAL_RUN_TYPES:
            cut_off = context["data_interval_end"]
            log.info("cut-off from data_interval_end: %s (run %s, replaying "
                     "the interval it was scheduled for)",
                     cut_off, run.run_type)
        else:
            cut_off = context["logical_date"]
            log.info("cut-off from logical_date: %s (run %s, triggered by %s). "
                     "data_interval_end would have been %s -- the end of the "
                     "last WEEKLY interval, before the data that caused this "
                     "run",
                     cut_off, run.run_type,
                     (run.conf or {}).get("requested_by", "hand"),
                     context["data_interval_end"])

        horizon = settings.label_horizon
        if settings.include_stream:
            # NOT CLAMPED WHEN THE STREAM IS A SOURCE. The clamp froze the
            # cut-off on 2020-06-21, so the recipe digest never changed and
            # build_or_reuse handed back the same Parquet files every time: the
            # retraining refitted on identical data. The horizon still bounds
            # the HISTORICAL rows, inside sql/datasets/training_set.sql, where
            # it can apply to one source and not to the other.
            log.info("cut-off %s kept: the stream is a training source and its "
                     "verdicts are written as they fall due (horizon %s bounds "
                     "the historical rows only)",
                     cut_off.isoformat(), horizon.isoformat())
        elif cut_off > horizon:
            log.warning(
                "cut-off %s is past the labelling horizon %s: clamped to the "
                "horizon. Past it the rows carry is_fraud = 0 because nobody "
                "labelled them, not because they are not fraud.",
                cut_off.isoformat(), horizon.isoformat())
            cut_off = horizon

        return cut_off.isoformat()

    @task
    def resolve_features() -> list:
        """The feature list of this run, from --conf and never from an edit.

        WHY THIS IS A PARAMETER OF A RUN. N4 measured that txn_hour and
        txn_weekday mean the hour of the PAYMENT at training time and the hour
        of OUR CALL on the stream -- independent of the label by construction
        there -- and that the clock costs 0.155 of recall, 32 frauds out of 205.
        Deciding whether to drop them requires a model trained without them.

        Editing datasets.FEATURES would produce that model and, in the same
        move, make the recipe IN SERVICE unrebuildable: its digest is computed
        from the feature list, so the constant and the registered model would no
        longer agree. The digest exists precisely to let two recipes coexist, so
        the amputation travels in --conf and both stay reachable.

            .\fd-manager.ps1 train -Drop txn_hour,txn_weekday

        Block: B3, B4.
        Lot: B3-N4, B4-L0.
        Competency: B3-C1, B3-C5.
        Covers: B3-EC05, B3-EC23, B3-EI04.
        """
        from airflow.operators.python import get_current_context

        from ml4fd import config, datasets

        context = get_current_context()
        settings = config.load()
        override = (context["dag_run"].conf or {}).get("drop_features")

        if override is None:
            # THE DECLARED DECISION, and the default. governance.yml records
            # that the model does not use txn_hour or txn_weekday, with the
            # paired comparison that decided it. The weekly run therefore needs
            # no argument to do the right thing -- which is the point: a
            # decision that only applies when somebody remembers a flag is not
            # a decision.
            dropped = settings.drop_features
            source = "governance.yml"
        elif str(override).strip().lower() in ("", "none", "aucun"):
            # The escape hatch, and the reason the 7-feature recipe stays
            # rebuildable: --conf can only REMOVE columns, never add them back,
            # so without this there would be no way to reach the full recipe
            # once governance declares a removal.
            dropped = ()
            source = "--conf, explicitement aucune"
        else:
            dropped = override
            source = "--conf"

        features = datasets.without(dropped)
        removed = [name for name in datasets.FEATURES if name not in features]
        if removed:
            log.info("recette AMPUTEE de %s, selon %s. Empreinte distincte, "
                     "donc toute autre recette reste reconstructible.",
                     ", ".join(removed), source)
        else:
            log.warning(
                "recette COMPLETE (%s) : le modele lira txn_hour et "
                "txn_weekday, qui sur le flux portent l'heure de NOTRE appel "
                "et non celle du paiement. Mesure du 16 novembre 2026 : -18 "
                "fraudes sur 205 face a la recette amputee, p = 2.8e-4.",
                source)
        log.info("features de ce passage (%d) : %s",
                 len(features), ", ".join(features))
        return list(features)

    @task
    def guard(cut_off: str) -> dict:
        """Refuse to train on a window that is unhealthy or unlabelled.

        The window inspected is the TEST period: a missing label or a down
        ingestion there distorts the published metrics most directly, and the
        horizon check covers the whole span anyway.

        Block: B3, B4.
        Lot: B3-M1, B4-L0.
        Competency: B3-C3, B3-C5.
        Covers: B3-EC09, B3-EC14.
        """
        import datetime as _dt

        from ml4fd import config, datasets, db, guard as g

        settings = config.load()
        moment = _dt.datetime.fromisoformat(cut_off)
        train_end, test_end = datasets.parse_split_rule(settings.split_rule)

        with db.connect("fd_report") as conn:
            report = g.assert_window_healthy(
                conn, train_end, test_end, moment, settings.label_horizon,
                settings.min_positives, settings.verdict_delay())
        return report.as_log()

    @task
    def build_dataset(cut_off: str, features: list, _report: dict) -> dict:
        """Build the dataset, or find the one this recipe already produced.

        Block: B3, B4.
        Lot: B3-M3, B3-M4, B4-L0.
        Competency: B3-C1, B3-C2, B3-C5.
        Covers: B3-EC05, B3-EC23, B3-EI04.
        """
        import datetime as _dt

        from ml4fd import build, config, datasets, db

        settings = config.load()
        recipe = datasets.Recipe(
            family=settings.dataset_family,
            schema_version=settings.dataset_schema_version,
            cut_off=_dt.datetime.fromisoformat(cut_off),
            # From resolve_features, never from the constant: the two tasks must
            # build the SAME recipe or fit_model would look for a dataset under
            # a digest build_dataset never wrote.
            features=tuple(features),
            split_rule=settings.split_rule,
            # THE TWO TASKS MUST AGREE ON THE WHOLE RECIPE, this field
            # included: it enters the digest, so one of them omitting it
            # would make fit_model look for a dataset build_dataset
            # never wrote.
            include_stream=settings.include_stream)

        log.info("recipe %s -> version %s", recipe.digest()[:16], recipe.version())
        log.info("features: %s", ", ".join(recipe.features))

        with db.connect("fd_ingest") as conn:
            result = build.build_or_reuse(
                conn, recipe, settings.datasets_root, settings.label_horizon)

        log.info("dataset %s: train=%s (%s pos) test=%s (%s pos) reused=%s",
                 result["version"], result.get("n_train"),
                 result.get("n_train_pos"), result.get("n_test"),
                 result.get("n_test_pos"), result["reused"])
        # THE FRESH SHARE, IN THE LOG AND IN THE XCOM. "retrained on fresh
        # data" is a claim that needs a denominator: the stream delivers 7,200
        # rows a day against more than a million, so a share of 0.006 means the
        # retraining is mechanically fresh and materially the same model. The
        # figure belongs where someone reading the run will see it, not only in
        # a commit message.
        fresh = result.get("n_train_stream")
        if fresh is None:
            log.info("stream share: not counted on this run -- the dataset was "
                     "reused, and the share was counted when it was built")
        else:
            total = result.get("n_train") or 0
            log.info("stream share: %d of %d training rows (%.3f %%) came from "
                     "the stream", fresh, total,
                     100.0 * fresh / total if total else 0.0)
        log.info("base rates: train=%.4f %% test=%.4f %% -- a precision figure "
                 "cannot be read without them",
                 100 * float(result["base_rate_train"]),
                 100 * float(result["base_rate_test"]))
        return {k: str(v) for k, v in result.items()}

    @task
    def fit_model(cut_off: str, features: list, dataset: dict) -> dict:
        """Fit on the dataset, and report what may be reported.

        Block: B3, B4.
        Lot: B3-M5, B4-L0.
        Competency: B3-C2, B3-C5.
        Covers: B3-EE08, B3-EI04.
        """
        import datetime as _dt
        from pathlib import Path

        from ml4fd import config, datasets, registry, train

        settings = config.load()
        recipe = datasets.Recipe(
            family=settings.dataset_family,
            schema_version=settings.dataset_schema_version,
            cut_off=_dt.datetime.fromisoformat(cut_off),
            # From resolve_features, never from the constant: the two tasks must
            # build the SAME recipe or fit_model would look for a dataset under
            # a digest build_dataset never wrote.
            features=tuple(features),
            split_rule=settings.split_rule,
            # THE TWO TASKS MUST AGREE ON THE WHOLE RECIPE, this field
            # included: it enters the digest, so one of them omitting it
            # would make fit_model look for a dataset build_dataset
            # never wrote.
            include_stream=settings.include_stream)
        train_path, test_path = recipe.paths(settings.datasets_root)
        if not (train_path.is_file() and test_path.is_file()):
            raise FileNotFoundError(
                "the dataset of recipe %s is registered but its Parquet files "
                "are missing: %s" % (recipe.version(), train_path.parent))

        model, evaluation, choice = train.fit_and_evaluate(
            train_path, test_path, settings.decision_threshold,
            rule=settings.rule_baseline(),
            policy=settings.threshold_policy,
            capacity=settings.alert_capacity_per_day,
            cost_false_alert=settings.cost_false_alert,
            validation_share=settings.validation_share,
            features=recipe.features)

        # Printed whole, in one block, so the log of a run holds the figures and
        # not a pointer to them.
        log.info("rapport d'entrainement, jeu %s\n%s",
                 recipe.version(), train.render_report(evaluation))
        if choice is not None and choice.warning:
            log.warning("seuil : %s", choice.warning)

        directory = Path(train_path).parent
        path = train.save_model(model, directory)

        # Written beside the dataset, not only to MLflow: the chain from a model
        # back to its rows must survive a tracking server that is down, moved or
        # wiped (M8).
        report = train.render_report(evaluation)
        payload = registry.manifest(recipe, evaluation, choice, path)
        registry.write_manifest(payload, directory)
        # The same figures in the database (S6/R10), from the SAME payload: the
        # morning report and the dashboard compare against them without
        # reading the datasets directory.
        from ml4fd import db
        with db.connect("fd_ingest") as conn:
            registry.record_reference(conn, payload)
        registry.write_lock(registry.resolved_packages(), directory)
        (directory / "rapport.txt").write_text(report, encoding="utf-8")
        if evaluation.lift < 2.0:
            log.warning(
                "gain de x%.1f seulement sur le modele trivial : le modele "
                "n'a presque rien appris, quelle que soit son exactitude.",
                evaluation.lift)
        if evaluation.frauds_caught_over_rule <= 0:
            log.warning(
                "le modele ne prend AUCUNE fraude de plus qu'une regle a deux "
                "conditions (%s). Battre le modele trivial ne prouve rien ici : "
                "deux features portent l'essentiel du signal.",
                evaluation.rule_description)
        return {"version": recipe.version(), "model_path": str(path),
                "directory": str(directory),
                "threshold": "%.2f" % evaluation.threshold,
                "threshold_policy": choice.policy if choice else "fixed",
                "pr_auc": "%.4f" % evaluation.average_precision,
                "lift": "%.1f" % evaluation.lift,
                "recall": "%.4f" % evaluation.recall,
                "precision": "%.4f" % evaluation.precision,
                "base_rate_test": "%.6f" % evaluation.base_rate_test}

    @task
    def register_model(cut_off: str, fitted: dict) -> dict:
        """Log the run in MLflow and register the model.

        The model is RELOADED from disk rather than carried across the task
        boundary: that way a tracking server that is down loses the
        registration, not the fit -- and the manifest written next to the model
        still carries the whole chain.

        Block: B3, B4.
        Lot: B3-M7, B4-L0.
        Competency: B3-C1, B3-C3, B3-C4.
        Covers: B3-EE09, B3-EI06.
        """
        import json
        import os
        from pathlib import Path

        import joblib

        from ml4fd import registry, train

        directory = Path(fitted["directory"])
        payload = json.loads(
            (directory / registry.MANIFEST_FILE).read_text(encoding="utf-8"))
        model = joblib.load(directory / train.MODEL_FILE)
        report = (directory / "rapport.txt").read_text(encoding="utf-8")
        lock = (directory / registry.FREEZE_FILE).read_text(encoding="utf-8")

        run_id = registry.log_run(
            model, payload, report, lock,
            tracking_uri=os.environ.get("MLFLOW_TRACKING_URI"))
        log.info("run MLflow %s ; modele enregistre sous %s",
                 run_id, registry.REGISTERED_NAME)
        return {"run_id": str(run_id),
                "version": payload["dataset"]["version"]}

    @task
    def challenge_and_promote(registered: dict) -> dict:
        """The last link of the loop: challenge the candidate, and serve it if
        it passes.

        WHY THIS TASK EXISTS. Until 2026-11-20 this DAG ended at
        register_model, and the alias moved only when somebody ran
        .\\fd-manager.ps1 promote by hand. The cycle the specification asks to
        automate -- deployment, monitoring, retraining, challenging, deployment
        -- was therefore automatic for three links out of five, and the two
        that decide what the system SERVES were manual.

        IT TAKES NO GUARD AWAY. The decision is the same pure function of
        ml4fd/promote.py, through the same orchestration ml4fd/challenge.py
        gives the command line: the capacity ceiling is still asked first, the
        paired test still runs on rows neither model was fitted on, and the
        refusals are still journalled in fd_promotions. Automating the CALL
        does not automate the verdict.

        THE CANDIDATE IS THIS RUN'S, by run_id and not by "the newest
        registered" -- see registry.version_of_run for what the difference
        costs when two trainings overlap.

        A NON-DECISION IS A SKIP AND NOT A SUCCESS. The candidate already
        carrying the alias, or a shared window too thin to ask, are both
        different from a refusal: nothing is recorded and the champion stays.
        Marking them success would make a loop that decided nothing look in the
        interface exactly like one that promoted.

        Block: B4.
        Lot: B4-L4.
        Competency: B4-C5, B4-C6.
        Covers: B4-EC33, B4-EC34, B4-EC36, B4-EC37.
        """
        import os

        import mlflow
        from airflow.exceptions import AirflowSkipException

        from ml4fd import challenge, config, registry

        settings = config.load()
        uri = os.environ.get("MLFLOW_TRACKING_URI")
        if uri:
            mlflow.set_tracking_uri(uri)
        client = mlflow.MlflowClient()

        candidate_mv = registry.version_of_run(client, registered["run_id"])
        try:
            outcome = challenge.judge(client, settings, candidate_mv,
                                      apply=True, say=log.info)
        except challenge.AlreadyChampion as nothing_to_do:
            raise AirflowSkipException(str(nothing_to_do)) from nothing_to_do
        except challenge.Undecidable as unmeasurable:
            raise AirflowSkipException(str(unmeasurable)) from unmeasurable

        if outcome["promoted"]:
            log.warning("champion deplace sur la version %s : c'est ce que le "
                        "service lira au prochain scoring", outcome["candidate"])
        else:
            log.warning("candidat %s refuse, le champion %s reste en service : "
                        "%s", outcome["candidate"], outcome["champion"],
                        outcome["reason"])
        return outcome

    moment = resolve_cut_off()
    kept = resolve_features()
    challenge_and_promote(
        register_model(
            moment,
            fit_model(moment, kept,
                      build_dataset(moment, kept, guard(moment)))))


train_model()
