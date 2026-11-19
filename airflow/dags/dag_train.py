"""S2 -- training the fraud scoring model. Weekly.

Covers: EI06, EC08, EC09
Lot: M7
Competency: C3

M1 to M8: the guard, the cut-off and recipe, the dataset with its temporal
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
    description="S2: weekly training of the fraud scoring model",
    schedule="0 3 * * 1",               # Monday, 03:00 -- a distinct cadence (EI06)
    start_date=pendulum.datetime(2026, 1, 5, tz="Europe/Paris"),
    catchup=False,
    max_active_runs=1,
    tags=["S2", "BC03", "C2", "C3", "C5"],
    default_args={"retries": 0},
)
def train_model():

    @task
    def resolve_cut_off() -> str:
        """The cut-off of this run, from the interval and never from the clock.

        Covers: EI02, EC23
        Lot: M2
        Competency: C5
        """
        from airflow.operators.python import get_current_context

        from ml4fd import config

        context = get_current_context()
        settings = config.load()

        override = (context["dag_run"].conf or {}).get("cut_off")
        if override:
            cut_off = pendulum.parse(override)
            log.info("cut-off given in --conf: %s", cut_off)
        else:
            cut_off = context["data_interval_end"]
            log.info("cut-off from data_interval_end: %s", cut_off)

        horizon = settings.label_horizon
        if cut_off > horizon:
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

        Covers: EC05, EC23, EI04
        Lot: N4
        Competency: C1, C5
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

        Covers: EC09, EC14
        Lot: M1
        Competency: C3, C5
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

        Covers: EC05, EC23, EI04
        Lot: M3, M4
        Competency: C1, C2, C5
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
            split_rule=settings.split_rule)

        log.info("recipe %s -> version %s", recipe.digest()[:16], recipe.version())
        log.info("features: %s", ", ".join(recipe.features))

        with db.connect("fd_ingest") as conn:
            result = build.build_or_reuse(
                conn, recipe, settings.datasets_root, settings.label_horizon)

        log.info("dataset %s: train=%s (%s pos) test=%s (%s pos) reused=%s",
                 result["version"], result.get("n_train"),
                 result.get("n_train_pos"), result.get("n_test"),
                 result.get("n_test_pos"), result["reused"])
        log.info("base rates: train=%.4f %% test=%.4f %% -- a precision figure "
                 "cannot be read without them",
                 100 * float(result["base_rate_train"]),
                 100 * float(result["base_rate_test"]))
        return {k: str(v) for k, v in result.items()}

    @task
    def fit_model(cut_off: str, features: list, dataset: dict) -> dict:
        """Fit on the dataset, and report what may be reported.

        Covers: EE08, EI04
        Lot: M5
        Competency: C2, C5
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
            split_rule=settings.split_rule)
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

        Covers: EE09, EI06
        Lot: M7
        Competency: C1, C3, C4
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

    moment = resolve_cut_off()
    kept = resolve_features()
    register_model(
        moment,
        fit_model(moment, kept, build_dataset(moment, kept, guard(moment))))


train_model()
