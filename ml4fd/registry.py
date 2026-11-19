"""Registering a model with everything needed to find its data again.

Covers: EE09, EI06, EC23
Lot: M7, M8
Competency: C1, C3, C4

WHAT MAKES A MODEL AUDITABLE is not the model file. It is the chain from the
model back to the exact rows it was fitted on: the recipe digest, the cut-off,
the ordered feature list, the split rule, the seed, and the resolved library
versions. Any break in that chain turns a performance figure into a claim
nobody can check.

So every run logs the chain as PARAMETERS, not as prose in a description. A
parameter can be filtered on; a sentence cannot.

ON THE pip freeze. M8 asks for reproducibility, and the honest form of it is to
record what pip ACTUALLY resolved rather than what a requirements file asked
for: airflow/requirements.txt carries floors on purpose, so the resolved set is
the only exact statement of what ran.
"""
from __future__ import annotations

import datetime as dt
import inspect
import json
import logging
import subprocess
import sys
from pathlib import Path

log = logging.getLogger(__name__)

EXPERIMENT = "fraud-scoring"
REGISTERED_NAME = "fraud_scoring"
MANIFEST_FILE = "run.json"
FREEZE_FILE = "requirements.lock"


def resolved_packages() -> str:
    """What pip actually resolved, as a lock file's worth of text.

    Covers: EC23
    Lot: M8
    Competency: C4
    """
    try:
        done = subprocess.run([sys.executable, "-m", "pip", "freeze"],
                              capture_output=True, text=True, timeout=120,
                              check=True)
        return done.stdout
    except Exception as problem:            # noqa: BLE001 - never fail a run for this
        # A missing lock file is a gap in the audit trail, not a reason to throw
        # away a trained model. It is recorded as a gap.
        log.warning("pip freeze unavailable: %s", problem)
        return "# pip freeze unavailable: %s\n" % problem


def manifest(recipe, evaluation, choice, model_path: Path) -> dict:
    """Everything needed to rebuild this run, in one readable object.

    Written beside the dataset as well as logged, so the chain survives a
    tracking server that is down, moved or wiped.

    Covers: EC23
    Lot: M8
    Competency: C4
    """
    payload = {
        "written_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "dataset": {
            "version": recipe.version(),
            "recipe_digest": recipe.digest(),
            "family": recipe.family,
            "schema_version": recipe.schema_version,
            "cut_off": recipe.cut_off.astimezone(dt.timezone.utc).isoformat(),
            "split_rule": recipe.split_rule,
            "features": list(recipe.features),
        },
        "model": {
            "path": str(model_path),
            "seed": evaluation.seed,
            "versions": dict(evaluation.versions),
        },
        "counts": {
            "n_train": evaluation.n_train, "n_train_pos": evaluation.n_train_pos,
            "n_test": evaluation.n_test, "n_test_pos": evaluation.n_test_pos,
            "base_rate_train": evaluation.base_rate_train,
            "base_rate_test": evaluation.base_rate_test,
        },
        "metrics": {
            "average_precision": evaluation.average_precision,
            "roc_auc": evaluation.roc_auc,
            "baseline_average_precision": evaluation.baseline_average_precision,
            "lift": evaluation.lift,
            "recall": evaluation.recall,
            "precision": evaluation.precision,
            "true_positives": evaluation.true_positives,
            "false_positives": evaluation.false_positives,
            "false_negatives": evaluation.false_negatives,
            "true_negatives": evaluation.true_negatives,
            "alerts_per_day": evaluation.alerts_per_day,
            "rule_recall": evaluation.rule_recall,
            "rule_precision": evaluation.rule_precision,
            "frauds_caught_over_rule": evaluation.frauds_caught_over_rule,
        },
        # No model accuracy here either: the record cannot hold what the report
        # refuses to publish.
        "trivial_model": {
            "average_precision": evaluation.baseline_average_precision,
            "accuracy": evaluation.baseline_accuracy,
        },
        "threshold": None,
    }
    if choice is not None:
        payload["threshold"] = {
            "policy": choice.policy,
            "value": choice.threshold,
            "chosen_on": choice.chosen_on,
            "capacity_per_day": choice.capacity,
            "by_cost": choice.by_cost.threshold,
            "by_capacity": choice.by_capacity.threshold,
            "warning": choice.warning,
        }
    return payload


def write_manifest(payload: dict, directory: Path) -> Path:
    path = Path(directory) / MANIFEST_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True,
                               ensure_ascii=False) + "\n", encoding="utf-8")
    return path


REFERENCE_SQL = "datasets/record_reference.sql"


def reference_of(payload: dict) -> dict:
    """The five figures of a run.json the database keeps, and its digest.

    From the PAYLOAD -- the manifest as written -- and not from an Evaluation:
    the backfill has only the file, and one reader of the file is one rule.
    """
    metrics = payload["metrics"]
    threshold = payload.get("threshold") or {}
    return {"recipe_digest": payload["dataset"]["recipe_digest"],
            "recall": metrics["recall"],
            "precision": metrics["precision"],
            "alerts_per_day": metrics["alerts_per_day"],
            "threshold": threshold.get("value"),
            "measured_at": payload["written_at"]}


def record_reference(conn, payload: dict) -> dict:
    """Copy what M5 measured into the dataset's row of fd_datasets (S6/R10).

    RAISES if no row was updated: a manifest whose recipe is not registered is
    a broken chain (M8), not a reference to skip quietly.

    Covers: EE04, EC23
    Lot: R10
    Competency: C4
    """
    from ml4fd import db

    figures = reference_of(payload)
    updated = db.execute_file(conn, REFERENCE_SQL, **figures)
    if updated != 1:
        raise LookupError(
            "recette %s absente de fd_datasets : la reference de M5 n'a pas "
            "ete enregistree (%d ligne mise a jour)"
            % (figures["recipe_digest"][:12], updated))
    return figures


def write_lock(text: str, directory: Path) -> Path:
    path = Path(directory) / FREEZE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


# THE TYPES WE DECLARE AS TRUSTED FOR skops, AND WHY.
#
# MLflow 3 serialises sklearn models with skops rather than pickle, and skops
# refuses to load a type it has not been told to trust. Three appear here:
#
#   TreePredictor        the trees of HistGradientBoosting. skops' own warning is
#                        precise: it stores raw node indices that scikit-learn
#                        indexes WITHOUT bounds checking, so a crafted file can
#                        segfault the process at .predict() time.
#   functools.partial    a bound callable, used internally for the loss.
#   check_array          a reference to a scikit-learn validation function.
#
# DECLARING THEM TRUSTED IS A SECURITY DECISION, NOT A WORKAROUND, and it rests
# on one condition: the model is produced by this pipeline, in this container,
# from our own data, and is read back only from our own registry. The day a
# model could arrive from somewhere we do not control, this list is what lets a
# crafted file crash the scorer -- so the condition belongs in governance and
# not only in a comment.
#
# Listed explicitly, one by one, and NOT taken from get_untrusted_types(): skops
# warns against passing back everything it reports just to make a file load,
# and it is right. Each of the three was read before being written here.
TRUSTED_SKOPS_TYPES = (
    "functools.partial",
    "sklearn.ensemble._hist_gradient_boosting.predictor.TreePredictor",
    "sklearn.utils.validation.check_array",
)


def model_kwargs(log_model) -> dict:
    """The keyword arguments THIS MLflow's log_model actually accepts.

    MLflow 3 renamed the artifact slot from artifact_path to name, and added
    skops_trusted_types. Both are discovered by inspecting the signature rather
    than guessed: a parameter I could not verify is exactly the kind of bet that
    has already cost this project three rounds tonight.
    """
    parameters = inspect.signature(log_model).parameters
    kwargs = {"name" if "name" in parameters else "artifact_path": "model"}
    if "skops_trusted_types" in parameters:
        kwargs["skops_trusted_types"] = list(TRUSTED_SKOPS_TYPES)
    return kwargs


def load_kwargs(load_model) -> dict:
    """The keyword arguments THIS MLflow's load_model actually accepts.

    Same discovery as model_kwargs, and for the same reason on the way back:
    MLflow 3 serialises an sklearn model with skops, which refuses to
    deserialise a type it has not been told to trust -- and
    HistGradientBoosting's TreePredictor is one. Logging the model needed the
    declaration; loading it may need it too, and the parameter's name and its
    presence vary by version.

    Introspected rather than guessed, because a parameter I cannot verify from
    here is exactly the bet that cost this project three rounds on M7.

    Covers: EE09
    Lot: N4
    Competency: C3
    """
    parameters = inspect.signature(load_model).parameters
    if "skops_trusted_types" in parameters:
        return {"skops_trusted_types": list(TRUSTED_SKOPS_TYPES)}
    return {}


def params_from(payload: dict) -> dict:
    """The chain, as MLflow parameters. A parameter can be filtered on."""
    dataset = payload["dataset"]
    threshold = payload.get("threshold") or {}
    return {
        "dataset_version": dataset["version"],
        "recipe_digest": dataset["recipe_digest"],
        "schema_version": dataset["schema_version"],
        "cut_off": dataset["cut_off"],
        "split_rule": dataset["split_rule"],
        "features": ",".join(dataset["features"]),
        "n_features": len(dataset["features"]),
        "seed": payload["model"]["seed"],
        "threshold": threshold.get("value", "fixed"),
        "threshold_policy": threshold.get("policy", "fixed"),
        "threshold_chosen_on": threshold.get("chosen_on", "not chosen"),
    }


def metrics_from(payload: dict) -> dict:
    """The figures, as MLflow metrics.

    The two base rates travel WITH every other metric, never apart from them:
    the monthly rate of this source runs 0.185 % to 0.554 %, so two runs are not
    comparable without them.
    """
    metrics = {k: v for k, v in payload["metrics"].items()
               if isinstance(v, (int, float))}
    metrics.update({
        "base_rate_train": payload["counts"]["base_rate_train"],
        "base_rate_test": payload["counts"]["base_rate_test"],
        "n_train": payload["counts"]["n_train"],
        "n_test": payload["counts"]["n_test"],
        "pr_auc_trivial": payload["trivial_model"]["average_precision"],
    })
    return metrics


PROXY_LOCATION = "mlflow-artifacts:/"


def experiment_id(mlflow, name: str = EXPERIMENT) -> str:
    """Get the experiment, or create it with its artifact location PINNED.

    MLFLOW FREEZES artifact_location WHEN THE EXPERIMENT IS CREATED. Changing
    the server's --default-artifact-root afterwards does not touch an experiment
    that already exists: it keeps the path it was born with, forever. An
    experiment created while the root was /mlruns/artifacts therefore keeps
    sending clients to a local filesystem path, however the server is now
    configured -- which is why --serve-artifacts appeared to change nothing.

    So the location is set explicitly at creation, and an existing experiment
    pointing at a local path is REFUSED rather than used: writing would fail a
    few lines later with an error that names a directory instead of a cause.

    Covers: EE09, EC23
    Lot: M7
    Competency: C1, C4
    """
    existing = mlflow.get_experiment_by_name(name)
    if existing is None:
        return mlflow.create_experiment(name,
                                        artifact_location=PROXY_LOCATION)

    location = str(existing.artifact_location or "")
    if not location.startswith("mlflow-artifacts:"):
        raise RuntimeError(
            "the MLflow experiment %r was created with artifact_location=%r, a "
            "path on the SERVER's filesystem. MLflow freezes that location at "
            "creation, so no change to --default-artifact-root will fix it, and "
            "this client cannot write there. The experiment has to be recreated:"
            "\n    .\\fd-manager.ps1 mlflow-reset\n"
            "which drops the mlruns volume (MLflow history only -- the datasets, "
            "the models and run.json on disk are untouched)."
            % (name, location))
    return existing.experiment_id


def log_run(model, payload: dict, report: str, lock: str,
            tracking_uri: str | None = None) -> str | None:
    """Log the run from its MANIFEST, register the model, return the run id.

    Fed by the manifest rather than by the live objects, and deliberately: the
    manifest is what was written to disk, so MLflow and the file on disk cannot
    disagree about what ran. Reconstructing an Evaluation here would create a
    second source of truth, and two sources of truth are one too many.

    Covers: EE09, EI06
    Lot: M7
    Competency: C1, C3
    """
    import mlflow
    import mlflow.sklearn

    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)

    with mlflow.start_run(experiment_id=experiment_id(mlflow)) as run:
        mlflow.log_params(params_from(payload))
        mlflow.log_metrics(metrics_from(payload))
        mlflow.set_tags({
            "process": "S2",
            "dataset_version": payload["dataset"]["version"],
            "lots": "M1-M8",
        })
        # EVERY artifact write under one guard, not just the model. The first
        # version wrapped log_model alone and the failure came from log_text,
        # three lines earlier -- so the message never appeared.
        extra = model_kwargs(mlflow.sklearn.log_model)
        try:
            mlflow.log_text(report, "rapport.txt")
            mlflow.log_text(lock, FREEZE_FILE)
            mlflow.log_text(
                json.dumps(payload, indent=2, sort_keys=True,
                           ensure_ascii=False),
                MANIFEST_FILE)
            mlflow.sklearn.log_model(
                model, registered_model_name=REGISTERED_NAME, **extra)
        except OSError as problem:
            raise OSError(
                "an artifact could not be written to MLflow's store. If the "
                "server does NOT proxy artifacts (--serve-artifacts), the "
                "client writes them with its own filesystem at the path the "
                "server reports -- and a directory made by the mlflow "
                "container is not writable by uid 50000. Check that "
                "mlflow/Dockerfile passes --serve-artifacts and "
                "--default-artifact-root mlflow-artifacts:/. Original error: %s"
                % problem) from problem
        log.info("run %s logged, model registered as %s",
                 run.info.run_id, REGISTERED_NAME)
        return run.info.run_id
