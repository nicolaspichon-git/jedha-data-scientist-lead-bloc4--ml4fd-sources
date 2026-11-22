"""Scoring a transaction with the model the registry holds.

Block: B3, B4.
Lot: B3-N4, B3-L5, B4-L0.
Competency: B3-C2, B3-C3.
Covers: B3-EE09, B3-EI04, B3-EC16, B3-EC19, B3-EC24.

WHAT THIS MODULE REFUSES TO DECIDE. Three things come from the model and never
from this code:

  the feature ORDER   read from the manifest of the loaded version, so that a
                      recipe without txn_hour scores correctly without a line
                      of this file changing;
  the THRESHOLD       read from the same manifest. governance.yml declares the
                      policy, the capacity and the cost of a false alert -- the
                      decisions -- and the value is the measurement that
                      follows from them. A threshold is only meaningful for the
                      score distribution of the model it was chosen on, and the
                      model is retrained every week;
  the VERSION         resolved from the registry at each call and written with
                      every scored transaction. Without that column, "which
                      model let this one through?" has no answer, and it is the
                      first question an incident asks.

WHY THE CACHE RESOLVES THE VERSION EVERY TIME. Reloading the model per call
would cost more than scoring, so the model is cached -- but a cache that never
looks would go on scoring with last week's model forever, silently. So the
version is resolved on every call (one registry call) and the model is reloaded
only when the version has changed. The cache key is the version itself.

HOW THE MANIFEST IS TRUSTED. It is read from disk, beside the dataset, which is
where registry.manifest writes it precisely so the chain survives a tracking
server that is down or wiped. Reading it is not taking it on faith: the features
and the threshold are cross-checked against the parameters MLflow logged for
that model version, and a disagreement refuses to score. One of the two has then
been edited, and scoring with either would be scoring with something nobody
chose.
"""
from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import dataclass
from pathlib import Path

from ml4fd import datasets, db, registry

log = logging.getLogger(__name__)

ROWS_SQL = "scoring/rows_to_score.sql"
WRITE_SQL = "scoring/write_scores.sql"

# The scale of fd_scores.score and fd_scores.threshold, numeric(9,8).
#
# IT IS NOT COSMETIC. fd_scores carries CHECK (flagged = (score >= threshold)),
# and the comparison happens twice: in Python on a float, then in PostgreSQL on
# the stored numeric. Measured: a raw score of 0.929999996 against a threshold
# of 0.93 is False in Python and True once rounded to eight decimals, so the
# CHECK fails, the INSERT fails, and a whole ingestion cycle rolls back because
# of one transaction four billionths below the line.
#
# The fix is to compare the value that will be STORED, not the one before
# rounding. So the score is rounded here, once, and everything downstream --
# the comparison, the row, the constraint -- sees the same number.
SCORE_SCALE = 8

# Keyed by (registered name, version): see the note above on why the version is
# part of the key rather than checked on a timer.
_CACHE: dict[tuple[str, str], "Scorer"] = {}


@dataclass(frozen=True)
class Scorer:
    """A model version, and everything needed to use it accountably."""
    model: object
    features: tuple[str, ...]
    threshold: float
    model_version: str
    recipe_digest: str
    policy: str
    chosen_on: str

    def scores(self, frame) -> list[float]:
        """Probabilities of fraud, in the row order given.

        frame[list(self.features)] and not frame.values: the column ORDER is
        the model's, and a frame that happens to carry them in another order
        would score every row against the wrong features without raising.
        """
        missing = [name for name in self.features if name not in frame.columns]
        if missing:
            raise ValueError(
                "the rows to score lack %s, which recipe %s requires. The "
                "scoring query and the recipe have diverged."
                % (", ".join(missing), self.recipe_digest))
        return [float(value)
                for value in self.model.predict_proba(
                    frame[list(self.features)])[:, 1]]

    def flagged(self, score: float) -> bool:
        """At the threshold, inclusive -- the same comparison threshold.py made
        when it chose the value (score >= threshold). An exclusive test here
        would move the operating point by one grid step without saying so.

        The score is rounded to the stored scale FIRST, so this answer and
        PostgreSQL's CHECK on the same row cannot differ. See SCORE_SCALE.
        """
        return round(score, SCORE_SCALE) >= self.threshold


def manifest_path(settings, version: str) -> Path:
    """Where the manifest of a recipe VERSION lives.

    THE VERSION AND NOT THE DIGEST, and the parameter is named for it because
    the confusion is one line of code wide. datasets.Recipe.version() is the
    digest truncated to 12 characters, and that is what names the directory;
    digest() is the full 64-character sha256 and names nothing on disk. MLflow
    logs BOTH -- dataset_version and recipe_digest -- and score.load() read the
    wrong one, so it looked for

        datasets/fraud_scoring/8813b096e2e4049cf3...c666/run.json

    under a directory called 8813b096e2e4. The message blamed a missing
    manifest and told the operator to retrain a recipe that was perfectly well
    trained.
    """
    return (Path(settings.datasets_root) / settings.dataset_family / version
            / registry.MANIFEST_FILE)


def read_manifest(path: Path) -> dict:
    """The manifest, with the two fields scoring cannot do without."""
    if not path.is_file():
        siblings = sorted(p.name for p in path.parent.parent.iterdir()
                          if p.is_dir()) if path.parent.parent.is_dir() else []
        raise FileNotFoundError(
            "no %s at %s. The directory is named by the recipe VERSION -- "
            "datasets.Recipe.version(), the digest truncated to %d characters "
            "-- and never by the full digest; %r is %d characters long. "
            "Recipes present: %s. If the version is right, the model was "
            "registered without its manifest reaching the disk: rerun B3-S2 "
            "(.\\fd-manager.ps1 train)."
            % (registry.MANIFEST_FILE, path, datasets.VERSION_CHARS,
               path.parent.name, len(path.parent.name),
               ", ".join(siblings) or "aucune"))
    payload = json.loads(path.read_text(encoding="utf-8"))

    features = tuple(payload.get("dataset", {}).get("features") or ())
    if not features:
        raise ValueError("%s carries no feature list: there is no order to "
                         "score in" % path)

    chosen = payload.get("threshold") or {}
    if not chosen.get("value"):
        raise ValueError(
            "%s carries no threshold: B3-M6 did not choose one for this model, and "
            "this module will not substitute a default. 0.5 is the default of a "
            "library, and on the measured distribution it floods the analysts "
            "instead of alerting them. Set scoring.threshold_policy in "
            "governance.yml and retrain." % path)
    return payload


def agrees_with_registry(payload: dict, params: dict) -> list[str]:
    """The disagreements between the manifest on disk and the logged run.

    An empty list means they describe the same thing. Compared rather than
    assumed: both are written by the same run, so they can only differ if one
    has been edited since, and then neither is authoritative.
    """
    problems = []
    features = ",".join(payload["dataset"]["features"])
    if params.get("features") and params["features"] != features:
        problems.append(
            "features: run.json says %r, MLflow logged %r"
            % (features, params["features"]))

    logged = params.get("threshold")
    value = payload["threshold"]["value"]
    if logged not in (None, "fixed") and abs(float(logged) - float(value)) > 1e-9:
        problems.append("threshold: run.json says %s, MLflow logged %s"
                        % (value, logged))

    digest = payload["dataset"]["recipe_digest"]
    if params.get("recipe_digest") and params["recipe_digest"] != digest:
        problems.append("recipe digest: run.json says %s, MLflow logged %s"
                        % (digest, params["recipe_digest"]))

    # The version too, and not only the digest: the version is what the
    # directory is named, so a disagreement there means the manifest that was
    # read is not the one this model version was trained with.
    recipe_version = payload["dataset"]["version"]
    if (params.get("dataset_version")
            and params["dataset_version"] != recipe_version):
        problems.append("dataset version: run.json says %s, MLflow logged %s"
                        % (recipe_version, params["dataset_version"]))
    return problems


def latest_version(client, name: str = registry.REGISTERED_NAME):
    """The highest version of the registered model.

    NO LONGER WHAT THE SCORING PATH RESOLVES. Until L3 it was, and that was the
    defect: a retraining registered a version and the next cycle served it,
    better or worse. Serving now goes through champion_version() below. This
    one stays as a diagnostic -- "what is the newest version" is still a
    question somebody asks -- and answers nothing about what is in production.

    search_model_versions rather than get_registered_model().latest_versions:
    the latter is deprecated and its meaning changed between MLflow 2 and 3,
    while a version number is an integer and the largest one is unambiguous.
    """
    versions = list(client.search_model_versions("name='%s'" % name))
    if not versions:
        raise LookupError(
            "no version of model %r in the registry: B3-S2 has not registered a "
            "model yet, or the tracking server is not the one it registered "
            "with. Check MLFLOW_TRACKING_URI, then .\\fd-manager.ps1 train." % name)
    return max(versions, key=lambda version: int(version.version))


def champion_version(client, name: str = registry.REGISTERED_NAME,
                     alias: str = registry.CHAMPION_ALIAS):
    """The version the promotion gate put in production -- never the newest.

    Block: B4.
    Lot: B4-L3, B4-L0.
    Competency: B4-C5.
    Covers: B4-EC36.

    NO FALLBACK TO THE NEWEST VERSION, and that absence is the whole point. A
    fallback would restore exactly what L3 removes: a retraining serving its
    own result without having been judged. Refusing, and naming the remedy, is
    the honest failure -- the same choice as a dashboard that will not start
    rather than one that is open.
    """
    version = registry.champion(client, name, alias)
    if version is None:
        raise LookupError(
            "no version of model %r carries the alias %r, so nothing is in "
            "production. A model is registered by S2 and promoted by the gate "
            "of ml4fd/promote.py -- registering is not promoting. Run:\n"
            "    .\\fd-manager.ps1 promote\n"
            "which judges the newest version against the champion and, if it "
            "passes, moves the alias." % (name, alias))
    return version


def load(settings, tracking_uri: str | None = None,
         name: str = registry.REGISTERED_NAME) -> Scorer:
    """The model in production -- the one carrying the champion alias.

    Block: B3, B4.
    Lot: B3-N4, B4-L3, B4-L0.
    Competency: B4-C2, B4-C3, B4-C5.
    Covers: B3-EE09, B3-EC24, B4-EC13, B4-EC36.

    THE CACHE NEEDS NO INVALIDATION, and this is why. The version is resolved
    from the registry BEFORE the cache is consulted, and the key is
    (name, version). So a promotion that moves the alias yields a different
    key, a miss, and the new model -- without anybody having to remember to
    clear anything. forget() exists for the tests and for a deliberate reload,
    not for promotions.
    """
    import mlflow
    import mlflow.sklearn

    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)

    client = mlflow.MlflowClient()
    version = champion_version(client, name, settings.champion_alias)
    key = (name, str(version.version))
    if key in _CACHE:
        return _CACHE[key]

    params = dict(client.get_run(version.run_id).data.params)
    # TWO DIFFERENT THINGS, and each is used for what it is. dataset_version is
    # the 12-character truncation that NAMES THE DIRECTORY; recipe_digest is
    # the full sha256 that identifies the recipe and is cross-checked below.
    # Using the digest as a path is the defect of 2026-11-16.
    recipe_version = params.get("dataset_version")
    digest = params.get("recipe_digest")
    if not (recipe_version and digest):
        raise LookupError(
            "run %s of model version %s logged no %s, so its manifest cannot "
            "be found. It was not produced by this pipeline, and "
            "governance.yml declares model_registry_trust: own-pipeline-only."
            % (version.run_id, version.version,
               "dataset_version" if not recipe_version else "recipe_digest"))

    payload = read_manifest(manifest_path(settings, recipe_version))
    problems = agrees_with_registry(payload, params)
    if problems:
        raise ValueError(
            "the manifest on disk and the run logged in MLflow disagree about "
            "model version %s, so neither is authoritative: %s"
            % (version.version, "; ".join(problems)))

    chosen = payload["threshold"]
    # load_kwargs, not a bare call: MLflow 3 deserialises with skops, which
    # refuses a type it was not told to trust, and HistGradientBoosting's
    # TreePredictor is one of them. The parameter is discovered by signature --
    # its presence and its name vary by version, and governance.yml records the
    # condition the declaration rests on (model_registry_trust).
    scorer = Scorer(
        model=mlflow.sklearn.load_model(
            "models:/%s/%s" % (name, version.version),
            **registry.load_kwargs(mlflow.sklearn.load_model)),
        features=tuple(payload["dataset"]["features"]),
        threshold=float(chosen["value"]),
        model_version=str(version.version),
        recipe_digest=digest,
        policy=str(chosen.get("policy") or "?"),
        chosen_on=str(chosen.get("chosen_on") or "?"))

    _CACHE.clear()          # one version at a time: an old one is never reused
    _CACHE[key] = scorer
    log.info("modele %s version %s, recette %s, seuil %.2f (%s, choisi sur %s)",
             name, scorer.model_version, digest, scorer.threshold,
             scorer.policy, scorer.chosen_on)
    return scorer


BATCH_PATH = "/score/batch"


def _jsonable(value):
    """One feature value, in a form json can carry.

    DECIMAL BECOMES FLOAT, and that loses nothing that matters: train.py fits
    on floats -- the Parquet columns are cast before the fit -- so the model
    has never seen a Decimal. json cannot serialise one, and sending it as a
    STRING would be worse: the service builds a frame from what it receives,
    and a numeric column of strings beside a genuinely textual `category` is
    how a silent dtype confusion starts.
    """
    import decimal

    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


def remote_rows(frame, endpoint: str, timeout: int) -> list[dict]:
    """The rows fd_scores expects, decided by the service and not in-process.

    WHY THE PIPELINE NO LONGER LOADS THE MODEL. Until L5 dag_ingest called
    load() in the scheduler, so the model lived in two processes -- the
    scheduler's and the service's -- each resolving the champion alias on its
    own. A service nobody calls is not a decoupling, it is a second path. After
    this, exactly one process holds a model for serving.

    THE SAME SHAPE as as_rows() returns, so write() and everything downstream
    are untouched: the seam is the middle of score_batch and nothing else.

    THE DECISION COMES FROM THE SERVICE, flag included, and is not recomputed
    here against a locally read threshold. One place compares, one rounding,
    one answer -- the alternative is two comparisons that can disagree about a
    score four billionths from the line (see SCORE_SCALE).

    EVERY ROW IS ACCOUNTED FOR. A response short of one txn_id, or carrying one
    that was not asked about, is refused rather than written: fd_scores would
    otherwise hold a partial batch, and nothing downstream would say so.

    Block: B3, B4.
    Lot: B3-L5, B4-L0.
    Competency: B3-C3.
    Covers: B3-EC16, B3-EC19, B3-EC24.
    """
    import requests

    features = [name for name in frame.columns if name != "txn_id"]
    payload = {"rows": [
        {"txn_id": str(row["txn_id"]),
         "features": {name: _jsonable(row[name]) for name in features}}
        for _, row in frame.iterrows()]}

    url = endpoint.rstrip("/") + BATCH_PATH
    answer = requests.post(url, json=payload, timeout=timeout)
    if answer.status_code != 200:
        raise RuntimeError(
            "le service de scoring a refuse le lot (%d) : %s\n"
            "  son etat :  %s/health\n"
            "  ses journaux :  .\\fd-manager.ps1 logs -Service scoring-service"
            % (answer.status_code, answer.text[:400], endpoint.rstrip("/")))
    body = answer.json()

    asked = [str(value) for value in frame["txn_id"].tolist()]
    decided = {str(one["txn_id"]): one for one in body["decisions"]}
    missing = [txn_id for txn_id in asked if txn_id not in decided]
    extra = [txn_id for txn_id in decided if txn_id not in set(asked)]
    if missing or extra:
        raise RuntimeError(
            "le service a repondu sur un lot different de celui demande : "
            "%d ligne(s) sans reponse, %d reponse(s) non demandee(s). Rien "
            "n'est ecrit : fd_scores porterait un lot partiel sans que rien "
            "ne le dise." % (len(missing), len(extra)))

    threshold = float(body["threshold"])
    return [{"txn_id": txn_id,
             "score": round(float(decided[txn_id]["score"]), SCORE_SCALE),
             "threshold": threshold,
             "model_version": str(body["model_version"]),
             "recipe_digest": str(body["recipe_digest"]),
             "flagged": bool(decided[txn_id]["flagged"])}
            for txn_id in asked]


def forget() -> None:
    """Drop the cached model. For the tests, and for a deliberate reload."""
    _CACHE.clear()


def rows_to_score(conn, batch_id):
    """The transactions of a batch, as a frame carrying the query's own names.

    The column names come from the cursor, never from a list in this module:
    the recipe chooses which of them to use and in which order, and a Python
    copy of the select list drifting from the SQL would reorder the features
    without raising.
    """
    import pandas as pd

    rows, names = db.query_file_columns(conn, ROWS_SQL, batch_id=batch_id)
    return pd.DataFrame(rows, columns=names)


def write(conn, scored: list[dict]) -> int:
    """Persist the scores. Returns the number of rows written.

    executemany with the statement read once: four rows a minute today, but the
    same code serves a replay of a whole batch.
    """
    if not scored:
        return 0
    statement = db.sql_path(WRITE_SQL).read_text(encoding="utf-8")
    with conn.cursor() as cursor:
        cursor.executemany(statement, scored)
        return cursor.rowcount


def as_rows(frame, scores, scorer: Scorer) -> list[dict]:
    """One record per scored transaction, carrying its model version."""
    rows = []
    for txn_id, raw in zip(frame["txn_id"].tolist(), scores):
        score = round(float(raw), SCORE_SCALE)
        rows.append({"txn_id": txn_id,
                     "score": score,
                     "threshold": scorer.threshold,
                     "model_version": scorer.model_version,
                     "recipe_digest": scorer.recipe_digest,
                     "flagged": scorer.flagged(score)})
    return rows


def describe(scorer: Scorer) -> str:
    fields = dataclasses.asdict(scorer)
    fields.pop("model", None)
    return ", ".join("%s=%s" % item for item in sorted(fields.items()))
