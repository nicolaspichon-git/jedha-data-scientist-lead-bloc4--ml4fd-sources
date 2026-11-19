"""What the scoring refuses, and what it reads from the model rather than itself.

Covers: EE09, EI04, EC23, EC24
Lot: N4
Competency: C5

Neither MLflow nor scikit-learn is needed here: what is tested is the contract
around the model -- where the feature order comes from, where the threshold
comes from, what happens when the two records of them disagree, and the one
boundary that can roll back a whole ingestion cycle.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
import pytest

from ml4fd import datasets, db, score

RECIPE = "de96c967f7e6"
FEATURES = ["amt", "f_tx_count", "f_avg_amount", "f_home_distance_km",
            "txn_hour", "txn_weekday", "category"]


# The REAL shapes, measured on datasets/fraud_scoring/de96c967f7e6/run.json:
# the version is the digest truncated to 12 characters and names the directory;
# the digest is the full 64-character sha256 and names nothing on disk.
VERSION = "de96c967f7e6"
DIGEST = "de96c967f7e65c4d22c1cd681ebd142f18d32f525bacc64bc28bb9d6e10d1159"


def _payload(threshold=0.93, features=None, digest=DIGEST, version=VERSION):
    return {
        "dataset": {"features": list(features if features is not None
                                     else FEATURES),
                    "recipe_digest": digest, "version": version},
        "threshold": {"value": threshold, "policy": "capacity",
                      "chosen_on": "la validation 2020-10-01..2020-10-31"},
    }


def _params(payload):
    """The parameters MLflow logged for the run, as registry.params_from makes
    them -- INCLUDING dataset_version, because that is the one that names the
    directory and the one score.load read wrong."""
    return {"features": ",".join(payload["dataset"]["features"]),
            "threshold": str(payload["threshold"]["value"]),
            "recipe_digest": payload["dataset"]["recipe_digest"],
            "dataset_version": payload["dataset"]["version"]}


def _write(tmp_path, payload) -> Path:
    path = tmp_path / "run.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


class _Model:
    """Returns the SECOND column as the probability, like predict_proba.

    A numpy array and not a list of lists: score.scores() slices with [:, 1],
    so a list would fail here on the fake rather than on the code -- and a fake
    that cannot stand in for the real call tests nothing.
    """

    def __init__(self, probabilities):
        self.probabilities = probabilities
        self.seen = None

    def predict_proba(self, frame):
        import numpy as np
        self.seen = list(frame.columns)
        return np.array([[1 - p, p]
                         for p in self.probabilities[:len(frame)]])


def _scorer(model, threshold=0.93, features=None):
    return score.Scorer(
        model=model, features=tuple(features if features is not None
                                    else FEATURES),
        threshold=threshold, model_version="1", recipe_digest=RECIPE,
        policy="capacity", chosen_on="la validation")


def _sql(relative: str) -> str:
    return db.sql_path(relative).read_text(encoding="utf-8")


def _without_comments(body: str) -> str:
    return "\n".join(line for line in body.splitlines()
                     if not line.strip().startswith("--"))


# --- the manifest, and what it must carry -------------------------------------

def test_a_missing_manifest_names_the_remedy(tmp_path):
    with pytest.raises(FileNotFoundError, match="fd-manager.ps1 train"):
        score.read_manifest(tmp_path / "run.json")


def test_a_manifest_without_features_is_refused(tmp_path):
    payload = _payload()
    payload["dataset"]["features"] = []
    with pytest.raises(ValueError, match="no feature list"):
        score.read_manifest(_write(tmp_path, payload))


def test_a_manifest_without_a_threshold_is_refused(tmp_path):
    """The refusal that matters. A default of 0.5 on the measured distribution
    floods the analysts instead of alerting them, so there is no default."""
    payload = _payload()
    payload["threshold"] = None
    with pytest.raises(ValueError, match="carries no threshold"):
        score.read_manifest(_write(tmp_path, payload))


def test_a_manifest_with_a_threshold_of_zero_is_refused(tmp_path):
    """0 would flag every transaction, and `if not value` must catch it rather
    than letting it read as a deliberate choice."""
    with pytest.raises(ValueError, match="carries no threshold"):
        score.read_manifest(_write(tmp_path, _payload(threshold=0)))


def test_a_complete_manifest_is_read(tmp_path):
    payload = score.read_manifest(_write(tmp_path, _payload()))
    assert payload["dataset"]["features"] == FEATURES
    assert payload["threshold"]["value"] == 0.93


# --- the two records of the same run ------------------------------------------

def test_a_manifest_that_agrees_with_the_registry_passes():
    payload = _payload()
    assert score.agrees_with_registry(payload, _params(payload)) == []


def test_a_different_feature_order_is_a_disagreement():
    """Not merely a different SET: the ORDER is what the model was fitted on."""
    payload = _payload()
    params = _params(payload)
    params["features"] = ",".join(reversed(FEATURES))
    problems = score.agrees_with_registry(payload, params)
    assert len(problems) == 1
    assert "features" in problems[0]


def test_a_different_threshold_is_a_disagreement():
    payload = _payload()
    params = _params(payload)
    params["threshold"] = "0.5"
    assert any("threshold" in problem
               for problem in score.agrees_with_registry(payload, params))


def test_a_different_digest_is_a_disagreement():
    payload = _payload()
    params = _params(payload)
    params["recipe_digest"] = "ffffffffffff"
    assert any("digest" in problem
               for problem in score.agrees_with_registry(payload, params))


def test_a_run_logged_without_a_policy_is_not_a_disagreement():
    """registry.params_from writes "fixed" when no policy chose the threshold.
    Reading that as a numeric disagreement would refuse a sound model."""
    payload = _payload()
    params = _params(payload)
    params["threshold"] = "fixed"
    assert score.agrees_with_registry(payload, params) == []


# --- the feature order comes from the model, not from this code ---------------

def test_the_model_sees_the_columns_in_the_recipe_order():
    model = _Model([0.5])
    frame = pd.DataFrame([{name: 1 for name in reversed(FEATURES)}])
    _scorer(model).scores(frame)
    assert model.seen == FEATURES


def test_a_recipe_without_txn_hour_scores_without_touching_this_module():
    """The decision N4 puts to the user must not require a code change."""
    amputated = [name for name in FEATURES
                 if name not in ("txn_hour", "txn_weekday")]
    model = _Model([0.1])
    frame = pd.DataFrame([{name: 1 for name in FEATURES}])
    _scorer(model, features=amputated).scores(frame)
    assert model.seen == amputated


def test_a_recipe_asking_for_a_column_the_query_lacks_is_refused_by_name():
    model = _Model([0.1])
    frame = pd.DataFrame([{"amt": 1.0}])
    with pytest.raises(ValueError, match="f_tx_count"):
        _scorer(model).scores(frame)


def test_the_module_does_not_name_the_feature_list_itself():
    """If score.py referred to datasets.FEATURES, a retrained recipe would be
    scored against the code's idea of the features instead of the model's."""
    body = Path(score.__file__).read_text(encoding="utf-8")
    assert "datasets.FEATURES" not in body
    assert "FEATURES" not in re.sub(r"#.*", "", body)


# --- the boundary that can roll back an ingestion cycle -----------------------

def test_a_score_just_below_the_threshold_decides_as_it_will_be_stored():
    """fd_scores carries CHECK (flagged = (score >= threshold)). Measured:
    0.929999996 is below 0.93 as a float and equal to it once rounded to the
    column's eight decimals, so comparing before rounding makes the INSERT fail
    and rolls back the whole cycle for one transaction four billionths out."""
    scorer = _scorer(_Model([]), threshold=0.93)
    assert scorer.flagged(0.929999996) is True


def test_the_stored_score_and_the_stored_decision_cannot_disagree():
    raw = [0.929999996, 0.930000004, 0.93, 0.92999, 0.95]
    scorer = _scorer(_Model(raw), threshold=0.93)
    frame = pd.DataFrame({"txn_id": ["a", "b", "c", "d", "e"]})
    for row in score.as_rows(frame, raw, scorer):
        assert row["flagged"] == (row["score"] >= row["threshold"]), row


def test_the_score_is_rounded_to_the_column_scale():
    scorer = _scorer(_Model([]), threshold=0.93)
    frame = pd.DataFrame({"txn_id": ["a"]})
    row = score.as_rows(frame, [0.1234567891234], scorer)[0]
    assert row["score"] == round(0.1234567891234, score.SCORE_SCALE)


def test_every_row_carries_the_model_version_and_the_recipe():
    scorer = _scorer(_Model([]))
    frame = pd.DataFrame({"txn_id": ["a", "b"]})
    for row in score.as_rows(frame, [0.1, 0.99], scorer):
        assert row["model_version"] == "1"
        assert row["recipe_digest"] == RECIPE
        assert row["threshold"] == 0.93


# --- the registry -------------------------------------------------------------

class _Version:
    def __init__(self, version, run_id="r"):
        self.version = version
        self.run_id = run_id


class _Client:
    def __init__(self, versions):
        self.versions = versions

    def search_model_versions(self, _filter):
        return self.versions


def test_version_ten_is_newer_than_version_nine():
    """Compared as integers. Lexicographically "9" > "10", so a string
    comparison would pin the registry to version 9 forever -- after nine weekly
    retrains, silently."""
    client = _Client([_Version("9"), _Version("10"), _Version("2")])
    assert score.latest_version(client, "fraud_scoring").version == "10"


def test_an_empty_registry_names_what_to_check():
    with pytest.raises(LookupError, match="MLFLOW_TRACKING_URI"):
        score.latest_version(_Client([]), "fraud_scoring")


def test_forget_empties_the_cache():
    score._CACHE[("fraud_scoring", "1")] = "sentinel"
    score.forget()
    assert not score._CACHE


# --- the SQL ------------------------------------------------------------------

def test_the_derived_columns_match_the_training_set_word_for_word():
    """A model trained on one expression and scored on another is a model
    nobody has measured."""
    def derived(relative):
        body = _without_comments(_sql(relative))
        return [re.sub(r"\s+", " ", line).strip().rstrip(",")
                for line in body.splitlines()
                if "extract(" in line and "AS txn_" in line]

    assert derived("scoring/rows_to_score.sql") == \
        derived("datasets/training_set.sql")
    assert derived("scoring/rows_to_score.sql")      # and not both empty


def test_the_scoring_query_offers_every_feature_of_the_current_recipe():
    body = _without_comments(_sql("scoring/rows_to_score.sql"))
    for name in datasets.FEATURES:
        assert name in body, name


def test_the_scoring_query_refuses_a_row_without_features():
    """A NULL f_tx_count would be read by the model as "no history" where the
    truth is "not computed"."""
    body = _without_comments(_sql("scoring/rows_to_score.sql"))
    assert "f_tx_count IS NOT NULL" in body


def test_the_scoring_query_is_scoped_to_one_batch():
    body = _without_comments(_sql("scoring/rows_to_score.sql"))
    assert "batch_id = %(batch_id)s" in body


def test_the_write_is_idempotent_on_the_transaction_and_the_version():
    body = _without_comments(_sql("scoring/write_scores.sql"))
    assert "ON CONFLICT (txn_id, model_version) DO NOTHING" in body
    assert "DO UPDATE" not in body


def test_the_table_keys_on_the_transaction_and_the_version():
    """A key on txn_id alone would let a retrain erase the score the previous
    model gave -- and that score is the answer to the first question of the
    first incident."""
    body = _without_comments(_sql("ddl/080_scores.sql"))
    assert "PRIMARY KEY (txn_id, model_version)" in body
    assert "CHECK (flagged = (score >= threshold))" in body


def test_the_ingesting_role_cannot_rewrite_a_score():
    body = _sql("ddl/080_scores.sql")
    assert "REVOKE UPDATE, DELETE, TRUNCATE ON fd_scores FROM fd_ingest" in body


def test_no_label_column_lives_in_the_scores_table():
    """fd_ground_truth stays the only place a verdict lives (F02.3)."""
    body = _without_comments(_sql("ddl/080_scores.sql"))
    for forbidden in ("is_fraud", "label", "verdict"):
        assert forbidden not in body, forbidden


# --- la version nomme le repertoire, l'empreinte ne nomme rien ----------------

def test_the_manifest_path_is_built_from_the_version_not_the_digest():
    """THE defect of 16 November 2026. MLflow logs both; score.load read the
    wrong one and looked for

        datasets/fraud_scoring/8813b096e2e4049cf3...c666/run.json

    under a directory called 8813b096e2e4. The message blamed a missing
    manifest and told the operator to retrain a recipe that was perfectly well
    trained.
    """
    import types

    from ml4fd import datasets

    settings = types.SimpleNamespace(datasets_root="/opt/project/datasets",
                                     dataset_family="fraud_scoring")
    path = score.manifest_path(settings, VERSION)
    assert path.parent.name == VERSION
    assert len(path.parent.name) == datasets.VERSION_CHARS
    assert DIGEST not in str(path)


def test_the_version_is_the_digest_truncated():
    """Stated here so the two values cannot be taken for interchangeable."""
    from ml4fd import datasets

    assert DIGEST.startswith(VERSION)
    assert len(VERSION) == datasets.VERSION_CHARS
    assert len(DIGEST) == 64


def test_a_missing_manifest_says_what_it_looked_for(tmp_path):
    """The old message named neither the length it received nor the recipes
    that are present, so it read as "retrain" when the truth was "wrong
    directory"."""
    import types

    (tmp_path / "fraud_scoring" / VERSION).mkdir(parents=True)
    settings = types.SimpleNamespace(datasets_root=str(tmp_path),
                                     dataset_family="fraud_scoring")
    with pytest.raises(FileNotFoundError) as problem:
        score.read_manifest(score.manifest_path(settings, DIGEST))
    message = str(problem.value)
    assert "64 characters long" in message
    assert VERSION in message            # the recipe that IS there
    assert "VERSION" in message


def test_a_differing_dataset_version_is_a_disagreement():
    """The version is what the directory is named, so a disagreement there
    means the manifest that was read is not the one this model version was
    trained with."""
    payload = _payload()
    params = _params(payload)
    params["dataset_version"] = "ffffffffffff"
    problems = score.agrees_with_registry(payload, params)
    assert any("dataset version" in problem for problem in problems)


def test_the_loader_reads_the_version_for_the_path():
    body = Path(score.__file__).read_text(encoding="utf-8")
    assert 'recipe_version = params.get("dataset_version")' in body
    assert "manifest_path(settings, recipe_version)" in body
    assert "manifest_path(settings, digest)" not in body
