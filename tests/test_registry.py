"""The chain from a model back to its rows, and the manifest that carries it.

mlflow is absent from the authoring environment, so log_run is not exercised
here. What IS exercised is everything it reads: the manifest, and the two
translations into parameters and metrics. That is where the chain can break
silently."""
import datetime as dt
import json

import pytest

from ml4fd import datasets, registry
from ml4fd.threshold import Point, ThresholdChoice
from ml4fd.train import Evaluation

UTC = dt.timezone.utc


@pytest.fixture
def recipe():
    return datasets.Recipe(
        family="fraud_scoring", schema_version="v2",
        cut_off=dt.datetime(2020, 12, 22, 23, 13, 39, tzinfo=UTC),
        features=datasets.FEATURES,
        split_rule="temporal:train<=2020-10-31,test<=2020-11-30")


@pytest.fixture
def evaluation():
    return Evaluation(
        n_train=343_546, n_train_pos=1_593, n_test=50_349, n_test_pos=205,
        base_rate_train=1_593 / 343_546, base_rate_test=205 / 50_349,
        average_precision=0.7914, roc_auc=0.9977,
        baseline_average_precision=205 / 50_349,
        baseline_accuracy=1 - 205 / 50_349,
        threshold=0.46, recall=0.9610, precision=0.2688,
        true_positives=197, false_positives=536,
        false_negatives=8, true_negatives=49_608,
        rule_recall=0.5707, rule_precision=0.3703, rule_alerts_per_day=45.2,
        rule_description="montant > 300 ET heure dans {22, 23, 0, 1, 2, 3}",
        versions={"scikit-learn": "1.9.1", "numpy": "1.26.4"})


@pytest.fixture
def choice():
    p = Point(threshold=0.46, true_positives=197, false_positives=536,
              false_negatives=8, true_negatives=49_608,
              amount_missed=2_900.0, cost_false_alerts=2_144.0)
    return ThresholdChoice(policy="capacity", chosen=p, by_cost=p,
                           by_capacity=p, n_rows=50_349,
                           chosen_on="la validation 2020-08-20..2020-10-31",
                           capacity=60)


# --- the chain ------------------------------------------------------------

def test_the_manifest_carries_the_whole_chain(recipe, evaluation, choice):
    """Recipe digest, cut-off, ordered features, split rule, seed, versions. A
    break anywhere turns a performance figure into a claim nobody can check."""
    payload = registry.manifest(recipe, evaluation, choice, "model.joblib")
    assert payload["dataset"]["recipe_digest"] == recipe.digest()
    assert payload["dataset"]["cut_off"].startswith("2020-12-22")
    assert payload["dataset"]["features"] == list(datasets.FEATURES)
    assert payload["dataset"]["split_rule"] == recipe.split_rule
    assert payload["model"]["seed"] == evaluation.seed
    assert payload["model"]["versions"]["scikit-learn"] == "1.9.1"


def test_the_feature_order_is_preserved_not_sorted(recipe, evaluation, choice):
    """A model trained on columns in one order cannot be fed them in another."""
    payload = registry.manifest(recipe, evaluation, choice, "model.joblib")
    assert payload["dataset"]["features"] != sorted(datasets.FEATURES)
    assert payload["dataset"]["features"][0] == "amt"


def test_the_manifest_records_no_model_accuracy(recipe, evaluation, choice):
    """The record cannot hold what the report refuses to publish."""
    payload = registry.manifest(recipe, evaluation, choice, "model.joblib")
    assert not any("accuracy" in k for k in payload["metrics"])
    assert "accuracy" in payload["trivial_model"]      # named for what it is


def test_the_manifest_records_how_the_threshold_was_chosen(
        recipe, evaluation, choice):
    payload = registry.manifest(recipe, evaluation, choice, "model.joblib")
    assert payload["threshold"]["policy"] == "capacity"
    assert "validation" in payload["threshold"]["chosen_on"]
    assert payload["threshold"]["by_cost"] == 0.46


def test_a_run_without_a_chosen_threshold_says_so(recipe, evaluation):
    payload = registry.manifest(recipe, evaluation, None, "model.joblib")
    assert payload["threshold"] is None


def test_the_manifest_is_json_serialisable(recipe, evaluation, choice):
    """It is written to disk: a value json cannot render would lose the chain."""
    payload = registry.manifest(recipe, evaluation, choice, "model.joblib")
    assert json.loads(json.dumps(payload)) == payload


# --- what MLflow is fed ---------------------------------------------------

def test_the_parameters_carry_the_identifiers_not_the_figures(
        recipe, evaluation, choice):
    """A parameter can be filtered on; a sentence in a description cannot."""
    params = registry.params_from(
        registry.manifest(recipe, evaluation, choice, "model.joblib"))
    assert params["dataset_version"] == recipe.version()
    assert params["recipe_digest"] == recipe.digest()
    assert params["threshold_policy"] == "capacity"
    assert params["n_features"] == len(datasets.FEATURES)


def test_the_metrics_always_carry_both_base_rates(recipe, evaluation, choice):
    """0.185 % to 0.554 % across months: two runs are not comparable without."""
    metrics = registry.metrics_from(
        registry.manifest(recipe, evaluation, choice, "model.joblib"))
    assert metrics["base_rate_train"] == pytest.approx(1_593 / 343_546)
    assert metrics["base_rate_test"] == pytest.approx(205 / 50_349)


def test_the_metrics_hold_only_numbers(recipe, evaluation, choice):
    """MLflow refuses a non-numeric metric, and would lose the whole run."""
    metrics = registry.metrics_from(
        registry.manifest(recipe, evaluation, choice, "model.joblib"))
    for name, value in metrics.items():
        assert isinstance(value, (int, float)), name


def test_the_metrics_include_the_gain_over_the_analyst_rule(
        recipe, evaluation, choice):
    metrics = registry.metrics_from(
        registry.manifest(recipe, evaluation, choice, "model.joblib"))
    assert metrics["frauds_caught_over_rule"] == 197 - round(0.5707 * 205)


# --- the artefacts on disk ------------------------------------------------

def test_the_manifest_and_the_lock_are_written(tmp_path, recipe, evaluation,
                                               choice):
    """They live beside the dataset so the chain survives a tracking server
    that is down, moved or wiped."""
    payload = registry.manifest(recipe, evaluation, choice, "model.joblib")
    written = registry.write_manifest(payload, tmp_path)
    locked = registry.write_lock("pandas==2.3.3\n", tmp_path)
    assert json.loads(written.read_text(encoding="utf-8")) == payload
    assert locked.read_text(encoding="utf-8") == "pandas==2.3.3\n"


def test_an_unavailable_pip_freeze_is_recorded_as_a_gap_not_a_crash():
    """A missing lock file is a gap in the audit trail, not a reason to throw
    away a trained model."""
    text = registry.resolved_packages()
    assert isinstance(text, str) and text


# --- the trusted-type declaration -----------------------------------------

def test_the_trusted_type_list_is_short_and_explicit():
    """skops warns against passing back everything get_untrusted_types()
    reports just to make a file load. Three types, each read before listing."""
    assert len(registry.TRUSTED_SKOPS_TYPES) == 3
    assert all(isinstance(t, str) and "." in t
               for t in registry.TRUSTED_SKOPS_TYPES)


def test_the_tree_predictor_is_the_one_that_matters():
    """Its node indices are read by scikit-learn without bounds checking."""
    assert any("TreePredictor" in t for t in registry.TRUSTED_SKOPS_TYPES)


def test_the_model_keywords_are_discovered_not_guessed():
    """MLflow 3 renamed artifact_path to name and added skops_trusted_types.
    Guessing a parameter has cost this project three rounds."""
    def mlflow_2(sk_model, artifact_path=None, registered_model_name=None):
        pass

    def mlflow_3(sk_model, name=None, registered_model_name=None,
                 skops_trusted_types=None):
        pass

    assert registry.model_kwargs(mlflow_2) == {"artifact_path": "model"}
    assert registry.model_kwargs(mlflow_3) == {
        "name": "model",
        "skops_trusted_types": list(registry.TRUSTED_SKOPS_TYPES)}


def test_an_mlflow_without_skops_gets_no_skops_argument():
    """Passing an unknown keyword would raise before anything is logged."""
    def older(sk_model, artifact_path=None):
        pass

    assert "skops_trusted_types" not in registry.model_kwargs(older)


def test_load_kwargs_declares_the_trusted_types_when_the_parameter_exists():
    """Loading needs the declaration as much as logging did: skops refuses a
    type it was not told to trust, on the way back as on the way out."""
    def with_parameter(uri, skops_trusted_types=None):
        pass

    got = registry.load_kwargs(with_parameter)
    assert got["skops_trusted_types"] == list(registry.TRUSTED_SKOPS_TYPES)


def test_load_kwargs_passes_nothing_the_signature_does_not_accept():
    """A version of MLflow without the parameter must not receive it: the call
    would fail on a keyword rather than on anything real."""
    def without_parameter(uri):
        pass

    assert registry.load_kwargs(without_parameter) == {}


def test_the_trusted_types_are_declared_one_by_one():
    """skops warns against passing back everything get_untrusted_types()
    reports, which would trust whatever a crafted file happens to contain."""
    assert len(registry.TRUSTED_SKOPS_TYPES) == 3
    assert any("TreePredictor" in name
               for name in registry.TRUSTED_SKOPS_TYPES)
