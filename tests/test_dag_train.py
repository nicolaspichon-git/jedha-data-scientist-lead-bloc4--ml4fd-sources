"""What the training DAG must thread through, without importing Airflow.

Covers: EC05, EC23, EI04
Lot: N4
Competency: C1, C5

The DAG cannot be imported here -- Airflow is not installed in the authoring
environment -- so these read its source. That is a weaker test than running it,
and it is still worth having: the defect it guards against is one I would have
shipped. build_dataset and fit_model each build a Recipe, and if one read
datasets.FEATURES while the other read the run's amputated list, fit_model would
look for a dataset under a digest build_dataset never wrote -- and the error
would name a missing Parquet file, not a mismatched recipe.
"""
from __future__ import annotations

import re
from pathlib import Path

SOURCE = (Path(__file__).resolve().parents[1] / "airflow" / "dags"
          / "dag_train.py")


def _body() -> str:
    return SOURCE.read_text(encoding="utf-8")


def _task(name: str) -> str:
    body = _body()
    start = body.index("def %s(" % name)
    rest = body[start + 1:]
    following = re.search(r"\n    @task\b|\n    moment = ", rest)
    return rest[:following.start()] if following else rest


def test_the_run_can_amputate_the_recipe_from_conf():
    """Editing datasets.FEATURES would produce the amputated model and, in the
    same move, make the recipe IN SERVICE unrebuildable: its digest is computed
    from the feature list."""
    body = _task("resolve_features")
    assert 'conf or {}).get("drop_features")' in body
    assert "datasets.without(" in body


def test_both_recipes_read_the_resolved_list_and_not_the_constant():
    """THE test of this file."""
    for name in ("build_dataset", "fit_model"):
        body = _task(name)
        assert "features=tuple(features)" in body, name
        assert "features=datasets.FEATURES" not in body, name


def test_both_tasks_receive_the_list_as_a_parameter():
    for name in ("build_dataset", "fit_model"):
        assert re.search(r"def %s\(cut_off: str, features: list" % name,
                         _body()), name


def test_the_fit_is_given_the_recipe_s_features():
    """The Parquet still holds the dropped columns -- demoted to metadata -- so
    fit_and_evaluate reading the module constant would train on columns the
    recipe says it does not use, and the manifest would record a feature list
    the model never saw."""
    assert "features=recipe.features" in _task("fit_model")


def test_the_two_tasks_are_wired_to_the_same_resolution():
    """One resolve_features for the whole run: calling it twice could, in
    principle, read a conf that changed between the two."""
    wiring = _body()[_body().index("    moment = resolve_cut_off()"):]
    # Counted in the WIRING only: "def resolve_features()" also contains the
    # call text, so counting over the whole file counts the definition too --
    # which is how this test failed the first time, on itself rather than on
    # the DAG.
    assert wiring.count("resolve_features()") == 1
    assert "kept = resolve_features()" in wiring
    assert wiring.count("kept") == 3      # the assignment and the two tasks


def test_an_amputated_run_is_logged_as_such():
    """A run that silently trained something else would be indistinguishable in
    the service log from the weekly one."""
    assert "AMPUTEE" in _task("resolve_features")


def test_the_cut_off_still_comes_from_the_interval_and_never_from_the_clock():
    """Guarded here because this file now edits the same wiring."""
    body = _task("resolve_cut_off")
    assert 'context["data_interval_end"]' in body
    assert "now()" not in body
    assert "utcnow" not in body


def test_the_declared_removal_is_the_default_of_a_weekly_run():
    """A decision that only applies when somebody remembers a flag is not a
    decision. The weekly run reads governance.yml and needs no argument."""
    body = _task("resolve_features")
    assert "settings.drop_features" in body
    assert 'source = "governance.yml"' in body


def test_conf_can_ask_for_the_full_recipe():
    """THE escape hatch, and the reason every recipe stays rebuildable: a drop
    list can only REMOVE columns, never add them back, so without this there
    would be no way to reach the 7-feature recipe once governance declares a
    removal."""
    body = _task("resolve_features")
    assert '("", "none", "aucun")' in body


def test_a_full_recipe_run_is_warned_about():
    """It is now the unusual case, and the one the measurement argues against."""
    body = _task("resolve_features")
    assert "recette COMPLETE" in body
    assert "2.8e-4" in body
