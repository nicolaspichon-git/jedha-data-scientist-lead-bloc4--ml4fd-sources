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


# --- L4: a triggered retraining must see the data that triggered it ---------

def test_a_triggered_run_takes_its_cut_off_from_the_trigger_instant():
    """THE REGRESSION THIS PINS, measured 2026-11-20. dag_train is scheduled
    weekly; Airflow infers a manual run's data interval from the timetable, so
    every run triggered between two Mondays received data_interval_end =
    2026-11-16T02:00:00+00:00 -- always BEFORE the data that caused it. The
    cut-off is part of the recipe digest, so three retrainings in one afternoon
    reused one Parquet and v4 published exactly v2's metrics.

    A streamed row carries a 2026-11-20 label_ts and training_set.sql keeps
    only label_ts <= cut_off, so at that cut-off the stream contributed zero
    rows however much of it had arrived: a drift-triggered retraining could
    never see the drift that triggered it."""
    body = _task("resolve_cut_off")
    assert 'context["logical_date"]' in body
    assert "run.run_type in INTERVAL_RUN_TYPES" in body


def test_a_scheduled_run_still_replays_the_interval_it_was_scheduled_for():
    """The other half of the same branch, and the reason the fix is a branch
    rather than a replacement: a scheduled run that read the trigger instant
    would rebuild a different dataset on every replay, which is EI02 broken in
    the opposite direction."""
    body = _task("resolve_cut_off")
    assert 'context["data_interval_end"]' in body
    assert '"scheduled"' in _body()


def test_a_backfill_replays_its_interval_and_is_not_treated_as_a_trigger():
    """A backfill exists to reproduce a past interval, so it reads the interval
    like a scheduled run. Lumping it with manual runs would make a backfill
    build today's dataset and label it with last year's date."""
    found = re.search(r"INTERVAL_RUN_TYPES = \(([^)]*)\)", _body())
    assert found, "INTERVAL_RUN_TYPES is not declared"
    assert "backfill" in found.group(1)
    assert "manual" not in found.group(1)


def test_the_cut_off_is_never_read_from_the_clock_whichever_branch_runs():
    """logical_date is fixed when the run is created and stored on the dag_run,
    so clearing a task and re-running it reads the same instant. utcnow()
    inside the task would not, and EI02 would be a promise nobody keeps."""
    body = _task("resolve_cut_off")
    for forbidden in ("now()", "utcnow", "datetime.today", "time.time"):
        assert forbidden not in body


def test_the_log_says_what_the_other_branch_would_have_given():
    """The defect was invisible for three retrainings because the log stated a
    cut-off without stating that another reading existed. A triggered run now
    prints the data_interval_end it did NOT use."""
    body = _task("resolve_cut_off")
    # The branch that READS logical_date must also print the interval it
    # declined. Anchored on the read and not on "else:", which on the previous
    # single-branch version was itself the data_interval_end branch -- this
    # test passed on the broken code until it was anchored properly.
    tail = body[body.index('context["logical_date"]'):]
    assert 'context["data_interval_end"]' in tail


# --- L4: the loop closes at the end of this DAG ------------------------------

def test_the_chain_ends_on_the_promotion_and_not_on_the_registration():
    """THE GAP THIS CLOSES, found 2026-11-20 by asking when promote runs: it
    never did. No DAG, no cron and no entrypoint called it, so the cycle the
    specification asks to automate -- deployment, monitoring, retraining,
    challenging, deployment -- was automatic for three links out of five, and
    the two deciding what the system SERVES were a command typed by hand."""
    body = _body()
    chain = body[body.index("    moment = resolve_cut_off()"):]
    assert "challenge_and_promote(" in chain
    # downstream of the registration, not beside it: there is nothing to judge
    # before a version exists
    assert chain.index("challenge_and_promote(") < chain.index("register_model(")


def test_the_gate_judges_this_run_s_model_and_not_the_newest_registered():
    """Two overlapping trainings -- a drift-triggered run and the Monday
    schedule -- would otherwise let the gate compare a champion to a version
    some other run had produced."""
    body = _task("challenge_and_promote")
    assert "registry.version_of_run(client, registered[\"run_id\"])" in body
    assert "latest_version" not in body


def test_the_task_acts_rather_than_running_dry():
    """apply=True is the whole point: a gate that only reported would leave the
    alias where it was, and the loop would end one step short of serving."""
    body = _task("challenge_and_promote")
    assert "apply=True" in body


def test_a_non_decision_skips_and_does_not_report_a_success():
    """The candidate already carrying the alias, or a shared window too thin to
    ask, record nothing and leave the champion in place. Marking them success
    would make a loop that decided nothing look, in the interface, exactly like
    one that promoted."""
    body = _task("challenge_and_promote")
    assert "AirflowSkipException" in body
    for non_decision in ("AlreadyChampion", "Undecidable"):
        assert non_decision in body


def test_the_outcome_is_logged_at_warning_whichever_way_it_went():
    """A promotion changes what the system serves and a refusal explains why it
    did not; both are read later, in a log somebody scrolls. info would bury
    them among the twenty lines this DAG already writes."""
    body = _task("challenge_and_promote")
    tail = body[body.index('if outcome["promoted"]'):]
    assert tail.count("log.warning") == 2


def test_the_task_holds_no_decision_of_its_own():
    """Every threshold stays in ml4fd/promote.py and governance.yml. A
    comparison written here would be a second gate, reachable only from the
    DAG, and silently able to disagree with the one the operator sees."""
    body = _task("challenge_and_promote")
    for forbidden in ("significance", "alert_capacity", "p_value",
                      "promote.decide"):
        assert forbidden not in body
