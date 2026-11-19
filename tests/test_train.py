"""What a training run is allowed to claim.

sklearn is absent from the authoring environment, so the fit is not exercised
here -- only the reporting rules, which is where the plan's acceptance criteria
actually live."""
import pytest

from ml4fd import train
from ml4fd.train import SEED, Evaluation, render_report


def evaluation(**changes):
    # The figures of a REAL run: dag_train, 2026-11-16 00:07, dataset
    # eac9d0a92eef. A fixture of invented numbers tests the code against itself;
    # this one tests it against something that happened.
    base = dict(
        n_train=343_546, n_train_pos=1_593, n_test=50_349, n_test_pos=205,
        base_rate_train=1_593 / 343_546, base_rate_test=205 / 50_349,
        average_precision=0.7914, roc_auc=0.9977,
        baseline_average_precision=205 / 50_349,
        baseline_accuracy=1 - 205 / 50_349,
        threshold=0.5, recall=0.9610, precision=0.2688,
        true_positives=197, false_positives=536,
        false_negatives=8, true_negatives=49_608,
        rule_recall=0.5707, rule_precision=0.3703, rule_alerts_per_day=45.2,
        rule_description="montant > 300 ET heure dans {22, 23, 0, 1, 2, 3}",
        versions={"scikit-learn": "1.9.0"})
    base.update(changes)
    return Evaluation(**base)


# --- the trap, structurally -----------------------------------------------

def test_the_model_has_no_accuracy_field():
    """A metric that cannot be stored cannot be published by accident.

    At a 0.414 % base rate, a model that never predicts fraud scores 99.6 %.
    Accuracy is not a weak metric here, it is a misleading one."""
    fields = set(Evaluation.__dataclass_fields__)
    assert "accuracy" not in fields
    assert not {f for f in fields
                if "accuracy" in f and not f.startswith("baseline")}


def test_the_report_names_the_trivial_model_accuracy():
    """That one figure IS the argument for ignoring accuracy."""
    rendered = render_report(evaluation())
    assert "99.593 %" in rendered
    assert "JAMAIS" in rendered


def test_the_report_says_the_model_accuracy_is_not_computed():
    assert "n'est pas calculee" in render_report(evaluation())


# --- the base rate travels with every metric ------------------------------

def test_the_report_carries_both_base_rates():
    """Measured monthly rate runs 0.185 % to 0.554 % -- a factor of three. Two
    weekly runs are not comparable without them."""
    rendered = render_report(evaluation())
    assert "0.4637 %" in rendered      # train
    assert "0.4072 %" in rendered      # test


def test_the_baseline_pr_auc_is_the_base_rate():
    ev = evaluation()
    assert ev.baseline_average_precision == pytest.approx(ev.base_rate_test)


# --- lift: did the model learn anything at all? ---------------------------

def test_lift_is_the_ratio_to_the_trivial_model():
    assert evaluation().lift == pytest.approx(0.7914 / (205 / 50_349), rel=1e-6)


def test_a_lift_of_one_means_nothing_was_learned():
    """Whatever the accuracy would have said."""
    ev = evaluation(average_precision=205 / 50_349)
    assert ev.lift == pytest.approx(1.0)


def test_lift_survives_a_zero_baseline():
    assert evaluation(baseline_average_precision=0.0).lift == 0.0


# --- the operational reading ----------------------------------------------

def test_alerts_per_day_uses_the_api_rate_not_the_file_rate():
    """7,200 txn/day is what the API announces; the file holds 2,879. The two
    must not be conflated."""
    ev = evaluation()
    assert ev.alerts_per_day == pytest.approx(7200 * (197 + 536) / 50_349)


def test_alerts_per_day_survives_an_empty_test_set():
    assert evaluation(n_test=0).alerts_per_day == 0.0


# --- reproducibility ------------------------------------------------------

def test_the_seed_is_recorded_not_merely_fixed():
    """An unrecorded seed is not a fixed seed."""
    assert "%d" % SEED in render_report(evaluation())


def test_the_report_lists_the_library_versions():
    assert "scikit-learn" in render_report(evaluation())


def test_the_report_lists_the_features_in_order():
    rendered = render_report(evaluation())
    assert "amt, f_tx_count, f_avg_amount, f_home_distance_km" in rendered


def test_the_threshold_is_presented_as_a_reference_not_a_verdict():
    """M6 decides the threshold. M5 only reports at a stated one."""
    assert "pas un verdict" in render_report(evaluation())


# --- the analyst's rule, the baseline that actually competes ---------------

def test_the_gain_over_the_rule_is_counted_in_frauds():
    """Measured: the rule catches 117 of 205, the model 197. The model's
    precision is WORSE (0.269 against 0.370), so a single score would hide the
    trade-off -- the gain has to be stated in frauds."""
    assert evaluation().frauds_caught_over_rule == 197 - round(0.5707 * 205)


def test_a_model_matching_the_rule_shows_no_gain():
    ev = evaluation(true_positives=117, rule_recall=117 / 205)
    assert ev.frauds_caught_over_rule == 0


def test_the_report_names_the_rule_it_was_compared_against():
    """An unnamed baseline is not a baseline."""
    rendered = render_report(evaluation())
    assert "montant > 300" in rendered
    assert "qu'il faut battre" in rendered


def test_the_report_shows_the_rule_beating_the_model_on_precision():
    """0.3703 against 0.2688: the report must not hide it."""
    rendered = render_report(evaluation())
    assert "0.3703" in rendered and "0.2688" in rendered


def test_an_unevaluated_rule_is_said_so_rather_than_shown_as_zero():
    assert "non evaluee" in render_report(evaluation(rule_description=""))


def _source(function) -> str:
    """The body of a function, to assert on a decision the code makes rather
    than on a number it happens to produce."""
    import inspect

    return inspect.getsource(function)


# --- la liste de features vient de la recette ----------------------------------

def test_fit_and_evaluate_accepts_a_feature_list():
    """A run may drop txn_hour and txn_weekday, and the Parquet still holds them
    -- demoted to metadata -- so reading the module constant here would feed the
    model columns its own recipe says it does not use."""
    import inspect

    assert "features" in inspect.signature(train.fit_and_evaluate).parameters


def test_the_columns_are_not_read_from_the_constant_unconditionally():
    body = _source(train.fit_and_evaluate)
    assert "columns = list(features) if features else" in body


def test_the_fit_declares_only_the_categoricals_present():
    """Naming an absent column would fail inside sklearn with a message about
    the estimator rather than about the recipe."""
    body = _source(train._fit)
    assert "if name in x.columns" in body


def test_the_rule_baseline_still_reads_the_payment_hour():
    """It is a payment-hour rule, and the baseline the model must beat whichever
    recipe is in service. That is why a dropped feature stays in the file."""
    assert "txn_hour" in _source(train.rule_scores)
