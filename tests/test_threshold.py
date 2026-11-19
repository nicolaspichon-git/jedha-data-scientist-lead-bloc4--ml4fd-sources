"""Choosing the operating threshold. Pure arithmetic, so it is all testable."""
import pytest

from ml4fd.threshold import GRID, Point, choose, render, sweep


def point(**changes):
    base = dict(threshold=0.5, true_positives=10, false_positives=90,
                false_negatives=5, true_negatives=9_895,
                amount_missed=1_500.0, cost_false_alerts=360.0)
    base.update(changes)
    return Point(**base)


# --- the grid -------------------------------------------------------------

def test_the_grid_is_fixed_not_derived_from_the_scores():
    """The same data must give the same threshold whatever the model's score
    distribution -- and 0.37 is a number a human can read out in a review."""
    assert GRID[0] == 0.01 and GRID[-1] == 0.99
    assert len(GRID) == 99
    assert GRID == tuple(sorted(GRID))


# --- one point's arithmetic ----------------------------------------------

def test_recall_and_precision_come_from_the_matrix():
    p = point()
    assert p.recall == pytest.approx(10 / 15)
    assert p.precision == pytest.approx(10 / 100)


def test_a_point_that_flags_nothing_has_no_precision_rather_than_a_crash():
    p = point(true_positives=0, false_positives=0, false_negatives=15)
    assert p.precision == 0.0
    assert p.recall == 0.0


def test_alerts_per_day_uses_the_api_rate_not_the_file_rate():
    """7,200/day is the API's; the file holds 2,879. An alert count is an
    operational promise, so the two must not be conflated."""
    assert point().alerts_per_day(10_000) == pytest.approx(7200 * 100 / 10_000)


def test_the_total_cost_adds_the_money_lost_to_the_analyst_minutes():
    assert point().total_cost == pytest.approx(1_500.0 + 360.0)


# --- the sweep ------------------------------------------------------------

def test_the_sweep_costs_a_missed_fraud_its_own_amount():
    """The one number in this lot nobody has to believe: it is measured."""
    labels = [True, True, False]
    scores = [0.10, 0.90, 0.95]
    amounts = [500.0, 300.0, 20.0]
    points = {p.threshold: p for p in sweep(labels, scores, amounts, 4.0)}
    # At 0.50 the 500-euro fraud is missed and the 300 one caught.
    assert points[0.50].amount_missed == pytest.approx(500.0)
    assert points[0.50].false_positives == 1
    assert points[0.50].cost_false_alerts == pytest.approx(4.0)


def test_a_higher_threshold_never_flags_more():
    labels = [i % 7 == 0 for i in range(300)]
    scores = [(i % 100) / 100 for i in range(300)]
    amounts = [100.0] * 300
    points = sweep(labels, scores, amounts, 4.0)
    flagged = [p.flagged for p in points]
    assert flagged == sorted(flagged, reverse=True)


def test_an_empty_validation_slice_is_refused():
    with pytest.raises(ValueError, match="empty validation slice"):
        sweep([], [], [], 4.0)


# --- the policies ---------------------------------------------------------

def points_for_policies():
    """Three candidates: cheap but noisy, dear but quiet, and a middle."""
    return (
        point(threshold=0.20, true_positives=15, false_positives=900,
              false_negatives=0, amount_missed=0.0, cost_false_alerts=3_600.0),
        point(threshold=0.50, true_positives=12, false_positives=80,
              false_negatives=3, amount_missed=900.0, cost_false_alerts=320.0),
        point(threshold=0.90, true_positives=6, false_positives=4,
              false_negatives=9, amount_missed=2_700.0, cost_false_alerts=16.0),
    )


def test_the_cost_policy_picks_the_cheapest_total():
    choice = choose(points_for_policies(), "cost", capacity=60,
                    n_rows=10_000, chosen_on="validation")
    assert choice.threshold == 0.50          # 1220 against 3600 and 2716


def test_the_capacity_policy_picks_the_best_recall_within_the_ceiling():
    choice = choose(points_for_policies(), "capacity", capacity=60,
                    n_rows=10_000, chosen_on="validation")
    # 0.20 -> 659 alerts/day, 0.50 -> 66.2, 0.90 -> 7.2. Only the last fits.
    assert choice.threshold == 0.90


def test_both_policies_are_computed_whichever_one_decides():
    """A threshold chosen by a policy nobody can see is a number nobody can
    challenge."""
    choice = choose(points_for_policies(), "cost", capacity=60,
                    n_rows=10_000, chosen_on="validation")
    assert choice.by_cost.threshold == 0.50
    assert choice.by_capacity.threshold == 0.90
    assert choice.chosen is choice.by_cost


def test_a_cost_tie_prefers_the_higher_threshold():
    """Fewer alerts for the same money is strictly better for the analyst."""
    tied = (point(threshold=0.30, amount_missed=1_000.0, cost_false_alerts=0.0),
            point(threshold=0.70, amount_missed=1_000.0, cost_false_alerts=0.0))
    assert choose(tied, "cost", 60, 10_000, "val").threshold == 0.70


def test_an_unreachable_ceiling_warns_rather_than_hides():
    """The published alert count would otherwise be a promise nobody can keep."""
    choice = choose(points_for_policies(), "capacity", capacity=1,
                    n_rows=10_000, chosen_on="validation")
    assert choice.warning
    assert "not an operable promise" in choice.warning
    assert choice.threshold == 0.90          # the strictest available


def test_a_reachable_ceiling_warns_about_nothing():
    assert not choose(points_for_policies(), "capacity", capacity=60,
                      n_rows=10_000, chosen_on="validation").warning


def test_no_candidate_at_all_is_refused():
    with pytest.raises(ValueError, match="no candidate"):
        choose((), "cost", 60, 10_000, "val")


# --- the report -----------------------------------------------------------

def test_the_report_says_the_threshold_was_not_chosen_on_the_test_set():
    """The one mistake in this lot that no metric would reveal."""
    rendered = render(choose(points_for_policies(), "cost", 60, 10_000,
                             "validation 2020-08-20..2020-10-31"))
    assert "JAMAIS sur le jeu de test" in rendered
    assert "2020-08-20..2020-10-31" in rendered


def test_the_report_shows_the_policy_that_did_not_decide():
    rendered = render(choose(points_for_policies(), "cost", 60, 10_000, "val"))
    assert "au moindre cout" in rendered and "dans la capacite" in rendered
    assert "SEUIL RETENU 0.50" in rendered


def test_the_report_carries_the_warning_when_there_is_one():
    rendered = render(choose(points_for_policies(), "capacity", 1, 10_000, "val"))
    assert "AVERTISSEMENT" in rendered
