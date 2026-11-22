"""The paired comparison, and the agreements it must refuse to count.

Block: B3, B4.
Lot: B3-N4, B4-L0.
Competency: B3-C5.
Covers: B3-EI04, B3-EC24.
"""
from __future__ import annotations

import pytest

from ml4fd import paired


def test_the_table_counts_the_four_cells():
    first = [True, True, False, False, True]
    second = [True, False, True, False, True]
    table = paired.compare(first, second)
    assert (table.both, table.only_first, table.only_second, table.neither) \
        == (2, 1, 1, 1)
    assert table.n == 5


def test_only_the_disagreements_carry_information():
    """The whole point. Two tables with the same disagreements must give the
    same p, however many rows they agree on -- that is what the aggregate
    comparison of two recalls cannot do."""
    small = paired.Paired(both=10, only_first=2, only_second=11, neither=3)
    large = paired.Paired(both=5000, only_first=2, only_second=11, neither=9000)
    assert small.p_value == large.p_value


def test_the_measured_illustration_reproduces():
    """The three rows quoted in the module docstring, which is where the claim
    that the paired view is stronger actually rests."""
    assert round(paired.two_sided_binomial(2, 11), 3) == 0.022
    assert round(paired.two_sided_binomial(6, 15), 3) == 0.078
    assert round(paired.two_sided_binomial(20, 29), 3) == 0.253


def test_the_same_gain_decides_or_not_depending_on_the_agreement():
    """Nine frauds gained in all three cases."""
    decides = paired.Paired(both=100, only_first=2, only_second=11, neither=92)
    does_not = paired.Paired(both=100, only_first=20, only_second=29, neither=56)
    assert decides.difference == does_not.difference == 9
    assert decides.p_value < 0.05
    assert does_not.p_value >= 0.05


def test_no_disagreement_is_no_evidence_and_not_an_error():
    table = paired.Paired(both=205, only_first=0, only_second=0, neither=0)
    assert table.p_value == 1.0
    assert table.favours == "aucun"
    assert "rien ne permet de preferer" in paired.render(table)


def test_the_test_is_symmetric():
    assert paired.two_sided_binomial(3, 11) == paired.two_sided_binomial(11, 3)


def test_a_two_sided_probability_never_exceeds_one():
    """2 * tail would pass 1 at an even split, and a probability above 1 would
    be reported as a perfectly ordinary non-significant result."""
    for first, second in ((1, 1), (5, 5), (50, 50), (0, 0)):
        assert paired.two_sided_binomial(first, second) <= 1.0


def test_the_difference_is_signed_towards_the_second():
    table = paired.compare([True, False], [False, True],
                           first_name="actuel", second_name="ampute")
    assert table.difference == 0
    table = paired.compare([False, False], [True, True],
                           first_name="actuel", second_name="ampute")
    assert table.difference == 2
    assert table.favours == "ampute"


def test_the_labels_restrict_the_comparison_to_the_frauds():
    """Which model catches more FRAUD is the usual question; the same table over
    the non-frauds answers the other one, so the filter is a parameter."""
    first = [True, True, False, True]
    second = [True, False, True, True]
    labels = [1, 1, 0, 0]
    table = paired.compare(first, second, labels=labels)
    assert table.n == 2
    assert (table.both, table.only_first) == (1, 1)


def test_two_decisions_of_different_length_are_refused():
    """They would silently pair row i of one with row i of the other and
    compare transactions that have nothing to do with each other."""
    with pytest.raises(ValueError, match="SAME rows"):
        paired.compare([True, False], [True])


def test_labels_of_the_wrong_length_are_refused():
    with pytest.raises(ValueError, match="labels"):
        paired.compare([True, False], [True, False], labels=[1])


def test_the_decision_is_taken_at_the_threshold_inclusive():
    """threshold.py compared score >= threshold when it chose the value, and
    fd_scores stores that decision. An exclusive test here would compare two
    decisions neither of which was taken."""
    assert paired.flags([0.93], 0.93) == [True]
    assert paired.flags([0.9299999], 0.93) == [False]


def test_the_report_names_the_verdict_rather_than_leaving_it_to_be_inferred():
    body = paired.render(
        paired.Paired(both=100, only_first=2, only_second=11, neither=92,
                      first_name="actuel", second_name="ampute"))
    assert "p = 0.022" in body
    assert "significatif" in body
    assert "ampute" in body


def test_a_non_significant_report_warns_against_reading_it_as_equivalence():
    body = paired.render(
        paired.Paired(both=100, only_first=20, only_second=29, neither=56))
    assert "ne tranchent pas" in body


# --- l'emboitement, ou le test ne s'applique pas -------------------------------

def test_two_thresholds_on_the_same_scores_are_nested():
    """The lower threshold flags everything the higher one flags, by
    arithmetic. So one disagreement cell is empty by construction."""
    scores = [0.1, 0.5, 0.9, 0.92, 0.95]
    table = paired.compare(paired.flags(scores, 0.93),
                           paired.flags(scores, 0.89), nested=True)
    assert table.only_first == 0
    assert table.only_second == 2


def test_a_nested_comparison_refuses_to_give_a_probability():
    """None and not 1.0: 1.0 reads as "no evidence of a difference", while the
    truth is that the test cannot be asked of these two decisions."""
    table = paired.Paired(both=129, only_first=0, only_second=7, neither=69,
                          nested=True)
    assert table.p_value is None


def test_the_degenerate_test_depends_only_on_the_count_of_disagreements():
    """THE defect this guards against. The threshold re-choice of 16 November
    2026 reported p = 0.016 on 7 disagreements, which is exactly 2 x (1/2)^7 --
    a number carrying no information from the data at all."""
    assert round(paired.two_sided_binomial(0, 7), 6) == round(2 * 0.5 ** 7, 6)
    for count in range(6, 12):
        assert paired.two_sided_binomial(0, count) < 0.05


def test_a_nested_report_names_the_trade_off_instead_of_a_verdict():
    body = paired.render(
        paired.Paired(both=129, only_first=0, only_second=7, neither=69,
                      nested=True, first_name="seuil 0.93",
                      second_name="seuil 0.89"))
    assert "ne s'applique pas" in body
    assert "arbitrage" in body
    assert "significatif" not in body


def test_an_empty_cell_without_declared_nesting_gets_a_note_not_a_verdict():
    """A cell can be empty by chance, so the figure is still given -- with the
    warning that the caller may owe a nested=True."""
    body = paired.render(
        paired.Paired(both=100, only_first=0, only_second=8, neither=50))
    assert "nested=True" in body
    assert "p = " in body


def test_a_comparison_of_two_models_is_not_nested_by_default():
    """Two different models can disagree in both directions, which is what
    makes the test meaningful there."""
    table = paired.compare([True, False, True], [False, True, True])
    assert table.nested is False
    assert table.p_value is not None
