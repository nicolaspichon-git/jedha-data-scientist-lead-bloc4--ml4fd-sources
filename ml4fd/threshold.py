"""Choosing the operating threshold, and showing the work.

Block: B3, B4.
Lot: B3-M6, B4-L0.
Competency: B3-C2, B3-C5.
Covers: B3-EE08, B3-EI04, B3-EC09.

0.5 IS NOT A DECISION. It is the default of a library, and at a 0.4 % base rate
it produced 105 alerts a day for a precision of 0.27 -- where the analyst's
two-condition rule produced 45 for 0.37. A threshold is a business trade-off
between the money a missed fraud costs and the minutes a false alert costs, so
it is chosen from declared quantities and the whole curve is reported.

TWO POLICIES, BOTH COMPUTED, ONE DECIDING:

  cost     -- minimise money lost. The cost of a missed fraud is ITS OWN
              AMOUNT, measured per transaction rather than assumed. That is the
              one number in this file nobody has to believe.
  capacity -- the highest recall that stays within what an analyst can review in
              a morning. This is what G2 actually constrains: "each morning,
              review the previous day's payments and frauds" has a ceiling.

Reporting both is the point. A threshold chosen by a policy nobody can see is a
number nobody can challenge.

WHERE IT IS CHOSEN. On the last slice of the TRAINING period, never on the test
set. A threshold tuned on the test set makes the published recall and precision
optimistic by construction -- the one mistake in this lot that no metric would
reveal.
"""
from __future__ import annotations

from dataclasses import dataclass

# The rate the API announces, not the 2,879/day of the historical file. The two
# must not be conflated: an alert count is an operational promise.
API_TXN_PER_DAY = 7200.0

# A fixed grid rather than the observed scores: the same data must give the same
# threshold whatever the model's score distribution, and a threshold of 0.37 is
# a number a human can read out in a review.
GRID = tuple(round(0.01 * i, 2) for i in range(1, 100))


@dataclass(frozen=True)
class Point:
    """One threshold, and everything it implies."""
    threshold: float
    true_positives: int
    false_positives: int
    false_negatives: int
    true_negatives: int
    amount_missed: float        # money of the frauds not caught
    cost_false_alerts: float    # analyst minutes, priced

    @property
    def flagged(self) -> int:
        return self.true_positives + self.false_positives

    @property
    def recall(self) -> float:
        caught = self.true_positives + self.false_negatives
        return self.true_positives / caught if caught else 0.0

    @property
    def precision(self) -> float:
        return self.true_positives / self.flagged if self.flagged else 0.0

    @property
    def total_cost(self) -> float:
        return self.amount_missed + self.cost_false_alerts

    def alerts_per_day(self, n_rows: int) -> float:
        return API_TXN_PER_DAY * self.flagged / n_rows if n_rows else 0.0


@dataclass(frozen=True)
class ThresholdChoice:
    """The threshold, the policy that chose it, and the one it did not."""
    policy: str
    chosen: Point
    by_cost: Point
    by_capacity: Point
    n_rows: int
    chosen_on: str
    capacity: int
    warning: str = ""

    @property
    def threshold(self) -> float:
        return self.chosen.threshold


def _count(rows, cost_false_alert: float, threshold: float) -> Point:
    """One threshold's consequences over rows already materialised."""
    tp = fp = fn = tn = 0
    missed = 0.0
    for label, score, amount in rows:
        flagged = score >= threshold
        if label:
            if flagged:
                tp += 1
            else:
                fn += 1
                missed += float(amount)
        else:
            if flagged:
                fp += 1
            else:
                tn += 1
    return Point(threshold=threshold, true_positives=tp, false_positives=fp,
                 false_negatives=fn, true_negatives=tn, amount_missed=missed,
                 cost_false_alerts=fp * cost_false_alert)


def point_at(labels, scores, amounts, cost_false_alert: float,
             threshold: float) -> Point:
    """The consequences of ONE threshold, in a single pass.

    Split out of sweep rather than duplicated, so there is one implementation of
    this count. The reason a caller needs one threshold instead of the grid:
    N4 rescores the test set 32 times to measure what txn_hour does to the model
    on the stream, and the grid would be 99 thresholds times 50,349 rows times
    32 scenarios -- 160 million iterations of this loop to read 32 numbers.

    Block: B3, B4.
    Lot: B3-N4.
    Competency: B3-C5.
    Covers: B3-EI04.
    """
    rows = list(zip(labels, scores, amounts))
    if not rows:
        raise ValueError("no row to score: a threshold says nothing about an "
                         "empty set")
    return _count(rows, cost_false_alert, threshold)


def sweep(labels, scores, amounts, cost_false_alert: float) -> tuple[Point, ...]:
    """Every threshold on the grid, with its consequences.

    Pure Python on purpose: no numpy, so the arithmetic that decides the
    threshold can be exercised anywhere, including where scikit-learn is not
    installed.

    Block: B3, B4.
    Lot: B3-M6.
    Competency: B3-C5.
    Covers: B3-EI04.
    """
    rows = list(zip(labels, scores, amounts))
    if not rows:
        raise ValueError("no row to sweep: a threshold cannot be chosen on an "
                         "empty validation slice")
    return tuple(_count(rows, cost_false_alert, threshold) for threshold in GRID)


def choose(points: tuple[Point, ...], policy: str, capacity: int,
           n_rows: int, chosen_on: str) -> ThresholdChoice:
    """Apply the declared policy, and keep the other one for the report.

    Block: B3, B4.
    Lot: B3-M6.
    Competency: B3-C2, B3-C5.
    Covers: B3-EE08, B3-EC09.
    """
    if not points:
        raise ValueError("no candidate threshold")

    # Cost: cheapest, and on a tie the HIGHER threshold -- fewer alerts for the
    # same money is strictly better for the analyst.
    by_cost = min(points, key=lambda p: (p.total_cost, -p.threshold))

    # Capacity: best recall within the ceiling, and on a tie the better
    # precision.
    affordable = [p for p in points if p.alerts_per_day(n_rows) <= capacity]
    warning = ""
    if affordable:
        by_capacity = max(affordable, key=lambda p: (p.recall, p.precision))
    else:
        # Even the strictest threshold exceeds the ceiling. Not a reason to
        # stop -- the model is still useful -- but the report must say it, or
        # the published alert count would be a promise nobody can keep.
        by_capacity = max(points, key=lambda p: p.threshold)
        warning = (
            "no threshold on the grid stays within %d alerts a day: the "
            "strictest (%.2f) still produces %.1f. Either the ceiling rises or "
            "the model improves -- the figure below is not an operable promise."
            % (capacity, by_capacity.threshold,
               by_capacity.alerts_per_day(n_rows)))

    chosen = by_cost if policy == "cost" else by_capacity
    return ThresholdChoice(policy=policy, chosen=chosen, by_cost=by_cost,
                           by_capacity=by_capacity, n_rows=n_rows,
                           chosen_on=chosen_on, capacity=capacity,
                           warning=warning)


def render(choice: ThresholdChoice) -> str:
    """The threshold decision, auditable.

    Block: B3, B4.
    Lot: B3-M6.
    Competency: B3-C5.
    Covers: B3-EI04.
    """
    def describe(label: str, point: Point) -> list[str]:
        return [
            "  %-26s seuil %.2f" % (label, point.threshold),
            "    rappel %.4f   precision %.4f   alertes/jour %.1f"
            % (point.recall, point.precision, point.alerts_per_day(choice.n_rows)),
            "    fraude non prise %10.2f   fausses alertes %8.2f   total %10.2f"
            % (point.amount_missed, point.cost_false_alerts, point.total_cost),
        ]

    lines = [
        "CHOIX DU SEUIL -- politique declaree : %s" % choice.policy,
        "  choisi sur %s, JAMAIS sur le jeu de test" % choice.chosen_on,
        "  plafond declare : %d alertes/jour" % choice.capacity,
        "",
    ]
    lines += describe("au moindre cout", choice.by_cost)
    lines.append("")
    lines += describe("dans la capacite", choice.by_capacity)
    lines.append("")
    lines.append("  => SEUIL RETENU %.2f (%s)" % (choice.threshold, choice.policy))
    if choice.warning:
        lines += ["", "  AVERTISSEMENT : " + choice.warning]
    return "\n".join(lines)
