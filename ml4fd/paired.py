"""Comparing two decisions on the SAME transactions, fraud by fraud.

Block: B3, B4.
Lot: B3-N4, B4-L0.
Competency: B3-C5.
Covers: B3-EI04, B3-EC24.

WHY NOT SIMPLY COMPARE TWO RECALLS. The test set holds 205 frauds. A recall
measured on 205 positives carries a binomial noise of about +/-0.034, and the
difference of two such numbers carries +/-0.048. So a gain of 0.044 -- nine
frauds -- reads as 0.9 sigma and settles nothing. With this many positives that
route will almost never decide anything, however long we stare at it.

But the two recalls are not independent: both decisions are evaluated on the
SAME 205 frauds. So instead of two aggregate rates, the comparison looks at each
fraud and asks which decision caught it:

                    B catches    B misses
    A catches          both       only A
    A misses          only B       neither

"both" and "neither" carry NO information -- the two decisions agree there, and
in the aggregate view those agreements only inflate the noise. Everything is in
the two disagreement cells: among the frauds where the decisions differ, is the
split far enough from even that chance does not explain it? That is McNemar's
test, and for a 50/50 null it is an exact binomial test on the discordant pairs.

Why it is so much stronger, on the same gain of nine frauds (measured):

    only A   only B   discordant   p
         2       11           13   0.022   decides
         6       15           21   0.078   does not
        20       29           49   0.253   does not

The gain is nine in all three rows. What changes is how much the two decisions
AGREE: the more frauds they both catch, the sharper a gain of nine becomes. The
aggregate view cannot see that difference, having thrown away which fraud went
where.

Pure Python, like ml4fd/threshold.py and for the same reason: the arithmetic
that decides whether a model change is real must be exercisable where neither
scikit-learn nor numpy is installed.
"""
from __future__ import annotations

from dataclasses import dataclass
from math import comb


@dataclass(frozen=True)
class Paired:
    """The 2x2 table of two decisions over the same rows, and what it implies.

    nested says the caller KNOWS one decision flags a superset of the other, in
    which case the test below does not apply -- see two_sided_binomial. Two
    thresholds on the same scores are the case: the lower one flags everything
    the higher one flags, by arithmetic and not by observation.
    """
    both: int
    only_first: int
    only_second: int
    neither: int
    first_name: str = "A"
    second_name: str = "B"
    nested: bool = False

    @property
    def n(self) -> int:
        return self.both + self.only_first + self.only_second + self.neither

    @property
    def discordant(self) -> int:
        """The only pairs that carry information."""
        return self.only_first + self.only_second

    @property
    def caught_first(self) -> int:
        return self.both + self.only_first

    @property
    def caught_second(self) -> int:
        return self.both + self.only_second

    @property
    def difference(self) -> int:
        """Positive when the SECOND decision catches more."""
        return self.only_second - self.only_first

    @property
    def p_value(self) -> float:
        """The exact test, or None when the comparison is nested.

        None and not 1.0: 1.0 would read as "no evidence of a difference",
        while the truth is that this test cannot be asked of these two
        decisions at all.
        """
        if self.nested:
            return None
        return two_sided_binomial(self.only_first, self.only_second)

    @property
    def nested_in_fact(self) -> bool:
        """One side disagrees nowhere. Suggestive of nesting, never proof of
        it -- a cell can be empty by chance -- so it informs a note, never the
        verdict. Only the caller knows whether the nesting is structural."""
        return self.discordant > 0 and 0 in (self.only_first, self.only_second)

    @property
    def favours(self) -> str:
        if self.difference > 0:
            return self.second_name
        if self.difference < 0:
            return self.first_name
        return "aucun"


def two_sided_binomial(first: int, second: int) -> float:
    """Exact two-sided binomial test of a 50/50 split of the discordant pairs.

    Exact and not the chi-square approximation: with a handful of discordant
    pairs -- 13 in the measured illustration -- the approximation is unreliable
    exactly where the decision is close.

    No discordant pair at all returns 1.0: two decisions that never differ give
    no evidence that either is better, which is a result and not an error.

    WHEN THIS TEST DOES NOT APPLY, and it cost me a wrong report. Its null
    hypothesis is that a disagreement falls either way with equal chance. For
    two MODELS that holds. For two THRESHOLDS ON THE SAME SCORES it does not:
    the lower threshold flags a superset, so one cell is zero by arithmetic and
    the test degenerates to 2 x (1/2)^n -- a number that depends only on HOW
    MANY disagreements there are and on nothing in the data. It then declares
    significance for any n >= 6, always. Measured: the threshold re-choice of
    16 November 2026 reported p = 0.016 on 7 disagreements, which is exactly
    2 x (1/2)^7 and says nothing at all.

    Callers that know the decisions are nested pass nested=True to Paired, and
    p_value then returns None rather than a figure that reads as evidence.
    """
    n = first + second
    if n == 0:
        return 1.0
    extreme = max(first, second)
    tail = sum(comb(n, k) for k in range(extreme, n + 1)) / float(2 ** n)
    return min(2.0 * tail, 1.0)


def compare(first_flags, second_flags, labels=None,
            first_name: str = "A", second_name: str = "B",
            nested: bool = False) -> Paired:
    """Build the table from two sequences of decisions over the same rows.

    labels, when given, restricts the comparison to the rows where the label is
    true -- the frauds. Restricting is the usual question ("which model catches
    more fraud?"), but the same table over the NON-frauds answers the other one
    ("which raises fewer false alerts?"), so the filter is a parameter rather
     than a built-in assumption.
    """
    first = list(first_flags)
    second = list(second_flags)
    if len(first) != len(second):
        raise ValueError(
            "%d decisions against %d: a paired comparison needs the two "
            "decisions over the SAME rows, in the same order"
            % (len(first), len(second)))

    if labels is not None:
        keep = list(labels)
        if len(keep) != len(first):
            raise ValueError(
                "%d labels for %d decisions" % (len(keep), len(first)))
        rows = [(a, b) for a, b, label in zip(first, second, keep) if label]
    else:
        rows = list(zip(first, second))

    both = sum(1 for a, b in rows if a and b)
    only_first = sum(1 for a, b in rows if a and not b)
    only_second = sum(1 for a, b in rows if b and not a)
    neither = sum(1 for a, b in rows if not a and not b)
    return Paired(both=both, only_first=only_first, only_second=only_second,
                  neither=neither, first_name=first_name,
                  second_name=second_name, nested=nested)


def flags(scores, threshold: float) -> list[bool]:
    """The decision each score implies, at the threshold, inclusive.

    Inclusive because that is what threshold.py compared when it chose the
    value, and what fd_scores stores. An exclusive test here would compare two
    decisions neither of which is the one taken.
    """
    return [float(score) >= threshold for score in scores]


def render(table: Paired, subject: str = "fraudes") -> str:
    """The table and its verdict, stated rather than left to be inferred."""
    lines = [
        "|  | %s attrape | %s manque |" % (table.second_name, table.second_name),
        "|---|---|---|",
        "| **%s attrape** | %d | %d |"
        % (table.first_name, table.both, table.only_first),
        "| **%s manque** | %d | %d |"
        % (table.first_name, table.only_second, table.neither),
        "",
        "- %s sur %d %s pour %s, %d pour %s."
        % (table.caught_first, table.n, subject, table.first_name,
           table.caught_second, table.second_name),
        "- Desaccords : **%d** (%d pour %s, %d pour %s). Les %d accords "
        "n'apportent aucune information."
        % (table.discordant, table.only_first, table.first_name,
           table.only_second, table.second_name,
           table.both + table.neither),
    ]
    if not table.discordant:
        lines.append("- Les deux decisions ne different sur aucune ligne : "
                     "**rien ne permet de preferer l'une**.")
        return "\n".join(lines)

    if table.nested:
        lines += [
            "- **Le test binomial ne s'applique pas ici.** Les deux decisions "
            "sont emboitees : l'une signale un sur-ensemble de l'autre, donc "
            "une case est nulle PAR ARITHMETIQUE et le test degenere en "
            "2 x (1/2)^n --- un nombre qui ne depend que du nombre de "
            "desaccords et de rien dans les donnees, et qui declare la "
            "significativite des que n vaut 6.",
            "- Ce qu'il faut lire est l'**arbitrage** : %+d %s de plus, contre "
            "le volume d'alertes que cela coute."
            % (table.difference, subject),
        ]
        return "\n".join(lines)

    verdict = ("l'ecart est significatif au seuil de 5 %"
               if table.p_value < 0.05
               else "l'ecart N'EST PAS significatif au seuil de 5 %")
    lines += [
        "- Test binomial exact sur les %d desaccords : **p = %.3f**, donc %s."
        % (table.discordant, table.p_value, verdict),
        "- Avantage : **%s** (%+d %s)." % (table.favours, table.difference,
                                           subject),
    ]
    if table.nested_in_fact:
        lines.append(
            "- Une des deux cases de desaccord est vide. Si c'est structurel "
            "--- deux seuils sur les memes scores le sont --- le test ci-dessus "
            "ne s'applique pas et l'appelant doit passer nested=True.")
    if table.p_value >= 0.05:
        lines.append(
            "- Ne pas lire un p superieur a 0,05 comme \"les deux se valent\" : "
            "il dit que CES donnees ne tranchent pas, ce qui avec %d "
            "desaccords peut tenir au seul nombre de desaccords."
            % table.discordant)
    return "\n".join(lines)
