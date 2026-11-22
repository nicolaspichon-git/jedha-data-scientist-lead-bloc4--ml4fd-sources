"""Whether a newly trained model ever reaches production.

Block: B4.
Lot: B4-L3.
Competency: B4-C5.
Covers: B4-EC34, B4-EC35, B4-EC36.

REGISTERING IS NOT PROMOTING, and until B4-L3 only the first existed. Every run of
S2 registers a version, and ml4fd/score.py resolved the HIGHEST version number
-- so a retraining served its own result, better or worse. Automating the
retraining on top of that, which B4-L4 does, would have automated the serving of
an unvetted model.

THE DECISION IS A PURE FUNCTION. decide() takes figures and returns a verdict:
it reads no database, loads no model, calls no registry. That is what lets the
suite exercise every branch -- the refusals above all, which are the ones that
matter and the ones a live run almost never produces. The I/O lives in
record() and in tools/promote.py.

WHY THE COMPARISON IS PAIRED and not a difference of two recalls:
ml4fd/paired.py carries the arithmetic. In short, the test set holds 205 frauds
and the difference of two recalls measured on them is buried in noise.

THE ASYMMETRY IS DELIBERATE. The gate refuses a candidate the champion beats
significantly; it does NOT require the candidate to win. A retraining on
fresher data that merely holds the line is worth serving, and demanding
significance in its favour would freeze the model in place for good. That
choice is declared in governance.yml, under promotion.significance, rather than
buried here.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from ml4fd import db

log = logging.getLogger(__name__)

RECORD_SQL = "promotions/record_decision.sql"

PROMOTED = "promoted"
REFUSED = "refused"


@dataclass(frozen=True)
class Policy:
    """The promotion section of governance.yml, as four numbers."""
    significance: float
    alert_capacity: int
    respects_capacity: bool
    first_unopposed: bool

    @classmethod
    def from_settings(cls, settings) -> "Policy":
        return cls(
            significance=float(settings.promotion_significance),
            alert_capacity=int(settings.alert_capacity_per_day),
            respects_capacity=bool(settings.promotion_respects_capacity),
            first_unopposed=bool(settings.promote_first_unopposed))


@dataclass(frozen=True)
class Side:
    """One side of the decision: a registered version and its figures.

    recall and alerts_per_day come from fd_datasets, measured when the recipe
    was evaluated -- never recomputed here. A gate that re-measured would be
    comparing its own arithmetic with the published figures, and the two would
    drift.
    """
    version: str
    digest: str | None = None
    recall: float | None = None
    alerts_per_day: float | None = None


@dataclass(frozen=True)
class Decision:
    """The verdict, and everything fd_promotions needs to record it."""
    promoted: bool
    reason: str
    candidate: Side
    champion: Side | None
    table: object | None          # paired.Paired, or None when unopposed
    policy: Policy

    @property
    def decision(self) -> str:
        return PROMOTED if self.promoted else REFUSED

    def row(self, run_id: str | None = None) -> dict:
        """The parameters of sql/promotions/record_decision.sql.

        The policy in force is written into the row, not merely applied:
        governance.yml can change, and a decision taken under the old numbers
        must stay re-derivable.
        """
        table = self.table
        return {
            "run_id": run_id,
            "candidate_version": self.candidate.version,
            "candidate_digest": self.candidate.digest,
            "champion_before": self.champion.version if self.champion else None,
            "decision": self.decision,
            "reason": self.reason,
            "candidate_recall": self.candidate.recall,
            "champion_recall": self.champion.recall if self.champion else None,
            "alerts_per_day": self.candidate.alerts_per_day,
            "alert_capacity": self.policy.alert_capacity,
            "only_candidate": None if table is None else table.only_second,
            "only_champion": None if table is None else table.only_first,
            "p_value": None if table is None else table.p_value,
            "significance": self.policy.significance,
        }


def decide(candidate: Side, champion: Side | None, table, policy: Policy
           ) -> Decision:
    """The gate. The champion is the FIRST side of the table, the candidate the
    SECOND -- so table.difference is positive when the candidate catches more,
    and only_first counts the frauds only the champion catches.

    The order of the checks is itself a decision:

      1. the alert budget, because it depends on no comparison. A candidate
         that floods the morning queue is refused however good its recall:
         alerts nobody reads are not detections.
      2. the absence of a champion, because there is then nothing to compare.
      3. the paired test, which is the only question left.
    """
    def verdict(promoted: bool, reason: str) -> Decision:
        return Decision(promoted=promoted, reason=reason,
                        candidate=candidate, champion=champion,
                        table=table, policy=policy)

    volume = candidate.alerts_per_day
    if policy.respects_capacity and volume is not None:
        if volume > policy.alert_capacity:
            return verdict(False,
                           "refus : %.1f alertes par jour, au-dela de la "
                           "capacite declaree de %d"
                           % (volume, policy.alert_capacity))

    if champion is None:
        if policy.first_unopposed:
            return verdict(True,
                           "promu sans opposition : aucun champion en service, "
                           "donc rien a quoi comparer")
        return verdict(False,
                       "refus : aucun champion en service, et la politique "
                       "n'autorise pas une promotion sans opposition")

    if table is None:
        return verdict(False,
                       "refus : un champion est en service (version %s) et "
                       "aucune comparaison appariee n'a ete fournie"
                       % champion.version)

    p = table.p_value
    if p is None:
        # paired.Paired returns None when the comparison is nested, and nested
        # means the test cannot be asked of these two decisions at all. Two
        # different models are not nested, so arriving here means the caller
        # declared a nesting that does not hold -- refuse rather than guess.
        return verdict(False,
                       "refus : la comparaison est declaree emboitee, et le "
                       "test apparie ne peut pas s'appliquer")

    if table.difference < 0 and p <= policy.significance:
        return verdict(False,
                       "refus : le champion attrape %d fraudes que le candidat "
                       "manque, contre %d en sens inverse, p = %.4f au seuil "
                       "de %.2f"
                       % (table.only_first, table.only_second, p,
                          policy.significance))

    if table.difference > 0 and p <= policy.significance:
        return verdict(True,
                       "promu : le candidat attrape %d fraudes que le champion "
                       "manque, contre %d en sens inverse, p = %.4f"
                       % (table.only_second, table.only_first, p))

    return verdict(True,
                   "promu : aucune difference significative avec le champion "
                   "(%d contre %d fraudes discordantes, p = %.4f), et le "
                   "candidat est le plus recent"
                   % (table.only_second, table.only_first, p))


def record(conn, decision: Decision, run_id: str | None = None) -> None:
    """Write the decision to fd_promotions, promoted or refused.

    BOTH OUTCOMES ARE RECORDED. A journal that held only the promotions would
    answer "what is in production" and never "why version 8 is not", which is
    the question somebody actually asks.
    """
    db.execute_file(conn, RECORD_SQL, **decision.row(run_id))
    log.info("promotion %s : version %s (champion %s) -- %s",
             decision.decision, decision.candidate.version,
             decision.champion.version if decision.champion else "aucun",
             decision.reason)
