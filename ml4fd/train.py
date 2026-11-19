"""Training the scoring model, and reporting it.

Covers: EE08, EI04
Lot: M5
Competency: C2, C5

THE TRAP THIS MODULE IS BUILT AROUND. The source holds 0.414 % positives over
the usable horizon, so A MODEL THAT NEVER PREDICTS FRAUD SCORES 99.6 %
ACCURACY. Accuracy is therefore not a weak metric here, it is a misleading one,
and the model's accuracy is NEVER COMPUTED -- the Evaluation record below has no
field for it, so it cannot be reported by accident.

What the report does carry is the accuracy of the TRIVIAL model, named for what
it is, because that single figure is the argument for ignoring accuracy
altogether.

THE SECOND RULE. Precision depends mechanically on the base rate: halving the
base rate roughly halves the precision at constant recall. Measured on this
source, the monthly rate runs from 0.185 % to 0.554 % -- a factor of three. So
every metric is reported WITH the base rate of its own test window, or two
weekly runs would not be comparable and the difference would be an artefact.

THE THIRD RULE. The threshold is not the model (M6). This module produces
probabilities and reports threshold-free metrics first; the threshold-dependent
figures are given at a stated threshold, as a reference rather than a verdict.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from ml4fd import datasets

log = logging.getLogger(__name__)

SEED = 20261116          # fixed AND recorded: an unrecorded seed is not fixed
MODEL_FILE = "model.joblib"
CATEGORICAL = ("category",)


@dataclass(frozen=True)
class Evaluation:
    """What a training run is allowed to claim.

    No accuracy field for the model. That is the point, not an oversight: a
    metric that cannot be stored cannot be published.
    """
    n_train: int
    n_train_pos: int
    n_test: int
    n_test_pos: int
    base_rate_train: float
    base_rate_test: float

    # Threshold-free: these describe the model, not a decision.
    average_precision: float
    roc_auc: float

    # The trivial model, for scale.
    baseline_average_precision: float      # equals the base rate
    baseline_accuracy: float               # the figure that makes the trap plain

    # At one stated threshold. A reference, not a verdict (M6 decides).
    threshold: float
    recall: float
    precision: float
    true_positives: int
    false_positives: int
    false_negatives: int
    true_negatives: int

    # THE ANALYST'S RULE, the baseline that actually competes. Two conditions
    # carry most of the signal in this source, so beating "never fraud" proves
    # nothing; beating the rule does.
    rule_recall: float = 0.0
    rule_precision: float = 0.0
    rule_alerts_per_day: float = 0.0
    rule_description: str = ""

    seed: int = SEED
    features: tuple[str, ...] = datasets.FEATURES
    versions: dict = field(default_factory=dict)

    @property
    def frauds_caught_over_rule(self) -> int:
        """Frauds the model catches that the rule misses.

        The honest headline. The model may well have WORSE precision than the
        rule -- that is the trade-off M6 resolves -- so the gain has to be
        stated in frauds, not in a single score.
        """
        return self.true_positives - round(self.rule_recall * self.n_test_pos)

    @property
    def lift(self) -> float:
        """How many times better than the trivial model, in PR-AUC terms.

        A lift near 1 means the model learned nothing, whatever its accuracy
        would have said.
        """
        if not self.baseline_average_precision:
            return 0.0
        return self.average_precision / self.baseline_average_precision

    @property
    def alerts_per_day(self) -> float:
        """Alerts an analyst would receive at this threshold, on the API rate.

        7,200 transactions a day is the rate the API announces, not the 2,879 of
        the historical file -- the two must not be conflated.
        """
        if not self.n_test:
            return 0.0
        flagged = self.true_positives + self.false_positives
        return 7200.0 * flagged / self.n_test


def render_report(ev: Evaluation) -> str:
    """The report. Carries no accuracy for the model, and never will.

    Covers: EI04
    Lot: M5
    Competency: C5
    """
    lines = [
        "jeu          train=%d (%d pos, %.4f %%)  test=%d (%d pos, %.4f %%)"
        % (ev.n_train, ev.n_train_pos, 100 * ev.base_rate_train,
           ev.n_test, ev.n_test_pos, 100 * ev.base_rate_test),
        "",
        "SANS SEUIL -- ce qui decrit le modele",
        "  PR-AUC                  %.4f" % ev.average_precision,
        "  ROC-AUC                 %.4f" % ev.roc_auc,
        "  PR-AUC du modele trivial %.4f   (= le taux de base)"
        % ev.baseline_average_precision,
        "  gain sur le trivial     x %.1f" % ev.lift,
        "",
        "LE PIEGE, chiffre",
        "  un modele qui ne predit JAMAIS de fraude obtient %.3f %% d'exactitude."
        % (100 * ev.baseline_accuracy),
        "  l'exactitude du modele n'est pas calculee : elle serait du meme ordre",
        "  et ne dirait rien.",
        "",
        "LA REGLE D'ANALYSTE, qu'il faut battre",
        "  %s" % (ev.rule_description or "non evaluee"),
        "  rappel                  %.4f" % ev.rule_recall,
        "  precision               %.4f" % ev.rule_precision,
        "  alertes par jour        %.1f" % ev.rule_alerts_per_day,
        "",
        "AU SEUIL %.2f -- une reference, pas un verdict (M6 decide)" % ev.threshold,
        "  rappel                  %.4f" % ev.recall,
        "  precision               %.4f" % ev.precision,
        "  matrice                 VP=%d  FP=%d  FN=%d  VN=%d"
        % (ev.true_positives, ev.false_positives,
           ev.false_negatives, ev.true_negatives),
        "  alertes par jour        %.1f   (au debit de l'API, 7 200 txn/jour)"
        % ev.alerts_per_day,
        "  fraudes prises que la regle manque  %+d" % ev.frauds_caught_over_rule,
        "",
        "REPRODUCTIBILITE",
        "  graine                  %d" % ev.seed,
        "  features                %s" % ", ".join(ev.features),
    ]
    for name, version in sorted(ev.versions.items()):
        lines.append("  %-23s %s" % (name, version))
    return "\n".join(lines)


def split_validation(frame, share: float):
    """Cut the training frame into (fit, validation), the last `share` by TIME.

    THE CUT IS ON A TIME BOUNDARY, NOT A ROW INDEX. Cutting by row would put
    transactions sharing an instant on both sides -- the same tie problem as the
    feature reconciliation of L4, and here it would leak the validation slice
    into the fit.

    Covers: EE08
    Lot: M6
    Competency: C2, C5
    """
    ordered = frame.sort_values(["txn_ts", "txn_id"])
    index = int(len(ordered) * (1.0 - share))
    boundary = ordered["txn_ts"].iloc[index]
    fit = ordered[ordered["txn_ts"] < boundary]
    validation = ordered[ordered["txn_ts"] >= boundary]
    if fit.empty or validation.empty:
        raise ValueError(
            "a validation share of %.2f leaves one side empty on this training "
            "period: every row shares too few distinct instants." % share)
    return fit, validation


def load_split(path: Path):
    """Read one Parquet side into a frame, features typed for the model."""
    import pandas as pd

    frame = pd.read_parquet(path)
    for name in CATEGORICAL:
        frame[name] = frame[name].astype("category")
    return frame


def rule_scores(frame, min_amount: float, night_hours: tuple[int, ...]):
    """The analyst's rule, applied to a frame: amount high AND hour nocturnal.

    Covers: EI04
    Lot: M5
    Competency: C5
    """
    return ((frame["amt"].astype(float) > min_amount)
            & frame["txn_hour"].isin(list(night_hours)))


def _fit(x, y):
    """One fitted classifier. Isolated so the two fits are identical by
    construction rather than by careful copying.

    categorical_features is intersected with the columns actually present: a
    recipe may drop any feature (datasets.without), and naming an absent column
    here would fail inside sklearn with a message about the estimator rather
    than about the recipe.
    """
    from sklearn.ensemble import HistGradientBoostingClassifier

    present = [name for name in CATEGORICAL if name in x.columns]
    model = HistGradientBoostingClassifier(
        categorical_features=present,
        random_state=SEED,
        class_weight="balanced",     # the imbalance, handled in the fit
    )
    model.fit(x, y)
    return model


def fit_and_evaluate(train_path: Path, test_path: Path, threshold: float,
                     rule: tuple[float, tuple[int, ...]] | None = None,
                     policy: str | None = None, capacity: int = 0,
                     cost_false_alert: float = 0.0,
                     validation_share: float = 0.0,
                     features: tuple[str, ...] | None = None
                     ) -> tuple[object, Evaluation, object]:
    """Fit on train, choose the threshold, evaluate on test.

    THE ORDER MATTERS AND IS NOT NEGOTIABLE:

      1. split the training period in two, by time;
      2. fit on the earlier part, score the later part;
      3. choose the threshold THERE (M6);
      4. refit on the WHOLE training period;
      5. evaluate on test, at that threshold.

    A threshold chosen on the test set would make the published recall and
    precision optimistic by construction, and no metric in the report would
    reveal it.

    HYPOTHESIS WRITTEN DOWN: the refit model's scores are taken to be
    distributed closely enough to the sub-model's for the threshold to carry
    over. That is the standard compromise -- the alternative is to publish a
    model fitted on 80 % of the data -- but it is an assumption, not a result.

    HistGradientBoostingClassifier for three reasons that matter here: it takes
    categoricals natively, it takes NaN natively -- f_avg_amount is NULL for
    each cardholder's first transaction, by the convention of ruling 6 -- and it
    is fast enough on 300,000 rows to keep a weekly cadence cheap.

    Covers: EE08
    Lot: M5
    Competency: C2
    """
    import sklearn
    from sklearn.metrics import (average_precision_score, confusion_matrix,
                                 precision_score, recall_score, roc_auc_score)

    from ml4fd import threshold as th

    train, test = load_split(train_path), load_split(test_path)
    # THE RECIPE'S features, not the module's constant. A run may drop
    # txn_hour and txn_weekday (datasets.without), and the Parquet still holds
    # them -- demoted to metadata -- so reading the constant here would feed the
    # model columns its own recipe says it does not use, and the manifest would
    # record a feature list the model never saw.
    columns = list(features) if features else list(datasets.FEATURES)
    x_test, y_test = test[columns], test[datasets.LABEL].astype(int)

    choice = None
    if policy:
        fit_part, validation = split_validation(train, validation_share)
        probe = _fit(fit_part[columns], fit_part[datasets.LABEL].astype(int))
        probe_scores = probe.predict_proba(validation[columns])[:, 1]
        points = th.sweep(validation[datasets.LABEL].astype(bool).tolist(),
                          probe_scores.tolist(),
                          validation["amt"].astype(float).tolist(),
                          cost_false_alert)
        window = "%s..%s" % (validation["txn_ts"].min().date(),
                             validation["txn_ts"].max().date())
        choice = th.choose(points, policy, capacity, len(validation),
                           "la validation " + window)
        threshold = choice.threshold
        log.info("%s", th.render(choice))

    x_train, y_train = train[columns], train[datasets.LABEL].astype(int)
    model = _fit(x_train, y_train)
    scores = model.predict_proba(x_test)[:, 1]
    predicted = (scores >= threshold).astype(int)

    base_rate_test = float(y_test.mean())
    # A constant scorer's average precision equals the base rate. Computed
    # rather than asserted, so the comparison rests on the same code path.
    import numpy as np
    baseline_ap = float(average_precision_score(y_test, np.zeros(len(y_test))))
    tn, fp, fn, tp = confusion_matrix(y_test, predicted, labels=[0, 1]).ravel()

    rule_recall = rule_precision = rule_rate = 0.0
    rule_text = ""
    if rule:
        min_amount, night_hours = rule
        flagged = rule_scores(test, min_amount, night_hours)
        rule_recall = float(recall_score(y_test, flagged, zero_division=0))
        rule_precision = float(precision_score(y_test, flagged, zero_division=0))
        rule_rate = 7200.0 * int(flagged.sum()) / len(test) if len(test) else 0.0
        rule_text = ("montant > %g ET heure dans {%s}"
                     % (min_amount, ", ".join(str(h) for h in night_hours)))

    evaluation = Evaluation(
        n_train=len(train), n_train_pos=int(y_train.sum()),
        n_test=len(test), n_test_pos=int(y_test.sum()),
        base_rate_train=float(y_train.mean()), base_rate_test=base_rate_test,
        average_precision=float(average_precision_score(y_test, scores)),
        roc_auc=float(roc_auc_score(y_test, scores)),
        baseline_average_precision=baseline_ap,
        baseline_accuracy=1.0 - base_rate_test,
        threshold=threshold,
        recall=float(recall_score(y_test, predicted, zero_division=0)),
        precision=float(precision_score(y_test, predicted, zero_division=0)),
        true_positives=int(tp), false_positives=int(fp),
        false_negatives=int(fn), true_negatives=int(tn),
        rule_recall=rule_recall, rule_precision=rule_precision,
        rule_alerts_per_day=rule_rate, rule_description=rule_text,
        versions={"scikit-learn": sklearn.__version__,
                  "numpy": np.__version__},
    )
    return model, evaluation, choice


def save_model(model, directory: Path) -> Path:
    """Write the model beside its dataset. M7 registers it in MLflow."""
    import joblib

    path = Path(directory) / MODEL_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path)
    log.info("model written: %s", path)
    return path
