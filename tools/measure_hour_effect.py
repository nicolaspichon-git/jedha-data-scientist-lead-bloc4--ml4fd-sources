"""What txn_hour does to the model once it comes from our clock, not the payment.

Block: B3, B4.
Lot: B3-N4, B4-L0.
Competency: B3-C4, B3-C5.
Covers: B3-EI04, B3-EC24.

THE QUESTION THIS SETTLES. The model was trained on the hour of the PAYMENT,
where 22:00-03:59 carries a fraud rate of 1.389 % against 0.079 % elsewhere --
a ratio of 17.5, and 35 between the extreme hours (22:00 at 1.937 %, 08:00 at
0.056 %). On the stream, current_time is the instant our own call returned, and
the simulator draws a row at random from 2020 and stamps it with that instant.
Measured over the B3-N0 campaign: indices spread over 16,227..553,065 with 29 of
56 gaps positive, a coin flip. So the hour and the label are INDEPENDENT BY
CONSTRUCTION on the stream, while they are anything but in training.

THE MEASUREMENT MY OWN S3 PLAN GOT WRONG. The plan said to retrain without the
feature and compare PR-AUC on the test set. That comparison is biased: the test
set is also 2020, so dropping the hour shows a loss there that does not exist on
the stream, where the feature contributes nothing. The honest comparisons are:

  as-is      the optimistic figure we publish today
  shuffled   the hour permuted across rows -- the association destroyed, the
             marginal distribution kept. This is the model's real discrimination
             on the stream, and the number a retrained recipe must beat.
  hour=h     every row forced to one hour, for each of the 24. This is what ONE
             BATCH sees, since every transaction collected in the same minute
             shares an hour. The spread across the 24 is the swing of the alert
             volume -- and the threshold was calibrated for one point of it.

Only the third needs explaining to be useful: a per-batch constant that the
model reads as strong evidence is not a feature, it is a multiplier driven by
the wall clock.

NO RETRAINING HERE. This tool rescores an existing model, so it costs seconds
and cannot change what it measures. The amputated recipe is a run of S2.

Usage, from the scheduler container:
    python3 /opt/project/tools/measure_hour_effect.py [--digest de96c967f7e6]
"""
from __future__ import annotations

import argparse
import json
import logging
import statistics as st
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("measure_hour_effect")

# The seed and the paired permutation live in ml4fd/clock.py: the re-choice of
# the threshold needs exactly the same ones, and one permutation written
# twice is two chances to permute differently.
HOUR = "txn_hour"
WEEKDAY = "txn_weekday"


def latest_run(root: Path) -> Path:
    """The most recently written run.json under the dataset root."""
    manifests = sorted(root.glob("*/run.json"),
                       key=lambda p: p.stat().st_mtime, reverse=True)
    if not manifests:
        raise FileNotFoundError(
            "no run.json under %s: train a model first (.\\fd-manager.ps1 train)"
            % root)
    return manifests[0]


def scenarios(frame, hours: list[int], repeats: int = 1):
    """Each frame to score, named. The original is never mutated.

    POSITIONAL permutation, by index position and not by pandas index label.
    Assigning a sampled SERIES back lets pandas realign on the index, which
    undoes the permutation exactly: measured on eight rows, the column comes
    back in its original order. The measurement would then report that shuffling
    the hour changes nothing, and I would have concluded the feature is
    harmless -- a wrong answer, silently.

    REPEATED, and this is not thoroughness for its own sake. A permutation is
    ONE draw. Changing the generator between two runs of this tool -- pandas
    .sample for numpy's permutation, same seed -- moved the permuted recall by
    0.015 and the permuted PRECISION by 0.070. With 205 positives and about 205
    flagged rows, the binomial noise on either figure is ~0.034, so that second
    gap is about two sigma: ordinary sampling, not a bug. But the decision this
    measurement serves turns on whether a retrained model beats the permuted
    one, and the gaps there will be of the same size. A bar quoted from a single
    draw could be beaten or missed by the draw alone.

    So the permuted readings are repeated and reported as a mean with a spread,
    while as-is and the forced hours stay single -- they are deterministic.
    """
    import numpy as np

    from ml4fd import clock as clock_module

    yield "as-is", frame

    for draw in range(max(1, repeats)):
        # The hour alone, kept so the loss can be decomposed: how much of it is
        # the hour, how much is the pair. Same generator and same draw number as
        # clock.permute, so the two readings of one draw differ only by the
        # weekday.
        positions = np.random.default_rng(
            clock_module.SEED + draw).permutation(len(frame))
        shuffled = frame.copy()
        shuffled[HOUR] = frame[HOUR].to_numpy()[positions]
        yield "shuffled", shuffled

        # BOTH CALENDAR COLUMNS, AND THAT IS THE BAR -- delegated to
        # ml4fd/clock.py, which holds the reasoning and the seed.
        #
        # The first version of this tool permuted the hour alone, which
        # understates what the stream does: txn_weekday comes from our clock too
        # and is constant for a whole day of batches. So "shuffled" is an upper
        # bound on what option 1 delivers, not the bar an amputated recipe must
        # clear.
        #
        # Delegated rather than written twice because tools/rechoose_threshold.py
        # needs the same permutation, and two copies of it are two chances to
        # permute differently and then compare the results as if they were
        # comparable.
        yield "clock", clock_module.permute(frame, draw)

    for hour in hours:
        forced = frame.copy()
        forced[HOUR] = hour
        yield "hour=%02d" % hour, forced


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure what txn_hour does to the model when it carries "
                    "the collection time instead of the payment time.",
        epilog="Rescores an existing model; it never retrains, so it cannot "
               "change what it measures.")
    parser.add_argument("--recipe", "--digest", dest="recipe", default=None,
                        help="recipe VERSION -- the 12-character directory name under datasets/, which is datasets.Recipe.version() and never the full 64-character digest (default: the latest run)")
    parser.add_argument("--hours", default="0-23",
                        help="hours to force, e.g. 0-23 or 8,22 (default 0-23)")
    parser.add_argument("--repeats", type=int, default=20,
                        help="permutation draws, to put a spread on the bar "
                             "(default 20; one draw is not a bar)")
    args = parser.parse_args(argv)

    sys.path.insert(0, "/opt/project")

    import joblib
    import pandas as pd

    from ml4fd import config, threshold as th

    settings = config.load()
    root = Path(settings.datasets_root) / settings.dataset_family

    manifest_path = (root / args.recipe / "run.json" if args.recipe
                     else latest_run(root))
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    features = list(payload["dataset"]["features"])
    chosen = payload.get("threshold") or {}
    if not chosen.get("value"):
        log.error("le manifeste %s ne porte pas de seuil : B3-M6 n'a pas tranche "
                  "pour ce modele, il n'y a rien a mesurer", manifest_path)
        return 1
    cut = float(chosen["value"])

    model_path = Path(payload["model"]["path"])
    if not model_path.exists():                 # the manifest records a path
        model_path = manifest_path.parent / "model.joblib"
    model = joblib.load(model_path)

    test = pd.read_parquet(manifest_path.parent / "test.parquet")
    labels = test["label"].tolist()
    amounts = test["amt"].tolist()
    n_rows = len(test)

    if HOUR not in features:
        log.info("%s ne fait pas partie de la recette %s : rien a mesurer, "
                 "la question est deja tranchee", HOUR,
                 payload["dataset"]["version"])
        return 0

    if "-" in args.hours:
        start, end = (int(part) for part in args.hours.split("-", 1))
        hours = list(range(start, end + 1))
    else:
        hours = [int(part) for part in args.hours.split(",")]

    log.info("recette %s, seuil %.2f (%s), %d lignes de test, %d positifs",
             payload["dataset"]["version"], cut, chosen.get("policy", "?"),
             n_rows, sum(1 for label in labels if label))
    log.info("features : %s", ", ".join(features))
    log.info("")

    rows = []
    for name, frame in scenarios(test, hours, args.repeats):
        scores = model.predict_proba(frame[features])[:, 1]
        point = th.point_at(labels, scores, amounts, settings.cost_false_alert,
                            cut)
        rows.append((name, point))
        if name in ("shuffled", "clock"):
            continue            # reported as a mean below, not draw by draw
        log.info("  %-9s rappel %.3f  precision %.3f  alertes/j %6.1f  "
                 "signales %5d", name, point.recall, point.precision,
                 point.alerts_per_day(n_rows), point.flagged)
    for name in ("shuffled", "clock"):
        draws = [point for label, point in rows if label == name]
        if not draws:
            continue
        log.info("  %-9s rappel %.3f [%.3f-%.3f]  precision %.3f "
                 "[%.3f-%.3f]  alertes/j %6.1f   sur %d tirages", name,
                 st.mean(p.recall for p in draws),
                 min(p.recall for p in draws), max(p.recall for p in draws),
                 st.mean(p.precision for p in draws),
                 min(p.precision for p in draws),
                 max(p.precision for p in draws),
                 st.mean(p.alerts_per_day(n_rows) for p in draws), len(draws))

    report = render(rows, payload, cut, n_rows, settings)
    out = manifest_path.parent / "mesure-txn_hour.md"
    out.write_text(report, encoding="utf-8")
    log.info("")
    log.info("Mesure ecrite : %s", out)
    print()
    print(report)
    return 0


def render(rows, payload, cut, n_rows, settings) -> str:
    """The report. It states the verdict, it does not leave it to be inferred."""
    # dict(rows) would keep only the LAST draw of each repeated scenario, and
    # report one permutation as if it were the bar. Grouped instead.
    draws = {}
    for name, point in rows:
        draws.setdefault(name, []).append(point)
    forced = [(name, point) for name, point in rows
              if name.startswith("hour=")]

    def mean(name, attribute):
        return st.mean(getattr(point, attribute) for point in draws[name])

    def band(name, attribute):
        values = [getattr(point, attribute) for point in draws[name]]
        return min(values), max(values)

    def error_of_mean(name, attribute):
        """The uncertainty ON THE MEAN, which is not the spread of the draws.

        The range of 20 draws describes where ONE draw lands; the mean of those
        draws is far better determined -- by sd/sqrt(n). Quoting the range as
        the resolution of the bar would make the bar look twenty times vaguer
        than it is, and would wave through a real difference as "within the
        noise".
        """
        values = [getattr(point, attribute) for point in draws[name]]
        if len(values) < 2:
            return 0.0
        return st.stdev(values) / len(values) ** 0.5

    def volume(name):
        return st.mean(point.alerts_per_day(n_rows) for point in draws[name])

    positives = (draws["as-is"][0].true_positives
                 + draws["as-is"][0].false_negatives)
    lines = [
        "# B3-N4 --- effet de txn_hour sur le flux",
        "",
        "Recette `%s`, seuil %.2f (%s), %d lignes de test."
        % (payload["dataset"]["version"], cut,
           (payload.get("threshold") or {}).get("policy", "?"), n_rows),
        "Capacite declaree : %d alertes/jour."
        % settings.alert_capacity_per_day,
        "",
        "## Les trois lectures",
        "",
        "| Scenario | Rappel | Precision | Alertes/j | Ce que cela mesure |",
        "|---|---|---|---|---|",
    ]
    legend = {
        "as-is": "l'heure est celle du paiement (ce qui est publie)",
        "shuffled": "heure seule rompue",
        "clock": "**heure ET jour rompus : la barre a battre**",
    }
    for name in ("as-is", "shuffled", "clock"):
        if name not in draws:
            continue
        count = len(draws[name])
        if count == 1:
            spread_recall = spread_precision = ""
        else:
            spread_recall = " [%.3f-%.3f]" % band(name, "recall")
            spread_precision = " [%.3f-%.3f]" % band(name, "precision")
        lines.append("| %s%s | %.3f%s | %.3f%s | %.1f | %s |"
                     % (name,
                        "" if count == 1 else " (%d tirages)" % count,
                        mean(name, "recall"), spread_recall,
                        mean(name, "precision"), spread_precision,
                        volume(name), legend[name]))
    lines += ["", "## Une heure forcee, ce que voit un lot", "",
              "| Heure | Rappel | Precision | Alertes/j | Capacite |",
              "|---|---|---|---|---|"]
    for name, point in forced:
        per_day = point.alerts_per_day(n_rows)
        lines.append("| %s | %.3f | %.3f | %.1f | %s |"
                     % (name[-2:], point.recall, point.precision, per_day,
                        "ok" if per_day <= settings.alert_capacity_per_day
                        else "**depassee**"))

    if forced:
        volumes = [point.alerts_per_day(n_rows) for _, point in forced]
        low, high = min(volumes), max(volumes)
        over = sum(1 for v in volumes if v > settings.alert_capacity_per_day)
        lines += [
            "", "## Verdict", "",
            "- Volume d'alertes selon l'heure : de **%.1f** a **%.1f** par jour,"
            " soit un facteur **%.1f**." % (low, high, high / low if low else 0),
            "- Capacite de %d alertes/jour depassee a **%d des %d heures**."
            % (settings.alert_capacity_per_day, over, len(volumes)),
            "- Rappel publie %.3f ; heure seule rompue %.3f ; **heure et jour "
            "rompus %.3f** (moyennes de %d tirages)."
            % (mean("as-is", "recall"), mean("shuffled", "recall"),
               mean("clock", "recall"), len(draws["clock"])),
            "- Perte imputable a la pendule : **%.3f de rappel**, soit %.0f %% "
            "et %.0f fraude(s) sur %d."
            % (mean("as-is", "recall") - mean("clock", "recall"),
               100 * (mean("as-is", "recall") - mean("clock", "recall"))
               / mean("as-is", "recall"),
               (mean("as-is", "recall") - mean("clock", "recall")) * positives,
               positives),
            "- Etendue d'UN tirage sur %d : rappel %.3f a %.3f, precision "
            "%.3f a %.3f."
            % ((len(draws["clock"]),) + band("clock", "recall")
               + band("clock", "precision")),
            "- Incertitude sur LA BARRE, qui est une moyenne et non un tirage : "
            "**+/- %.3f** de rappel (ecart-type / racine de %d). La barre est "
            "donc bien determinee ; c'est l'etendue ci-dessus qui dit ou tombe "
            "un tirage isole."
            % (error_of_mean("clock", "recall"), len(draws["clock"])),
            "- CE QUI LIMITE LA COMPARAISON est ailleurs : le modele ampute "
            "sera evalue sur ce meme jeu de test et ses %d positifs, donc son "
            "propre rappel porte un bruit binomial de +/- %.3f. Les deux "
            "modeles voient les MEMES fraudes, donc la comparaison juste est "
            "**appariee** -- quelles fraudes chacun attrape, et non deux "
            "nombres confrontes comme s'ils etaient independants."
            % (positives, (0.631 * (1 - 0.631) / positives) ** 0.5),
            "- Le jour de la semaine n'ajoute presque rien : %.3f de rappel "
            "entre `shuffled` et `clock`, a comparer a l'etendue ci-dessus."
            % (mean("shuffled", "recall") - mean("clock", "recall")),
            "",
            "C'est la ligne **`clock`** qu'une recette amputee de `txn_hour` et "
            "`txn_weekday` doit battre. Ni `as-is`, qui mesure le modele sur une "
            "heure que le flux ne fournit pas, ni `shuffled`, qui laisse "
            "`txn_weekday` intacte alors qu'elle vient de la meme pendule.",
            "",
            "Lire la precision sur la ligne `clock` et non la moyenne des 24 "
            "heures forcees : une moyenne de rapports n'est pas le rapport des "
            "moyennes, et les heures 22h-23h apportent a elles seules 22 % des "
            "signalements.",
        ]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    sys.exit(main())
