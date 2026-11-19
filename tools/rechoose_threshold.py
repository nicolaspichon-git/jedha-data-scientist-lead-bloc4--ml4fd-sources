"""Re-choose the operating threshold on the distribution the STREAM produces.

Covers: EE08, EI04, EC09, EC24
Lot: N4
Competency: C2, C5

THE PROBLEM, MEASURED. M6 swept the grid on the last slice of the training
period and, under the capacity policy, kept the highest recall whose volume
stays under 60 alerts a day. That gave 0.93, for 38.5 alerts a day. But the
sweep ran on a distribution where txn_hour was the hour of the PAYMENT. On the
stream the hour comes from our clock and carries nothing, so the scores shift:
the same 0.93 now yields 31.1 alerts a day. The number has not changed; its
place in the distribution has.

So the capacity policy no longer does what it was written to do. It was meant
to USE the capacity; it uses half of it. And unused capacity is forgone recall:
a lower threshold would flag more, catch more fraud, and still stay under 60.

WHY A PROBE IS REFITTED HERE RATHER THAN REUSING THE REGISTERED MODEL. The
threshold must be chosen on scores the model has NOT seen in training. The
validation slice sits inside the registered model's training data, so scoring it
with that model would choose a threshold on memorised rows -- the very trap M6
avoided. S2 did not keep the probe's scores, so re-choosing a threshold
honestly costs one fit on the earlier part of the training period. That is the
price, and it is announced rather than dodged.

AND IT IS NEVER CHOSEN ON THE TEST SET. A threshold tuned on test makes the
published recall optimistic by construction, and no metric in the report would
reveal it.

REPEATED, because one permutation is one draw: the measurement of N4 showed a
single draw moving the permuted precision by 0.070, about two sigma. So the
sweep is run on several clock-permuted validations and the chosen thresholds are
reported as a distribution.

Usage, from the scheduler container:
    python3 /opt/project/tools/rechoose_threshold.py [--repeats 10]
"""
from __future__ import annotations

import argparse
import json
import logging
import statistics as st
import sys
from collections import Counter
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("rechoose_threshold")

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Re-choose the operating threshold on the stream's score "
                    "distribution, then test whether it catches significantly "
                    "more fraud than the current one.",
        epilog="Refits a probe on the earlier part of the training period: a "
               "threshold chosen on rows the model memorised would be no "
               "threshold at all.")
    parser.add_argument("--recipe", "--digest", dest="recipe", default=None,
                        help="recipe VERSION -- the 12-character directory name under datasets/, which is datasets.Recipe.version() and never the full 64-character digest (default: the latest run)")
    parser.add_argument("--repeats", type=int, default=10,
                        help="clock permutations to sweep (default 10)")
    args = parser.parse_args(argv)

    sys.path.insert(0, "/opt/project")


    from ml4fd import (clock, config, paired, registry, threshold as th,
                       train)

    settings = config.load()
    root = Path(settings.datasets_root) / settings.dataset_family
    folder = (root / args.recipe if args.recipe
              else max((p.parent for p in root.glob("*/run.json")),
                       key=lambda p: (p / registry.MANIFEST_FILE).stat().st_mtime))
    payload = json.loads((folder / registry.MANIFEST_FILE)
                         .read_text(encoding="utf-8"))

    features = list(payload["dataset"]["features"])
    current = float((payload.get("threshold") or {}).get("value") or 0)
    if not current:
        log.error("le manifeste de %s ne porte pas de seuil : il n'y a rien a "
                  "comparer", folder.name)
        return 1

    if not clock.present(features):
        log.info("la recette %s ne porte ni txn_hour ni txn_weekday : la "
                 "distribution du flux est celle de l'entrainement, le seuil "
                 "n'a pas a etre rechoisi", payload["dataset"]["version"])
        return 0

    log.info("recette %s, seuil actuel %.2f (%s)",
             payload["dataset"]["version"], current,
             (payload.get("threshold") or {}).get("policy", "?"))
    log.info("politique %s, capacite %d alertes/jour, cout d'une fausse "
             "alerte %.2f", settings.threshold_policy,
             settings.alert_capacity_per_day, settings.cost_false_alert)

    # --- the probe, on the earlier part of the training period only
    frame = train.load_split(folder / "train.parquet")
    fit_part, validation = train.split_validation(frame,
                                                  settings.validation_share)
    window = "%s..%s" % (validation["txn_ts"].min().date(),
                         validation["txn_ts"].max().date())
    log.info("ajustement d'une sonde sur %d lignes, validation %d lignes (%s)",
             len(fit_part), len(validation), window)
    probe = train._fit(fit_part[features],
                       fit_part["label"].astype(int))

    labels = validation["label"].astype(bool).tolist()
    amounts = validation["amt"].astype(float).tolist()
    n_rows = len(validation)

    # --- the sweep, once per clock permutation
    chosen = []
    sweeps = []
    for draw in range(max(1, args.repeats)):
        permuted = clock.permute(validation, draw)
        scores = probe.predict_proba(permuted[features])[:, 1]
        points = th.sweep(labels, scores.tolist(), amounts,
                          settings.cost_false_alert)
        sweeps.append(points)
        choice = th.choose(points, settings.threshold_policy,
                           settings.alert_capacity_per_day, n_rows,
                           "la validation %s, pendule rompue" % window)
        chosen.append(choice)
        log.info("  tirage %2d : seuil %.2f  rappel %.3f  precision %.3f  "
                 "alertes/j %5.1f", draw, choice.threshold,
                 choice.chosen.recall, choice.chosen.precision,
                 choice.chosen.alerts_per_day(n_rows))

    values = [choice.threshold for choice in chosen]
    tally = Counter(values)
    middle = st.median(values)
    # The median must be snapped to a GRID value: the threshold has to be one
    # the sweep actually evaluated, and the median of 0.88 and 0.89 is not.
    #
    # ON A TIE, THE HIGHER THRESHOLD, and not the more frequent one. That is the
    # convention threshold.choose already declares -- "fewer alerts for the same
    # money is strictly better for the analyst" -- and my first version broke
    # ties by frequency, which on the measured draws landed on 0.88 where the
    # declared convention gives 0.89. Not a detail: the lower one sat at 58.6
    # alerts a day with a draw at 59.9, one draw away from breaking the ceiling
    # the policy exists to respect.
    proposed = min(tally, key=lambda value: (abs(value - middle), -value))

    # AND THE PROPOSED THRESHOLD IS RE-READ ON EVERY DRAW. The figures logged
    # above are each draw at ITS OWN choice, which is not the same thing: the
    # mean of those would describe a threshold that moves, reported as if it
    # were the one proposed.
    at_proposed = []
    for points in sweeps:
        match = [point for point in points
                 if abs(point.threshold - proposed) < 1e-9]
        if not match:
            raise AssertionError(
                "the proposed threshold %.2f is not on the grid the sweep "
                "evaluated: it cannot have been chosen by it" % proposed)
        at_proposed.append(match[0])

    log.info("")
    log.info("seuils retenus : %s",
             ", ".join("%.2f x%d" % (value, count)
                       for value, count in sorted(tally.items())))
    log.info("seuil propose  : %.2f   (actuel %.2f)", proposed, current)
    log.info("a ce seuil, sur les %d tirages : rappel %.3f [%.3f-%.3f]  "
             "precision %.3f  alertes/j %.1f [%.1f-%.1f]",
             len(at_proposed),
             st.mean(p.recall for p in at_proposed),
             min(p.recall for p in at_proposed),
             max(p.recall for p in at_proposed),
             st.mean(p.precision for p in at_proposed),
             st.mean(p.alerts_per_day(n_rows) for p in at_proposed),
             min(p.alerts_per_day(n_rows) for p in at_proposed),
             max(p.alerts_per_day(n_rows) for p in at_proposed))

    # --- does the proposed threshold catch significantly more, on TEST?
    #
    # The sweep chose on validation; the comparison is reported on test, which
    # is sound because the test set decides nothing here -- the threshold is
    # already fixed. It only says what the choice is worth.
    test = train.load_split(folder / "test.parquet")
    test_labels = test["label"].astype(int).tolist()
    tables = []
    for draw in range(max(1, args.repeats)):
        permuted = clock.permute(test, draw)
        scores = _model_scores(folder, features, permuted)
        # nested=True, and it is not a precaution: two thresholds on the SAME
        # scores are nested -- the lower one flags everything the higher one
        # flags, by arithmetic. So the binomial test degenerates to 2x(1/2)^n
        # and declares significance for any n >= 6, whatever the data says.
        # What this table is for is the TRADE-OFF: frauds gained against alert
        # volume added.
        tables.append(paired.compare(
            paired.flags(scores, current), paired.flags(scores, proposed),
            labels=test_labels, first_name="seuil %.2f" % current,
            second_name="seuil %.2f" % proposed, nested=True))

    # THE FIGURES FIRST, THE REPORT AFTERWARDS. The lesson of ingest_once,
    # applied where I had failed to apply it: a defect in the REPORT must never
    # destroy the work. The probe fit and ten sweeps cost minutes; a NameError
    # in render() threw all of it away once.
    raw = {
        "recipe": payload["dataset"]["version"],
        "current_threshold": current,
        "proposed_threshold": proposed,
        "policy": settings.threshold_policy,
        "capacity_per_day": settings.alert_capacity_per_day,
        "validation_window": window,
        "validation_rows": n_rows,
        "test_rows": len(test),
        # Each draw at ITS OWN choice...
        "draws": [{"threshold": choice.threshold,
                   "recall": choice.chosen.recall,
                   "precision": choice.chosen.precision,
                   "alerts_per_day": choice.chosen.alerts_per_day(n_rows)}
                  for choice in chosen],
        # ...and every draw at the SINGLE proposed threshold, which is what the
        # proposal is actually worth.
        "at_proposed": [{"recall": point.recall,
                         "precision": point.precision,
                         "alerts_per_day": point.alerts_per_day(n_rows)}
                        for point in at_proposed],
        "paired": [{"both": table.both, "only_current": table.only_first,
                    "only_proposed": table.only_second,
                    "neither": table.neither, "p_value": table.p_value,
                    "difference": table.difference}
                   for table in tables],
    }
    figures = folder / "mesure-seuil-flux.json"
    figures.write_text(json.dumps(raw, indent=2, sort_keys=True),
                       encoding="utf-8")
    log.info("")
    log.info("Chiffres bruts ecrits : %s", figures)

    try:
        report = render(payload, current, proposed, tally, at_proposed, tables,
                        n_rows, len(test), window, settings)
    except Exception as problem:            # noqa: BLE001
        log.error("le RAPPORT n'a pas pu etre produit : %r", problem)
        log.error("les chiffres, eux, sont intacts dans %s : le rendu est du "
                  "compte rendu, pas du travail.", figures)
        return 1

    out = folder / "mesure-seuil-flux.md"
    out.write_text(report, encoding="utf-8")
    log.info("Mesure ecrite : %s", out)
    print()
    print(report)
    return 0


_MODEL = []


def _model_scores(folder: Path, features, frame):
    """The REGISTERED model's scores. Loaded once, from the joblib beside the
    manifest -- the comparison is about two thresholds on one model, so the
    model has to be the one that will actually score."""
    import joblib

    if not _MODEL:
        _MODEL.append(joblib.load(folder / "model.joblib"))
    return _MODEL[0].predict_proba(frame[features])[:, 1]


def render(payload, current, proposed, tally, at_proposed, tables, n_validation,
           n_test, window, settings) -> str:
    """The report. at_proposed holds every draw read at the SINGLE proposed
    threshold -- not each draw at its own choice, which would describe a moving
    threshold as if it were the one being proposed."""
    # Imported HERE and not only in main(): this function is called at module
    # level scope, so a name bound inside main() is not in scope for it. That
    # was the defect -- the probe fit and ten sweeps ran, then the report raised
    # NameError and the whole measurement was lost.
    from ml4fd import paired

    recalls = [point.recall for point in at_proposed]
    volumes = [point.alerts_per_day(n_validation) for point in at_proposed]
    gains = [table.difference for table in tables]

    lines = [
        "# N4 --- le seuil rechoisi sur la distribution du flux",
        "",
        "Recette `%s`. Politique **%s**, capacite **%d** alertes/jour."
        % (payload["dataset"]["version"], settings.threshold_policy,
           settings.alert_capacity_per_day),
        "Balayage sur la validation %s, pendule rompue, %d tirages."
        % (window, len(at_proposed)),
        "",
        "Les chiffres ci-dessous sont lus **au seuil proposé**, sur chacun des "
        "tirages --- et non chaque tirage a son propre choix, qui decrirait un "
        "seuil mobile comme s'il etait celui qu'on propose.",
        "",
        "## Le seuil",
        "",
        "| | Seuil | Rappel (validation) | Alertes/j (validation) |",
        "|---|---|---|---|",
        "| actuel, choisi sur l'heure du paiement | %.2f | --- | --- |"
        % current,
        "| rechoisi sur la pendule rompue | **%.2f** | %.3f [%.3f-%.3f] | "
        "%.1f [%.1f-%.1f] |"
        % (proposed, st.mean(recalls), min(recalls), max(recalls),
           st.mean(volumes), min(volumes), max(volumes)),
        "",
        "Seuils retenus par tirage : %s."
        % ", ".join("%.2f (x%d)" % item for item in sorted(tally.items())),
        "",
        "## Ce que le nouveau seuil change, fraude par fraude",
        "",
        "Comparaison **appariee** sur les %d lignes du jeu de test : les deux "
        "seuils voient les memes fraudes, donc ce qui compte est sur lesquelles "
        "ils divergent, non la difference de deux taux." % n_test,
        "",
    ]
    lines.append(paired.render(tables[0]) if tables else "")
    lines += [
        "",
        "Sur les %d tirages : **%+d fraudes** de plus en median, de %+d a %+d."
        % (len(tables), st.median(gains), min(gains), max(gains)),
        "",
        "Aucune significativite n'est annoncee ici, et c'est voulu : les deux "
        "decisions sont **emboitees** --- un seuil plus bas signale un "
        "sur-ensemble --- donc le test binomial degenere en 2 x (1/2)^n et ne "
        "mesure que le nombre de desaccords. L'arbitrage est entre les fraudes "
        "gagnees et le volume d'alertes ajoute, et il se lit au-dessus.",
        "",
        "## Verdict",
        "",
    ]
    if proposed == current:
        lines.append(
            "- Le balayage sur la distribution du flux **retombe sur le seuil "
            "actuel**. Rien a changer, et c'est un resultat : la capacite "
            "inemployee ne vient pas d'un seuil mal place.")
    else:
        direction = "abaisse" if proposed < current else "releve"
        lines += [
            "- Le seuil est **%s de %.2f a %.2f**." % (direction, current,
                                                       proposed),
            "- Il emploie %.1f alertes/jour de la capacite de %d, contre un "
            "seuil actuel qui en laissait la moitie inemployee."
            % (st.mean(volumes), settings.alert_capacity_per_day),
        ]
    lines.append(
        "- Gain de **%+d fraudes** en median sur %d positifs, pour un volume "
        "qui passe de la moitie de la capacite a %.1f alertes/jour sur %d."
        % (st.median(gains), tables[0].n if tables else 0,
           st.mean(volumes), settings.alert_capacity_per_day))
    lines += [
        "",
        "**Ce seuil n'est pas inscrit dans `governance.yml` par cet outil.** Le "
        "seuil opérant vit dans le manifeste de la version de modele, que seul "
        "S2 ecrit : le porter ici reviendrait a avoir deux sources de verite. "
        "Pour l'appliquer, il faut que M6 le choisisse, donc que S2 balaie sur "
        "une validation a pendule rompue --- ce qui est la decision a prendre, "
        "et non un effet de bord d'une mesure.",
        "",
        "**Et la comparaison des recettes doit se faire a seuil rechoisi des "
        "deux cotes.** Confronter un modele bien regle a un modele au seuil "
        "perime ne mesure pas les modeles, il mesure le reglage.",
        "",
        "**L'hypothese de M6 est reconduite telle quelle, et elle reste une "
        "hypothese.** Le seuil est choisi sur les scores d'une SONDE ajustee "
        "sur les 80 % les plus anciens de l'entrainement, puis applique aux "
        "scores du modele ajuste sur la totalite. On suppose les deux "
        "distributions assez proches pour que le seuil se transporte. C'est le "
        "compromis usuel --- l'alternative serait de publier un modele ajuste "
        "sur 80 % des donnees --- mais c'est une supposition, pas un resultat, "
        "et elle vaut ici exactement comme en M6.",
    ]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    sys.exit(main())
