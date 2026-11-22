"""Two recipes, compared fraud by fraud on the same test set.

Block: B3, B4.
Lot: B3-N4, B4-L0.
Competency: B3-C2, B3-C5.
Covers: B3-EE08, B3-EI04, B3-EC24.

THE QUESTION THIS SETTLES. Does a recipe amputated of txn_hour and txn_weekday
catch more fraud, on the stream, than the current recipe does? The current
recipe's figure on the stream is the clock-permuted reading of B3-N4 -- 0.631 of
recall over 20 draws -- and the amputated one needs none, since it reads no
calendar column: its score does not depend on the clock at all.

WHY THE COMPARISON IS PAIRED. Both recipes are evaluated on the SAME 205
frauds, so comparing two recalls throws away which fraud went where and leaves
a noise of +/-0.048 on the difference -- enough to make a gain of nine frauds
read as 0.9 sigma. ml4fd/paired.py explains the arithmetic; this tool feeds it.

WHY EACH SIDE USES ITS OWN THRESHOLD. Confronting a well-tuned model with one
left at a stale threshold does not measure the models, it measures the tuning.
So each side is read at the threshold its own manifest carries -- and if one of
them was never re-chosen on the stream's distribution, this tool says so rather
than quietly comparing anyway.

A CAVEAT STATED RATHER THAN BURIED. The current recipe's decisions depend on
the clock permutation, so its side of the table is a draw. The comparison is
therefore run once per draw and the verdict counts how many draws decide --
never a single table chosen after the fact.

Usage, from the scheduler container:
    python3 /opt/project/tools/compare_recipes.py --against <digest>
"""
from __future__ import annotations

import argparse
import json
import logging
import statistics as st
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("compare_recipes")


def load(folder: Path) -> dict:
    from ml4fd import registry

    path = folder / registry.MANIFEST_FILE
    if not path.is_file():
        raise FileNotFoundError(
            "no %s under %s: that recipe has not been trained."
            % (registry.MANIFEST_FILE, folder))
    return json.loads(path.read_text(encoding="utf-8"))


def scores_of(folder: Path, features, frame):
    import joblib

    model = joblib.load(folder / "model.joblib")
    return model.predict_proba(frame[features])[:, 1]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare two trained recipes fraud by fraud on the same "
                    "test set, each at its own threshold.",
        epilog="The current recipe reads the clock, so its decisions depend on "
               "the permutation: the comparison runs once per draw.")
    parser.add_argument("--current", default=None,
                        help="VERSION of the recipe in service -- the "
                             "12-character directory name under "
                             "datasets/ (default: the one that reads "
                             "the clock)")
    parser.add_argument("--against", required=True,
                        help="VERSION of the candidate recipe -- its "
                             "directory name under datasets/")
    parser.add_argument("--repeats", type=int, default=20,
                        help="clock permutations (default 20)")
    parser.add_argument("--current-threshold", type=float, default=None,
                        help="the threshold to read the recipe in service at, "
                             "in place of the one its manifest carries. This "
                             "is how a re-chosen threshold enters the "
                             "comparison: the manifest still holds the value "
                             "B4-M6 chose on the hour of the PAYMENT, and only B4-S2 "
                             "may rewrite a manifest.")
    args = parser.parse_args(argv)

    sys.path.insert(0, "/opt/project")

    from ml4fd import clock, config, paired, train

    settings = config.load()
    root = Path(settings.datasets_root) / settings.dataset_family

    candidate_folder = root / args.against
    candidate = load(candidate_folder)

    if args.current:
        current_folder = root / args.current
    else:
        reading = [folder for folder in root.iterdir()
                   if (folder / "run.json").is_file()
                   and folder.name != args.against
                   and clock.present(load(folder)["dataset"]["features"])]
        if len(reading) != 1:
            log.error("impossible de designer la recette en service : %d "
                      "recette(s) lisent la pendule. Passer --current.",
                      len(reading))
            return 1
        current_folder = reading[0]
    current = load(current_folder)

    current_features = list(current["dataset"]["features"])
    candidate_features = list(candidate["dataset"]["features"])
    current_cut = float((current.get("threshold") or {}).get("value") or 0)
    candidate_cut = float((candidate.get("threshold") or {}).get("value") or 0)
    current_source = "son manifeste"
    if args.current_threshold:
        current_cut = float(args.current_threshold)
        current_source = "la ligne de commande"
    if not (current_cut and candidate_cut):
        log.error("une des deux recettes ne porte pas de seuil : il n'y a rien "
                  "a comparer")
        return 1

    log.info("en service : %s, %d features, seuil %.2f (%s)",
             current["dataset"]["version"], len(current_features), current_cut,
             current_source)
    log.info("candidate  : %s, %d features, seuil %.2f",
             candidate["dataset"]["version"], len(candidate_features),
             candidate_cut)

    if clock.present(candidate_features):
        log.warning("la candidate lit encore la pendule (%s) : ce n'est pas "
                    "une recette amputee",
                    ", ".join(clock.present(candidate_features)))

    # A recipe that reads no calendar column needs no re-choice: its validation
    # distribution IS its stream distribution, so its manifest threshold is
    # already the right one. The warning is only for a recipe that reads the
    # clock and whose threshold was chosen on the hour of the payment.
    for name, payload, override in (("en service", current,
                                     args.current_threshold),
                                    ("candidate", candidate, None)):
        if override:
            log.info("le seuil de la recette %s vient de la ligne de commande "
                     "(%.2f) : c'est ainsi qu'un seuil rechoisi entre dans la "
                     "comparaison, puisque seul S2 reecrit un manifeste.",
                     name, float(override))
            continue
        chosen_on = str((payload.get("threshold") or {}).get("chosen_on") or "")
        if "pendule" not in chosen_on and clock.present(
                payload["dataset"]["features"]):
            log.warning("le seuil de la recette %s a ete choisi sur %r, donc "
                        "sur l'heure du paiement et non sur la distribution du "
                        "flux. La comparaison mesure alors aussi le reglage : "
                        "passer --current-threshold avec la valeur que "
                        "rechoose_threshold a proposee.", name, chosen_on)

    # The same test set for both, and it must be the same one or the comparison
    # is not paired at all.
    if current["dataset"]["split_rule"] != candidate["dataset"]["split_rule"]:
        log.error("les deux recettes n'ont pas la meme regle de decoupage "
                  "(%r contre %r) : leurs jeux de test ne sont pas les memes "
                  "lignes, et une comparaison appariee n'a pas de sens",
                  current["dataset"]["split_rule"],
                  candidate["dataset"]["split_rule"])
        return 1

    test = train.load_split(candidate_folder / "test.parquet")
    labels = test["label"].astype(int).tolist()

    # The candidate reads no calendar column, so its decisions are the same for
    # every draw: computed once.
    candidate_flags = paired.flags(
        scores_of(candidate_folder, candidate_features, test), candidate_cut)

    tables = []
    for draw in range(max(1, args.repeats)):
        permuted = clock.permute(test, draw)
        current_flags = paired.flags(
            scores_of(current_folder, current_features, permuted), current_cut)
        tables.append(paired.compare(
            current_flags, candidate_flags, labels=labels,
            first_name=current["dataset"]["version"][:8],
            second_name=candidate["dataset"]["version"][:8]))
        log.info("  tirage %2d : %+d fraude(s) pour la candidate, p = %.3f",
                 draw, tables[-1].difference, tables[-1].p_value)

    report = render(current, candidate, tables, len(test), settings,
                    current_cut, current_source, candidate_cut)
    out = candidate_folder / "comparaison-appariee.md"
    out.write_text(report, encoding="utf-8")
    log.info("")
    log.info("Comparaison ecrite : %s", out)
    print()
    print(report)
    return 0


def render(current, candidate, tables, n_test, settings, current_cut,
           current_source, candidate_cut) -> str:
    from ml4fd import paired

    gains = [table.difference for table in tables]
    decisive = [table for table in tables if table.p_value < 0.05]
    for_candidate = sum(1 for table in decisive if table.difference > 0)
    for_current = sum(1 for table in decisive if table.difference < 0)

    lines = [
        "# B3-N4 --- comparaison appariee de deux recettes",
        "",
        "| | Recette | Features | Seuil |",
        "|---|---|---|---|",
        "| en service | `%s` | %d | %.2f (%s) |"
        % (current["dataset"]["version"], len(current["dataset"]["features"]),
           current_cut, current_source),
        "| candidate | `%s` | %d | %.2f (son manifeste) |"
        % (candidate["dataset"]["version"],
           len(candidate["dataset"]["features"]), candidate_cut),
        "",
        "Jeu de test : %d lignes, %d fraudes. %d tirages de pendule."
        % (n_test, tables[0].n if tables else 0, len(tables)),
        "",
        "## Un tirage, en detail",
        "",
        paired.render(tables[0]) if tables else "",
        "",
        "## Sur l'ensemble des tirages",
        "",
        "- Avantage de la candidate : median **%+d fraude(s)**, de %+d a %+d."
        % (st.median(gains), min(gains), max(gains)),
        "- Ecart significatif a 5 %% dans **%d tirages sur %d** : %d en faveur "
        "de la candidate, %d en faveur de la recette en service."
        % (len(decisive), len(tables), for_candidate, for_current),
        "",
        "## Verdict",
        "",
    ]
    if for_candidate and not for_current and len(decisive) > len(tables) / 2:
        lines.append(
            "- La candidate attrape significativement plus de fraudes dans la "
            "majorite des tirages, et jamais moins. **Elle l'emporte**, et elle "
            "l'emporte en plus sur la stabilite : ne lisant pas la pendule, "
            "elle ne subit ni le facteur 6,5 sur le volume d'alertes ni "
            "l'effondrement du rappel de 04h a 05h.")
    elif for_current and not for_candidate:
        lines.append(
            "- La recette en service attrape significativement plus de fraudes. "
            "**Retirer les deux colonnes coute du rappel**, et il faut alors "
            "mettre ce cout en face de ce que la stabilite vaut : un volume "
            "d'alertes qui varie d'un facteur 6,5 selon l'heure, et 64 % des "
            "fraudes manquees de 04h a 05h.")
    else:
        lines.append(
            "- **Aucun tirage ne tranche dans un sens net.** Avec %d fraudes au "
            "jeu de test, un ecart de cette taille n'est pas decidable sur ces "
            "donnees -- ce qui n'est pas la meme chose que \"les deux se "
            "valent\". La decision revient alors a ce qui, lui, est mesure : la "
            "stabilite. La recette amputee ne subit ni le facteur 6,5 sur le "
            "volume d'alertes, ni le rappel de 0,361 a 04h-05h, ni la precision "
            "de 0,25 a 22h-23h."
            % (tables[0].n if tables else 0))
    lines += [
        "",
        "Le rappel agrege de chaque cote n'est volontairement pas mis en avant : "
        "sur %d positifs il porte +/-0,034, et la difference de deux tels "
        "nombres +/-0,048. C'est precisement ce que la vue appariee evite."
        % (tables[0].n if tables else 0),
    ]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    sys.exit(main())
