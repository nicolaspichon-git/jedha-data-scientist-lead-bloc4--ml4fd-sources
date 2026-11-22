"""Score the same payload twice, with each verdict, and require the same score.

Block: B3, B4.
Lot: B3-N6, B4-L0.
Competency: B3-C4, B3-C5.
Covers: B3-EE03, B3-EI03, B3-EI12, B3-EC17.

THE ONLY ACCEPTANCE CRITERION OF THIS PLAN THAT MEASURES AN ABSENCE. Every
other one checks that a function does something; this one checks that a value
does NOT reach a model. The distinction matters because a leak leaves no trace
in any metric: a model that reads the label scores beautifully, and the report
would be the proudest page of the dossier.

The API delivers is_fraud in the SAME payload as the features -- B3-N0 measured it,
21 columns, is_fraud present as a str -- so the label sits one line of code away
from the scoring. tests/test_leak.py reads the code and the schema for the four
structural guarantees. This runs the chain, with the REGISTERED model, and
compares the two scores.

WHY THE TYPED COMPARISON IS THE REAL PROOF. If two payloads differing only in
is_fraud produce the SAME typed transaction, field for field, then no step after
that can tell them apart, whatever it does. Scoring them both is the
demonstration; the identity of the typed rows is the argument.

Usage, from the scheduler container:
    python3 /opt/project/tools/check_leak.py
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("check_leak")

# A payload of the measured shape. The values are the ones B3-N0 published as safe
# to record -- no name, no address, no date of birth -- and cc_num is the
# 19-digit card the measurement found, so the check also crosses the precision
# boundary that cost S1 a ruling.
PAYLOAD = {
    "trans_num": "aca54e7c407bb98811b688f3f055c4c7",
    "current_time": "1794817599732",
    "cc_num": "4992346398065154184",
    "merchant": "fraud_Dach-Borer",
    "category": "grocery_net",
    "amt": "371.94",
    "merch_lat": "39.065279",
    "merch_long": "-76.195157",
    "lat": "38.2674",
    "long": "-76.4954",
}

# The history features a stream transaction would carry. Identical for both
# variants by construction -- they are read from fd_transactions, which has no
# column for a verdict -- so fixed values here keep the check independent of
# whatever happens to be in the database.
HISTORY = {"f_tx_count": 635, "f_avg_amount": 67.42}

VERDICT_WORDS = ("is_fraud", "label", "fraud", "verdict", "target")


def check(name: str, passed: bool, detail: str = "") -> bool:
    log.info("  [%s] %s%s", "OK " if passed else "ECHEC", name,
             "  -- %s" % detail if detail else "")
    return passed


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Require the same score for a payload labelled fraudulent "
                    "and the same payload labelled honest.",
        epilog="The only acceptance criterion of S3 that measures an absence.")
    parser.add_argument("--recipe", "--digest", dest="recipe", default=None,
                        help="recipe VERSION -- the 12-character directory name under datasets/, which is datasets.Recipe.version() and never the full 64-character digest (default: the latest run)")
    args = parser.parse_args(argv)

    sys.path.insert(0, "/opt/project")

    import joblib
    import pandas as pd

    from ml4fd import checks, config, registry, transform

    settings = config.load()
    root = Path(settings.datasets_root) / settings.dataset_family
    folder = (root / args.recipe if args.recipe
              else max((p.parent for p in root.glob("*/run.json")),
                       key=lambda p: (p / registry.MANIFEST_FILE).stat().st_mtime))
    payload = json.loads((folder / registry.MANIFEST_FILE)
                         .read_text(encoding="utf-8"))
    features = list(payload["dataset"]["features"])
    cut = float((payload.get("threshold") or {}).get("value") or 0)

    log.info("recette %s, %d features, seuil %.2f",
             payload["dataset"]["version"], len(features), cut)
    log.info("")

    passed = []

    # 1. The recipe itself names no verdict.
    offending = [name for name in features
                 if any(word in name for word in VERDICT_WORDS)]
    passed.append(check("la recette ne nomme aucun verdict",
                        not offending, ", ".join(offending)))

    # 2. Validation does not differ. A record rejected on one label and
    #    accepted on the other would be a leak through the quarantine.
    rejects = {}
    for label in ("0", "1"):
        record = dict(PAYLOAD, is_fraud=label)
        # check_record returns a LIST of Reject, not a tuple. Verified by
        # introspection before writing this line: unpacking it as a pair would
        # have worked by accident on a record with exactly two rejects.
        rejects[label] = checks.check_record(
            record, 1, profile=checks.STREAM_PROFILE)
    passed.append(check("la validation ne distingue pas les deux etiquettes",
                        rejects["0"] == rejects["1"],
                        "%d / %d motif(s)" % (len(rejects["0"]),
                                              len(rejects["1"]))))

    # 3. THE ARGUMENT: the typed transactions are identical, field for field.
    typed = {}
    for label in ("0", "1"):
        typed[label] = transform.to_transaction(
            dict(PAYLOAD, is_fraud=label), settings.pepper,
            profile=checks.STREAM_PROFILE)
    differing = [key for key in set(typed["0"]) | set(typed["1"])
                 if typed["0"].get(key) != typed["1"].get(key)]
    passed.append(check("la transaction typee est identique, champ par champ",
                        not differing, ", ".join(differing)))
    passed.append(check("aucun champ de la transaction ne nomme un verdict",
                        not [k for k in typed["0"]
                             if any(w in k for w in VERDICT_WORDS)]))

    # 4. THE DEMONSTRATION: the registered model, called on both.
    model = joblib.load(folder / "model.joblib")
    scores = {}
    for label in ("0", "1"):
        row = dict(HISTORY)
        row["amt"] = float(typed[label]["amt"])
        row["category"] = typed[label]["category"]
        row["f_home_distance_km"] = float(
            typed[label].get("f_home_distance_km") or 0.0)
        stamp = typed[label]["txn_ts"]
        row["txn_hour"] = stamp.hour
        row["txn_weekday"] = stamp.isoweekday()
        frame = pd.DataFrame([row])
        frame["category"] = frame["category"].astype("category")
        scores[label] = float(model.predict_proba(frame[features])[:, 1][0])

    log.info("")
    log.info("  is_fraud = 0  ->  score %.17f", scores["0"])
    log.info("  is_fraud = 1  ->  score %.17f", scores["1"])
    log.info("")
    passed.append(check("MEME SCORE pour les deux etiquettes",
                        scores["0"] == scores["1"],
                        "ecart %.3e" % abs(scores["0"] - scores["1"])))

    # 5. And the decision that follows is the same too, which is what an
    #    analyst would actually see.
    passed.append(check("meme decision au seuil %.2f" % cut,
                        (scores["0"] >= cut) == (scores["1"] >= cut),
                        "signalee" if scores["0"] >= cut else "non signalee"))

    log.info("")
    if all(passed):
        log.info("%d/%d : aucune fuite du verdict vers le scoring.",
                 len(passed), len(passed))
        log.info("L'identite des transactions typees est l'argument ; les deux "
                 "scores en sont la demonstration.")
        return 0
    log.error("%d/%d controle(s) passent : IL Y A UNE FUITE.",
              sum(passed), len(passed))
    return 1


if __name__ == "__main__":
    sys.exit(main())
