"""Judge the newest registered model against the champion, and move the alias.

Covers: EC34, EC36, EC37   Lot: L3   Competency: C5

REGISTERING IS NOT PROMOTING. Every run of S2 registers a version; this is the
only thing that puts one in production. The decision itself lives in
ml4fd/promote.py as a pure function, tested branch by branch; this module is
its I/O -- it resolves the two sides, measures them on shared ground, and
writes the outcome down.

DRY BY DEFAULT, --apply TO ACT. The same choice as purge, digests and
abandoned: looking at what would happen must not change what is served.

ON WHICH ROWS THE TWO MODELS ARE COMPARED, which is the one real difficulty
here. A paired test needs both decisions over the SAME rows, and those rows
must be rows NEITHER model was fitted on. The candidate's own test split does
not qualify: a retraining on fresher data moves the boundary, so part of its
test set sits inside the champion's training period -- and the champion would
score those rows from memory. Comparing there would favour the champion by
construction, every time, and would block exactly the retraining L4 exists to
automate.

So the comparison runs on the candidate's test split RESTRICTED to the rows
after the champion's own training boundary, which dataset.split_rule declares.
Both models are then strangers to every row. When that window is too thin the
module promotes nothing and says why: a verdict drawn from twenty frauds would
be noise wearing a p-value.

Usage, from the scheduler container:
    python3 /opt/project/tools/promote.py
    python3 /opt/project/tools/promote.py --apply
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path

log = logging.getLogger("promote")

# Below this many frauds in the shared window, the paired test is not asked.
# Not a statistical threshold -- paired.py shows the test can decide on a dozen
# discordant pairs when the split is lopsided -- but a floor under which a
# verdict says more about the window than about the two models. Refusing leaves
# the champion in place, which is the safe direction.
MIN_SHARED_FRAUDS = 50


def folder_of(settings, dataset_version: str) -> Path:
    """The directory of a recipe, named by its VERSION and never its digest.

    Derived from score.manifest_path rather than rebuilt, so the two cannot
    drift apart: that function carries the warning and the incident.
    """
    from ml4fd import score

    return score.manifest_path(settings, dataset_version).parent


def payload_for(client, settings, model_version) -> dict:
    """The manifest of the recipe a registered version was produced from.

    Read through score.read_manifest, which refuses a manifest without a
    feature list or without a threshold -- the two fields this gate needs and
    the two it must never substitute a default for.
    """
    from ml4fd import score

    params = dict(client.get_run(model_version.run_id).data.params)
    version = params.get("dataset_version")
    if not version:
        raise LookupError(
            "run %s of model version %s logged no dataset_version, so its "
            "manifest cannot be found." % (model_version.run_id,
                                           model_version.version))
    return score.read_manifest(score.manifest_path(settings, str(version)))


def training_boundary(payload: dict) -> str:
    """The last day the model was fitted on, out of dataset.split_rule.

    Parsed rather than stored again: split_rule is what the manifest declares,
    and a second copy of the same boundary could disagree with it.
    """
    rule = str(payload["dataset"]["split_rule"])
    found = re.search(r"train<=(\d{4}-\d{2}-\d{2})", rule)
    if not found:
        raise ValueError(
            "split_rule %r declares no training boundary of the form "
            "train<=YYYY-MM-DD, so the shared window cannot be computed" % rule)
    return found.group(1)


def side_of(payload: dict, version):
    """One side of the decision, out of its manifest.

    The figures are READ, never recomputed: they are what was measured and
    published when the recipe was evaluated, and a gate that re-measured would
    be comparing its own arithmetic with the published numbers.
    """
    from ml4fd import promote

    metrics = payload["metrics"]
    return promote.Side(
        version=str(version),
        digest=str(payload["dataset"]["recipe_digest"]),
        recall=float(metrics["recall"]),
        alerts_per_day=float(metrics["alerts_per_day"]))


def shared_window(settings, candidate_payload: dict, champion_payload: dict):
    """The candidate's test rows that neither model was fitted on."""
    import pandas as pd
    from ml4fd import train

    frame = train.load_split(
        folder_of(settings, str(candidate_payload["dataset"]["version"]))
        / "test.parquet")
    cut = pd.Timestamp(training_boundary(champion_payload) + " 23:59:59")
    stamps = frame["txn_ts"]
    if getattr(stamps.dtype, "tz", None) is not None:
        cut = cut.tz_localize("UTC")
    return frame[stamps > cut]


def labels_of(frame):
    """The fraud label of each row, named once.

    datasets.LABEL rather than a literal, and the literal would have been the
    wrong one: the payment API and fd_transactions call this is_fraud, the
    Parquet splits call it label. Writing the API's name here cost a KeyError
    that no test could show, because every test of the gate works on a table
    that is already built (2026-11-19).
    """
    from ml4fd import datasets

    return frame[datasets.LABEL].astype(int)


def scores_of(settings, payload: dict, frame):
    """The candidate's or champion's probability on every row of the frame."""
    import joblib

    folder = folder_of(settings, str(payload["dataset"]["version"]))
    model = joblib.load(folder / "model.joblib")
    return model.predict_proba(frame[list(payload["dataset"]["features"])])[:, 1]


def comparison(settings, candidate_payload, champion_payload, frame):
    """The paired table, each side read at ITS OWN threshold.

    Confronting a well-tuned model with one left at a stale threshold does not
    measure the models, it measures the tuning -- compare_recipes.py says the
    same thing, for the same reason.

    The CHAMPION is the first side and the candidate the second, which is the
    order ml4fd/promote.decide documents: a difference in favour of the second
    column is a difference in favour of the candidate.
    """
    from ml4fd import paired

    return paired.compare(
        paired.flags(scores_of(settings, champion_payload, frame),
                     float(champion_payload["threshold"]["value"])),
        paired.flags(scores_of(settings, candidate_payload, frame),
                     float(candidate_payload["threshold"]["value"])),
        labels=labels_of(frame),
        first_name="champion", second_name="candidat")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Judge the newest registered model against the champion "
                    "and, if it passes, move the champion alias to it.",
        epilog="Dry by default: without --apply nothing is promoted and "
               "nothing is journalled.")
    parser.add_argument("--apply", action="store_true",
                        help="move the alias and write the decision down")
    parser.add_argument("--candidate", default=None,
                        help="registry VERSION to judge (default: the newest)")
    args = parser.parse_args(argv)

    sys.path.insert(0, "/opt/project")

    import mlflow
    from ml4fd import config, db, paired, promote, registry, score

    settings = config.load()
    uri = os.environ.get("MLFLOW_TRACKING_URI")
    if uri:
        mlflow.set_tracking_uri(uri)
    client = mlflow.MlflowClient()

    candidate_mv = (client.get_model_version(registry.REGISTERED_NAME,
                                             str(args.candidate))
                    if args.candidate else score.latest_version(client))
    champion_mv = registry.champion(client, alias=settings.champion_alias)

    if champion_mv is not None and \
            str(champion_mv.version) == str(candidate_mv.version):
        print("la version %s porte deja l'alias %s : rien a decider."
              % (candidate_mv.version, settings.champion_alias))
        return 0

    candidate_payload = payload_for(client, settings, candidate_mv)
    candidate = side_of(candidate_payload, candidate_mv.version)
    print("candidat : version %s, recette %s, rappel %.3f, %.1f alertes/jour"
          % (candidate.version, candidate_payload["dataset"]["version"],
             candidate.recall, candidate.alerts_per_day))

    champion = None
    table = None
    if champion_mv is None:
        print("aucun champion en service.")
    else:
        champion_payload = payload_for(client, settings, champion_mv)
        champion = side_of(champion_payload, champion_mv.version)
        print("champion : version %s, recette %s, rappel %.3f"
              % (champion.version, champion_payload["dataset"]["version"],
                 champion.recall))

        frame = shared_window(settings, candidate_payload, champion_payload)
        frauds = int(labels_of(frame).sum())
        print("fenetre partagee : %d lignes, %d fraudes -- posterieures au %s, "
              "fin d'entrainement du champion"
              % (len(frame), frauds, training_boundary(champion_payload)))
        if frauds < MIN_SHARED_FRAUDS:
            print("REFUS DE DECIDER : %d fraudes, sous le plancher de %d. Le "
                  "champion reste en service et rien n'est enregistre -- la "
                  "mesure etait impossible, ce qui n'est pas un verdict sur "
                  "le modele." % (frauds, MIN_SHARED_FRAUDS))
            return 1
        table = comparison(settings, candidate_payload, champion_payload, frame)
        print()
        print(paired.render(table, subject="fraudes"))

    decision = promote.decide(
        candidate, champion, table, promote.Policy.from_settings(settings))
    print(decision.reason)

    if not args.apply:
        print("\n(essai a blanc : ni alias deplace, ni decision enregistree. "
              "Relancer avec --apply.)")
        return 0

    if decision.promoted:
        registry.set_champion(client, candidate_mv.version,
                              alias=settings.champion_alias)
    with db.connect("fd_ingest", settings) as conn:
        promote.record(conn, decision, run_id=candidate_mv.run_id)
        conn.commit()
    print("decision enregistree dans fd_promotions.")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sys.exit(main())
