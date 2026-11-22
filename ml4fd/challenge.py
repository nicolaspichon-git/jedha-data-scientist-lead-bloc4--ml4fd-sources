"""The challenging of a candidate against the champion, and its outcome.

Block: B4.
Lot: B4-L3.
Competency: B4-C5.
Covers: B4-EC34, B4-EC36, B4-EC37.

WHY THIS MODULE EXISTS, AND WHY IT IS NOT ml4fd/promote.py. promote.py holds
the DECISION as a pure function, tested branch by branch without mlflow, and
that purity is worth keeping: it is what lets the refusals be exercised at all.
This module is its I/O -- it resolves the two sides, measures them on shared
ground, asks promote.decide, and writes the outcome down.

WHY IT IS NOT tools/promote.py EITHER, which is where all of this lived until
2026-11-20. Nothing promoted automatically: dag_train ended at register_model
and the alias moved only when somebody ran the command by hand. Closing that
gap means the DAG and the command must reach the SAME orchestration, not two
copies of it -- a second copy is how a gate ends up refusing in one path and
promoting in the other. tools/ holds scripts, loaded by path and never
imported, so the shared code comes here and tools/promote.py becomes its CLI.

THE TWO NON-DECISIONS ARE EXCEPTIONS AND NOT FALSE. "The candidate already
carries the alias" and "the shared window is too thin to ask" are both
different from "the candidate was refused": nothing is recorded in either case
and the champion stays. Returning False for all three would have let a caller
report a refusal that never happened.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

log = logging.getLogger(__name__)


class AlreadyChampion(Exception):
    """The candidate is the version already in service: nothing to decide."""


class Undecidable(Exception):
    """The shared window holds too few frauds for the paired test to be asked.

    Not a verdict on the model. A p-value drawn from twenty frauds would be
    noise wearing a decimal point, and refusing leaves the champion in place,
    which is the safe direction.
    """


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
    """The candidate's test rows that neither model was fitted on.

    WHY THAT STILL HOLDS SINCE datasets.include_stream, and it is no longer for
    the reason it used to be. It used to hold because split_rule partitioned
    time: everything up to train<= was fitted on, everything after was not. The
    stream broke that partition -- a model is now fitted on rows of 2026 while
    the test slice is May 2020 -- and the guarantee survives only because
    sql/datasets/training_set.sql puts every streamed row in `train` and never
    in `test`.

    So moving a streamed row into the test side would silently hand both models
    rows one of them had learned, and nothing here would notice: the boundary
    read from split_rule would still say 2020-04-30. The two decisions are tied
    and the tie is only written down here.
    """
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


def judge(client, settings, candidate_mv, apply: bool, say=log.info) -> dict:
    """Resolve the two sides, decide, and -- with apply -- act and record.

    ONE ORCHESTRATION, TWO CALLERS: tools/promote.py for the hand, dag_train's
    challenge_and_promote for the loop. The dry run and the applied run differ
    by exactly the last block, so what the operator reads before acting is what
    the DAG does when it acts.

    say IS A PARAMETER because the two callers publish differently: the command
    prints to a terminal a person is reading, the task writes to a log a person
    will read later. Passing the sink keeps one set of sentences.

    ACT, THEN RECORD -- the alias moves before fd_promotions is written, the
    same ordering sql/ddl/100_ops_state.sql argues for. A crash between the two
    leaves a promotion that happened and was not journalled, which a reader can
    detect; the reverse leaves a journal claiming something that did not.

    Block: B4.
    Lot: B4-L4.
    Competency: B4-C5.
    Covers: B4-EC34, B4-EC36, B4-EC37.
    """
    from ml4fd import db, paired, promote, registry

    champion_mv = registry.champion(client, alias=settings.champion_alias)
    if champion_mv is not None and \
            str(champion_mv.version) == str(candidate_mv.version):
        raise AlreadyChampion(
            "la version %s porte deja l'alias %s : rien a decider."
            % (candidate_mv.version, settings.champion_alias))

    candidate_payload = payload_for(client, settings, candidate_mv)
    candidate = side_of(candidate_payload, candidate_mv.version)
    say("candidat : version %s, recette %s, rappel %.3f, %.1f alertes/jour"
        % (candidate.version, candidate_payload["dataset"]["version"],
           candidate.recall, candidate.alerts_per_day))

    champion = None
    table = None
    if champion_mv is None:
        say("aucun champion en service.")
    else:
        champion_payload = payload_for(client, settings, champion_mv)
        champion = side_of(champion_payload, champion_mv.version)
        say("champion : version %s, recette %s, rappel %.3f"
            % (champion.version, champion_payload["dataset"]["version"],
               champion.recall))

        frame = shared_window(settings, candidate_payload, champion_payload)
        frauds = int(labels_of(frame).sum())
        say("fenetre partagee : %d lignes, %d fraudes -- posterieures au %s, "
            "fin d'entrainement du champion"
            % (len(frame), frauds, training_boundary(champion_payload)))
        if frauds < MIN_SHARED_FRAUDS:
            raise Undecidable(
                "refus de decider : %d fraudes, sous le plancher de %d. Le "
                "champion reste en service et rien n'est enregistre -- la "
                "mesure etait impossible, ce qui n'est pas un verdict sur le "
                "modele." % (frauds, MIN_SHARED_FRAUDS))
        table = comparison(settings, candidate_payload, champion_payload, frame)
        say("\n" + paired.render(table, subject="fraudes"))

    decision = promote.decide(
        candidate, champion, table, promote.Policy.from_settings(settings))
    say(decision.reason)

    outcome = {"candidate": str(candidate_mv.version),
               "champion": None if champion is None else str(champion.version),
               "promoted": bool(decision.promoted),
               "reason": decision.reason,
               "applied": bool(apply)}
    if not apply:
        return outcome

    if decision.promoted:
        registry.set_champion(client, candidate_mv.version,
                              alias=settings.champion_alias)
    with db.connect("fd_ingest", settings) as conn:
        promote.record(conn, decision, run_id=candidate_mv.run_id)
        conn.commit()
    say("decision enregistree dans fd_promotions.")
    return outcome
