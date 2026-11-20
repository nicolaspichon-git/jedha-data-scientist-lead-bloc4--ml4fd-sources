"""Judge the newest registered model against the champion, and move the alias.

Covers: EC34, EC36, EC37   Lot: L3   Competency: C5

REGISTERING IS NOT PROMOTING. Every run of S2 registers a version; this
command and dag_train's challenge_and_promote are the two things that put one
in production.

THE WORK MOVED TO ml4fd/challenge.py ON 2026-11-20, and this file became its
command line. Until then nothing promoted automatically -- dag_train ended at
register_model -- so closing the loop meant the DAG had to reach this
orchestration too. tools/ holds scripts, loaded by path and never imported, so
the shared code could not stay here. The decision itself is still the pure
function of ml4fd/promote.py.

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
import sys

log = logging.getLogger("promote")

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

    from ml4fd import challenge, config, registry, score

    settings = config.load()
    uri = os.environ.get("MLFLOW_TRACKING_URI")
    if uri:
        mlflow.set_tracking_uri(uri)
    client = mlflow.MlflowClient()

    candidate_mv = (client.get_model_version(registry.REGISTERED_NAME,
                                             str(args.candidate))
                    if args.candidate else score.latest_version(client))

    try:
        challenge.judge(client, settings, candidate_mv,
                        apply=args.apply, say=print)
    except challenge.AlreadyChampion as nothing_to_do:
        print(nothing_to_do)
        return 0
    except challenge.Undecidable as unmeasurable:
        # EXIT 1 AND NOT 0, as before the move: an operator who scripted this
        # command reads the status, and "I could not measure" must not look
        # like "I measured and it passed".
        print(unmeasurable)
        return 1

    if not args.apply:
        print("\n(essai a blanc : ni alias deplace, ni decision enregistree. "
              "Relancer avec --apply.)")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sys.exit(main())
