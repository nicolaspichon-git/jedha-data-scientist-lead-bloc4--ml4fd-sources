"""The four pipeline controls, run by hand, with or without notifying.

Block: B3, B4.
Lot: B3-N9, B4-L0.
Competency: B3-C4, B3-C5.
Covers: B3-EC14.

WHY THIS EXISTS BESIDE THE DAG TASK. The DAG runs the controls every minute and
notifies on a CHANGE of state, which is the right behaviour for a channel and
the wrong one for a human who wants to know where things stand right now. This
runner prints all four verdicts whatever their state, and notifies nothing
unless asked -- so an operator can look without consuming the transition that
would have told them later.

--notify sends the transitions and records them, exactly as the DAG does. Useful
to prove the channel works, and the one way to find out that a declared file is
in a directory nobody mounted before an incident does it for you.

Usage, from the scheduler container:
    python3 /opt/project/tools/ops_check.py [--dag dag_ingest]
                                            [--window 60] [--notify]
"""
from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("ops_check")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dag", default="dag_ingest",
                        help="the DAG whose runs are judged")
    parser.add_argument("--window", type=int, default=None,
                        help="window in minutes; the declared one by default")
    parser.add_argument("--notify", action="store_true",
                        help="send and record the transitions, as the DAG does")
    args = parser.parse_args()

    from ml4fd import config, db, health

    settings = config.load()
    window = args.window or settings.ops_window_minutes

    with db.connect("fd_ingest") as conn:
        start, end = health.rolling_window(conn, window)
        facts = health.facts_of(conn, args.dag, start, end)
        verdicts = health.controls_for(facts, settings)
        stored = health.stored_state(conn)
        changed = health.transitions(verdicts, stored)

        log.info("DAG %s, fenetre de %d min", args.dag, window)
        log.info("")
        for name, value in sorted(facts.as_dict().items()):
            log.info("  %-28s %s", name, value)
        log.info("")
        for verdict in verdicts:
            mark = "ALERTE" if verdict.alert else "ok    "
            log.info("  [%s] %-18s %s", mark, verdict.control, verdict.detail)
        log.info("")

        if not changed:
            log.info("aucun changement d'etat : rien a notifier")
            log.info("etat enregistre : %s",
                     stored or "vide (tous les controles comptent pour ok)")
            # An alert already announced is NOT a reason to exit non-zero: this
            # tool reports, and the channel has already done the telling.
            return 0

        log.info("%d changement(s) d'etat :", len(changed))
        for change in changed:
            log.info("  %s -> %s : %s", change.control, change.state,
                     change.detail)

        if not args.notify:
            log.info("")
            log.info("rien n'a ete envoye ni enregistre (ajouter --notify). "
                     "La prochaine execution du DAG transmettra ces "
                     "changements : les regarder ici ne les consomme pas.")
            return 0

        report = {"dag_id": args.dag, "run_id": None,
                  "window_minutes": window, "facts": facts.as_dict(),
                  "controls": [v.as_dict() for v in verdicts],
                  "transitions": [c.as_dict() for c in changed],
                  "in_alert": sum(1 for v in verdicts if v.alert)}
        sent = health.notify_ops(conn, settings, report)

    log.info("")
    log.info("canal %s : %d envoye(s), %d en echec",
             sent["channel"], sent["sent"], sent["failed"])
    if sent["channel"] == "file":
        log.info("destination : %s", settings.ops_file)
    return 1 if sent["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
