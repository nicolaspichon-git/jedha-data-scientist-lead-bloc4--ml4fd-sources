"""Recompute every stored feature, then prove the reconciliation passes.

Covers: EE07
Lot: N3
Competency: C5

WHY THIS EXISTS. features.backfill() lived inside dag_load_history, which also
reloads 555,719 CSV rows. A stream batch that escaped features.apply_batch --
the four transactions ingested before N3 existed, plus the three of the
-Corrupt run -- needs the backfill and nothing else. Running S1's DAG to repair
seven rows would reload the history to fix the symptom.

WHY IT IS NOT A DAG TASK. It repairs; it is not part of the normal course. If
it is ever needed twice, something upstream is dropping batches and the fix
belongs there -- apply_batch already raises with this script named as the
remedy, precisely so a silent repair never becomes the routine.

THE ORDER MATTERS. backfill, then coverage, then reconcile: the backfill is the
work, coverage says the sample is not vacuous, and the reconciliation is the
verdict. Reporting before the verdict would announce a repair that may not have
held.

Usage, from the scheduler container:
    python3 /opt/project/tools/backfill_features.py [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("backfill_features")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Recompute the stored features and reconcile the two "
                    "computation modes.",
        epilog="--dry-run only counts the rows that lack features, and "
               "changes nothing.")
    parser.add_argument("--dry-run", action="store_true",
                        help="count what is missing without writing")
    args = parser.parse_args(argv)

    sys.path.insert(0, "/opt/project")

    from ml4fd import db, features

    with db.connect("fd_ingest") as conn:
        before = features.coverage(conn)
        log.info("avant : %s", before)

        if not before["total_rows"]:
            log.error("fd_transactions est vide : rien a recalculer")
            return 1

        if args.dry_run:
            log.info("--dry-run : %d ligne(s) sans features stockees",
                     before["rows_without_features"])
            return 0

        # The backfill rewrites EVERY row, not only the ones that are missing.
        # That is deliberate: a window function over the whole table is the one
        # computation mode the reconciliation trusts (ruling 2), and a partial
        # recompute would reintroduce the per-row mode it is meant to confront.
        rows = features.backfill(conn)
        log.info("recalcul sur %d ligne(s)", rows)

        after = features.coverage(conn)
        log.info("apres : %s", after)
        if after["rows_without_features"]:
            log.error("%d ligne(s) restent sans features : le recalcul n'a pas "
                      "tenu, la reconciliation echouera",
                      after["rows_without_features"])
            return 1
        if not after["tie_rows"]:
            log.warning("aucune egalite d'horodatage : la reconciliation ne "
                        "peut pas distinguer ROWS de RANGE ... EXCLUDE GROUP, "
                        "la decision 2 reste non prouvee par ce passage")

        divergences = features.reconcile(conn)
        if divergences:
            log.error("RECONCILIATION EN ECHEC apres recalcul : %d "
                      "divergence(s)", len(divergences))
            for row in divergences[:5]:
                log.error("  %s", row)
            return 1

        conn.commit()

    log.info("")
    log.info("features recalculees, les deux modes s'accordent sur les quatre "
             "strates")
    return 0


if __name__ == "__main__":
    sys.exit(main())
