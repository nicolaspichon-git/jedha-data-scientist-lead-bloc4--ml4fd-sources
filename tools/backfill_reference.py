"""Copy what M5 measured into fd_datasets, for recipes fitted before R10.

Covers: EE04, EC23
Lot: R10
Competency: C4

WHY THIS EXISTS. Since S6/R10, dag_train.fit_model writes the five figures of
the test set into the dataset's row of fd_datasets right after run.json. The
recipes fitted before that carry them only in run.json, on disk. This reads
each of those manifests ONCE and copies the figures, through the same writer
as the DAG (registry.record_reference): one rule for what is copied.

ONLY THE ROWS WITHOUT FIGURES, unless --all: a refit already wrote its own.
A manifest that cannot be read, or that names another recipe than the row it
sits beside, is reported and skipped -- the others are still copied -- and the
exit code says that something was skipped.

Usage, from the scheduler container:
    python3 /opt/project/tools/backfill_reference.py [--dry-run] [--all]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("backfill_reference")

PENDING = ("SELECT recipe_digest, version FROM fd_datasets "
           "WHERE test_recall IS NULL ORDER BY created_at")
EVERY = "SELECT recipe_digest, version FROM fd_datasets ORDER BY created_at"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Copy the M5 figures of each run.json into fd_datasets.")
    parser.add_argument("--dry-run", action="store_true",
                        help="read the manifests, write nothing")
    parser.add_argument("--all", action="store_true",
                        help="rewrite the rows that already carry figures too")
    args = parser.parse_args(argv)

    sys.path.insert(0, "/opt/project")
    from ml4fd import config, db, registry, score

    settings = config.load()
    copied, skipped = 0, 0
    with db.connect("fd_ingest", settings) as conn:
        with conn.cursor() as cur:
            cur.execute(EVERY if args.all else PENDING)
            rows = cur.fetchall()
        if not rows:
            log.info("aucune recette sans reference : rien a copier")
            return 0
        for digest, version in rows:
            path = score.manifest_path(settings, version)
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                figures = registry.reference_of(payload)
            except (OSError, ValueError, KeyError) as problem:
                log.warning("recette %s : manifeste illisible (%s) -- ignoree",
                            version, problem)
                skipped += 1
                continue
            if figures["recipe_digest"] != digest:
                log.warning("recette %s : le manifeste nomme la recette %s -- "
                            "ignoree", version, figures["recipe_digest"][:12])
                skipped += 1
                continue
            if args.dry_run:
                log.info("recette %s : rappel %.4f, precision %.4f (non ecrit)",
                         version, figures["recall"], figures["precision"])
            else:
                registry.record_reference(conn, payload)
                log.info("recette %s : rappel %.4f, precision %.4f copies",
                         version, figures["recall"], figures["precision"])
            copied += 1
    log.info("%d recette(s) %s, %d ignoree(s)", copied,
             "lues" if args.dry_run else "copiees", skipped)
    return 1 if skipped else 0


if __name__ == "__main__":
    raise SystemExit(main())
