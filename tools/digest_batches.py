"""Fingerprint the batches of the raw zone that have none.

Block: B3, B4.
Lot: B3-Q1, B4-L0.
Competency: B3-C4.
Covers: B3-EC23, B3-EI12.

WHY THIS EXISTS. The purge of S5 refuses a batch whose fingerprint was never
recorded: evidence is not destroyed before it has been taken. Every writer of
the raw zone now records one when it writes -- but the batches loaded before the
column existed have none, and left alone they would be unpurgeable for ever.
This takes theirs, while the raw zone still holds them.

A fingerprint taken now proves the batch is unchanged SINCE NOW, not since it
was written: it is recorded with origin="backfill" so an audit can tell the two
apart. That is the honest limit of a measure taken late, and the reason to run
this before the first purge rather than after.

LISTS by default and records only with --apply: looking at a journal must not
change it.

Usage, from the scheduler container:
    python3 /opt/project/tools/digest_batches.py [--apply]
"""
from __future__ import annotations

import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("digest_batches")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="record the fingerprints; the default only lists")
    args = parser.parse_args(argv)

    from ml4fd import db, runs

    with db.connect("fd_ingest") as conn:
        try:
            runs.assert_digests(conn)
        except runs.MigrationMissing as missing:
            print(missing)
            return 1
        pending = runs.undigested_batches(conn)
        if not pending:
            print("Chaque lot de la zone brute a son empreinte : rien a faire.")
            return 0

        print("%d lot(s) sans empreinte :" % len(pending))
        for batch in pending:
            print("  %s  %-24s %9d lignes  ingere le %s"
                  % (batch["batch_id"], batch["source"], batch["rows"],
                     str(batch["first_at"])[:19]))
        if not args.apply:
            print()
            print("Rien n'est ecrit. Pour relever ces empreintes :")
            print("  .\\fd-manager.ps1 digests -Apply")
            return 0

        for batch in pending:
            run_id = runs.backfill_batch(conn, batch["batch_id"])
            conn.commit()
            print("  empreinte relevee : lot %s -> execution %s"
                  % (batch["batch_id"], run_id))
        print("Origine \"backfill\" : l'empreinte atteste le lot tel qu'il est "
              "aujourd'hui, pas tel qu'il a ete ecrit.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
