"""Forget one named batch. ROWS LEAVE THE DATABASE HERE.

Covers: EC18, EC19, EC23, EC26
Lot: Q7
Competency: C2, C3

It plans, prints the plan, refuses on any of the plan's grounds, then deletes
in committed slices and journals each store in fd_purges -- the purge's own
machinery (purge_batch, the fingerprint check, the resumption marker), under
the only role that may destroy.

--force is the confirmation. Without it nothing is deleted, and the exit code
says why rather than the output alone.

TWO FLAGS AND NOT ONE, because they answer two different questions.
--force says "yes, delete". --even-if-successful answers a different one: a
batch whose run succeeded while owning no transaction and no verdict is a
duplicate load, and its success means only that every task passed. The refusals
that rest on OWNED DATA are lifted by neither.

Usage, from the scheduler container:
    python3 /opt/project/tools/forget_run.py <batch_id> --force
                                             [--even-if-successful]
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import uuid
import zoneinfo

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("forget_run")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch_id", help="le lot a oublier")
    parser.add_argument("--force", action="store_true",
                        help="confirmation explicite : supprimer les lignes")
    parser.add_argument("--even-if-successful", action="store_true",
                        dest="even_if_successful",
                        help="lever la precaution d'une execution reussie, "
                             "quand le lot ne possede ni transaction ni verdict")
    args = parser.parse_args(argv)
    try:
        batch_id = str(uuid.UUID(args.batch_id))
    except ValueError:
        print("identifiant de lot attendu (uuid), lu %r." % args.batch_id)
        print("  Les lister :  .\\fd-manager.ps1 batches")
        return 1

    from ml4fd import config, db, purge, runs

    settings = config.load()
    now = dt.datetime.now(zoneinfo.ZoneInfo(settings.report_timezone))

    try:
        # TWO CONNECTIONS, TWO ROLES, and that is the property rather than a
        # nuisance: the role that decides cannot destroy, and the role that
        # destroys cannot see what the decision rests on (it holds no grant on
        # fd_transactions, which S5's acceptance reads from the catalogue).
        # forget() reads the runs again under fd_purge before deleting, so the
        # window between the two is closed where it can be.
        with db.connect("fd_ingest", settings) as reader:
            runs.assert_digests(reader)
            plan = purge.forget_plan(reader, batch_id)
        print(purge.render_forget(plan))
        print()
        # The data refusals answer to nobody; --even-if-successful lifts the
        # caution alone.
        purge.assert_forgettable(plan, force=args.even_if_successful)
        if not args.force:
            print("Rien n'a ete supprime : --force est la confirmation.")
            return 2
        with db.connect(purge.ROLE, settings) as conn:
            summary = purge.forget(conn, plan, now, confirm=True,
                                   force=args.even_if_successful)
    except (db.RoleMissing, runs.MigrationMissing) as missing:
        print(missing)
        return 1
    except purge.ForgetRefused as refused:
        print("REFUSE : %s" % refused)
        return 2
    except purge.PurgeRefused as refused:
        print("REFUSEE : %s" % refused)
        return 2

    for outcome in summary["outcomes"]:
        print("  %-14s %d ligne(s) supprimee(s) sur %d attendue(s)%s"
              % (outcome["store"], outcome["deleted"], outcome["expected"],
                 "  REFUSE : %s" % outcome["why"] if outcome.get("refused")
                 else ""))
    print()
    print("Lot %s : %d ligne(s) supprimee(s) au total."
          % (summary["batch_id"], summary["deleted"]))
    return 3 if summary["refused"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
