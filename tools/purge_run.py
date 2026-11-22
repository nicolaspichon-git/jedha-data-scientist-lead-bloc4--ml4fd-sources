"""Carry out the purge. ROWS LEAVE THE DATABASE HERE.

Block: B3, B4.
Lot: B3-Q3, B4-L0.
Competency: B3-C2, B3-C3.
Covers: B3-EE05, B3-EI11, B3-EC19, B3-EC26.

A RUNNER OF ITS OWN, and not a --apply of the dry run. A destructive command
that is one flag away from a harmless one gets typed by mistake eventually;
the dry run (tools/purge_plan.py) has no way at all to delete, and this file
does nothing else.

WHAT IT DOES, in order: it plans, it prints the plan, it verifies each batch's
fingerprint again, it deletes in committed slices, it prints what went. The
plan it prints is the plan it executes -- the same object, in the same
connection -- so there is no window in which the two could describe different
sets.

THE GUARD STOPS IT. Beyond the declared share (purge.max_share) nothing is
deleted and the run ends with the figures and a refusal. --force is the
explicit confirmation that governance asks for, and it is the only thing that
grants it.

Usage, from the scheduler container:
    python3 /opt/project/tools/purge_run.py [--before 2026-11-01]
                                            [--verify] [--force]
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import zoneinfo

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("purge_run")


def _instant(value: str, zone) -> dt.datetime:
    try:
        moment = dt.datetime.fromisoformat(value)
    except ValueError:
        raise SystemExit(
            "--before attend une date ISO (2026-11-01) ou un instant "
            "(2026-11-01T04:00), lu %r" % value)
    return moment if moment.tzinfo else moment.replace(tzinfo=zone)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", default=None,
                        help="purger jusqu'a cette date, pour tous les "
                             "magasins (demonstration et essai)")
    parser.add_argument("--verify", action="store_true",
                        help="recalculer les empreintes des le mode a blanc ; "
                             "la purge le fait de toute facon, lot par lot")
    parser.add_argument("--force", action="store_true",
                        help="confirmation explicite : purger au-dela de la "
                             "part declaree")
    args = parser.parse_args(argv)

    from ml4fd import config, db, purge, runs

    settings = config.load()
    zone = zoneinfo.ZoneInfo(settings.report_timezone)
    before = _instant(args.before, zone) if args.before else None
    now = dt.datetime.now(zone)

    try:
        # Under the role that may destroy, and under no other: the raw zone is
        # append-only for everybody else (sql/ddl/120_purge_role.sql).
        with db.connect(purge.ROLE) as conn:
            report = purge.plan(conn, settings, as_of=now, before=before,
                                verify=args.verify)
            print(purge.render(report))
            print()

            if report["blocked"] and not args.force:
                purge.assert_allowed(report)        # raises, nothing deleted

            if not report["rows"]:
                print("Rien a supprimer.")
                return 0

            # run_id left at None: journalled as a purge run by hand.
            summary = purge.execute(conn, settings, report, now,
                                    confirm=args.force)
    except (db.RoleMissing, runs.MigrationMissing) as missing:
        print(missing)
        return 1
    except purge.PurgeRefused as refused:
        print("REFUSEE : %s" % refused)
        print()
        print("Rien n'a ete supprime. Pour passer outre en connaissance de "
              "cause :")
        print("  .\\fd-manager.ps1 purge -Apply -Force")
        return 2

    print(purge.render_execution(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
