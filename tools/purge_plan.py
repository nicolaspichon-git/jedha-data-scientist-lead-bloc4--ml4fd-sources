"""The dry run of the purge, by hand. Nothing is deleted.

Covers: EC26, EI13
Lot: Q2
Competency: C5

WHY A RUNNER OF ITS OWN. S5 is the only destructive process of the project, and
at 90 days of retention with three batches loaded yesterday the real purge
would delete nothing before February 2027. A destructive process that cannot be
tried is one that is discovered in production -- so this shows what would go,
on today's boundary or on a chosen one, and shows the refusals and the guard.

--before OVERRIDES THE BOUNDARY, for every store at once. It is a tool of
demonstration and of trial, not a parameter of production: the DAG of Q5 does
not expose it, because a parameter of a demonstration left in an orchestrator
ends up being used.

--verify RECOMPUTES EACH FINGERPRINT instead of only reading that one was
recorded. It costs an md5 over every row of each candidate batch -- about 555
thousand rows and 200 MB for a historical one -- so it is not the default here.
The purge itself always does it, batch by batch, immediately before deleting.

Usage, from the scheduler container:
    python3 /opt/project/tools/purge_plan.py [--before 2026-11-01] [--verify]
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import zoneinfo

logging.basicConfig(level=logging.WARNING, format="%(levelname)-7s %(message)s")
log = logging.getLogger("purge_plan")


def _instant(value: str, zone) -> dt.datetime:
    """A date or a timestamp on the command line, made aware in the declared
    zone. Naive would be refused downstream, and rightly: a boundary read in
    the wrong zone deletes the wrong day."""
    try:
        moment = dt.datetime.fromisoformat(value)
    except ValueError:
        raise SystemExit(
            "--before attend une date ISO (2026-11-01) ou un instant "
            "(2026-11-01T02:00), lu %r" % value)
    return moment if moment.tzinfo else moment.replace(tzinfo=zone)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", default=None,
                        help="purger jusqu'a cette date, pour tous les "
                             "magasins (demonstration et essai)")
    parser.add_argument("--verify", action="store_true",
                        help="recalculer les empreintes au lieu de verifier "
                             "qu'elles ont ete relevees")
    args = parser.parse_args(argv)

    from ml4fd import config, db, purge, runs

    settings = config.load()
    zone = zoneinfo.ZoneInfo(settings.report_timezone)
    before = _instant(args.before, zone) if args.before else None

    # UNDER THE PURGE'S OWN ROLE, read-only though this is: a dry run that
    # reads as fd_ingest would not notice that fd_purge lacks a privilege the
    # real purge needs, and would promise a purge that cannot happen.
    try:
        with db.connect(purge.ROLE) as conn:
            report = purge.plan(conn, settings, as_of=dt.datetime.now(zone),
                                before=before, verify=args.verify)
    except (db.RoleMissing, runs.MigrationMissing) as missing:
        # A remedy, not a traceback: the operator reads one command.
        print(missing)
        return 1

    print(purge.render(report))
    print()
    if report["rows"]:
        print("Pour supprimer : .\\fd-manager.ps1 purge -Apply  (lot Q3)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
