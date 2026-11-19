"""One morning report, produced by hand.

Covers: EE04, EI04
Lot: P2
Competency: C2, C5

WHY THIS EXISTS RATHER THAN A DAG. The DAG is lot P4. What P1 and P2 have to
show is that the four queries, the assembly and the rendering produce a document
an analyst can read -- which is about content, not orchestration. This runner
exercises exactly that chain, and P4 will wrap the same functions without
rewriting any of them. The same reasoning as tools/ingest_once.py in N2.

THE WINDOW IS BUILT FROM A DATE AND THE DECLARED ZONE, never from the clock: the
calendar day given on the command line, from midnight to midnight in
report.timezone, with the knowledge instant at the end of it unless one is
given. A replay of 3 November therefore produces the document of 3 November,
which is the criterion of P2 and the central trap of S4.

Usage, from the scheduler container:
    python3 /opt/project/tools/daily_report.py [--date 2026-11-16]
                                               [--knowledge-at 2026-11-17T06:00]
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
import zoneinfo

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("daily_report")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=None,
                        help="the calendar day to cover; yesterday by default")
    parser.add_argument("--knowledge-at", default=None,
                        help="instant of knowledge; the end of the day by default")
    parser.add_argument("--dry-run", action="store_true",
                        help="rendre le document et l'afficher SANS rien "
                             "ecrire ni enregistrer")
    args = parser.parse_args()

    from ml4fd import config, db, report

    settings = config.load()
    zone = zoneinfo.ZoneInfo(settings.report_timezone)

    if args.date:
        day = dt.date.fromisoformat(args.date)
    else:
        day = dt.datetime.now(zone).date() - dt.timedelta(days=1)

    known = None                 # window_for() defaults to the end of the day
    if args.knowledge_at:
        known = dt.datetime.fromisoformat(args.knowledge_at)
        if known.tzinfo is None:
            known = known.replace(tzinfo=zone)

    window = report.window_for(settings, day, knowledge_at=known)

    # fd_report, deliberately: if the chain needed more than the exploited
    # layer, this is where it would fail, and that failure is the guarantee.
    with db.connect("fd_report") as conn:
        if args.dry_run:
            # NOTHING IS WRITTEN. The earlier version wrote the document and
            # skipped the table, which left fd_reports vouching for a file that
            # had been overwritten -- the acceptance of S4 caught it.
            text = report.preview(conn, settings, window)
            print(text)
            log.info("")
            log.info("--dry-run : rien n'est ecrit sur le disque, rien n'est "
                     "enregistre. Le document ci-dessus est celui que la "
                     "production ecrirait, a l'octet pres.")
            log.info("il serait ecrit dans %s",
                     report.path_for(settings, window.day))
            return 0
        recorded = report.produce(conn, settings, window)

    log.info("")
    for key in ("report_date", "knowledge_date", "n_payments", "n_alerts",
                "n_verdicts", "n_pending", "model_version", "threshold"):
        log.info("  %-16s %s", key, recorded[key])
    log.info("  %-16s %s", "content_digest", recorded["content_digest"][:16] + "...")
    log.info("")
    log.info("document : %s", recorded["path"])
    log.info("")
    log.info("enregistre dans fd_reports, APRES le document : enregistrer "
             "d'abord laisserait la table decrire un document que personne "
             "n'a ecrit, et le controle d'absence annoncerait un matin sain.")
    log.info("")
    log.info("les matins manquants :  fd-manager check  (s4_absence.sql)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
