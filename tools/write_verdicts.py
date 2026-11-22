"""Write the withheld verdicts by hand, outside any scoring.

Block: B3, B4.
Lot: B3-N6, B4-L0.
Competency: B3-C3, B3-C4.
Covers: B3-EE03, B3-EI03, B3-EI12, B3-EC23.

The companion of dag_ingest_verdicts, for the same reason tools/ingest_once.py is
the companion of the real-time DAG: the acceptance criterion of this lot has to
be demonstrable on screen without waiting for 02:00.

IT SHARES NOTHING WITH THE SCORING, and that is the point of the lot rather than
a side effect: it imports neither ml4fd.score nor ml4fd.notify, it starts from
the raw zone, and a test reads its imports to check that rather than trusting
this paragraph.

Usage, from the scheduler container:
    python3 /opt/project/tools/write_verdicts.py [--dry-run]
"""
from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")
log = logging.getLogger("write_verdicts")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Write the withheld verdicts of the stream, and report the "
                    "blind band.",
        epilog="The verdict is real and is written at once with "
               "label_ts = txn_ts + delay; what holds it back is that every "
               "consumer filters on label_ts <= cut_off.")
    parser.add_argument("--dry-run", action="store_true",
                        help="count what is missing without writing")
    args = parser.parse_args(argv)

    sys.path.insert(0, "/opt/project")

    from ml4fd import config, db, stream, verdicts

    settings = config.load()
    delay = settings.verdict_delay()
    log.info("delai declare : %s (mode %s)", delay, settings.verdict_mode)

    with db.connect("fd_ingest") as conn:
        awaiting = verdicts.batches_awaiting(conn, stream.SOURCE)
        for batch_id, transactions, without, earliest, latest in awaiting:
            log.info("  lot %s : %d transaction(s) dont %d sans verdict, "
                     "%s a %s", batch_id, transactions, without, earliest,
                     latest)
        if not awaiting:
            log.info("  aucun lot en attente")

        if args.dry_run:
            band = verdicts.withheld(conn, stream.SOURCE)
            log.info("--dry-run : rien ecrit. Bande aveugle : %d non echus sur "
                     "%d", band["not_yet_due"], band["verdicts"])
            return 0

        report = verdicts.write_due(conn, delay, stream.SOURCE)
        conn.commit()
        band = verdicts.withheld(conn, stream.SOURCE)

    log.info("")
    log.info("%s", verdicts.render(report, band, delay))
    if band["distinct_delays"] > 1:
        log.warning("%d delais distincts (%s a %s) : le delai a change entre "
                    "deux passages", band["distinct_delays"],
                    band["min_delay"], band["max_delay"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
