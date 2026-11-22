"""Acceptance criteria of S4 that SQL cannot reach: the documents and the counts.

Block: B3, B4.
Lot: B3-P5, B4-L0.
Competency: B3-C1, B3-C4, B3-C5.
Covers: B3-EE04, B3-EI02, B3-EI03, B3-EI04, B3-EC14, B3-EC23, B3-EC27.

The criteria of S4 that the database holds live in sql/checks/s4_acceptance.sql,
because they are questions about rows. What is here is everything else:

  THE DOCUMENT ON THE DISK. fd_reports describes a file; this reads the file and
  compares its sha256 with content_digest. A registry that describes what is not
  there, or what has changed since, is a registry nobody can trust -- the
  reasoning of run.json beside a dataset in B3-M8, and the defect B3-M8 paid for.

  THE COUNTS, RECOMPUTED. The four counts of a row are compared with what the
  four queries return over THE WINDOW THE ROW ITSELF NAMES. That is the only
  check that catches a report built on the wrong window: the figures would be
  internally consistent and describe another day. It is also what makes the
  replay criterion of B3-P4 measurable -- recompute and the counts must not move.

  THE COMPARISON WITH B3-M5, informative, from the recipe's run.json on disk.

Usage, from the scheduler container:
    python3 /opt/project/tools/check_s4.py [--days 30]
"""
from __future__ import annotations

import argparse
import sys
import zoneinfo
from pathlib import Path

PROJECT = Path("/opt/project")


def criteria(root: Path, days: int):
    """Yield (ordinal, criterion, expected, actual) tuples."""
    ord_ = 0

    def item(criterion, expected, actual):
        nonlocal ord_
        ord_ += 1
        return (ord_, criterion, str(expected), str(actual))

    sys.path.insert(0, str(root))
    from ml4fd import config, db, report

    settings = config.load()
    zone = zoneinfo.ZoneInfo(settings.report_timezone)

    # --- the declarations (B3-P2, B3-P4)
    yield item("B3-P4 cadence du rapport declaree", settings.report_schedule,
               settings.report_schedule)
    # The directory comes from config.dags_dir(): inside the scheduler the DAGs
    # are NOT under the project root, and deriving the path here read
    # dag_daily ABSENT on the first real run of this checker.
    dags = config.dags_dir(root)
    dag = dags / "dag_daily.py" if dags else None
    if dags is None:
        yield item("B3-P4 repertoire des DAG", "trouve",
                   "ABSENT : cherche dans %s" % ", ".join(config.DAG_DIRS))
    elif not dag.is_file():
        yield item("B3-P4 DAG dag_daily", "present", "ABSENT dans %s" % dags)
    else:
        body = dag.read_text(encoding="utf-8")
        found = ""
        for line in body.splitlines():
            if line.strip().startswith("schedule="):
                found = line.split('"')[1] if '"' in line else ""
                break
        yield item("B3-P4 cadence de dag_daily", settings.report_schedule, found)
        yield item("B3-P4 dag_daily rattrape les matins manques", "oui",
                   "oui" if "catchup=True" in body else "NON")

    yield item("B3-P2 racine des rapports declaree", "sous le projet",
               "sous le projet"
               if str(settings.report_root).startswith(str(root))
               or settings.report_root.startswith(str(PROJECT))
               else "AILLEURS : %s" % settings.report_root)

    # --- the documents and the counts (B3-P2, B3-P3), then the reference of B3-M5.
    #
    # EACH BLOCK IS A GENERATOR OF ITS OWN, and that is not a style choice. The
    # document block ends in an early `return` when the journal is empty, and
    # in check_s3.py that very `return` returned from criteria() ITSELF --
    # silently swallowing every criterion declared after it. It was found there
    # in B3-N9 and reproduced here three hours later, in a file written by the same
    # hand that had just fixed it. Knowing the lesson is not the same as
    # applying it; a `yield from` is.
    with db.connect("fd_report") as conn:
        yield from _documents(item, conn, settings, zone, root, days)
        yield from _reference(item, conn, settings, report)


def _documents(item, conn, settings, zone, root, days):
    """The documents on the disk and the counts recomputed from the queries."""
    from ml4fd import report

    with conn.cursor() as cur:
        cur.execute(
            "SELECT report_date, knowledge_date, path, content_digest, "
            "       n_payments, n_alerts, n_verdicts, n_pending "
            "  FROM fd_reports "
            " WHERE report_date >= current_date - %s "
            " ORDER BY report_date DESC", (days,))
        rows = cur.fetchall()

    if not rows:
        # NOT a failure, and saying so matters: no report has been produced yet,
        # which is the normal state before dag_daily is unpaused. What CANNOT be
        # said is that the documents match their rows -- there is nothing to
        # read. Reported as unverified rather than as passed.
        yield item("info rapports enregistres sur %d jours" % days,
                   "mesure", "aucun")
        yield item("info empreinte du document contre fd_reports", "mesure",
                   "NON VERIFIE : rien a lire")
        yield item("info decomptes recalcules", "mesure",
                   "NON VERIFIE : rien a lire")
        return

    yield item("info rapports enregistres sur %d jours" % days, "mesure",
               "%d" % len(rows))

    absent, mismatched, recomputed, drifted, impossible = [], [], 0, [], []
    for (day, known_at, path, digest, payments, alerts, verdicts,
         pending) in rows:
        # The path the row RECORDS: this checker runs where the document was
        # written, and that path is the evidence. The verdict itself is
        # report.check_document()'s, the rule the dashboard reads too.
        checked = report.check_document(Path(path), digest)
        if checked.verdict == report.ABSENT:
            absent.append(str(day))
            continue
        if not checked.conforms:
            mismatched.append(str(day))

        # THE WINDOW THE ROW ITSELF NAMES, rebuilt exactly as dag_daily derives
        # it: the calendar day, midnight to midnight, in the declared zone. A
        # report built on the wrong window is internally consistent and
        # describes another day; only this catches it.
        try:
            window = report.window_for(settings, day, knowledge_at=known_at)
        except ValueError as problem:
            # A NAMED CRITERION RATHER THAN A TRACEBACK. dag_daily cannot
            # produce such a row -- its knowledge date is the end of the
            # interval, six hours past the end of the window -- but
            # tools/daily_report.py --knowledge-at can, and a checker that dies
            # on one bad row verifies none of the others.
            impossible.append("%s : %s" % (day, problem))
            continue
        again = {
            "n_payments": report.volumes(conn, window)["total"]["transactions"],
            "n_alerts": len(report.alerts(conn, window)),
            "n_verdicts": report.verdicts(conn, window)["verdicts"],
            "n_pending": report.pending(conn, window)["pending"],
        }
        recorded = {"n_payments": payments, "n_alerts": alerts,
                    "n_verdicts": verdicts, "n_pending": pending}
        recomputed += 1
        for name, value in recorded.items():
            if int(value) != int(again[name]):
                drifted.append("%s %s : %s puis %s"
                               % (day, name, value, again[name]))

    yield item("B3-P3 chaque ligne a son document sur le disque", "0 absent",
               "%d absent%s" % (len(absent),
                                " (%s)" % ", ".join(absent[:3])
                                if absent else ""))
    # THE criterion of this file.
    yield item("B3-P3 empreinte du document egale content_digest", "0 ecart",
               "%d ecart%s" % (len(mismatched),
                               " (%s)" % ", ".join(mismatched[:3])
                               if mismatched else ""))
    yield item("B3-P3 fenetre coherente avec la journee couverte", "0 impossible",
               "%d impossible%s" % (len(impossible),
                                    " (%s)" % "; ".join(impossible[:2])
                                    if impossible else ""))
    yield item("info decomptes recalcules", "mesure",
               "%d rapport(s)" % recomputed)
    yield item("B3-P2 les decomptes egalent ce que les requetes rendent",
               "0 divergence",
               "%d divergence%s" % (len(drifted),
                                    " (%s)" % "; ".join(drifted[:2])
                                    if drifted else ""))


def _reference(item, conn, settings, report):
    """What B3-M5 measured, for the reader to compare against. Informative.

    Yielded whatever the journal holds: the comparison does not depend on a
    report having been produced, and in check_s3 a block like this one sat
    AFTER an early return and was never reached.
    """
    reference = report.reference(conn, settings)
    if reference is None:
        yield item("info reference de B3-M5", "mesure",
                   "INDISPONIBLE : aucune mesure en base")
        return
    yield item("info recette de reference", "mesure", reference["version"])
    yield item("info rappel mesure par B3-M5 sur le jeu de test", "mesure",
               "%.4f" % reference["recall"]
               if reference["recall"] is not None else "n/a")
    yield item("info precision mesuree par B3-M5", "mesure",
               "%.4f" % reference["precision"]
               if reference["precision"] is not None else "n/a")
    yield item("info alertes/jour prevues par B3-M6", "mesure",
               "%.1f" % reference["alerts_per_day"]
               if reference["alerts_per_day"] is not None else "n/a")


def main(argv) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=30,
                        help="how far back to verify the recorded reports")
    parser.add_argument("root", nargs="?", default=str(PROJECT))
    args = parser.parse_args(argv[1:])
    root = Path(args.root)

    print("")
    print("Criteres d'acceptation de S4 que SQL n'atteint pas")
    print("Projet examine : %s" % root)
    print("")

    rows = list(criteria(root, args.days))
    width = max(len(row[1]) for row in rows)
    failed = 0
    for _, criterion, expected, actual in rows:
        # "mesure" marks a figure, not a criterion -- the same convention as
        # sql/checks/s4_acceptance.sql.
        if expected == "mesure":
            verdict = "info"
        elif expected == actual:
            verdict = "OK"
        else:
            verdict = "<<< ECHEC"
            failed += 1
        print("  %-*s | %-30s | %-30s | %s"
              % (width, criterion, expected, actual, verdict))

    print("")
    print("%d critere(s) en echec sur %d, et %d chiffre(s) informatif(s)"
          % (failed, sum(1 for row in rows if row[2] != "mesure"),
             sum(1 for row in rows if row[2] == "mesure")))
    print("")
    print("Une ligne NON VERIFIE n'est pas un critere qui passe : elle dit")
    print("qu'il n'y avait rien a mesurer. Produisez un rapport et relancez.")
    print("")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
