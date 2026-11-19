"""Acceptance criteria of S5 that SQL cannot reach: the declaration, the DAG,
the files.

Covers: EC16, EC18, EC19, EC23, EC27
Lot: Q6
Competency: C1, C4, C5

The criteria of S5 that the database holds live in sql/checks/s5_acceptance.sql,
because they are questions about rows and about privileges. What is here is
everything else:

  THE DECLARED DURATIONS AGAINST THE APPLIED BOUNDARIES. governance.yml says
  ninety days; fd_purges records the cutoff each purge actually used. The two
  agreeing is the whole of EC18 -- "une duree non appliquee n'est pas une
  politique" -- and it is a question neither side can answer alone: the SQL
  cannot read the governance, and the governance cannot know what ran.

  THE DAG AND ITS CADENCE, read from the file: weekly, on a Sunday, no
  catch-up, and NO WAY TO OVERRIDE THE BOUNDARY. That last one is the criterion
  Q5 exists for -- a parameter of a demonstration left in an orchestrator ends
  up being used, and here that means deleting data that had not reached its
  retention.

  THE TRAINING SETS ON THE DISK. fd_datasets names two parquet files per
  version; this checks they are there. A registry describing files that are
  gone is the M8 defect, and the purge is the one process that could cause it.

  AND WHAT IS NOT PURGED AT ALL. The training sets under datasets/ are
  declared kept (retention.kept.datasets_directory): measured, they carry no
  identifying field. An earlier version of governance.yml declared "the five
  latest versions" among the PURGED stores and nothing deleted a file -- a rule
  of disk hygiene dressed as a retention rule, which is how a declared duration
  becomes a duration nobody applies. The decision was moved rather than
  implemented; the volume is still reported here.

Usage, from the scheduler container:
    python3 /opt/project/tools/check_s5.py
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PROJECT = Path("/opt/project")


def criteria(root: Path):
    """Yield (ordinal, criterion, expected, actual) tuples."""
    ord_ = 0

    def item(criterion, expected, actual):
        nonlocal ord_
        ord_ += 1
        return (ord_, criterion, str(expected), str(actual))

    sys.path.insert(0, str(root))
    from ml4fd import config, db, purge

    settings = config.load()

    yield from _declaration(item, settings)
    yield from _dag(item, root, settings)

    with db.connect(purge.ROLE) as conn:
        yield from _applied(item, conn, settings)
    # The dataset files are described by a table fd_purge may not read, so this
    # part connects as the reporting role. Two roles in one checker, named,
    # rather than one role granted more than it needs.
    with db.connect("fd_report") as conn:
        yield from _datasets(item, conn, settings, root)


def _declaration(item, settings):
    """Every store of the project carries a decision. Read from governance."""
    yield item("Q0 duree de la zone brute", "90 jours",
               "%d jours" % settings.raw_retention_days)
    yield item("Q0 duree de la quarantaine, egale a celle de la zone brute",
               "90 jours", "%d jours" % settings.quarantine_retention_days)
    yield item("Q0 duree des journaux d'Airflow, au plus celle de la zone brute",
               "oui",
               "oui" if settings.airflow_logs_retention_days
               <= settings.raw_retention_days else
               "NON : %d jours" % settings.airflow_logs_retention_days)
    yield item("Q2 part declaree du garde-fou", "0.5",
               "%s" % settings.purge_max_share)


def _dag(item, root: Path, settings):
    """The orchestration of Q5, read from the file."""
    from ml4fd import config

    dags = config.dags_dir(root)
    if dags is None:
        yield item("Q5 repertoire des DAG", "trouve",
                   "ABSENT : cherche dans %s" % ", ".join(config.DAG_DIRS))
        return
    yield item("info repertoire des DAG lu", "mesure", str(dags))

    path = dags / "dag_purge.py"
    if not path.is_file():
        yield item("Q5 DAG dag_purge", "present", "ABSENT dans %s" % dags)
        return
    body = path.read_text(encoding="utf-8")

    found = re.search(r'schedule="([^"]+)"', body)
    schedule = found.group(1) if found else "aucune"
    yield item("Q5 cadence de dag_purge contre la declaration",
               settings.purge_schedule, schedule)

    parts = schedule.split()
    yield item("Q5 cadence hebdomadaire, le dimanche", "oui",
               "oui" if len(parts) == 5 and parts[4] == "0"
               and parts[2] == "*" and parts[3] == "*" else "NON : " + schedule)

    yield item("Q5 dag_purge ne rattrape pas les dimanches manques", "oui",
               "oui" if "catchup=False" in body else "NON")

    # THE CRITERION Q5 EXISTS FOR. The source minus its docstrings, because the
    # module explains at length what it must never do, and a grep over the
    # prose would find the explanation.
    code = _code(body)
    yield item("Q5 aucune surcharge de la borne exposee par le DAG", "oui",
               "oui" if ("before" not in code and "params" not in code)
               else "NON")
    yield item("Q5 aucune horloge lue par le DAG", "oui",
               "oui" if not any(clock in code for clock in
                                ("now()", "utcnow()", "today()",
                                 "datetime.now")) else "NON")


def _code(body: str) -> str:
    """The source with comments and docstrings removed. The fifth time this
    project pays for the same mistake: a test grepping for a forbidden
    construct finds the paragraph forbidding it."""
    import ast
    tree = ast.parse(body)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                             ast.AsyncFunctionDef)):
            first = node.body[0] if node.body else None
            if (isinstance(first, ast.Expr)
                    and isinstance(first.value, ast.Constant)
                    and isinstance(first.value.value, str)):
                node.body.pop(0)
    return ast.unparse(tree)


def _applied(item, conn, settings):
    """The declared durations against the boundaries the purges used.

    NEITHER SIDE CAN ANSWER THIS ALONE: the SQL file cannot read governance.yml,
    and governance.yml cannot know what ran. This is the whole of EC18.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT store, max(ran_at), max(as_of), max(cutoff), "
            "       sum(n_deleted), sum(n_refused) "
            # RETENTION PURGES ONLY. A targeted forget (Q7) records its
            # batch's last row as cutoff: compared with ninety days it reads
            # "0 jours" and fails a purge that never happened.
            "FROM fd_purges WHERE batch_id IS NULL "
            "GROUP BY store ORDER BY store")
        rows = cur.fetchall()
        cur.execute("SELECT count(DISTINCT batch_id), coalesce(sum(n_deleted), 0) "
                    "FROM fd_purges WHERE batch_id IS NOT NULL")
        forgotten, forgotten_rows = cur.fetchone()

    if forgotten:
        yield item("info oublis cibles (forget), hors retention", "mesure",
                   "%d lot(s), %d ligne(s)" % (forgotten, forgotten_rows))

    if not rows:
        # NOT a criterion that passes: at ninety days of retention with batches
        # loaded this month, the first purge with anything to remove is months
        # away. Reported, so that nobody reads silence as compliance.
        yield item("info purges de retention enregistrees", "mesure",
                   "aucune : rien n'a encore atteint la duree declaree")
        return

    expected = {"fd_raw_events": settings.raw_retention_days,
                "fd_quarantine": settings.quarantine_retention_days}
    for store, ran_at, as_of, cutoff, deleted, refused in rows:
        days = expected.get(store)
        if days is None:
            yield item("Q6 magasin journalise mais non declare : %s" % store,
                       "declare", "INCONNU")
            continue
        observed = (as_of - cutoff).days
        yield item("Q6 borne appliquee a %s contre la duree declaree" % store,
                   "%d jours" % days, "%d jours" % observed)
        yield item("info %s : derniere purge, lignes supprimees, refus" % store,
                   "mesure", "%s, %d, %d"
                   % (ran_at.date(), int(deleted or 0), int(refused or 0)))


def _datasets(item, conn, settings, root: Path):
    """The parquet files of every recorded dataset version, on the disk."""
    with conn.cursor() as cur:
        cur.execute("SELECT version, train_path, test_path FROM fd_datasets "
                    "ORDER BY created_at")
        rows = cur.fetchall()

    missing, versions = [], []
    for version, train_path, test_path in rows:
        versions.append(version)
        for declared in (train_path, test_path):
            path = Path(declared)
            if not path.is_absolute():
                path = root / declared
            if not path.is_file():
                missing.append("%s -> %s" % (version, declared))

    yield item("Q6 fichiers de jeux decrits par fd_datasets et absents du disque",
               "0", "%d" % len(missing))
    if missing:
        yield item("info jeux manquants", "mesure", " | ".join(missing[:3]))
    yield item("info versions de jeux enregistrees", "mesure",
               "%d" % len(versions))

    # NOT PURGED, AND DECLARED AS SUCH (retention.kept.datasets_directory):
    # these files carry no identifying field, so limiting their retention is
    # not what this policy is for. The number on the disk is reported because
    # the volume is a housekeeping question somebody may want to act on -- by
    # hand, outside S5.
    family = Path(settings.datasets_root) / settings.dataset_family
    on_disk = sorted(p.name for p in family.glob("*")) if family.is_dir() else []
    yield item("info versions presentes sous datasets/", "mesure",
               "%d (conservees : voir retention.kept)" % len(on_disk))


def main(argv) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", default=str(PROJECT))
    args = parser.parse_args(argv[1:])
    root = Path(args.root)

    print("")
    print("Criteres d'acceptation de S5 que SQL n'atteint pas")
    print("Projet examine : %s" % root)
    print("")

    rows = list(criteria(root))
    width = max(len(row[1]) for row in rows)
    failed = 0
    for _, criterion, expected, actual in rows:
        if expected == "mesure":
            verdict = "info"
        elif expected == actual:
            verdict = "OK"
        else:
            verdict = "<<< ECHEC"
            failed += 1
        print("  %-*s | %-22s | %-22s | %s"
              % (width, criterion, expected, actual, verdict))

    print("")
    print("%d critere(s) en echec sur %d, et %d chiffre(s) informatif(s)"
          % (failed, sum(1 for row in rows if row[2] != "mesure"),
             sum(1 for row in rows if row[2] == "mesure")))
    print("")
    print("Les jeux d'entrainement ne sont pas purges : ils ne portent aucun")
    print("champ identifiant, et retention.kept en donne la raison ecrite.")
    print("")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
