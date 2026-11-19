"""Acceptance criteria of S3 that SQL cannot reach: the files and the declarations.

Covers: EE09, EI06, EC09, EC17, EC24
Lot: N8
Competency: C1, C4, C5

The criteria of N1 to N7 that the database holds live in
sql/checks/s3_acceptance.sql, because they are questions about rows. What is
here is everything else: the alert file and what it must NOT contain, the two
DAGs and their cadences, and the agreement between governance.yml and the model
version actually in service.

THE ONE THAT MATTERS MOST IS THE ALERT FILE. A notification is the one thing
this pipeline sends OUTSIDE itself, so it is the one place where a leak of
personal data leaves the building. fd_transactions holds no identifying column,
so it cannot happen by accident -- and that is an argument, not a measurement.
This reads the file.

Usage, from the scheduler container:
    python3 /opt/project/tools/check_s3.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT = Path("/opt/project")

# Never, under any key, in anything that leaves this system.
FORBIDDEN = ("first", "last", "street", "city", "state", "zip", "lat", "long",
             "city_pop", "job", "dob", "gender", "cc_num", "client_key",
             "is_fraud", "label")

DAGS = {
    "dag_ingest": "* * * * *",
    "dag_ingest_verdicts": None,          # daily; any cadence but the minute
}

# Where the DAG files are is NOT this tool's to decide: ml4fd.config.dags_dir()
# holds the one definition (the repo layout and the container's differ).

# The operations channel carries states of the chain, so the transaction's own
# facts are forbidden there ON TOP of the personal ones -- not because they are
# personal (they are not) but because an operations file accumulating them would
# become a second, unmanaged copy of the alert stream, outside the capacity
# policy and outside the queue the analyst reads.
OPS_FORBIDDEN = FORBIDDEN + ("txn_id", "amt", "merchant", "score", "threshold")


def _read_file(path: Path, forbidden, required):
    """What is in a channel's file: lines, unreadable ones, leaked keys, gaps.

    ONE READER FOR BOTH CHANNELS. "What must not be in a message that leaves
    this system" is the same question asked of two destinations, and asking it
    twice in two pieces of code is how the second one comes to be weaker.
    """
    lines = [line for line in path.read_text(encoding="utf-8").splitlines()
             if line.strip()]
    bad_json, leaked, missing = 0, set(), 0
    for line in lines:
        try:
            payload = json.loads(line)
        except ValueError:
            bad_json += 1
            continue
        leaked |= {key for key in payload if key in forbidden}
        if not required <= set(payload):
            missing += 1
    return lines, bad_json, leaked, missing


def criteria(root: Path):
    """Yield (ordinal, criterion, expected, actual) tuples."""
    ord_ = 0

    def item(criterion, expected, actual):
        nonlocal ord_
        ord_ += 1
        return (ord_, criterion, str(expected), str(actual))

    sys.path.insert(0, str(root))
    from ml4fd import clock, config, datasets, health, notify
    from ml4fd.threshold import API_TXN_PER_DAY

    settings = config.load()

    # --- the cadence against the calibration (N7)
    observed = settings.observed_per_day()
    yield item("N7 appels/jour contre le debit annonce",
               "%d" % int(API_TXN_PER_DAY), "%d" % observed)
    yield item("N7 appels par minute sous le limiteur mesure (13)",
               "oui", "oui" if settings.calls_per_run <= 13 else "NON")

    # --- the two DAGs and their cadences (EI06)
    dags = config.dags_dir(root)
    if dags is None:
        # NAMED, like config.governance_path() does: "absent" without saying
        # where one looked sends the reader to check the DAGs, which are fine.
        yield item("N7 repertoire des DAG", "trouve",
                   "ABSENT : cherche dans %s"
                   % ", ".join(config.DAG_DIRS))
    else:
        yield item("info repertoire des DAG lu", "mesure", str(dags))
    for name, expected_schedule in DAGS.items() if dags else ():
        # Parenthesised: % and / have the SAME precedence and associate to
        # the left, so without them the % applies to the Path.
        path = dags / ("%s.py" % name)
        if not path.is_file():
            yield item("N7 DAG %s" % name, "present", "ABSENT")
            continue
        body = path.read_text(encoding="utf-8")
        found = ""
        for line in body.splitlines():
            if line.strip().startswith("schedule="):
                found = line.split('"')[1] if '"' in line else ""
                break
        if expected_schedule is None:
            yield item("N6 cadence de %s distincte de la minute" % name,
                       "oui", "oui" if found != "* * * * *" else "A LA MINUTE")
        else:
            yield item("N7 cadence de %s" % name, expected_schedule, found)

    # --- the recipe in service against what governance declares (N4)
    yield item("N4 colonnes retirees declarees",
               ",".join(settings.drop_features) or "aucune",
               ",".join(settings.drop_features) or "aucune")
    kept = datasets.without(settings.drop_features)
    yield item("N4 la recette declaree reste entrainable",
               "au moins 1 feature",
               "%d features" % len(kept) if kept else "AUCUNE")
    yield item("N4 la recette declaree ne lit plus la pendule",
               "aucune colonne de calendrier",
               ", ".join(clock.present(kept)) or "aucune colonne de calendrier")

    # --- the notification channel (N5)
    channel = settings.alert_channel
    try:
        notify.channel_of(channel)
        known = "connu"
    except ValueError:
        known = "INCONNU"
    yield item("N5 canal de notification declare", "connu", known)
    yield item("N5 semantique de livraison declaree", "at-least-once",
               settings.alert_delivery)
    yield item("N5 plafond de capacite applique", "oui",
               "oui" if settings.enforce_capacity else "NON")

    # --- THE ALERT FILE, then the operations file. Both in generators of their
    # own, because each ends in an early `return` and those used to return from
    # criteria() ITSELF: every criterion added after them would have been
    # silently skipped, which is exactly what happened when N9 tried to add
    # three.
    yield from _alert_file(item, settings, channel)

    # --- N9: the operations channel, stage (3c) of diagram 3.3
    try:
        health.channel_of(settings.ops_channel)
        ops_known = "connu"
    except ValueError:
        ops_known = "INCONNU"
    yield item("N9 canal d'exploitation declare", "connu", ops_known)
    yield item("N9 destinations distinctes des deux canaux", "distinctes",
               "distinctes"
               if Path(settings.alert_file) != Path(settings.ops_file)
               else "IDENTIQUES")
    yield item("N9 les quatre controles de la vue 3.3 sont la", "4",
               "%d" % len(health.CONTROLS))
    yield from _ops_file(item, settings)


def _alert_file(item, settings, channel):
    """N5's criteria on the alert file: the one thing that leaves this system."""
    path = Path(settings.alert_file)
    if channel != "file":
        yield item("info canal declare, pas de fichier a lire", "mesure",
                   channel)
        return
    if not path.is_file():
        # NOT A FAILURE, and saying so matters. No alert has been notified yet
        # -- which is the normal state of a pipeline whose model has flagged
        # nothing, and of a check run somewhere other than where the file is
        # written. What CANNOT be said is that no alert ever leaked a personal
        # field: there is nothing to read. The criterion is reported as
        # unverified rather than as passed.
        yield item("info fichier d'alertes %s" % path.name, "mesure",
                   "absent : aucune alerte notifiee")
        yield item("info aucun champ nominatif dans une alerte", "mesure",
                   "NON VERIFIE : rien a lire")
        return

    lines, bad_json, leaked, missing = _read_file(
        path, FORBIDDEN, {"txn_id", "score", "threshold", "model_version"})
    yield item("info alertes ecrites dans le fichier", "mesure", "%d" % len(lines))
    if not lines:
        yield item("info aucun champ nominatif dans une alerte", "mesure",
                   "NON VERIFIE : fichier vide")
        return

    # THE ACTUAL CARRIES THE UNIT OF THE EXPECTED, or the two never compare
    # equal: "0" against "0 illisible" failed on a clean file. check_s4.py
    # writes "%d absent" for the same reason.
    yield item("N5 chaque ligne est du JSON", "0 illisible",
               "%d illisible" % bad_json)
    # THE criterion of this file.
    yield item("N5 aucun champ nominatif dans une alerte", "aucun",
               ", ".join(sorted(leaked)) or "aucun")
    yield item("N5 chaque alerte porte sa tracabilite",
               "0 incomplete", "%d incomplete" % missing)


def _ops_file(item, settings):
    """N9's criteria on the operations file.

    THE SAME QUESTION AS THE ALERT FILE, ASKED OF THE OTHER DESTINATION, plus
    one the alert file cannot be asked: that no transaction travelled here. An
    operations channel is read by whoever keeps the pipeline running, and a
    transaction in it has escaped the capacity policy and the analyst's queue
    both.
    """
    path = Path(settings.ops_file)
    if settings.ops_channel != "file":
        yield item("info canal d'exploitation sans fichier a lire", "mesure",
                   settings.ops_channel)
        return
    if not path.is_file():
        # NOT a failure, and saying so matters: no control has changed state,
        # which is the normal state of a healthy pipeline. What cannot be said
        # is that no message ever carried a transaction -- there is nothing to
        # read.
        yield item("info fichier d'exploitation %s" % path.name, "mesure",
                   "absent : aucun changement d'etat notifie")
        yield item("info aucune transaction dans un message d'exploitation",
                   "mesure", "NON VERIFIE : rien a lire")
        return

    lines, bad_json, leaked, missing = _read_file(
        path, OPS_FORBIDDEN, {"control", "became", "detail"})
    yield item("info messages d'exploitation ecrits", "mesure", "%d" % len(lines))
    if not lines:
        yield item("info aucune transaction dans un message d'exploitation",
                   "mesure", "NON VERIFIE : fichier vide")
        return

    yield item("N9 chaque ligne est du JSON", "0 illisible",
               "%d illisible" % bad_json)
    # THE criterion of this file.
    yield item("N9 aucune transaction dans un message d'exploitation", "aucune",
               ", ".join(sorted(leaked)) or "aucune")
    yield item("N9 chaque message nomme son controle et son changement",
               "0 incomplet", "%d incomplet" % missing)


def verdict(expected: str, actual: str) -> str:
    """The verdict of one row: "info", "OK" or "<<< ECHEC". ONE RULE.

    Out of main() so the tests apply the rule the report applies. They used to
    assert the (expected, actual) pair -- ("0 illisible", "0") -- and passed
    while the report said ECHEC on the same pair: a test named "a clean file
    passes" that never asked whether it passed.

    "mesure" marks a figure, not a criterion -- the same convention
    sql/checks/s3_acceptance.sql uses. A figure that failed would be a check
    refusing reality.
    """
    if expected == "mesure":
        return "info"
    ok = expected == actual or (expected.startswith("au moins")
                                and not actual.startswith("AUCUN"))
    return "OK" if ok else "<<< ECHEC"


def main(argv) -> int:
    root = Path(argv[1]) if len(argv) > 1 else PROJECT
    if not (root / "ml4fd").is_dir():
        print("%s ne contient pas le paquet ml4fd/" % root)
        return 1

    print()
    print("Criteres d'acceptation de S3 que SQL n'atteint pas")
    print("Projet examine : %s" % root)
    print()
    rows = list(criteria(root))
    width = max(len(c) for _, c, _, _ in rows)
    failures = informative = 0
    for _, criterion, expected, actual in rows:
        said = verdict(expected, actual)
        informative += said == "info"
        failures += said == "<<< ECHEC"
        print("  %-*s | %-30s | %-30s | %s"
              % (width, criterion, expected, actual, said))
    print()
    print("%d critere(s) en echec sur %d, et %d chiffre(s) informatif(s)"
          % (failures, len(rows) - informative, informative))
    if any(actual.startswith("NON VERIFIE") for _, _, _, actual in rows):
        print()
        print("Une ligne NON VERIFIE n'est pas un critere qui passe : elle dit")
        print("qu'il n'y avait rien a mesurer. Notifiez une alerte et relancez.")
    if not failures:
        print()
        print("La fuite du verdict se mesure a part, parce qu'elle mesure une")
        print("absence :  .\\fd-manager.ps1 check-leak")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
