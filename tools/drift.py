#!/usr/bin/env python3
"""Switch the injected amount drift on and off -- the writer the switch lacked.

    python3 tools/drift.py                 # what is in force
    python3 tools/drift.py --factor 3      # every served amount multiplied by 3
    python3 tools/drift.py --off           # back to no drift

FROM THE MANAGER THE OPTIONS ARE NAMED IN POWERSHELL: `fd-manager drift`,
`-Factor 3`, `-Off`. The POSIX flags above are this file's own, so it stays
runnable on its own inside the container; fd-manager.ps1 translates. Reading
--off here and typing it at the manager would make PowerShell treat it as a
positional argument.

Block: B4.
Lot: B4-L4.
Competency: B4-C5, B4-C6.
Covers: B4-EC39, B4-EC51.

WHY THIS FILE EXISTS. governance.yml declared sources.drift_switch, the
producer read it on every request, and NOTHING wrote it. A switch with a reader
and no writer is a loop that works and cannot be shown, which the plan of B4-L4
calls out in those terms: without an injectable drift "la boucle fonctionne et
reste indemontrable, ce qui, pour B4-C6, revient au meme".

A DRIFT MUST ENTER AS DATA OR IT IS NOT A DRIFT. This writes a file; the
producer then multiplies the amount of every transaction it serves, BEFORE the
raw zone. The shift travels through validation, the features, the scoring and
the monitoring exactly as a real change of the stream would. Nothing is mocked,
and no measurement is told what to find.

WHY A FILE, AND NOT A DAG CONF OR AN ENVIRONMENT VARIABLE. A conf lasts one
execution and the switch has to outlast it. An environment variable needs the
container recreated at every flip, which would restart the very service whose
behaviour is being observed.

WHAT IT REFUSES, AND WHAT IT ONLY WARNS ABOUT. A factor at or below zero is
refused: the producer reads such a value as "no drift", so writing one would be
a switch that says one thing and does another. A factor large enough for the
cap to bind is only warned about, because it is sometimes what an operator
wants -- but it is said out loud, since above validation.max_amount the
transactions are quarantined and the run would then measure the quality control
instead of the drift: a data_quality alert where the demonstration meant to show
an input_drift one.

OFF IS A WRITE AND NOT A DELETE. {"factor": 1.0} reads as no drift, and it
leaves a dated trace of when the drift was lifted. Deleting the file works too
-- the producer treats a missing file as no drift -- but then nothing says
whether it was ever on.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

# The key the producer reads. Named once here, asserted against producer/app.py
# by tests/test_drift.py: a writer and a reader of one file that disagree on the
# key would leave the switch silently dead, since drift_factor() answers 1.0 to
# anything it cannot parse.
FACTOR_KEY = "factor"

# WHERE THE HISTORY GOES, beside the switch and under runtime/ with it. The
# switch is OVERWRITTEN at every change, which was enough while the stream fed
# nothing: a drifted transaction was served, scored, and that was the end of
# it. Since datasets.include_stream the stream is a TRAINING source, so a
# drifted transaction enters the corpus and stays there -- and "which rows
# carry an amount this demonstration multiplied" became a question the system
# could not answer, because the only record of when the drift was on had been
# overwritten by the next flip.
#
# One line per flip, appended, never rewritten. Crossing its intervals with
# fd_transactions.txn_ts is then what identifies the fabricated rows.
JOURNAL_NAME = "drift-log.jsonl"


def switch_path(settings, root: Path) -> Path:
    """The declared switch, resolved against the project root if relative."""
    declared = Path(settings.drift_switch)
    return declared if declared.is_absolute() else root / declared


def in_force(path: Path) -> float:
    """The factor the producer would read right now, by the producer's rule.

    THE SAME RULE AND NOT A SECOND ONE: anything unreadable, absent, or at or
    below zero is 1.0. A reporting tool that disagreed with the service about
    what is in force would be worse than no tool.
    """
    try:
        value = float(json.loads(path.read_text(encoding="utf-8"))[FACTOR_KEY])
    except Exception:                       # noqa: BLE001
        return 1.0
    return value if value > 0 else 1.0


def journal_path(switch: Path) -> Path:
    """The history, beside the switch it records."""
    return switch.with_name(JOURNAL_NAME)


def current_record(path: Path) -> dict | None:
    """The switch file as it stands, or None if there is nothing readable.

    READ BEFORE THE WRITE, because write() overwrites it: the set_at of the
    injection in force is the only record of when it began, and seeding the
    journal from it is what keeps that date instead of the date somebody
    happened to run this tool.
    """
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:                       # noqa: BLE001
        return None


def append_journal(switch: Path, entry: dict) -> None:
    """One line, appended. The file is never rewritten."""
    journal = journal_path(switch)
    journal.parent.mkdir(parents=True, exist_ok=True)
    with journal.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, sort_keys=True) + "\n")


def seed_journal(switch: Path, previous: dict | None) -> bool:
    """Record an injection that was already in force, from its own set_at.

    THE ONE CASE WHERE THE HISTORY CAN BE RECOVERED. The journal was added on
    2026-11-20, after a drift had been switched on; the switch file carries the
    moment it was set, so the first entry can state the true start of the
    contaminated window rather than the moment the journal came into being.
    Nothing is invented: the date is read from the file.

    Returns whether a seed was written.
    """
    if journal_path(switch).exists() or not previous:
        return False
    try:
        factor = float(previous[FACTOR_KEY])
    except (KeyError, TypeError, ValueError):
        return False
    if factor <= 1.0:
        return False
    append_journal(switch, {
        FACTOR_KEY: factor,
        "at": previous.get("set_at", "unknown"),
        "note": "reconstruit depuis l'interrupteur : cette injection etait "
                "deja en cours quand le journal a ete ajoute",
        "reconstructed": True})
    return True


def write(path: Path, factor: float, note: str) -> None:
    """Write the switch. The extra keys are for a human; the producer ignores
    them, since it reads one key and treats the rest as none of its business."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {FACTOR_KEY: round(float(factor), 4),
               "set_at": dt.datetime.now(dt.timezone.utc).isoformat(
                   timespec="seconds"),
               "note": note}
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def report(path: Path, factor: float, cap: float) -> None:
    print("Interrupteur : %s" % path)
    if factor == 1.0:
        print("Facteur      : 1.0 -- aucune derive injectee")
    else:
        print("Facteur      : %.4g -- chaque montant servi est multiplie" % factor)
        clipped_above = cap / factor
        print("Plafond      : %.2f (validation.max_amount)" % cap)
        print("Ecrete au-dela de %.2f de montant d'origine" % clipped_above)
        if clipped_above < 500:
            print("")
            print("ATTENTION : le montant moyen d'une fraude sur la fenetre")
            print("servie est de 505,50, donc la plupart des fraudes seraient")
            print("ecretees. La mesure porterait sur le controle qualite et non")
            print("sur la derive.")
    journal = journal_path(path)
    if journal.exists():
        lines = [log_entry for log_entry in journal.read_text(encoding="utf-8").splitlines() if log_entry.strip()]
        last = json.loads(lines[-1]) if lines else {}
        print("Journal      : %s" % journal.name)
        print("               %d bascule(s), la derniere le %s"
              % (len(lines), last.get("at", "?")))
    else:
        print("Journal      : aucun encore (il s'ouvre a la prochaine bascule)")
    print("")
    print("Lu a chaque appel : aucun redemarrage du producer n'est necessaire.")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Injected amount drift: show it, set it, lift it.",
        epilog="The producer reads this file on every request, so a change "
               "takes effect at the next transaction served.")
    parser.add_argument("--factor", type=float, metavar="N",
                        help="multiply every served amount by N (N > 0)")
    parser.add_argument("--off", action="store_true",
                        help="lift the drift, by writing a factor of 1.0")
    parser.add_argument("--note", default="", metavar="TEXT",
                        help="recorded in the file, for whoever reads it next")
    args = parser.parse_args(argv)

    if args.factor is not None and args.off:
        print("--factor et --off ensemble : choisissez.")
        return 1

    from ml4fd import config

    settings = config.load()
    root = config.governance_path().parent
    path = switch_path(settings, root)
    cap = float(settings.max_amount)

    if args.factor is None and not args.off:
        report(path, in_force(path), cap)
        return 0

    # READ BEFORE ANY WRITE: write() overwrites the switch, and its set_at is
    # the only record of when the injection in force began.
    previous = current_record(path)

    def apply(factor: float, note: str) -> None:
        """Seed if needed, change the switch, then record the change.

        WRITE THEN RECORD, the order this project uses everywhere -- fd_frauds,
        fd_ops_state, fd_retrain_requests. A crash between the two leaves a
        flip unrecorded, which understates the contaminated window; the reverse
        would have the journal claim a window that never existed, and a reader
        would exclude rows that are perfectly good.
        """
        if seed_journal(path, previous):
            print("Journal amorce sur l'injection deja en cours.")
        write(path, factor, note)
        append_journal(path, {
            FACTOR_KEY: round(float(factor), 4),
            "at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "note": note})

    if args.off:
        apply(1.0, args.note or "derive levee")
        print("Derive levee.")
        report(path, in_force(path), cap)
        return 0

    if args.factor <= 0:
        print("Un facteur de %r serait lu comme 'aucune derive' par le "
              "producer." % args.factor)
        print("Pour lever la derive : --off")
        return 1

    apply(args.factor, args.note or "derive injectee")
    print("Derive injectee.")
    report(path, in_force(path), cap)
    return 0


if __name__ == "__main__":
    sys.exit(main())
