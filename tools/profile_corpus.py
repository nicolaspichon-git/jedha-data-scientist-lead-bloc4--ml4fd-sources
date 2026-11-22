"""Measure a corpus, so governance.yml quotes facts and not recollections.

Block: B3, B4.
Lot: B3-L4, B4-L0.
Competency: B3-C1, B3-C6.
Covers: B3-EI04, B3-EC05.

WHY A TOOL AND NOT A NOTEBOOK. Every number in governance.yml that reads
"Measured:" was measured once, by hand, on fraudTest.csv -- the horizon, the
base rates, the amount bound, the fraud counts. Swapping the corpus invalidates
all of them at once, and a stale "Measured:" is worse than no comment: it reads
as evidence. This prints the same figures for any corpus, so the swap is a
re-measurement rather than a rewrite from memory.

WHAT IT MEASURES, and why each one is in governance.yml:

  rows, frauds, base rate   the denominators every recall and precision is read
                            against (fd_datasets stores them per dataset).
  first and last payment    the range a split rule has to fall inside.
  LAST LABELLED FRAUD       labels.horizon. The generator stops injecting
                            before the file ends, and the rows after that
                            carry is_fraud = 0 because nobody labelled them,
                            not because they are not fraud. A cut-off beyond
                            this point trains a model on false negatives.
  base rate by month        the spread that makes a precision unreadable
                            without its own window's rate.
  max amount                validation.max_amount, which must leave room above
                            the observed maximum without becoming meaningless.
  a proposed split          train, blind band and test, derived from the range
                            and the verdict delay rather than chosen.

Usage:
    python3 tools/profile_corpus.py .local_cache/fraudTrain.csv
    python3 tools/profile_corpus.py .local_cache/fraudTest.csv --delay-days 30
"""
from __future__ import annotations

import argparse
import collections
import csv
import datetime as dt
from pathlib import Path

TIME = "trans_date_trans_time"
LABEL = "is_fraud"
AMOUNT = "amt"


def measure(path: Path) -> dict:
    """One pass, counters only: a 350 MB file must not be held in memory."""
    rows = frauds = 0
    first = last = None
    last_fraud = None
    biggest = 0.0
    biggest_at = None
    by_month = collections.defaultdict(lambda: [0, 0])

    with path.open(encoding="utf-8", newline="") as handle:
        for record in csv.DictReader(handle):
            stamp = (record.get(TIME) or "").strip()
            if not stamp:
                continue
            rows += 1
            if first is None or stamp < first:
                first = stamp
            if last is None or stamp > last:
                last = stamp
            month = stamp[:7]
            by_month[month][0] += 1
            if (record.get(LABEL) or "").strip() == "1":
                frauds += 1
                by_month[month][1] += 1
                if last_fraud is None or stamp > last_fraud:
                    last_fraud = stamp
            try:
                amount = float(record.get(AMOUNT) or 0)
            except ValueError:
                continue
            if amount > biggest:
                biggest, biggest_at = amount, stamp

    return {"rows": rows, "frauds": frauds, "first": first, "last": last,
            "last_fraud": last_fraud, "max_amount": biggest,
            "max_amount_at": biggest_at, "by_month": dict(by_month)}


def proposed_split(facts: dict, delay_days: int) -> dict:
    """A split rule derived from the horizon and the verdict delay.

    THE TEST SIDE ENDS AT THE HORIZON and not at the last payment: the rows
    after the last labelled fraud are unlabelled, and scoring a model against
    them would count every fraud there as a miss.

    THE TRAIN SIDE ENDS ONE DELAY EARLIER, because a transaction's verdict is
    only known after that delay: training up to the horizon would use labels
    that, at the moment of training, nobody had yet. That blind band is the
    same one verdicts.delay_days sets for the stream.
    """
    horizon = dt.date.fromisoformat((facts["last_fraud"] or "")[:10])
    test_end = horizon
    train_end = test_end - dt.timedelta(days=delay_days)
    return {"train_end": train_end.isoformat(),
            "test_end": test_end.isoformat(),
            "rule": "temporal:train<=%s,test<=%s"
                    % (train_end.isoformat(), test_end.isoformat())}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure a corpus and print what governance.yml must say "
                    "about it.")
    parser.add_argument("csv", type=Path)
    parser.add_argument("--delay-days", type=int, default=30,
                        help="verdicts.delay_days, the blind band (default 30)")
    args = parser.parse_args(argv)

    if not args.csv.is_file():
        print("absent : %s" % args.csv)
        return 1

    facts = measure(args.csv)
    rate = facts["frauds"] / facts["rows"] if facts["rows"] else 0.0
    print("%s" % args.csv.name)
    print("  lignes                 %d" % facts["rows"])
    print("  fraudes                %d  (%.4f %%)" % (facts["frauds"],
                                                      100 * rate))
    print("  premier paiement       %s" % facts["first"])
    print("  dernier paiement       %s" % facts["last"])
    print("  DERNIERE FRAUDE        %s   <- labels.horizon" %
          facts["last_fraud"])
    print("  montant maximum        %.2f   le %s" % (facts["max_amount"],
                                                     facts["max_amount_at"]))
    print()
    print("  taux de base par mois (le minimum et le maximum decident si une")
    print("  precision se lit sans le taux de sa propre fenetre) :")
    rates = []
    for month in sorted(facts["by_month"]):
        count, bad = facts["by_month"][month]
        share = bad / count if count else 0.0
        rates.append(share)
        print("    %s  %7d lignes  %5d fraudes  %.4f %%"
              % (month, count, bad, 100 * share))
    if rates:
        print("    etendue : %.3f %% a %.3f %%" % (100 * min(rates),
                                                   100 * max(rates)))
    print()
    split = proposed_split(facts, args.delay_days)
    print("  decoupage propose, bande aveugle de %d jours :" % args.delay_days)
    print("    split_rule: \"%s\"" % split["rule"])
    print("    entrainement jusqu'au %s, test du lendemain au %s"
          % (split["train_end"], split["test_end"]))
    count, bad = 0, 0
    for month in sorted(facts["by_month"]):
        if split["train_end"][:7] <= month <= split["test_end"][:7]:
            count += facts["by_month"][month][0]
            bad += facts["by_month"][month][1]
    print("    ordre de grandeur du lot de test : ~%d fraudes" % bad)
    print("    (sous 50, promote.py refuse de decider ; sous 30, le controle")
    print("     de performance de B3-L4 se declare unmeasurable)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
