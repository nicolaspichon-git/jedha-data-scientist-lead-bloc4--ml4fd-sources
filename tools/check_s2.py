"""Acceptance criteria of S2 that SQL cannot reach: the files and the chain.

Covers: EC23, EI04
Lot: M8
Competency: C4, C5

The criteria of M0 to M4 live in sql/checks/s2_acceptance.sql, because they are
about rows. These are about ARTEFACTS -- the Parquet pair, the model, the
manifest, the lock file -- and about whether they agree with each other.

THE CRITERION THAT MATTERS MOST is the last: the version of scikit-learn named
in the report must be the version pinned in the lock file. If they disagree, the
model on disk was not produced by the environment the lock file describes, and
the whole reproducibility claim of M8 is void.

Run: python3 tools/check_s2.py [datasets_root]
"""
from __future__ import annotations

import datetime as dt
import json
import re
import sys
from pathlib import Path

REQUIRED_FILES = ("train.parquet", "test.parquet", "model.joblib",
                  "run.json", "requirements.lock", "rapport.txt")
HEX64 = re.compile(r"^[0-9a-f]{64}$")


def latest_dataset(root: Path) -> Path | None:
    """The most recently written dataset directory, whatever its family."""
    candidates = [p.parent for p in root.glob("*/*/run.json")]
    if not candidates:
        candidates = [p for p in root.glob("*/*") if p.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def criteria(directory: Path):
    """Yield (ordinal, criterion, expected, actual) tuples."""
    ord_ = 0

    def item(criterion, expected, actual):
        nonlocal ord_
        ord_ += 1
        return (ord_, criterion, str(expected), str(actual))

    for name in REQUIRED_FILES:
        path = directory / name
        yield item("M5-M8 fichier %s" % name, "present",
                   "present" if path.is_file() else "ABSENT")

    run_path = directory / "run.json"
    if not run_path.is_file():
        yield item("M8 manifeste lisible", "oui", "ABSENT")
        return

    try:
        payload = json.loads(run_path.read_text(encoding="utf-8"))
    except Exception as problem:                              # noqa: BLE001
        yield item("M8 manifeste lisible", "oui", "ILLISIBLE : %s" % problem)
        return

    dataset = payload.get("dataset", {})
    model = payload.get("model", {})
    metrics = payload.get("metrics", {})
    threshold = payload.get("threshold") or {}

    yield item("M8 empreinte de recette sur 64 hexa", "oui",
               "oui" if HEX64.match(dataset.get("recipe_digest", "")) else "NON")
    yield item("M8 coupure datee et avec fuseau", "oui",
               _dated(dataset.get("cut_off")))
    yield item("M8 liste de features non vide", "oui",
               "oui" if dataset.get("features") else "NON")
    yield item("M8 graine enregistree", "oui",
               "oui" if model.get("seed") else "NON")
    yield item("M8 versions de bibliotheques relevees", "oui",
               "oui" if model.get("versions") else "NON")

    policy = threshold.get("policy")
    yield item("M6 seuil choisi par une politique declaree", "oui",
               "oui" if policy in ("cost", "capacity")
               else "NON : %r" % (policy,))
    # The one mistake in M6 that no metric would reveal.
    yield item("M6 seuil choisi HORS du jeu de test", "oui",
               "oui" if "validation" in (threshold.get("chosen_on") or "")
               else "NON : " + (threshold.get("chosen_on") or "non renseigne"))
    yield item("M6 les deux politiques calculees", "oui",
               "oui" if {"by_cost", "by_capacity"} <= set(threshold)
               else "NON")

    yield item("M5 aucune exactitude du modele enregistree", "0",
               str(sum(1 for k in metrics if "accuracy" in k)))
    yield item("M5 taux de base des deux cotes", "oui",
               "oui" if payload.get("counts", {}).get("base_rate_test")
               else "NON")
    yield item("M5 gain sur la regle d'analyste", "> 0",
               "%+d" % metrics.get("frauds_caught_over_rule", 0))

    # Informative: the figures a reader needs beside any verdict above.
    yield item("info politique retenue", policy or "aucune", policy or "aucune")
    yield item("info seuil", threshold.get("value", "n/a"),
               threshold.get("value", "n/a"))
    yield item("info seuil au moindre cout", threshold.get("by_cost", "n/a"),
               threshold.get("by_cost", "n/a"))
    yield item("info avertissement de capacite",
               threshold.get("warning") or "aucun",
               threshold.get("warning") or "aucun")

    report = (directory / "rapport.txt")
    text = report.read_text(encoding="utf-8") if report.is_file() else ""
    yield item("M5 rapport sans exactitude du modele", "oui",
               "oui" if "n'est pas calculee" in text else "NON")

    # THE criterion of M8.
    lock = (directory / "requirements.lock")
    lock_text = lock.read_text(encoding="utf-8") if lock.is_file() else ""
    for package, version in sorted((model.get("versions") or {}).items()):
        pinned = _pinned(lock_text, package)
        yield item("M8 %s du rapport = celui du verrou" % package,
                   version, pinned or "ABSENT DU VERROU")


def _dated(value) -> str:
    try:
        moment = dt.datetime.fromisoformat(str(value))
    except Exception:                                         # noqa: BLE001
        return "NON : %r" % value
    return "oui" if moment.tzinfo is not None else "NON : sans fuseau"


def _pinned(lock_text: str, package: str) -> str:
    wanted = package.replace("_", "-").lower()
    for line in lock_text.splitlines():
        name, _, version = line.partition("==")
        if name.strip().replace("_", "-").lower() == wanted:
            return version.strip()
    return ""


def main(argv) -> int:
    root = Path(argv[1] if len(argv) > 1 else "runtime/datasets")
    if not root.is_dir():
        print("le dossier %s n'existe pas : S2 n'a encore rien construit" % root)
        return 1

    directory = latest_dataset(root)
    if directory is None:
        print("aucun jeu sous %s : S2 n'a encore rien construit" % root)
        return 1

    print()
    print("Jeu examine : %s" % directory)
    print()
    rows = list(criteria(directory))
    width = max(len(c) for _, c, _, _ in rows)
    failures = 0
    for _, criterion, expected, actual in rows:
        ok = expected == actual or (expected == "> 0" and actual.startswith("+")
                                    and actual != "+0")
        failures += 0 if ok else 1
        print("  %-*s | %-22s | %-22s | %s"
              % (width, criterion, expected, actual,
                 "OK" if ok else "<<< ECHEC"))
    print()
    print("%d critere(s) en echec sur %d" % (failures, len(rows)))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
