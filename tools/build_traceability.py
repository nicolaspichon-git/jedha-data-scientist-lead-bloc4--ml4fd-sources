#!/usr/bin/env python3
"""Build the traceability matrix: requirement -> D0 row -> code location.

    python tools/build_traceability.py            # writes TRACEABILITY.md
    python tools/build_traceability.py --check    # writes nothing, non-zero on a break

Covers: EC07, EC28
Lot: L8
Competency: C1

Why a script rather than a hand-kept table: a hand-written matrix drifts. D0's
did -- eleven citations had slipped by one rank after a renumbering. The --check
mode looks BOTH WAYS:

  - does every requirement cited in D0 section 0.4 exist in the requirements
    document? (this is the check that would have caught that drift the same day)
  - does every "Covers:" mark in the code name an existing requirement?

Documentary and architectural requirements legitimately carry no code: their
absence is counted, not penalised.

Code and comments are in English; the generated document stays in French, since
it is read alongside D0 and D1.
"""
from __future__ import annotations

import argparse
import io
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent          # step-2/
REQUIREMENTS = ROOT.parent / "step-0" / "ws" / "fraud-project--reprise-du-cahier-des-charges.md"
D0 = ROOT.parent / "step-0" / "fraud-project--d0-introduction.md"
SCANNED = ("ml4fd", "sql", "tests", "tools", "airflow", "dashboard")
REQ = re.compile(r"\b((?:EE|EI|EC)[0-9]{2})\b")
MARK = re.compile(r"Covers:\s*(.+)")

# The requirements document spells a kind out, sometimes twice over ("documentaire,
# architecturale") and sometimes alongside something that is not a kind at all
# ("critere d'acceptation, architectural"). The matrix is read column by column,
# so it carries the letter. Order is fixed, never the order of the source text:
# the same pair must always read the same way.
KINDS = (("livrable", "L"), ("documentaire", "D"),
         ("architectural", "A"), ("fonctionnel", "F"))
LEGEND = "L = livrable, D = documentaire, A = architectural, F = fonctionnel"


def abbreviated(kind: str) -> str:
    """The kinds a requirement carries, as letters. Rendering only.

    The raw wording stays in req[...][1]: the counts below ask it whether a
    requirement is functional or a deliverable, and a letter they had to decode
    would be the same rule written twice.
    """
    letters = [short for word, short in KINDS if word in kind.lower()]
    return ", ".join(letters)


def read(path: Path) -> str:
    return io.open(path, encoding="utf-8").read()


def defined() -> dict[str, tuple[str, str]]:
    """Requirements as defined: identifier -> (statement, kind)."""
    out = {}
    for line in read(REQUIREMENTS).split("\n"):
        m = re.match(r"^\|\s*\*{0,2}((?:EE|EI|EC)[0-9]{2})\*{0,2}\s*\|", line)
        if not m:
            continue
        cells = [c.strip() for c in line.split("|")]
        text = re.sub(r"\s+", " ", cells[2]) if len(cells) > 2 else ""
        kind = next((c for c in cells[3:] if re.search(
            r"livrable|documentaire|architectural|fonctionnel", c)), "")
        out[m.group(1)] = (text, re.sub(r"\s+", " ", kind))
    return out


def cited_in_d0() -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Requirement -> D0 rows citing it. Live rows kept apart from struck ones."""
    body = read(D0)
    body = body.split("### 0.4.1.")[1].split("### 0.4.4.")[0]
    live: dict[str, list[str]] = {}
    struck: dict[str, list[str]] = {}
    for line in body.split("\n"):
        m = re.match(r"^\|\s*\*{0,2}~{0,2}((?:F|K|D)[0-9]{2}(?:\.[0-9])?)", line)
        if not m:
            continue
        cells = line.split("|")
        target = struck if "~~" in line else live
        for r in REQ.findall(cells[-2] if len(cells) > 2 else ""):
            target.setdefault(r, []).append(m.group(1))
    return live, struck


def tagged() -> dict[str, list[str]]:
    """Requirement -> code locations, from the "Covers:" marks."""
    out: dict[str, list[str]] = {}
    for sub in SCANNED:
        for path in sorted((ROOT / sub).rglob("*")):
            if path.suffix not in (".py", ".sql"):
                continue
            lines = read(path).split("\n")
            for i, line in enumerate(lines):
                m = MARK.search(line)
                if not m:
                    continue
                scope = ""
                for prev in reversed(lines[:i]):
                    d = re.match(r"\s*(?:def|class)\s+(\w+)", prev)
                    if d:
                        scope = " " + d.group(1) + "()"
                        break
                where = "%s:%d%s" % (path.relative_to(ROOT), i + 1, scope)
                for r in REQ.findall(m.group(1)):
                    out.setdefault(r, []).append(where)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()

    req = defined()
    live, struck = cited_in_d0()
    code = tagged()

    broken: list[str] = []
    for r, rows in live.items():
        if r not in req:
            broken.append("D0 cites %s (row %s), which the requirements do not define"
                          % (r, ", ".join(rows)))
    for r, places in code.items():
        if r not in req:
            broken.append("code marks %s (at %s), which the requirements do not define"
                          % (r, ", ".join(places)))

    carried = sorted(set(code) & set(req))
    no_code = [r for r in sorted(req) if r not in code]
    needs_code = [r for r in no_code
                  if "fonctionnel" in req[r][1] or "livrable" in req[r][1]]

    if args.check:
        for b in broken:
            print("  BROKEN  %s" % b)
        print("\n  %d requirements defined; %d carried by code; "
              "%d without code, of which %d functional or deliverable"
              % (len(req), len(carried), len(no_code), len(needs_code)))
        if broken:
            print("\n  failed: %d traceability break(s)" % len(broken))
            return 1
        print("  ok: no dangling reference either way")
        return 0

    out = ["# Matrice de traçabilité", "",
           "> Fichier **généré** par `tools/build_traceability.py`. Ne pas éditer à la main.",
           "> Les emplacements viennent des marques `Covers:` du code ; les lignes de D0 de §0.4.",
           "",
           "> Nature : " + LEGEND + ".",
           "",
           "| Exigence | Nature | Énoncé | D0 §0.4 | Code |",
           "| --- | --- | --- | --- | --- |"]
    for r in sorted(req):
        text, kind = req[r]
        rows = ", ".join(live.get(r, [])) or ("~~%s~~" % ", ".join(struck[r])
                                              if r in struck else "—")
        places = "<br/>".join("`%s`" % p for p in code.get(r, [])) or "—"
        out.append("| **%s** | %s | %s | %s | %s |"
                   % (r, abbreviated(kind) or "—", text[:118], rows, places))
    out += ["", "## Décompte", "",
            "- exigences définies : **%d**" % len(req),
            "- portées par du code : **%d**" % len(carried),
            "- sans code, de nature fonctionnelle ou livrable : **%d**" % len(needs_code),
            "- ruptures de traçabilité : **%d**" % len(broken), ""]
    (ROOT / "TRACEABILITY.md").write_text("\n".join(out), encoding="utf-8")
    print("TRACEABILITY.md written: %d requirements, %d carried by code, %d break(s)"
          % (len(req), len(carried), len(broken)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
