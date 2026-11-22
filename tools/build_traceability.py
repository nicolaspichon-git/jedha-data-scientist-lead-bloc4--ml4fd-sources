#!/usr/bin/env python3
"""Build the traceability matrix: requirement -> the code that covers it.

    python tools/build_traceability.py            # writes TRACEABILITY.md
    python tools/build_traceability.py --check    # writes nothing, non-zero on a break

Block: B4.
Lot: B3-L8, B4-L0.
Competency: B4-C1, B4-C6.
Covers: B3-EC07, B3-EC28, B4-EC50.

B4-EC50 IS THIS FILE'S OWN REQUIREMENT -- "tenir une matrice de tracabilite
competence -> exigence -> element de la solution -> emplacement" -- and it went
unmarked while the generator read a document that did not exist. Marked here
rather than anywhere else: the requirement asks for the matrix to be KEPT, and
what keeps it is the generator, not the file it writes.

NOTE FOR THE READER OF THAT REQUIREMENT. It names four things and the table
carries three columns for them: Comp., Exigence, and Code -- which merges the
element of the solution with its location, as "file:line function()". Splitting
them is a change to the table, not to this mark, and it is listed as such.

WHY A SCRIPT RATHER THAN A HAND-KEPT TABLE: a hand-written matrix drifts. The
BC03 one did -- eleven citations had slipped by one rank after a renumbering,
and nothing said so until somebody read two documents side by side.

WHAT CHANGED ON 2026-11-20, and both changes are reductions.

  1. THE SOURCE. REQUIREMENTS pointed at a BC03 file that does not exist in
     this dossier, so defined() returned an empty dict, the matrix was never
     generated, and --check reported "0 requirements defined" as a success.
     An empty expectation is met by anything. It now reads
     step-0/ml4fd-project--d0-requirements.md, which holds the 51 EC.

  2. NO FUNCTIONAL DERIVATION. BC03 decomposed each requirement into F/K/D rows
     in D0 section 0.4 and the matrix carried a column for them. This project
     does not derive functions, so that column and the document it read are
     gone rather than left empty: a column nobody fills is a column a reader
     has to ask about.

ONLY THE EC, FOR NOW. The complete cahier des charges is still being written,
and until it names its own requirements the EC are the only identifiers this
matrix can be complete about. The EE and EI marks the code still carries are
COUNTED AND LISTED, never reported as breaks -- they name requirements of a
document this matrix does not read yet, which is a different thing from naming
nothing. Reporting them as breaks would train the reader to ignore the report.

THE COMPETENCY COMES FROM THE SECTION a requirement sits under, not from the
Source column. Source says where a requirement comes from ("B4-C2/ B4-EC05", "[R1, §Ex.]")
and happens to carry a competency most of the time; the section heading says
which competency the requirement belongs to, always. A jury reads competency ->
requirement -> code, so that is the column the matrix carries.

Code and comments are in English; the generated document stays in French.
"""
from __future__ import annotations

import argparse
import io
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent          # step-2/
REQUIREMENTS = (ROOT.parent / "step-0"
                / "ml4fd-project--d0-requirements.md")
MATRIX = ROOT / "TRACEABILITY.md"
# EVERY TOP-LEVEL DIRECTORY THAT CARRIES "Covers:" MARKS, and a test ties
# this tuple to what the repository actually holds. scoring_service/ was
# missing on the day it was created, 2026-11-20: its seven marks -- EC15
# to EC23, the whole of L5 -- were simply not seen, so the matrix reported
# EC23 as carried by no code at all while the service that carries it sat
# beside it. A list of places to look is a list that goes stale the moment
# somebody adds a place.
SCANNED = ("ml4fd", "sql", "tests", "tools", "airflow", "dashboard",
           "scoring_service", "producer", ".github")

# IN SCOPE and OUT OF SCOPE, declared apart. The first is what the matrix is
# complete about; the second is what the code may legitimately mark while the
# cahier des charges is being written.
#
# BOTH CARRY THEIR BLOCK PREFIX, since 2026-11-21. BC03 and BC04 each number
# their requirements from EC01, so a bare EC07 names two different things and
# this file used to count BC03's marks as BC04's. In scope is B4- only; every
# B3- requirement, EC included, is out of scope. A bare EC is neither: it is a
# mark not yet resolved, and counting it would credit the dossier with a guess.
IN_SCOPE = re.compile(r"(?<![\w-])(B4-EC[0-9]{2})(?![\w])")
OUT_OF_SCOPE = re.compile(r"(?<![\w-])(B3-(?:EE|EI|EC)[0-9]{2})(?![\w])")
SECTION = re.compile(r"^\|\s*\|\s*\*\*(B4-C[0-9])\s*:")
MARK = re.compile(r"^\s*(?:#|--|\*)?\s*Covers:\s*(.+)")
COMPETENCY = re.compile(r"Competency:\s*([^\n]+)")
COMP = re.compile(r"(?<![\w-])(B4-C[0-9])\b")

# The Type column of the requirements document spells a kind as a LETTER, and
# sometimes two ("D/F"). BC03's spelled it out in prose ("documentaire,
# architecturale"), so both forms are accepted: the tests of the prose form are
# the ones that pinned the ordering rule, and they are worth keeping.
#
# ORDER IS FIXED HERE, never the order of the source text: the same pair must
# always read the same way, or two requirements of the same two kinds would
# look different because the source happened to spell them the other way.
KINDS = (("livrable", "L"), ("documentaire", "D"), ("demonstration", "DM"),
         ("architectural", "A"), ("fonctionnel", "F"))
LETTERS = {"L": "L", "D": "D", "DM": "DM", "A": "A", "F": "F"}
LEGEND = ("L = livrable, D = documentaire, DM = demonstration, "
          "A = architectural, F = fonctionnel")
# THE ONE KIND THAT MUST END UP IN CODE. A documentary, architectural or
# demonstration requirement legitimately carries none: its absence is counted,
# not penalised.
#
# L LEFT THIS TUPLE ON 2026-11-20, and that is a correction rather than a
# loosening. A deliverable is a thing that must EXIST -- a presentation, a
# repository, a report -- and three of this dossier's four carry no line of
# code by their nature: B4-EC46 is a slide deck, B4-EC48 is five minutes of speech,
# B4-EC47 is "heberger le code sur GitHub", which was already true when it was
# first reported as missing code. Counting them as debt put three items on a
# to-do list that no amount of code could ever clear, which is the fastest way
# to teach a reader to skip a section.
#
# So L is listed apart, as what this tool cannot check: whether the thing
# exists. Saying "I do not verify this" is worth more than a false red.
NEEDS_CODE = ("F",)
IS_DELIVERABLE = "L"


def abbreviated(kind: str) -> str:
    """The kinds a requirement carries, as letters. Rendering only.

    LETTERS ARE MATCHED AS WHOLE TOKENS AND WORDS AS SUBSTRINGS, which is not
    an inconsistency but the only reading that works for both forms. "D" is a
    substring of "DM", so a substring test on letters would make every
    demonstration requirement also documentary; "architectural" is a substring
    of "architecturale", so a whole-token test on words would miss the feminine
    the source uses.
    """
    tokens = [token.strip() for token in re.split(r"[,/]", kind)]
    found = set()
    for token in tokens:
        if token in LETTERS:
            found.add(LETTERS[token])
            continue
        lowered = token.lower()
        for word, short in KINDS:
            if word in lowered:
                found.add(short)
    return ", ".join(short for _word, short in KINDS if short in found)


def read(path: Path) -> str:
    return io.open(path, encoding="utf-8").read()


def columns(header: str) -> dict[str, int]:
    """Column name -> index, read off the table's own header row.

    BY NAME AND NOT BY POSITION. The previous version counted cells, so it read
    the Source column as the kind the day a column was inserted -- and a matrix
    that mis-reads a column still renders, which is the kind of defect that
    survives a review.
    """
    cells = [cell.strip() for cell in header.split("|")]
    return {name: index for index, name in enumerate(cells) if name}


def defined() -> dict[str, dict]:
    """Requirements as defined: identifier -> statement, kind, competency.

    The competency is carried down from the last section heading seen, which is
    how the source groups them.
    """
    head: dict[str, int] | None = None
    competency = ""
    out: dict[str, dict] = {}
    for line in read(REQUIREMENTS).split("\n"):
        section = SECTION.match(line)
        if section:
            competency = section.group(1)
            continue
        if head is None:
            if line.startswith("|") and "Exigence" in line and "Type" in line:
                head = columns(line)
            continue
        found = re.match(r"^\|\s*\*{0,2}(B4-EC[0-9]{2})\*{0,2}\s*\|", line)
        if not found:
            continue
        cells = [cell.strip() for cell in line.split("|")]

        def cell(name: str) -> str:
            index = head.get(name, -1)
            return re.sub(r"\s+", " ", cells[index]) if 0 <= index < len(cells) else ""

        out[found.group(1)] = {
            "statement": cell("Exigence"),
            "kind": abbreviated(cell("Type")),
            "competency": competency,
            "source": cell("Source"),
        }
    if head is None:
        raise LookupError(
            "no requirements table found in %s: the header row naming "
            "Exigence and Type is what locates the columns, and without it "
            "the matrix would be generated empty -- which reads as a success."
            % REQUIREMENTS)
    return out


def tagged() -> tuple[dict, dict, dict]:
    """The "Covers:" marks of the code: in scope, out of scope, and what each
    marking block declares about its own competency.

    Requirement -> code locations, each location naming the file, the line and
    the enclosing function so the reader lands on the thing itself.
    """
    inside: dict[str, list[str]] = {}
    outside: dict[str, list[str]] = {}
    declared: dict[str, dict[str, tuple]] = {}
    for sub in SCANNED:
        folder = ROOT / sub
        if not folder.exists():
            continue
        for path in sorted(folder.rglob("*")):
            if path.suffix not in (".py", ".sql", ".yml", ".yaml"):
                continue
            lines = read(path).split("\n")
            for number, line in enumerate(lines):
                mark = MARK.search(line)
                if not mark:
                    continue
                scope = ""
                for previous in reversed(lines[:number]):
                    enclosing = re.match(r"\s*(?:def|class)\s+(\w+)", previous)
                    if enclosing:
                        scope = " " + enclosing.group(1) + "()"
                        break
                # as_posix(), NOT str(): on Windows str() yields
                # "ml4fd\\serve.py", so the generated matrix differed
                # from the one generated on Linux -- the same
                # repository, two documents -- and a reader comparing
                # two dossiers would see every path change. It also
                # broke a test of tests/test_traceability.py, which
                # looked for a path the repository spells with slashes.
                where = "%s:%d%s" % (path.relative_to(ROOT).as_posix(),
                                     number + 1, scope)
                # THE SECOND DECLARATION OF THE SAME BLOCK. A marking block
                # reads Block, Lot, Competency, Covers, in that order, so the
                # Competency: line sits just ABOVE Covers:, and the two say the
                # same thing twice -- which is what makes them checkable
                # against the document's own grouping. See disagreements().
                # The window looked below Covers: until 2026-11-21, when the
                # blocks were reordered; it then saw no competency at all and
                # compared nothing, silently.
                block = "\n".join(lines[max(0, number - 3):number + 1])
                said = COMPETENCY.search(block)
                claimed = tuple(COMP.findall(said.group(1))) if said else ()
                for name in IN_SCOPE.findall(mark.group(1)):
                    inside.setdefault(name, []).append(where)
                    declared.setdefault(name, {})[where] = claimed
                for name in OUT_OF_SCOPE.findall(mark.group(1)):
                    outside.setdefault(name, []).append(where)
    return inside, outside, declared


def breaks(req: dict[str, dict], code: dict[str, list[str]]) -> list[str]:
    """The one direction that can still be wrong: code naming a requirement the
    document does not define. A typo in a "Covers:" mark would otherwise look
    exactly like coverage.
    """
    return ["code marks %s (at %s), which the requirements do not define"
            % (name, ", ".join(places))
            for name, places in sorted(code.items()) if name not in req]


def disagreements(req: dict[str, dict], declared: dict) -> list[tuple]:
    """Marks whose own Competency line contradicts the document's grouping.

    WHAT THIS CATCHES, AND WHY NO SEMANTICS ARE NEEDED. The requirements
    document files every EC under a competency section; a marking block in the
    code declares a competency of its own, just above its Covers:. Two
    independent declarations of the same fact -- so they can be compared, and
    when they disagree one of them is wrong.

    MEASURED 2026-11-20, the first time this ran: the EC numbers of the early
    lots are the ones of the BC03 referential this project grew out of, and the
    document renumbered them. ml4fd/stream.py declares C2 and marks EC01, which
    the document files under C1 and states as "produire un cahier des charges".
    The mark is not slightly off; it names another requirement entirely.

    WHY THIS IS NOT A WARNING BUT A REFUSAL. A matrix built on those marks
    would claim that the API (EC16 to EC21) and the deployment chain (EC24 to
    EC27) are covered, by files that have nothing to do with either, while L5
    and L6 are still to be written. An uncovered requirement shown as covered
    is worse than no matrix at all: it is the one error a traceability document
    exists to make impossible.

    A mark with no Competency line is not judged. Absence is not disagreement,
    and inventing a verdict for it would put noise in a report whose whole
    value is that every line in it means something.

    Block: B3, B4.
    Lot: B3-?, B4-L0.
    Competency: B3-C2.
    Covers: B3-EC07.
    """
    out = []
    for name in sorted(declared):
        filed = req.get(name, {}).get("competency")
        if not filed:
            continue
        for where, claimed in sorted(declared[name].items()):
            if claimed and filed not in claimed:
                out.append((name, filed, ", ".join(claimed), where))
    return out


def by_competency(req: dict[str, dict], code: dict,
                  no_code: list[str]) -> list[str]:
    """One line per competency: carried, and still to carry.

    THE ACTUAL TO-DO LIST, and the reason it is worth a function. The flat list
    of 304 disagreeing marks is unreadable; grouped by competency it says in
    six lines where the work is. On 2026-11-20 it said: C6 complete, C5 one
    short, and C1 to C4 carried by marks of the referential this project grew
    out of, which the requirements document has since renumbered.
    """
    missing = set(no_code)
    buckets: dict[str, list[str]] = {}
    for name in sorted(req):
        buckets.setdefault(req[name]["competency"] or "-", []).append(name)
    out = []
    for competency, names in sorted(buckets.items()):
        absent = [name for name in names if name in missing]
        out.append("%s : %d/%d portee(s) par du code%s"
                   % (competency, len(names) - len(absent), len(names),
                      ", manque " + ", ".join(absent) if absent else ""))
    return out


def coverage(req: dict[str, dict], code: dict) -> tuple[list, list]:
    """Which requirements carry a "Covers:" mark, and which do not.

    PLAIN, AND DELIBERATELY SO. An earlier version of this weighted the count
    by the competency cross-check below -- and the weighting changed the answer
    by fifteen requirements depending on whether one agreeing mark was enough
    or all of them had to agree. Both readings are arguable, which is exactly
    why neither belongs in a figure somebody will quote at an examination. The
    count says what it can prove: this requirement is marked, that one is not.
    The cross-check stays, beside the table, as what it is -- a review aid.
    """
    carried = [name for name in sorted(req) if code.get(name)]
    no_code = [name for name in sorted(req) if not code.get(name)]
    return carried, no_code


def render(req: dict[str, dict], code: dict[str, list[str]],
           outside: dict[str, list[str]], broken: list[str],
           suspect: list[tuple], declared: dict) -> str:
    carried, no_code = coverage(req, code)
    flagged = {(name, where) for name, _filed, _claimed, where in suspect}
    def kinds_of(name: str) -> list[str]:
        return req[name]["kind"].split(", ")

    needs = [name for name in no_code
             if any(letter in kinds_of(name) for letter in NEEDS_CODE)]
    deliverables = [name for name in sorted(req)
                    if IS_DELIVERABLE in kinds_of(name)]

    out = ["# Matrice de tracabilite", "",
           "> Fichier **genere** par `tools/build_traceability.py`. "
           "Ne pas editer a la main.",
           "> Les emplacements viennent des marques `Covers:` du code.",
           "> Perimetre : les exigences de competence (B4-EC) de "
           "`step-0/ml4fd-project--d0-requirements.md`.",
           "",
           "> **A lire avant de se fier a la colonne Code.** BC03 et BC04 "
           "numerotent chacun leurs exigences a partir de EC01 : un `EC07` "
           "nu designe deux choses. Depuis le 21/11/2026, chaque marque porte "
           "son prefixe, et seules les marques `B4-` comptent ici. Les "
           "marques `B3-` -- l'heritage de BC03, dont ce projet est issu -- "
           "sont listees a part, hors perimetre, et ne couvrent aucune "
           "exigence de ce document : `ml4fd/stream.py` marque `B3-EC01`, "
           "qui n'est pas `B4-EC01` (*produire un cahier des charges*). Une "
           "marque sans prefixe n'est comptee nulle part.",
           "",
           "> Nature : " + LEGEND + ".",
           "",
           "| Exigence | Comp. | Nature | Enonce | Code |",
           "| --- | --- | --- | --- | --- |"]
    for name in sorted(req):
        entry = req[name]
        shown = ["`%s`%s" % (place, " (!)" if (name, place) in flagged else "")
                 for place in code.get(name, [])]
        out.append("| **%s** | %s | %s | %s | %s |"
                   % (name, entry["competency"] or "-", entry["kind"] or "-",
                      entry["statement"][:118], "<br/>".join(shown) or "-"))

    out += ["", "## Decompte", "",
            "- exigences definies : **%d**" % len(req),
            "- portees par du code : **%d**" % len(carried),
            "- sans code : **%d**, dont de nature fonctionnelle : **%d**"
            % (len(no_code), len(needs)),
            "- livrables, dont l'existence n'est pas verifiee ici : **%d**"
            % len(deliverables),
            "- marques a verifier (desaccord de competence) : **%d**"
            % len(suspect),
            "- ruptures de tracabilite : **%d**" % len(broken)]
    if needs:
        out += ["", "### Fonctionnelles, sans code", "",
                "Une exigence de nature F sans emplacement est soit non "
                "realisee, soit realisee sans marque `Covers:`. Les deux se "
                "corrigent, et pas au meme endroit.", "",
                ", ".join("**%s**" % name for name in needs)]
    if deliverables:
        out += ["", "### Livrables, hors code", "",
                "Une exigence de nature L designe une chose qui doit EXISTER "
                "-- une presentation, un depot, un rapport -- et non une ligne "
                "de code. Cet outil ne peut pas verifier son existence : il la "
                "nomme, et le dire vaut mieux qu'un faux rouge.", "",
                ", ".join("**%s**" % name for name in deliverables)]
    out += ["", "## Par competence", ""]
    out += ["- " + line for line in by_competency(req, code, no_code)]
    if suspect:
        out += ["", "### Marques en desaccord (aide au repointage)", "",
                "Une marque `Covers:` dont la ligne `Competency:` contredit la "
                "section sous laquelle le document classe l'exigence. Les deux "
                "sont des declarations du meme fait, donc l'une des deux est "
                "fausse. Une frontiere reelle en est un cas particulier et se "
                "corrige du cote de la marque : B4-EC33 est declenchee par le "
                "monitoring (B4-C6) et executee par le reentrainement (B4-C5), et "
                "`tests/test_monitor.py` a cesse d'y figurer le jour ou sa "
                "ligne `Competency:` a dit les deux. Ces emplacements portent "
                "**(!)** dans la colonne Code. Ils ne changent aucun compte : "
                "ce tableau trie, il ne juge pas.", "",
                "| Exigence | Document | Marque | Emplacement |",
                "| --- | --- | --- | --- |"]
        out += ["| **%s** | %s | %s | `%s` |" % row for row in suspect]
    if outside:
        out += ["", "### Hors perimetre : l'heritage de BC03", "",
                "Le code porte aussi des marques `B3-` -- EE, EI et EC du "
                "referentiel de BC03, dont ce projet reprend la base de code "
                "(lot B4-L0). Elles sont listees ici pour ne pas etre perdues, "
                "et ne comptent ni comme couverture ni comme ruptures.", "",
                ", ".join("**%s** (%d)" % (name, len(places))
                          for name, places in sorted(outside.items()))]
    return "\n".join(out) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the traceability matrix from the EC requirements "
                    "and the Covers: marks of the code.")
    parser.add_argument("--check", action="store_true",
                        help="write nothing; non-zero on a traceability break")
    args = parser.parse_args(argv)

    req = defined()
    code, outside, declared = tagged()
    broken = breaks(req, code)
    suspect = disagreements(req, declared)
    carried, no_code = coverage(req, code)

    if args.check:
        for one in broken:
            print("  rupture  %s" % one)
        print("\n  %d exigences definies ; %d portees par du code ; "
              "%d sans code" % (len(req), len(carried), len(no_code)))
        if suspect:
            print("  %d marque(s) a verifier (desaccord de competence)"
                  % len(suspect))
            for line in by_competency(req, code, no_code):
                print("    %s" % line)
        if outside:
            print("  %d identifiants B3- hors perimetre, non comptes"
                  % len(outside))
        # A DISAGREEMENT IS NOT A FAILURE HERE. 304 of them on the first
        # run, and some are legitimate -- see coverage(). A chain that is red
        # from the start tells nobody anything, so only a mark naming a
        # requirement the document does not define stops this.
        if broken:
            print("\n  echec : %d rupture(s) de tracabilite" % len(broken))
            return 1
        print("  ok : aucune marque ne nomme une exigence inconnue")
        return 0

    MATRIX.write_text(render(req, code, outside, broken, suspect, declared),
                      encoding="utf-8")
    print("%s ecrit : %d exigences, %d portees par du code, %d desaccord(s), "
          "%d rupture(s)" % (MATRIX.relative_to(ROOT), len(req), len(carried),
                             len(suspect), len(broken)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
