"""The traceability matrix abbreviates the kind of a requirement.

Covers: EC07
Lot: L8
Competency: C1

The matrix is read column by column against D0, and the Nature column held the
requirements document's own prose -- "architecturale", "documentaire,
architecturale", and once "critere d'acceptation, architectural", which is a
kind and something else at the same time. One letter per kind reads at a glance;
the point of the test is that the SAME pair always reads the same way, whatever
order the source text spells it in, and that the letters are explained in the
document rather than left to be guessed.
"""
from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MATRIX = ROOT / "TRACEABILITY.md"

# THE MATRIX IS GENERATED, NOT WRITTEN (tools/build_traceability.py). It was
# repointed on 2026-11-20 -- at step-0/ml4fd-project--d0-requirements.md, the
# EC of this dossier -- and generated for the first time, so the two guards
# below are armed now. They were written to abstain while the file was absent
# rather than fail, which is why the chain was green through the months the
# generator read two documents that did not exist. The abstention stays: it
# costs nothing and it is what makes a shape test safe to write early.
needs_matrix = pytest.mark.skipif(
    not MATRIX.exists(),
    reason="TRACEABILITY.md has not been generated for this dossier yet "
           "(tools/build_traceability.py still points at the BC03 "
           "documents; L7 repoints it).")

_spec = importlib.util.spec_from_file_location(
    "build_traceability", ROOT / "tools" / "build_traceability.py")
bt = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bt)


def test_each_kind_has_its_letter():
    assert bt.abbreviated("livrable") == "L"
    assert bt.abbreviated("documentaire") == "D"
    assert bt.abbreviated("architecturale") == "A"
    assert bt.abbreviated("fonctionnelle") == "F"


def test_the_order_is_the_tables_own_and_not_the_sources():
    """Two requirements of the same two kinds must not read differently because
    the cahier des charges happened to spell them the other way round."""
    assert bt.abbreviated("documentaire, architecturale") == "D, A"
    assert bt.abbreviated("architecturale, documentaire") == "D, A"


def test_what_is_not_a_kind_is_dropped():
    """"critere d'acceptation" is a status, not a nature: the column keeps the
    nature alone."""
    assert bt.abbreviated("critère d'acceptation, architectural") == "A"


def test_an_unstated_kind_stays_empty_rather_than_guessed():
    assert bt.abbreviated("") == ""
    assert bt.abbreviated("autre chose") == ""


@needs_matrix
def test_the_generated_matrix_carries_nothing_but_letters():
    """BY HEADER NAME AND NOT BY POSITION, since 2026-11-20: the table gained a
    Comp. column and this test read it as the Nature one, failing on 'C1'. A
    shape test anchored on a column INDEX breaks whenever the table it checks
    is improved, which trains the reader to adjust the test instead of reading
    what it says."""
    lines = MATRIX.read_text(encoding="utf-8").splitlines()
    start = next(i for i, line in enumerate(lines)
                 if line.startswith("|") and "Nature" in line)
    cells = [cell.strip() for cell in lines[start].split("|")]
    nature = cells.index("Nature")
    # BOUNDED TO ITS OWN TABLE. The document gained a second table whose rows
    # also begin with "| **" -- the disagreements, whose third column holds a
    # competency -- and an unbounded scan read 'C2' as a nature. A shape test
    # has to say WHICH table it is about.
    rows = []
    for line in lines[start + 2:]:
        if not line.startswith("|"):
            break
        rows.append(line)
    assert rows, "the matrix holds no requirement row"
    kinds = "|".join(short for _word, short in bt.KINDS)
    allowed = re.compile(r"^(?:(?:%s)(?:, (?:%s))*|[-\u2014])$" % (kinds, kinds))
    for row in rows:
        cell = row.split("|")[nature].strip()
        assert allowed.match(cell), "Nature column holds %r" % cell


@needs_matrix
def test_the_letters_are_explained_in_the_document():
    """A single letter in a column is unreadable without its legend, and the
    legend is generated with the table so the two cannot drift apart."""
    text = MATRIX.read_text(encoding="utf-8")
    assert bt.LEGEND in text
    for word, _short in bt.KINDS:
        assert word in bt.LEGEND


# --- L4: the generator reads the right document, and reads it by name --------

def test_the_requirements_document_exists():
    """THE FAILURE THIS FORBIDS, and it is the one that hid for months.
    REQUIREMENTS pointed at a BC03 file absent from this dossier, so defined()
    returned {} , the matrix was never written, and --check printed
    "0 requirements defined" and exited 0. An empty expectation is met by
    anything."""
    assert bt.REQUIREMENTS.exists(), bt.REQUIREMENTS


def test_a_document_without_a_requirements_table_is_refused(tmp_path):
    """The same defect, guarded at the function rather than at the path: no
    header row means no columns to read, and a matrix generated from nothing
    reads as a success. Exercised rather than asserted about the source text --
    a test that greps for a comment passes the day the comment survives the
    code being removed."""
    empty = tmp_path / "no-table.md"
    empty.write_text("# Exigences\n\nRien ici.\n", encoding="utf-8")
    kept = bt.REQUIREMENTS
    bt.REQUIREMENTS = empty
    try:
        with pytest.raises(LookupError, match="no requirements table"):
            bt.defined()
    finally:
        bt.REQUIREMENTS = kept


def test_the_columns_are_located_by_name():
    head = bt.columns("| # | Exigence | Source | Type | Ver. |")
    assert head["Exigence"] == 2 and head["Type"] == 4
    # a column inserted ahead of them must move them, not break the reading
    moved = bt.columns("| # | Lot | Exigence | Source | Type | Ver. |")
    assert moved["Exigence"] == 3 and moved["Type"] == 5


def test_every_requirement_of_the_document_is_read():
    req = bt.defined()
    assert len(req) == 51, len(req)
    assert req["EC01"]["competency"] == "C1"
    assert req["EC45"]["competency"] == "C6"
    assert req["EC45"]["statement"].startswith("Conserver l'historique")


def test_the_competency_comes_from_the_section_and_not_the_source_column():
    """Source says where a requirement comes from ("C2/EC05", "R1§Ex.") and
    carries a competency only most of the time; the section heading says which
    competency it belongs to, always."""
    req = bt.defined()
    assert req["EC02"]["source"] != req["EC02"]["competency"]
    assert req["EC02"]["competency"] == "C1"


# --- L4: only the EC are in scope, and the rest is not a break ---------------

def test_the_ee_and_ei_marks_are_counted_and_never_reported_as_breaks():
    """The code still carries marks of the referential whose complete cahier
    des charges is being written. Naming a requirement this matrix does not
    read yet is not the same as naming nothing, and reporting them as breaks
    would train the reader to ignore the report."""
    code, outside, _declared = bt.tagged()
    assert outside, "no EE/EI mark found: the tolerance has stopped being used"
    assert not any(name.startswith(("EE", "EI")) for name in code)
    req = bt.defined()
    assert bt.breaks(req, code) == []


def test_a_mark_naming_an_unknown_ec_is_a_break():
    """The one direction that can still be wrong: a typo in a Covers: mark
    would otherwise look exactly like coverage."""
    found = bt.breaks({"EC01": {}}, {"EC99": ["ml4fd/nowhere.py:1"]})
    assert len(found) == 1
    assert "EC99" in found[0] and "do not define" in found[0]


# --- L4: the counts stay factual --------------------------------------------

def test_the_count_says_marked_or_not_and_nothing_cleverer():
    """An earlier version weighted this by the competency cross-check, and the
    weighting moved the answer by fifteen requirements depending on whether one
    agreeing mark was enough or all of them had to agree. Both readings are
    arguable, which is why neither belongs in a figure quoted at an
    examination."""
    req = {"EC01": {}, "EC02": {}}
    carried, no_code = bt.coverage(req, {"EC01": ["a.py:1"]})
    assert carried == ["EC01"] and no_code == ["EC02"]


def test_a_disagreement_does_not_fail_the_check():
    """304 of them on the first run, some legitimate: EC33 is filed under C5
    and marked from tests/test_monitor.py, which declares C6, because
    monitoring triggering a retraining straddles the two. A chain that is red
    from the start tells nobody anything."""
    body = (ROOT / "tools" / "build_traceability.py").read_text(
        encoding="utf-8")
    check = body[body.index("if args.check:"):]
    assert "if broken:" in check
    assert "if broken or suspect:" not in check


def test_the_cross_check_compares_two_declarations_of_the_same_fact():
    req = {"EC01": {"competency": "C1"}}
    declared = {"EC01": {"ml4fd/stream.py:3": ("C2",),
                         "step-0/elsewhere.py:9": ("C1",)}}
    found = bt.disagreements(req, declared)
    assert [row[3] for row in found] == ["ml4fd/stream.py:3"]


def test_a_mark_without_a_competency_line_is_not_judged():
    """Absence is not disagreement, and inventing a verdict for it would put
    noise in a report whose whole value is that every line means something."""
    req = {"EC01": {"competency": "C1"}}
    assert bt.disagreements(req, {"EC01": {"sql/x.sql:1": ()}}) == []


# --- L4: the document warns about what it cannot yet prove -------------------

@needs_matrix
def test_the_matrix_says_the_old_marks_cannot_be_trusted_yet():
    """A traceability matrix exists to make one error impossible: showing an
    uncovered requirement as covered. While the early-lot marks name another
    referential, the document has to say so itself -- a warning I give in chat
    is a warning the jury never reads."""
    text = MATRIX.read_text(encoding="utf-8")
    assert "renumerote" in text
    assert "EC01" in text and "stream.py" in text


# --- L4: a deliverable is not functional debt --------------------------------

def test_only_a_functional_requirement_must_carry_code():
    """L LEFT NEEDS_CODE on 2026-11-20. A deliverable is a thing that must
    EXIST -- a slide deck, a repository, five minutes of speech -- and three of
    this dossier's four carry no line of code by their nature. EC47 is
    "heberger le code sur GitHub", which was already true when it was first
    reported as missing code. Three items on a to-do list that no amount of
    code could clear is the fastest way to teach a reader to skip a section."""
    assert bt.NEEDS_CODE == ("F",)
    assert bt.IS_DELIVERABLE == "L"


@needs_matrix
def test_the_deliverables_are_named_apart_from_the_functional_gaps():
    """Named rather than hidden: "this tool does not verify whether the thing
    exists" is worth more than a false red, and worth more than silence."""
    text = MATRIX.read_text(encoding="utf-8")
    functional = text.index("### Fonctionnelles, sans code")
    deliverables = text.index("### Livrables, hors code")
    assert functional < deliverables
    gaps = text[functional:deliverables]
    for name in ("EC46", "EC47", "EC48"):
        assert name not in gaps, "%s counted as functional debt" % name


# --- L4: the three marks of this lot ----------------------------------------

def test_the_unattended_retraining_is_marked():
    """EC31 asks for a retraining that runs without a hand on it. dag_train is
    that -- guard, dataset, fit, registration, gate, alias -- and the mark was
    missing rather than the implementation."""
    req = bt.defined()
    code, _outside, _declared = bt.tagged()
    assert req["EC31"]["competency"] == "C5"
    assert any("dag_train.py" in place for place in code.get("EC31", []))


def test_the_matrix_is_marked_by_its_own_generator():
    """EC50 asks for the matrix to be KEPT, and what keeps it is the generator,
    not the file it writes."""
    code, _outside, _declared = bt.tagged()
    assert any("build_traceability.py" in place
               for place in code.get("EC50", []))


def test_competence_five_is_complete_and_undoubted():
    """The result of this lot, pinned: every EC of C5 carries a mark, and none
    of those marks disagrees with the document. If this breaks, either a
    requirement lost its code or a Competency line drifted."""
    req = bt.defined()
    code, _outside, declared = bt.tagged()
    suspect = bt.disagreements(req, declared)
    five = [name for name in req if req[name]["competency"] == "C5"]
    assert five
    for name in five:
        assert code.get(name), "%s carries no code" % name
    assert not [row for row in suspect
                if req[row[0]]["competency"] == "C5"], suspect


def test_every_directory_that_carries_marks_is_scanned():
    """THE LIST OF PLACES TO LOOK GOES STALE THE MOMENT SOMEBODY ADDS A PLACE,
    and it did: scoring_service/ was created on 2026-11-20 and SCANNED was not
    extended, so the seven marks of L5 -- EC15 to EC23 -- were not seen at all.
    The matrix reported EC23 as carried by no code while the service carrying
    it sat in the next directory.

    _to_delete/ is excluded deliberately: it holds what is on its way out, and
    counting its marks would credit the dossier with code nobody runs."""
    root = Path(__file__).resolve().parents[1]
    carrying = set()
    for folder in sorted(p for p in root.iterdir() if p.is_dir()):
        if folder.name.startswith((".", "_")) or folder.name == "vendor":
            continue
        for path in folder.rglob("*"):
            if path.suffix in (".py", ".sql") \
                    and "__pycache__" not in str(path) \
                    and "Covers:" in path.read_text(encoding="utf-8"):
                carrying.add(folder.name)
                break
    assert carrying <= set(bt.SCANNED), \
        "ces repertoires portent des marques et ne sont pas balayes : %s" \
        % sorted(carrying - set(bt.SCANNED))


def test_the_whole_of_l5_is_visible_to_the_matrix():
    """The consequence, pinned on the lot that paid for the defect."""
    code, _outside, _declared = bt.tagged()
    for name in ("EC15", "EC16", "EC17", "EC19", "EC20", "EC21", "EC23"):
        # NORMALISED, because this ran green on Linux and red on Windows: the
        # generator built its locations with str(relative_to(ROOT)), which
        # yields a backslash there. The generator now uses as_posix(), and this
        # normalisation stays so the test states what it means rather than
        # resting on the fix.
        places = [place.replace("\\", "/") for place in code.get(name, [])]
        assert any("scoring_service/app.py" in place for place in places), name
