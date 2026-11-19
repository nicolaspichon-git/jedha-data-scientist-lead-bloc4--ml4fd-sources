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

# THE MATRIX IS GENERATED, NOT WRITTEN (tools/build_traceability.py), and it
# has not been generated for this dossier yet: the generator still reads the
# two BC03 documents, which do not exist here. L7 repoints it.
#
# A test on the SHAPE of a generated artefact abstains while the artefact is
# absent; it does not fail. Failing would make the first green run of the
# integration chain impossible, and a chain that is red from the start tells
# nobody anything. The guard arms itself again the day the file appears --
# nothing to remember, nothing to undo.
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
    rows = [l for l in MATRIX.read_text(encoding="utf-8").splitlines()
            if l.startswith("| **")]
    assert rows, "the matrix holds no requirement row"
    allowed = re.compile(r"^(?:[LDAF](?:, [LDAF])*|—)$")
    for row in rows:
        cell = row.split("|")[2].strip()
        assert allowed.match(cell), "Nature column holds %r" % cell


@needs_matrix
def test_the_letters_are_explained_in_the_document():
    """A single letter in a column is unreadable without its legend, and the
    legend is generated with the table so the two cannot drift apart."""
    text = MATRIX.read_text(encoding="utf-8")
    assert bt.LEGEND in text
    for word in ("livrable", "documentaire", "architectural", "fonctionnel"):
        assert word in bt.LEGEND
