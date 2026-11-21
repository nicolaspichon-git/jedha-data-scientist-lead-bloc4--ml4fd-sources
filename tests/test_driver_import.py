"""The database driver is imported where it is used, and not on import.

Covers: EC23
Lot: L5
Competency: C3, C4

WHY THIS IS A RULE AND NOT A STYLE. ml4fd/build.py, datasets.py, features.py
and guard.py already annotate "psycopg.Connection" under TYPE_CHECKING, so
importing them does not require the driver. db.py was the only module that
imported it for real, by accident rather than by decision, and on 2026-11-21
that accident cost the whole suite: Windows application control refused to
load the DLL inside psycopg_binary, db.py failed on import, and sixty test
modules refused to collect over a driver none of them opens a connection with.

THE SUITE OPENS NO CONNECTION. Every test either reads SQL files, parses
source, or hands a fake connection to a function. So the driver is a run-time
dependency of the processes that talk to PostgreSQL -- the DAGs, the tools, the
service, the dashboard -- and not of the code being tested. A module-level
import makes it a dependency of simply naming the module.

READ FROM THE SYNTAX, because the distinction is syntactic: an import at module
level is a dependency of the import, one inside a function or under
TYPE_CHECKING is not. A grep could not tell them apart.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DRIVER = "psycopg"
MODULES = sorted(path for path in (ROOT / "ml4fd").glob("*.py"))


def _imported_on_import(source: str) -> set[str]:
    """Modules imported when this source is imported: the module body, minus
    anything inside a function, a class, or an `if TYPE_CHECKING` block.

    TAKES SOURCE AND NOT A PATH, so the three tests below can hand it three
    lines instead of writing a probe file into tests/ -- which pytest would
    collect, and which the first version of this file did.
    """
    tree = ast.parse(source)
    found: set[str] = set()

    def gather(node):
        if isinstance(node, ast.Import):
            found.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            found.add(node.module.split(".")[0])

    for node in tree.body:
        gather(node)
        if isinstance(node, ast.If):
            # TYPE_CHECKING is false at run time, so what it holds is not a
            # dependency of the import. Any OTHER condition is.
            if "TYPE_CHECKING" in ast.unparse(node.test):
                continue
            for inner in ast.walk(node):
                gather(inner)
    return found


def _of(path: Path) -> set[str]:
    return _imported_on_import(path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("path", MODULES, ids=lambda p: p.name)
def test_no_module_of_ml4fd_needs_the_driver_to_be_imported(path):
    assert DRIVER not in _of(path), (
        "%s importe %s au niveau module : une machine sans libpq ne peut plus "
        "collecter la suite" % (path.name, DRIVER))


def test_the_helper_sees_a_module_level_import():
    """The test that tests this test: a rule read from the syntax is worth
    exactly what its reader is worth."""
    assert DRIVER in _imported_on_import("import psycopg\n")
    assert DRIVER in _imported_on_import("from psycopg import connect\n")


def test_the_helper_ignores_an_import_under_type_checking():
    assert DRIVER not in _imported_on_import(
        "from typing import TYPE_CHECKING\n"
        "if TYPE_CHECKING:\n    import psycopg\n")


def test_the_helper_does_not_ignore_an_import_under_another_condition():
    """`if sys.platform == "win32": import psycopg` IS a dependency of the
    import, and a reader that skipped every `if` would have missed it."""
    assert DRIVER in _imported_on_import(
        "import sys\n"
        "if sys.platform == 'win32':\n    import psycopg\n")


def test_the_helper_ignores_an_import_inside_a_function():
    assert DRIVER not in _imported_on_import(
        "def connect():\n    import psycopg\n    return psycopg\n")


def test_the_connection_is_still_opened_by_db():
    """The rule must not be satisfied by removing the driver altogether:
    db.connect() imports it where it connects."""
    body = (ROOT / "ml4fd" / "db.py").read_text(encoding="utf-8")
    tree = ast.parse(body)
    inside = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "connect":
            inside = [alias.name for inner in ast.walk(node)
                      if isinstance(inner, ast.Import)
                      for alias in inner.names]
    assert DRIVER in inside, "db.connect() doit importer le pilote lui-meme"
