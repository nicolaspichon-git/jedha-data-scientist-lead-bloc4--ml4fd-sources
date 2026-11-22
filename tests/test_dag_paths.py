"""No tool decides on its own where the DAG files are.

Block: B3, B4.
Lot: B4-L0.
Competency: ?.
Covers: ?.

The repo keeps them in airflow/dags; docker-compose.yml mounts that directory
at /opt/airflow/dags, which is NOT under the project root the tools are given
inside the scheduler. A tool that builds the path from its root therefore works
in every test (which hand it the repo) and fails in the only place it runs.

That defect was written twice. The first fix came with a test on the constant
of the tool it was found in, and the second tool, with its own derivation, went
straight past it. These tests hold the BEHAVIOUR instead: one definition, in
ml4fd.config, and nothing else in the code spelling the path.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from ml4fd import config

ROOT = Path(__file__).resolve().parents[1]
OWNER = ROOT / "ml4fd" / "config.py"
SCANNED = ("ml4fd", "tools", "airflow")


def _sources():
    for directory in SCANNED:
        for path in sorted((ROOT / directory).rglob("*.py")):
            if path != OWNER:
                yield path


def _docstrings(tree):
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                             ast.AsyncFunctionDef)):
            body = node.body
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)):
                found.add(id(body[0].value))
    return found


def spelled_paths(source: str):
    """String literals of the CODE that spell a DAG directory.

    Read through the AST: comments are gone and docstrings are set aside, so
    prose may name the path freely -- it has to, to explain any of this.
    """
    tree = ast.parse(source)
    prose = _docstrings(tree)
    hits = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and id(node) not in prose):
            value = node.value.replace("\\", "/")
            if "airflow/dags" in value or value.strip("/") == "dags":
                hits.append((node.lineno, node.value))
    return hits


@pytest.mark.parametrize("path", list(_sources()),
                         ids=lambda p: str(p.relative_to(ROOT)))
def test_no_source_spells_the_dag_directory_itself(path):
    hits = spelled_paths(path.read_text(encoding="utf-8"))
    assert not hits, (
        "%s builds a DAG path itself at %s; ask ml4fd.config.dags_dir()"
        % (path.relative_to(ROOT), hits))


def test_the_detector_sees_both_ways_of_spelling_it():
    """The guard, proven on the two forms the defect took."""
    joined = 'dag = root / "airflow" / "dags" / "dag_daily.py"\n'
    literal = 'DIRS = ("airflow/dags", "/opt/airflow/dags")\n'
    assert spelled_paths(joined)
    assert len(spelled_paths(literal)) == 2


def test_prose_may_name_the_path():
    source = ('"""Mounted at /opt/airflow/dags."""\n'
              "# see airflow/dags\n"
              "def f():\n"
              '    """Reads airflow/dags."""\n'
              "    return 1\n")
    assert spelled_paths(source) == []


def test_the_one_definition_names_what_compose_mounts():
    """Change the mount and the candidates must change with it."""
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "./airflow/dags:/opt/airflow/dags" in compose
    assert config.DAG_DIRS == ("airflow/dags", "/opt/airflow/dags")


def test_the_scan_covers_the_tools_that_read_dags():
    names = {path.name for path in _sources()}
    assert {"check_s3.py", "check_s4.py"} <= names
