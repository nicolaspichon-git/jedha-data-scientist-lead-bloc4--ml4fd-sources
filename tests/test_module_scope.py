"""A name a module-level function uses but another function imports.

Block: B3, B4.
Lot: B3-N4, B4-L0.
Competency: B3-C5.
Covers: B3-EC06.

THE DEFECT THIS EXISTS FOR. tools/rechoose_threshold.py imported `paired`
inside main() and used it inside render(). Python binds a function-level import
to that function's scope, so render() had no `paired` -- and the failure came
only at the END, after a probe fit and ten sweeps had run. Minutes of
computation thrown away by a reporting line.

It is the same family as two defects this project already paid for: a name used
without re-reading its definition, and a report that destroyed the work it was
reporting on. Those were fixed one at a time; this one is checked for the whole
repository, for good.

A static check and not a run: importing these modules needs Airflow, psycopg,
sklearn and mlflow, none of which is installed in the authoring environment --
which is precisely how such a name survives until production.
"""
from __future__ import annotations

import ast
import builtins
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGES = ("ml4fd", "tools", "airflow/dags")
BUILTINS = frozenset(dir(builtins))


def _sources():
    for package in PACKAGES:
        for path in sorted((ROOT / package).glob("*.py")):
            yield path


def _bound_in(function) -> set[str]:
    """Every name the function itself binds: arguments, imports, assignments,
    nested definitions, except-as, walrus."""
    bound = {arg.arg for arg in function.args.args}
    bound |= {arg.arg for arg in function.args.kwonlyargs}
    bound |= {arg.arg for arg in function.args.posonlyargs}
    if function.args.vararg:
        bound.add(function.args.vararg.arg)
    if function.args.kwarg:
        bound.add(function.args.kwarg.arg)
    for node in ast.walk(function):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                bound.add((alias.asname or alias.name).split(".")[0])
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            bound.add(node.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                               ast.ClassDef)):
            bound.add(node.name)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            bound.add(node.name)
        elif isinstance(node, ast.Global):
            bound.update(node.names)
    return bound


def _module_level(tree) -> set[str]:
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add((alias.asname or alias.name).split(".")[0])
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                               ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target,
                                                            ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.With):
            for item in node.items:
                if isinstance(item.optional_vars, ast.Name):
                    names.add(item.optional_vars.id)
    return names


def _offenders(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    functions = [node for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    module_level = _module_level(tree)

    imported_locally = {}
    for function in functions:
        for node in ast.walk(function):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    name = (alias.asname or alias.name).split(".")[0]
                    imported_locally.setdefault(name, function.name)

    found = []
    for function in functions:
        bound = _bound_in(function)
        for node in ast.walk(function):
            if not (isinstance(node, ast.Name)
                    and isinstance(node.ctx, ast.Load)):
                continue
            name = node.id
            if (name not in bound and name not in module_level
                    and name not in BUILTINS and name in imported_locally
                    and imported_locally[name] != function.name):
                found.append((function.name, name, imported_locally[name],
                              node.lineno))
    return sorted(set(found))


def test_no_function_uses_a_name_another_function_imports():
    problems = []
    for path in _sources():
        for function, name, where, line in _offenders(path):
            problems.append(
                "%s:%d  %s() emploie %r, importe seulement dans %s()"
                % (path.relative_to(ROOT), line, function, name, where))
    assert not problems, "\n".join([""] + problems)


def test_the_check_itself_catches_the_defect_it_was_written_for(tmp_path):
    """A check that cannot fail proves nothing. This is the shape of the bug,
    reduced: tools/rechoose_threshold.py, 16 November 2026."""
    path = tmp_path / "offender.py"
    path.write_text(
        "def main():\n"
        "    from ml4fd import paired\n"
        "    return render(paired.Paired(0, 0, 0, 0))\n"
        "\n"
        "def render(table):\n"
        "    return paired.render(table)\n",
        encoding="utf-8")
    found = _offenders(path)
    assert found
    assert found[0][0] == "render"
    assert found[0][1] == "paired"
    assert found[0][2] == "main"


def test_the_check_does_not_fire_when_each_function_imports_what_it_uses(
        tmp_path):
    path = tmp_path / "sound.py"
    path.write_text(
        "def main():\n"
        "    from ml4fd import paired\n"
        "    return render(paired)\n"
        "\n"
        "def render(table):\n"
        "    from ml4fd import paired\n"
        "    return paired.render(table)\n",
        encoding="utf-8")
    assert _offenders(path) == []


def test_every_scanned_package_actually_holds_modules():
    """A path typo would make this file pass by scanning nothing."""
    for package in PACKAGES:
        assert list((ROOT / package).glob("*.py")), package


# --- a percent sign in a parameterised query ----------------------------------

def _placeholder_free(text: str) -> str:
    """The SQL with its placeholders and its doubled %% removed."""
    return re.sub(r"%\(\w+\)s|%s|%%", "", text)


def test_no_parameterised_query_holds_a_bare_percent_sign():
    """THE TRAP THIS CLOSES, which N9 missed by luck.

    psycopg binds parameters CLIENT-SIDE with percent formatting, so in a query
    that carries placeholders every other % must be doubled -- including one in
    a comment. This project writes long comments inside its SQL, and a
    threshold written "5 %" rather than "five per cent" in a file that takes a
    parameter would raise IndexError or ValueError at execution: not at review,
    not in a test that reads the file, but in the task that runs it.

    Only files that actually carry a placeholder are concerned: the others are
    handed to psql or executed without parameters, where % means nothing.
    """
    offenders = []
    for path in sorted((ROOT / "sql").rglob("*.sql")):
        text = path.read_text(encoding="utf-8")
        if "%(" not in text and "%s" not in text:
            continue                      # no parameter: % is just a character
        if "%" in _placeholder_free(text):
            for number, line in enumerate(text.splitlines(), 1):
                if "%" in _placeholder_free(line):
                    offenders.append("%s:%d  %s"
                                     % (path.relative_to(ROOT), number,
                                        line.strip()[:70]))
    assert not offenders, (
        "signe %% nu dans une requete parametree :\n  " + "\n  ".join(offenders))


def test_the_percent_check_catches_what_it_was_written_for(tmp_path):
    """A check that cannot fail proves nothing."""
    assert "%" in _placeholder_free("-- au-dela de 5 % des lignes\n"
                                    "SELECT 1 WHERE x = %(y)s")
    assert "%" not in _placeholder_free("-- au-dela de 5 %% des lignes\n"
                                        "SELECT 1 WHERE x = %(y)s")
