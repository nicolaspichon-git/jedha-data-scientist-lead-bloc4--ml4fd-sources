"""Connections and SQL execution.

Covers: EC05, EC06, EC19
Lot: L0
Competency: C2, C4

SQL lives under sql/, not in Python strings: that is what keeps it readable and
comparable -- see sql/functions/ and sql/backfill/.

One accepted limitation: psycopg runs a single statement per call, whereas the
files under sql/ddl/ hold several and contain $$...$$ blocks. The DDL therefore
goes through "psql -f", via .\\fd-manager.ps1 init, and this module only executes
single-statement files (the function, the backfill, the checks).
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

from ml4fd import config

# THE DRIVER IS IMPORTED WHERE IT IS USED, which is the convention every other
# module of ml4fd/ already follows: build.py, datasets.py and features.py all
# annotate "psycopg.Connection" as a string so that importing them does not
# require the driver. This module was the only exception, by accident rather
# than by decision -- it needs psycopg in exactly two places at run time,
# connect() and the UndefinedObject it catches there.
#
# WHAT IT BUYS, measured 2026-11-21: the test suite imports db for its SQL
# loading and its constants and never opens a connection, so a machine where
# libpq cannot load had the WHOLE suite refuse to collect -- Windows
# application control blocked the DLL inside psycopg_binary, and sixty test
# modules died on an import none of them needed. The suite now runs there.
# ml4fd/config.py's own note on pinning the rule set applies here too: a
# dependency that only some code needs should be imported by that code.
if TYPE_CHECKING:  # pragma: no cover - annotations only
    import psycopg

# THE ANNOTATIONS ARE QUOTED, like build.py's and features.py's. Under
# `from __future__ import annotations` they are never evaluated, so an unquoted
# one would run fine -- but tests/test_module_scope.py reads the SYNTAX, and it
# guards a defect worth guarding: a name imported inside one function and used
# in another. Quoting says to that reader what the future import says to the
# interpreter, rather than asking it to be less strict.

SQL_DIRS = (Path("/opt/project/sql"), Path(__file__).resolve().parent.parent / "sql")

STORES = ("fd_raw_events", "fd_quarantine", "fd_transactions",
          "fd_ground_truth", "fd_pipeline_runs")


class RoleMissing(RuntimeError):
    """A role this code needs is not in the database yet. Named, with the
    command that creates it, rather than a driver error on SET ROLE."""


def sql_path(relative: str) -> Path:
    for base in SQL_DIRS:
        p = base / relative
        if p.is_file():
            return p
    raise FileNotFoundError("%s not found under %s" % (relative, SQL_DIRS))


@contextmanager
def connect(role: str = "fd_ingest", settings: config.Settings | None = None
            ) -> Iterator["psycopg.Connection"]:
    """Open a connection, then switch to the requested role.

    fd_ingest and fd_report have no LOGIN attribute: we connect as POSTGRES_USER
    and then SET ROLE. The session loses its superuser attribute at that point,
    so the revocation on fd_raw_events genuinely applies and the append-only
    property becomes verifiable at run time.
    """
    import psycopg

    s = settings or config.load()
    with psycopg.connect(**s.pg_params()) as conn:
        if role:
            # An identifier cannot be a SQL parameter, so validate it by hand.
            if not role.replace("_", "").isalnum():
                raise ValueError("invalid role name: %r" % role)
            with conn.cursor() as cur:
                try:
                    cur.execute("SET ROLE %s" % role)
                except psycopg.errors.UndefinedObject as problem:
                    # A ROLE ADDED BY A LATER LOT, on a database the DDL has
                    # not been replayed on. The driver says `role "fd_purge"
                    # does not exist`, which sends the reader hunting; the
                    # remedy is one command and belongs in the message.
                    raise RoleMissing(
                        "le role %s n'existe pas dans cette base : le DDL n'y "
                        "a pas ete rejoue depuis qu'il a ete ajoute.\n"
                        "  Appliquer le DDL :  .\\fd-manager.ps1 init\n"
                        "Il est additif et rejouable : rien n'est perdu."
                        % role) from problem
        yield conn


def staging_table(cur, name: str, like: str) -> None:
    """An EMPTY temporary table shaped like `like`, usable any number of times
    in one transaction. ONE definition, for every COPY-then-INSERT loader.

    THE DEFECT OF 2026-11-17, in dag_ingest_verdicts: `relation
    "tmp_fd_ground_truth" already exists`. The loaders created their staging
    table with ON COMMIT DROP -- gone at COMMIT, not at the end of the call --
    and verdicts.write_due() calls the loader once PER BATCH inside a single
    transaction. One batch awaiting a verdict, as every night until then, never
    showed it; the afternoon's batches did, on the second one. The loader
    carried an unwritten contract: callable once per transaction.

    So: created if absent, EMPTIED if present. ON COMMIT DROP is kept, so the
    session is clean after the commit as before. TRUNCATE rather than DELETE:
    the table is private to this session, and nothing else can see it.
    """
    if not (name.replace("_", "").isalnum() and like.replace("_", "").isalnum()):
        raise ValueError("invalid identifier: %r / %r" % (name, like))
    cur.execute("CREATE TEMP TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS) "
                "ON COMMIT DROP" % (name, like))
    cur.execute("TRUNCATE %s" % name)


def execute_file(conn: "psycopg.Connection", relative: str, **params: Any) -> int:
    """Run a single-statement .sql file. Returns the number of rows affected."""
    with conn.cursor() as cur:
        cur.execute(sql_path(relative).read_text(encoding="utf-8"), params or None)
        return cur.rowcount


def query_file(conn: "psycopg.Connection", relative: str, **params: Any) -> list[tuple]:
    """Run a single-statement .sql file and return its rows."""
    with conn.cursor() as cur:
        cur.execute(sql_path(relative).read_text(encoding="utf-8"), params or None)
        return cur.fetchall()


def query_file_columns(conn: "psycopg.Connection", relative: str, **params: Any
                       ) -> tuple[list[tuple], list[str]]:
    """Run a single-statement .sql file and return its rows AND column names.

    The names come from cursor.description, so a caller building a DataFrame
    does not keep a second copy of the select list in Python. That second copy
    is the whole reason this exists: N4 scores with the column ORDER the model's
    recipe asks for, and a Python tuple drifting from the SELECT would reorder
    the features silently -- scoring every row against the wrong ones without
    raising.

    Covers: EE07
    Lot: N4
    Competency: C5
    """
    with conn.cursor() as cur:
        cur.execute(sql_path(relative).read_text(encoding="utf-8"),
                    params or None)
        names = [column.name for column in (cur.description or ())]
        return cur.fetchall(), names


def table_counts(conn: "psycopg.Connection") -> dict[str, int]:
    """Row count of the five stores. Serves as a plumbing check."""
    out = {}
    with conn.cursor() as cur:
        for t in STORES:
            cur.execute("SELECT count(*) FROM %s" % t)
            out[t] = cur.fetchone()[0]
    return out
