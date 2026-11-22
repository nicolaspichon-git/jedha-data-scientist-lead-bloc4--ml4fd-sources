"""S5 / Q1: the fingerprint of a batch is taken, stored, and demanded.

Block: B3, B4.
Lot: B3-Q1, B4-L0.
Competency: B3-C4.
Covers: B3-EC23, B3-EI12.

WHY THIS LOT COMES BEFORE ANY PURGE. runs.digest() fingerprinted whole tables,
once, at the end of the historical load, and only wrote them to the Airflow log.
The minute DAG never computed one. The day S5 purges the raw zone and the logs
turn over, nothing would say what a batch held -- so the fingerprint is now of
the BATCH, taken by whoever writes it, stored on the run, and the purge refuses
a batch that has none.

These tests hold the four decisions, none of which a green happy path shows:
  - ONE definition of the digest, used by the writers, the backfill and the purge;
  - a writer that records nothing FAILS instead of passing in silence;
  - EVERY caller of the raw zone's writers takes one (a later writer cannot forget);
  - the purge's refusal names its reason.
"""
from __future__ import annotations

import ast
import json
import re
import uuid
from pathlib import Path

import pytest

import os

# config.load() refuses to run without them. Set here rather than
# inherited from whichever test module happens to import first.
os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import db, runs  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
BATCH = uuid.UUID("11111111-2222-3333-4444-555555555555")


# --- a connection we control -------------------------------------------------

class Conn:
    """Records what is executed and answers from a script."""

    def __init__(self, rowcounts=(), fetches=()):
        self.executed = []
        self.rowcounts = list(rowcounts)
        self.fetches = list(fetches)

    def cursor(self):
        conn = self

        class Cursor:
            rowcount = 1

            def __enter__(self): return self
            def __exit__(self, *exc): return False

            def execute(self, sql, params=None):
                conn.executed.append((sql, params))
                self.rowcount = conn.rowcounts.pop(0) if conn.rowcounts else 1

            def fetchone(self):
                return conn.fetches.pop(0) if conn.fetches else None

        return Cursor()

    def updates(self):
        return [(s, p) for s, p in self.executed if "UPDATE" in s]


@pytest.fixture
def files(monkeypatch):
    """db.query_file answered from a dict keyed by the relative SQL path."""
    answers = {}

    def fake(conn, relative, **params):
        answers.setdefault("_calls", []).append((relative, params))
        value = answers[relative]
        return value(params) if callable(value) else value

    monkeypatch.setattr(db, "query_file", fake)
    return answers


def _strip_sql(path):
    return "\n".join(line for line in path.read_text(encoding="utf-8").splitlines()
                     if not line.strip().startswith("--"))


# --- the SQL: one definition, read not guessed ---------------------------------

DIGEST_SQL = ROOT / "sql" / "checks" / "batch_digest.sql"
SQL_FILES = [DIGEST_SQL,
             ROOT / "sql" / "checks" / "undigested_batches.sql",
             ROOT / "sql" / "checks" / "batch_digest_recorded.sql"]


def test_the_digest_is_scoped_to_one_batch():
    """A digest over the whole table would change every time any batch arrived,
    and prove nothing about the one being purged."""
    text = _strip_sql(DIGEST_SQL)
    assert "WHERE  batch_id = %(batch_id)s" in text or \
        "WHERE batch_id = %(batch_id)s" in text


def test_the_digest_covers_the_line_and_the_payload_in_line_order():
    text = _strip_sql(DIGEST_SQL)
    assert "line_no::text" in text and "payload::text" in text
    assert "ORDER BY line_no" in text


def test_the_digest_hashes_per_row_and_not_one_giant_string():
    """555 thousand rows and some 200 MB of JSON per historical batch: a
    string_agg over the payloads themselves would build all of it in memory."""
    text = _strip_sql(DIGEST_SQL)
    assert re.search(r"string_agg\(\s*md5\(", text)


@pytest.mark.parametrize("path", SQL_FILES, ids=lambda p: p.name)
def test_a_percent_sign_in_these_files_is_always_a_parameter(path):
    """psycopg reads every % of a statement that has parameters. A comment
    saying "50 %" would raise at run time, in the one place nobody reads."""
    text = path.read_text(encoding="utf-8")
    stray = re.sub(r"%\(\w+\)s", "", text)
    assert "%" not in stray, "%s has a %% that is not a named parameter" % path.name


def test_a_batch_is_covered_by_any_run_that_carries_its_fingerprint():
    """Not "the run that owns it": a replay attaches a second run to the same
    batch, and the fingerprint belongs to the batch."""
    text = _strip_sql(ROOT / "sql" / "checks" / "undigested_batches.sql")
    assert "digests #>> '{raw_batch,batch_id}'" in text
    assert "NOT EXISTS" in text
    # tested per batch, not per row of a table of a million lines
    assert text.index("GROUP") < text.index("NOT EXISTS")


def test_the_listing_changes_nothing():
    text = _strip_sql(ROOT / "sql" / "checks" / "undigested_batches.sql").upper()
    for verb in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALTER", "DROP"):
        assert verb not in text


# --- the column ------------------------------------------------------------------

def test_the_column_is_declared_for_a_fresh_database_and_for_an_existing_one():
    """Declared twice on purpose, like f_home_distance_km: CREATE TABLE IF NOT
    EXISTS does nothing to a table that exists, so without the ALTER the
    existing database would keep a table with no place to put the fingerprint."""
    text = _strip_sql(ROOT / "sql" / "ddl" / "050_pipeline_runs.sql")
    create, rest = text.split(");", 1)
    assert "digests" in create
    assert re.search(r"ADD COLUMN IF NOT EXISTS digests\s+jsonb", rest)


def test_the_column_refuses_anything_but_an_object():
    text = _strip_sql(ROOT / "sql" / "ddl" / "050_pipeline_runs.sql")
    assert text.count("jsonb_typeof(digests) = 'object'") == 2


# --- taking and recording --------------------------------------------------------

def test_the_digest_comes_from_the_one_shared_file(files):
    files["checks/batch_digest.sql"] = [(555719, "abc")]
    assert runs.batch_digest(Conn(), BATCH) == {"rows": 555719, "digest": "abc"}
    assert files["_calls"] == [("checks/batch_digest.sql", {"batch_id": BATCH})]


def test_a_string_batch_id_is_accepted_as_a_uuid(files):
    """XCom and the command line hand over strings."""
    files["checks/batch_digest.sql"] = [(1, "x")]
    runs.batch_digest(Conn(), str(BATCH))
    assert files["_calls"][0][1]["batch_id"] == BATCH


def test_recording_adds_to_the_digests_and_never_replaces_them(files):
    """The historical load records the batch AND, at close, the stores. If the
    second replaced the first the batch would become impurgeable by closing."""
    files["checks/batch_digest.sql"] = [(3, "d1")]
    conn = Conn()
    runs.record_batch_digest(conn, "run-1", BATCH)
    sql, params = conn.updates()[0]
    assert "coalesce(digests, '{}'::jsonb) ||" in sql
    payload = json.loads(params[0])
    assert payload["raw_batch"] == {"rows": 3, "digest": "d1",
                                    "batch_id": str(BATCH), "origin": "ingest"}
    assert params[1] == "run-1"


def test_recording_on_a_run_that_does_not_exist_is_an_error(files):
    """RECORDING NOTHING WITHOUT SAYING SO IS HOW A BATCH BECOMES IMPURGEABLE --
    or how someone is tempted to loosen the purge."""
    files["checks/batch_digest.sql"] = [(3, "d1")]
    with pytest.raises(RuntimeError, match="has nowhere to go"):
        runs.record_batch_digest(Conn(rowcounts=[0]), "ghost", BATCH)


def test_an_unknown_origin_is_refused(files):
    files["checks/batch_digest.sql"] = [(3, "d1")]
    with pytest.raises(ValueError, match="origin"):
        runs.record_batch_digest(Conn(), "run-1", BATCH, origin="guess")


def test_the_store_digests_are_stored_beside_the_batch_one():
    conn = Conn()
    runs.record_store_digests(conn, "run-1", {"fd_transactions": (7, "t"),
                                              "fd_ground_truth": (5, "g")})
    payload = json.loads(conn.updates()[0][1][0])
    assert payload == {"stores": {"fd_transactions": {"rows": 7, "digest": "t"},
                                  "fd_ground_truth": {"rows": 5, "digest": "g"}}}


def test_the_store_digests_have_nowhere_to_go_on_a_missing_run():
    with pytest.raises(RuntimeError):
        runs.record_store_digests(Conn(rowcounts=[0]), "ghost", {})


# --- the refusal ---------------------------------------------------------------------

def _held(n=3, digest="d1", origin="ingest", run="run-1", purge=None):
    # Five columns since Q3: the purge marker is read with the fingerprint,
    # by the one query, so that the two answers cannot come from two instants.
    return [(n, digest, origin, run, purge)]


def test_a_batch_with_a_matching_fingerprint_may_be_purged(files):
    files["checks/batch_digest_recorded.sql"] = _held()
    files["checks/batch_digest.sql"] = [(3, "d1")]
    assert runs.refusal(Conn(), BATCH) is None


def test_a_batch_without_a_fingerprint_is_refused_and_says_why(files):
    files["checks/batch_digest_recorded.sql"] = []
    code, why = runs.refusal(Conn(), BATCH)
    assert code == runs.NO_DIGEST == "no_digest"
    assert str(BATCH) in why
    assert "digests -Apply" in why          # the way out is in the message


@pytest.mark.parametrize("now", [(3, "other"), (4, "d1")], ids=["bytes", "rows"])
def test_a_batch_that_is_not_what_was_recorded_is_refused(files, now):
    """The raw zone is append-only. A batch that differs from its fingerprint
    was changed, and deleting it would destroy the only evidence of that."""
    files["checks/batch_digest_recorded.sql"] = _held()
    files["checks/batch_digest.sql"] = [now]
    code, why = runs.refusal(Conn(), BATCH)
    assert code == runs.DIGEST_MISMATCH
    assert "run-1" in why and "d1" in why


# --- the backfill ------------------------------------------------------------------

def test_a_backfilled_digest_goes_on_the_run_that_wrote_the_batch(files):
    files["checks/batch_digest.sql"] = [(3, "d1")]
    conn = Conn(fetches=[("run-original",)])
    assert runs.backfill_batch(conn, BATCH) == "run-original"
    sql, params = conn.updates()[0]
    entry = json.loads(params[0])["raw_batch"]
    assert params[1] == "run-original"
    assert entry["origin"] == "backfill"        # an audit can tell it was late


def test_a_batch_with_no_run_gets_a_run_of_its_own_and_says_so(files):
    """It is never attached to an unrelated run, and the dag_id says what it is."""
    files["checks/batch_digest.sql"] = [(3, "d1")]
    conn = Conn(fetches=[None])
    run_id = runs.backfill_batch(conn, BATCH)
    assert run_id == "digest_backfill__%s" % BATCH
    inserted = [p for s, p in conn.executed if "INSERT INTO fd_pipeline_runs" in s]
    assert inserted and inserted[0][1] == "digest_backfill"


# --- nothing escapes: every writer of the raw zone fingerprints ------------------------

def _calls(path, name):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute) and node.func.attr == name]


def _writers():
    """Every source that calls a raw-zone writer, except the modules defining them."""
    defining = {ROOT / "ml4fd" / "raw.py", ROOT / "ml4fd" / "stream.py"}
    for directory in ("ml4fd", "airflow", "tools"):
        for path in sorted((ROOT / directory).rglob("*.py")):
            if path not in defining and _calls(path, "write_batch"):
                yield path


def test_the_scan_finds_the_three_writers():
    names = {path.name for path in _writers()}
    assert {"dag_ingest.py", "dag_load_history.py", "ingest_once.py"} <= names


@pytest.mark.parametrize("path", list(_writers()),
                         ids=lambda p: str(p.relative_to(ROOT)))
def test_every_writer_of_the_raw_zone_takes_a_fingerprint(path):
    """THE TEST FOR THE NEXT WRITER. A fourth way of loading the raw zone that
    forgets this is a batch the purge refuses for ever -- or the day someone
    loosens the purge to get past it, a batch destroyed without a trace."""
    assert _calls(path, "record_batch_digest"), (
        "%s writes the raw zone and never records the fingerprint of what it "
        "wrote" % path.relative_to(ROOT))


def test_the_historical_load_stores_the_digests_it_used_to_only_log():
    assert _calls(ROOT / "airflow" / "dags" / "dag_load_history.py",
                  "record_store_digests")


# --- the tool and the task ---------------------------------------------------------------

def _tool(monkeypatch, pending, capsys, argv):
    import contextlib
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "digest_batches", ROOT / "tools" / "digest_batches.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Live(Conn):
        commits = 0

        def commit(self):
            Live.commits += 1

    conn = Live()
    seen = []
    monkeypatch.setattr(db, "connect", lambda *a, **k: contextlib.nullcontext(conn))
    # The migration guard asks the catalogue, which this fake cannot answer;
    # tests/test_migration_guard.py holds it, including that this tool catches
    # it and exits non-zero.
    monkeypatch.setattr(runs, "assert_digests", lambda c: None)
    monkeypatch.setattr(runs, "undigested_batches", lambda c: pending)
    monkeypatch.setattr(runs, "backfill_batch",
                        lambda c, b: seen.append(b) or "run-%s" % b)
    code = module.main(argv)
    return code, seen, capsys.readouterr().out, Live.commits


PENDING = [{"batch_id": BATCH, "source": "fraudTest.csv", "rows": 555719,
            "first_at": "2026-11-15 10:00:00", "last_at": "2026-11-15 10:05:00"}]


def test_the_tool_only_lists_unless_asked(monkeypatch, capsys):
    code, seen, out, commits = _tool(monkeypatch, PENDING, capsys, [])
    assert code == 0 and seen == [] and commits == 0
    assert str(BATCH) in out and "-Apply" in out


def test_the_tool_records_with_apply_and_says_it_is_late(monkeypatch, capsys):
    code, seen, out, commits = _tool(monkeypatch, PENDING, capsys, ["--apply"])
    assert code == 0 and seen == [BATCH] and commits == 1
    assert "backfill" in out


def test_the_tool_has_nothing_to_do_when_everything_is_covered(monkeypatch, capsys):
    code, seen, out, _ = _tool(monkeypatch, [], capsys, ["--apply"])
    assert code == 0 and seen == []
    assert "rien a faire" in out


def test_the_task_exists_and_has_its_handler():
    text = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    assert "'digests'," in text                       # in the ValidateSet
    assert "    'digests' {" in text                  # and in the switch
    assert "tools/digest_batches.py" in text


def test_the_purge_mark_leaves_no_parameter_type_to_inference():
    """THE DEFECT, 2026-11-17, on the first real run of mark_purging():
    `could not determine data type of parameter $3`.

    jsonb_build_object is variadic "any". psycopg sends a Python str WITHOUT a
    type, so the server can infer it -- which is exactly what makes
    `batch_id = %s` work against a uuid column -- but inside a variadic "any"
    there is nothing to infer from. Every parameter of this statement therefore
    carries its cast.

    Reproduced on PostgreSQL 16 both ways: the statement is rejected at PREPARE
    time without the cast, and writes the mark with it, leaving the sibling
    store's mark intact.
    """
    import re
    source = (Path(__file__).resolve().parents[1] / "ml4fd" / "runs.py")
    body = source.read_text(encoding="utf-8")
    block = body[body.index("def _set_nested"):body.index("def mark_purging")]
    # The SQL alone: the docstring above it quotes `batch_id = %s` as the
    # counter-example, and counting that would be counting the explanation.
    statement = block[block.index("cur.execute("):]
    placeholders = re.findall(r"%s(::[a-z\[\]]+)?", statement)
    assert placeholders, "the statement lost its parameters"
    bare = [p for p in placeholders if not p]
    # run_id is compared to a text column: inference has a column to read.
    assert len(bare) <= 1, "%d parameter(s) left to inference" % len(bare)
    assert "jsonb_build_object(%s::text" in statement
