"""The four guarantees of N6, checked by analysis rather than by re-reading.

Covers: EE03, EI03, EI12, EC17
Lot: N6
Competency: C5

WHY THIS FILE IS DIFFERENT FROM EVERY OTHER TEST HERE. It measures an ABSENCE.
The API delivers is_fraud in the SAME payload as the features -- N0 measured it:
21 columns, is_fraud present as a str -- so the label sits one line of code away
from the scoring. No code review proves it did not get there, and no metric in
any report would reveal it: a model that reads the label scores beautifully.

So the four guarantees are read off the code and the schema:

  1. to_transaction does not know is_fraud.
  2. fd_transactions has no column to hold it -- the strongest of the four,
     since a leak would need a schema migration, hence a visible decision.
  3. The scoring path imports nothing that touches the verdict, and reaches no
     table that holds one.
  4. The verdict write shares no task with the scoring.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

from ml4fd import datasets, db, transform

ROOT = Path(__file__).resolve().parents[1]

# 32 characters, because transform refuses less: a shorter pepper would make the
# pseudonymisation reversible by enumeration over 924 cardholder numbers. The
# refusal is a guard of S1 doing its job, so the test meets it rather than
# weakening it.
PEPPER = "0123456789abcdef0123456789abcdef"

# Everything the scoring path is made of. A module added to this chain belongs
# in the list, and the tests below then apply to it.
SCORING_PATH = ("ml4fd/score.py", "sql/scoring/rows_to_score.sql",
                "sql/scoring/write_scores.sql", "sql/ddl/080_scores.sql")

VERDICT_WORDS = ("is_fraud", "fd_ground_truth", "label_ts")


def _imports(relative: str) -> set[str]:
    """Every module name the file imports, wherever the import sits.

    Walked over the whole tree and not read off the header: this project puts
    heavy imports inside functions on purpose, so a header-only reading would
    report an import-free module that imports everything.
    """
    tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module.split(".")[0])
            for alias in node.names:
                names.add(alias.name)
    return names


def _without_comments(body: str) -> str:
    body = "\n".join(line for line in body.splitlines()
                     if not line.strip().startswith("--")
                     and not line.strip().startswith("#"))
    return re.sub(r'""".*?"""', "", body, flags=re.S)


def _text(relative: str) -> str:
    return _without_comments((ROOT / relative).read_text(encoding="utf-8"))


# --- guarantee 1: the typing does not know the label --------------------------

def test_the_typing_module_never_names_the_label():
    body = _text("ml4fd/transform.py")
    for word in ("is_fraud", "label"):
        assert word not in body, word


def test_a_payload_carrying_the_label_yields_a_transaction_without_it():
    """The decisive unit check of guarantee 1: give it a payload with
    is_fraud = 1 and the typed transaction must not carry the value anywhere."""
    from ml4fd import checks

    payload = {"trans_num": "abc", "current_time": "1794792372826",
               "cc_num": "4992346398065154184", "merchant": "fraud_Dach-Borer",
               "category": "grocery_net", "amt": "371.94", "is_fraud": "1",
               "merch_lat": "39.065279", "merch_long": "-76.195157",
               "lat": "38.2674", "long": "-76.4954"}
    typed = transform.to_transaction(payload, PEPPER,
                                     profile=checks.STREAM_PROFILE)
    # Not just the obvious key: a column carrying the value under another name
    # would be a leak all the same.
    assert all(key not in typed for key in ("is_fraud", "label", "fraud",
                                            "verdict", "target", "y"))
    # And the value itself appears nowhere, under any key. "1" is too common to
    # look for, so the payload below uses a value that is not in any other
    # field.
    marked = dict(payload, is_fraud="LABEL-SENTINEL")
    typed = transform.to_transaction(marked, PEPPER,
                                     profile=checks.STREAM_PROFILE)
    assert "LABEL-SENTINEL" not in str(sorted(typed.items()))


def test_the_same_payload_with_either_label_types_identically():
    """THE criterion of this lot, at unit level: two payloads differing ONLY in
    is_fraud must produce the same typed transaction, field for field. If they
    do, no downstream step can tell them apart, whatever it does."""
    from ml4fd import checks

    base = {"trans_num": "abc", "current_time": "1794792372826",
            "cc_num": "4992346398065154184", "merchant": "fraud_Dach-Borer",
            "category": "grocery_net", "amt": "371.94",
            "merch_lat": "39.065279", "merch_long": "-76.195157",
            "lat": "38.2674", "long": "-76.4954"}
    honest = transform.to_transaction(dict(base, is_fraud="0"), PEPPER,
                                      profile=checks.STREAM_PROFILE)
    fraudulent = transform.to_transaction(dict(base, is_fraud="1"), PEPPER,
                                          profile=checks.STREAM_PROFILE)
    assert honest == fraudulent


# --- guarantee 2: the schema has nowhere to put it ----------------------------

def test_the_exploited_table_has_no_column_for_a_verdict():
    """The strongest guarantee of the four: a leak would need a schema
    migration, so a decision somebody has to write down."""
    body = _text("sql/ddl/030_transactions.sql")
    create = body[body.index("CREATE TABLE"):body.index(");")]
    for word in ("is_fraud", "label", "fraud", "verdict"):
        assert word not in create.lower(), word


def test_the_feature_list_holds_no_verdict():
    for name in datasets.FEATURES:
        assert name not in VERDICT_WORDS
        assert "fraud" not in name and "label" not in name


def test_the_label_is_not_among_the_columns_fed_to_a_model():
    """datasets.LABEL is stored in the Parquet and must never be a feature."""
    assert datasets.LABEL not in datasets.FEATURES
    assert datasets.LABEL in datasets.STORED_COLUMNS


# --- guarantee 3: the scoring path cannot reach a verdict ---------------------

def test_the_scoring_module_imports_nothing_that_touches_the_verdict():
    """By import analysis, not by re-reading."""
    forbidden = {"truth", "verdicts"}
    assert not (_imports("ml4fd/score.py") & forbidden)


def test_no_file_of_the_scoring_path_names_a_verdict():
    for relative in SCORING_PATH:
        body = _text(relative).lower()
        for word in VERDICT_WORDS:
            assert word not in body, "%s : %s" % (relative, word)


def test_the_scoring_query_joins_no_table_holding_a_verdict():
    body = _text("sql/scoring/rows_to_score.sql")
    assert "fd_transactions" in body
    assert "fd_ground_truth" not in body
    assert body.lower().count("join") == 0


def test_the_scores_table_holds_no_verdict_column():
    body = _text("sql/ddl/080_scores.sql")
    create = body[body.index("CREATE TABLE"):body.index("CONSTRAINT")]
    for word in ("is_fraud", "label", "verdict"):
        assert word not in create.lower(), word


# --- guarantee 4: the write shares no task with the scoring -------------------

def test_the_ingestion_runner_writes_no_verdict():
    """It did until N6. Removing it is the guarantee; this reads the imports so
    the removal cannot be undone unnoticed."""
    names = _imports("tools/ingest_once.py")
    assert "truth" not in names
    assert "verdicts" not in names
    body = _text("tools/ingest_once.py")
    assert "load_ground_truth" not in body


def test_the_verdict_runner_scores_nothing():
    """The reverse direction, which matters as much: a verdict task that also
    scored would put the label and the model in one process again."""
    names = _imports("tools/write_verdicts.py")
    assert "score" not in names
    assert "notify" not in names


def test_the_verdict_module_scores_nothing():
    names = _imports("ml4fd/verdicts.py")
    assert "score" not in names
    assert "registry" not in names


def _task_names(path):
    """The @task functions of a DAG file."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if any(getattr(d, "id", getattr(d, "attr", "")) == "task"
                   for d in node.decorator_list):
                found.add(node.name)
    return found


def test_the_verdict_dag_shares_no_task_with_any_other():
    """THE HAND-KEPT LIST HAD DRIFTED, and it omitted the one file the
    guarantee is about.

    This test used to compare the verdict DAG against dag_train and
    dag_load_history only -- not against dag_ingest, which is the DAG that
    SCORES. The fourth guarantee of N6 is precisely that the writing of the
    verdict shares nothing with the scoring, so the comparison that mattered
    was the one missing. Found while declaring dag_daily in P4.

    It now walks the directory, so a DAG added later is compared without
    anybody remembering to add it.
    """
    dags = sorted((ROOT / "airflow" / "dags").glob("dag_*.py"))
    assert len(dags) >= 4, [p.name for p in dags]

    verdict = ROOT / "airflow" / "dags" / "dag_ingest_verdicts.py"
    verdict_tasks = _task_names(verdict)
    assert verdict_tasks

    compared = []
    for path in dags:
        if path == verdict:
            continue
        compared.append(path.name)
        shared = verdict_tasks & _task_names(path)
        assert not shared, "%s partage %s" % (path.name, sorted(shared))
    # The one that must be in the comparison, named so its absence is loud.
    assert "dag_ingest.py" in compared


def test_the_verdict_dag_has_a_cadence_of_its_own():
    """EI06: the ingestion runs every minute, this once a day. Nothing is lost
    by waiting -- the verdict is not due for the declared delay anyway."""
    body = (ROOT / "airflow/dags/dag_ingest_verdicts.py").read_text(
        encoding="utf-8")
    schedule = re.search(r'schedule="([^"]+)"', body).group(1)
    assert schedule != "* * * * *"
    assert re.match(r"^\d+ \d+ \* \* \*$", schedule), schedule


# --- the withholding itself ---------------------------------------------------

def test_the_verdict_is_written_with_a_future_label_and_not_delayed():
    """Nothing is simulated: the verdict is real and written at once. What holds
    it back is label_ts = txn_ts + delay and every consumer filtering on
    label_ts <= cut_off -- the mechanism exercised on 555,719 rows by M3."""
    body = _text("ml4fd/truth.py")
    assert "label_ts" in body
    assert "delay" in body


def test_the_training_set_filters_on_the_label_time_and_not_the_transaction():
    """The filter IS the withholding. On txn_ts it would hand the model
    verdicts nobody knew at the cut-off."""
    body = _text("sql/datasets/training_set.sql")
    assert "g.label_ts <= %(cut_off)s" in body
    assert "t.txn_ts   <= %(cut_off)s" not in body


def test_the_band_can_be_counted():
    """A guarantee nobody can count is a promise."""
    body = _text("sql/verdicts/withheld.sql")
    assert "not_yet_due" in body
    assert "label_ts >  now()" in body or "label_ts > now()" in body


def test_the_verdict_write_filters_on_existence_not_on_a_reject_list():
    """fd_ground_truth references fd_transactions, so a quarantined line's
    verdict is refused by the foreign key -- and would take the whole batch's
    write with it. The daily run no longer knows what the ingestion rejected."""
    body = _text("ml4fd/verdicts.py")
    assert "lines_without_transaction" in body
    assert _text("sql/verdicts/known_transactions.sql").count("ANY(") == 1


# --- deux defauts de N6, trouves a l'execution le 16 novembre 2026 ------------

def test_the_awaiting_query_does_not_multiply_its_rows():
    """A CARTESIAN PRODUCT IS QUIET: it does not fail, it multiplies.

    The first version joined fd_raw_events to fd_transactions ON batch_id,
    which is a key on neither side: a batch of 4 raw lines and 4 transactions
    produced 4 x 4 = 16 rows, and the operator read "16 ligne(s) dont 16 sans
    verdict" for a batch holding four. Every count derived from it was the
    square of the truth.
    """
    body = _text("sql/verdicts/batches_awaiting_verdict.sql")
    assert "EXISTS" in body
    assert "JOIN   fd_raw_events" not in body
    assert "JOIN fd_raw_events" not in body
    # Counted on what a verdict attaches to: its foreign key is on txn_id.
    assert "FROM   fd_transactions t" in body


def test_the_event_identifier_refuses_to_be_truncated():
    """The defect that skipped four verdicts in silence.

    event_id returned "<trans_num>:" when the time was missing -- a VALID
    STRING. The verdict write passed the raw-zone envelope instead of the row it
    wraps, every identifier came back truncated, none matched a stored
    transaction, and the message read like normal operation.
    """
    import pytest as _pytest

    from ml4fd import checks

    envelope = {"row": {"trans_num": "abc", "current_time": "1794792372826"},
                "trans_num": "abc", "source_index": "31923"}
    with _pytest.raises(ValueError, match="current_time"):
        checks.STREAM_PROFILE.event_id(envelope)


def test_the_raw_envelope_is_unwrapped_by_the_module_that_wrote_it():
    """stream.as_payload wraps the row; stream.row_of unwraps it. Reading the
    raw zone from elsewhere and guessing at the shape is what caused the
    defect above."""
    from ml4fd import stream

    row = {"trans_num": "abc", "current_time": "1794792372826"}
    envelope = {"row": row, "trans_num": "abc", "source_index": "31923",
                "server_ms": 1, "our_ms": 2, "latency_ms": 3}
    assert stream.row_of(envelope) == row
    # Tolerant of a payload that IS a row: S1's CSV batches are stored that way.
    assert stream.row_of(row) == row


def test_the_verdict_write_unwraps_once_and_not_twice():
    """Unwrapping in one place and not the other is exactly the defect: the
    existence filter and load_ground_truth must see the same shape."""
    body = _text("ml4fd/verdicts.py")
    assert body.count("stream.row_of(payload)") == 1
    assert "stream.row_of(payload))\n                   for line_no, payload" \
        in body or "records = [(line_no, stream.row_of(payload))" in body
