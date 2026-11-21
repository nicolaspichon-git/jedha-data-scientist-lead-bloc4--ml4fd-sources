"""An acceptance file must be able to fail the chain it is run from.

Covers: EC07, EC14
Lot: N9
Competency: C5

THE DEFECT THIS EXISTS FOR, and it was found by reading a real run rather than
by any check. The three acceptance files printed a `verdict` column holding
`<<< ECHEC`, and psql exits 0 whatever that column says. fd-manager's
Invoke-Step reads only the exit code, so `fd-manager check` walked past SEVEN
failing criteria of S1 and would have ended on "Tous les criteres passent".

A green light that cannot turn red is worse than no light, because somebody
trusts it. These tests read the SQL rather than running it -- PostgreSQL is not
in the authoring environment -- so they check the SHAPE that makes the exit code
follow the verdict: the rows are materialised, displayed, then counted, and a
count above zero raises.

The display coming BEFORE the raise is part of the shape and not a detail: an
operator who gets an exception and no table knows that something failed and not
what, which sends them to read the SQL instead of the result.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
# FOUND AND NOT LISTED. The literal list held s1, s2 and s3 only: s4, s5 and
# s6 were written later and escaped these four tests for months, and l5 would
# have escaped them too. The form is the contract, so every file that claims to
# be an acceptance file is held to it.
FILES = tuple(sorted(path.name for path in
                     (ROOT / "sql" / "checks").glob("*_acceptance.sql")))


def _text(name: str) -> str:
    return (ROOT / "sql" / "checks" / name).read_text(encoding="utf-8")


@pytest.mark.parametrize("name", FILES)
def test_the_criteria_are_materialised_before_being_judged(name):
    """A temp table, so the same rows are displayed AND counted. Computing them
    twice would let the display and the verdict disagree."""
    assert "CREATE TEMP TABLE acceptance AS" in _text(name)


@pytest.mark.parametrize("name", FILES)
def test_a_failing_criterion_raises(name):
    text = _text(name)
    assert "RAISE EXCEPTION" in text
    assert "LIKE '%ECHEC%'" in text


@pytest.mark.parametrize("name", FILES)
def test_the_table_is_displayed_before_the_raise(name):
    """The operator must see attendu/constate, not only an error.

    THE LAST RAISE AND NOT THE FIRST. l5 and s6 open with a guard that names a
    missing role and its remedy, deliberately before anything else can fail on
    an unknown name -- a first-occurrence test read that guard as the summary
    and called both files defective."""
    text = _text(name)
    display = text.index("FROM   acceptance")
    raising = text.rindex("RAISE EXCEPTION")
    assert display < raising


@pytest.mark.parametrize("name", FILES)
def test_an_informative_figure_does_not_fail_the_chain(name):
    """`info` rows are measurements, not criteria -- criterion 13 of S3 is the
    reason: a figure that failed its own acceptance would be refusing reality.
    The count looks for ECHEC and nothing else."""
    text = _text(name)
    counting = text[text.index("INTO failed"):text.index("IF failed")]
    assert "ECHEC" in counting
    assert "info" not in counting


@pytest.mark.parametrize("name", FILES)
def test_the_failure_names_the_criteria(name):
    """"3 criteres en echec" without their names sends the reader back to the
    table to find which. The exception carries them."""
    text = _text(name)
    assert "string_agg(criterion" in text


@pytest.mark.parametrize("name", FILES)
def test_the_raise_message_has_an_argument_for_each_placeholder(name):
    """RAISE substitutes % positionally, and a mismatch is an error raised
    INSTEAD of the one being reported -- the failure would be announced as a
    defect of this file."""
    text = _text(name)
    message = text[text.index("RAISE EXCEPTION"):text.index("USING HINT")]
    quoted = re.search(r"'([^']*)'", message).group(1)
    assert quoted.count("%") == message.count(",")


# --- a process that measures a shared table must say which rows are its own ---

STREAM_SOURCE = "current-transactions"


def test_the_stream_source_literal_is_the_one_the_code_writes():
    """Both acceptance files hinge on this string. If ml4fd/stream.py changes
    it, S1 would silently count the stream again and S3 would count nothing."""
    from ml4fd import stream

    assert stream.SOURCE == STREAM_SOURCE
    for name in ("s1_acceptance.sql", "s3_acceptance.sql"):
        assert STREAM_SOURCE in _text(name), name


def test_the_stream_source_is_written_once_in_s1():
    """In the stream_source CTE, so there is ONE place to change."""
    assert _text("s1_acceptance.sql").count("'%s'" % STREAM_SOURCE) == 1


def test_s1_scopes_itself_to_the_historical_load():
    text = _text("s1_acceptance.sql")
    for cte in ("historical_batch", "historical_raw", "historical_quarantine",
                "historical_txn", "historical_truth", "historical_runs"):
        assert "%s AS (" % cte in text, cte


def test_no_criterion_of_s1_reads_a_shared_table_DIRECTLY():
    """THE DEFECT THIS GUARDS, and it cost seven false failures.

    S1's criteria counted the WHOLE tables, and they were written when nothing
    else wrote to them. S3 ingests the stream into the same raw zone, the same
    fd_transactions and the same fd_ground_truth: 555,857 typed transactions
    instead of 555,719 -- its 138 -- and 555,743 verdicts instead of 555,719 --
    its 24. A pipeline working perfectly failed its own acceptance.

    s3_acceptance.sql had always scoped itself. The asymmetry WAS the bug, so
    the rule is checked rather than remembered: past `criteria AS (` not one
    criterion may name a shared table. The scoping CTEs above it may, and
    information_schema is not shared.
    """
    text = _text("s1_acceptance.sql")
    body = text[text.index("criteria AS ("):]
    assert re.findall(r"FROM\s+(fd_\w+)", body) == []


def test_the_dag_checks_its_own_verdicts_and_not_everybody_s():
    """The same defect inside dag_load_history, where no acceptance check looks.

    Its L5 assertion read the whole of fd_ground_truth. The delay is 30 days on
    both sides today, so it passes; the day the stream carried another delay, a
    REPLAY of S1 would fail on "the delay is not uniform" and blame the
    historical load for a property of the stream.
    """
    dag = (ROOT / "airflow" / "dags" / "dag_load_history.py").read_text(
        encoding="utf-8")
    query = dag[dag.index("min(label_ts - txn_ts)"):]
    query = query[:query.index("cur.fetchone()")]
    assert "FROM   fd_ground_truth WHERE source = %s" in query
    assert "str(bid)" in query


# --- a process that measures a shared table must say which rows are its own --

STREAM_TABLES = ("fd_transactions", "fd_ground_truth", "fd_raw_events",
                 "fd_quarantine", "fd_pipeline_runs")


def _criteria_block(name: str) -> str:
    """The criteria themselves, without the scoping CTEs above them."""
    text = _text(name)
    return text[text.index("criteria AS ("):text.index("SELECT ord,")]


def test_the_criteria_of_s1_read_only_scoped_relations():
    """THE DEFECT THIS REPAIRS, and it cost seven false failures.

    These criteria counted the WHOLE tables, and they were written when nothing
    else wrote to them. S3 ingests the stream into the same raw zone, the same
    fd_transactions and the same fd_ground_truth: 555,857 typed transactions
    instead of 555,719 -- exactly the stream's 138 -- and 555,743 verdicts
    instead of 555,719, exactly its 24. A pipeline working perfectly reported
    seven criteria in ECHEC.

    s3_acceptance.sql has always scoped itself. The asymmetry WAS the bug.
    """
    offenders = []
    for number, line in enumerate(_criteria_block("s1_acceptance.sql")
                                  .splitlines(), 1):
        if line.lstrip().startswith("--"):
            continue
        # FROM or JOIN, so a table NAME used as a value is not a reading:
        # criteria 8 and 15 look a column up in information_schema and name
        # 'fd_transactions' as a string. The first version of this test
        # flagged them, which is the difference between reading a table and
        # mentioning one.
        for found in re.findall(r"\b(?:FROM|JOIN)\s+(fd_\w+)", line):
            if found in STREAM_TABLES:
                offenders.append("%d: %s" % (number, line.strip()))
    assert not offenders, (
        "critere de S1 lisant une table entiere :\n  " + "\n  ".join(offenders))


def test_the_stream_literal_is_written_once_and_is_the_one_the_code_uses():
    """A second copy is a second thing to forget. And it must BE the stream's
    name: ml4fd/stream.py writes it, so a rename there must break this."""
    from ml4fd import stream

    text = _text("s1_acceptance.sql")
    assert text.count(stream.SOURCE) == 1
    assert "SELECT '%s'::text AS name" % stream.SOURCE in text


def test_s1_and_s3_scope_on_the_same_literal_in_opposite_directions():
    """COMPLEMENTARY BY CONSTRUCTION: every raw batch belongs to exactly one of
    the two files. Scoped with two different literals, a batch could be counted
    twice or by neither, and nobody would notice the second case."""
    from ml4fd import stream

    assert "source <> (SELECT name FROM stream_source)" in \
        _text("s1_acceptance.sql")
    assert "source = '%s'" % stream.SOURCE in _text("s3_acceptance.sql")


def test_the_scope_of_s1_is_negative_and_says_why():
    """It has to be: S1 writes the CSV's FILE NAME as the source and its
    verdicts carry the BATCH UUID, so there is no single positive value to
    match. Only the stream has one."""
    text = _text("s1_acceptance.sql")
    assert "path.name" in text          # the reason, named in the comment
    assert "historical_batch AS (" in text


def test_the_load_dag_scopes_its_own_acceptance_assertion_too():
    """THE SAME DEFECT IN A PLACE NO ACCEPTANCE CHECK LOOKS. dag_load_history
    asserts that the verdict delay is uniform over fd_ground_truth -- and the
    stream writes there. The delay is 30 days on both sides today, so it
    passes; the day it differed, a REPLAY OF S1 would fail on "the delay is not
    uniform" and blame the historical load for a property of the stream."""
    body = (ROOT / "airflow" / "dags" / "dag_load_history.py").read_text(
        encoding="utf-8")
    assertion = body[body.index("The acceptance criteria of L5"):
                     body.index("The dissociation is structural")]
    assert "FROM   fd_ground_truth WHERE source = %s" in assertion
    assert "str(bid)" in assertion
