"""A run nobody closed, and the two files that find it and close it.

Block: B3, B4.
Lot: B3-N9, B4-L0.
Competency: B3-C4, B3-C5.
Covers: B3-EC12, B3-EC14.

WHY A RUN LEFT IN 'running' IS WORSE THAN AN UNTIDY TABLE. The guard of M1
counts state = 'success' and state = 'failed' to decide whether a window is
healthy enough to train on. A row in 'running' is in NEITHER, so it is not a
failure the guard can see -- it is a run that silently never happened. One such
row was found in the real journal by the acceptance criteria of S3, left there
when the real-time DAG still closed its failures with a task carrying
trigger_rule="one_failed", which cannot be scheduled while the RUN itself is
being killed.

THE VERDICT USED TO COME FROM AIRFLOW, and these tests used to require it. L1
gave Airflow a database of its own -- so that running a DAG grants no owner
rights on the ml4fd tables -- and PostgreSQL does not join across databases:
the join raised on its own guard on 2026-11-20, a day later. The tests below
were part of the defect, because test_the_verdict_comes_from_airflow asserted
the premise that L1 had just falsified and stayed green on a broken check.
A test that encodes a premise must be revisited when the premise moves.

These tests read the SQL rather than running it -- PostgreSQL is not in the
authoring environment -- and check the decisions: the rule is one rule, no file
reads a table of Airflow's, the closure is a failure and not a success, a
duration nobody measured is null rather than invented, and a check does not
write.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CHECK = ROOT / "sql" / "checks" / "abandoned_runs.sql"
CLOSE = ROOT / "sql" / "maintenance" / "030_close_abandoned_runs.sql"


def code_of(path: Path) -> str:
    """The file without its comment lines.

    Every test about what a file READS must look at the code alone. These
    headers deliberately recount what they used to read and why they stopped,
    so a plain `"dag_run" in text` would fail on the very explanation that
    makes the change understandable.
    """
    return "\n".join(line
                     for line in path.read_text(encoding="utf-8").splitlines()
                     if not line.lstrip().startswith("--"))


def test_the_rule_is_an_age_and_reads_no_table_of_airflows():
    """ONE RULE, AND NOT TWO. Reading dag_run where it happens to exist and
    falling back to an age where it does not would restore the defect Q6 was
    spent on: two readings of one rule, disagreeing, with `abandoned`
    reporting nothing to close while the acceptance of S3 counted one.

    AND NOT BY A SIDE DOOR EITHER. dblink and postgres_fdw would reach the
    other database, at the price of undoing the privilege boundary L1 built --
    a boundary traded away to tidy a journal would be a poor bargain.
    """
    for path in (CHECK, CLOSE):
        code = code_of(path)
        for table in ("dag_run", "task_instance", "dblink", "postgres_fdw"):
            assert table not in code, "%s : %s" % (path.name, table)
        assert "interval '1 hour'" in code, path.name
        assert "interval '5 minutes'" not in code, path.name

    # The age is not presented as free: the header says what it costs, so a
    # reader finds the loss without having to run the file.
    assert "WHAT IS LOST" in CHECK.read_text(encoding="utf-8")


@pytest.mark.parametrize("path", [CHECK, CLOSE])
def test_a_run_younger_than_an_hour_is_spared(path):
    """The one case that must never be touched: a live execution. Nothing in
    this database distinguishes a run that is still going from one that died,
    so the age is the whole protection -- and it protects by being generous."""
    code = code_of(path)
    assert "r.state = 'running'" in code
    assert "now() - interval '1 hour'" in code


def test_the_listing_shows_every_open_run_and_not_only_the_closable_ones():
    """THE DEFECT OF 17 NOVEMBER: `abandoned` reported nothing to close while
    the acceptance of S3 failed on one open run. The inner join hid the two
    cases an operator most needs to see -- a run with no dag_run row, and a run
    Airflow itself holds open."""
    code = code_of(CHECK)
    # THE AGE DECIDES THE VERDICT AND NEVER THE WHERE. A filter there hides the
    # rows an operator most needs to see, which is what the inner join did.
    assert "interval" not in code[code.index("WHERE"):], "un age dans le WHERE"
    assert "fermable" in code and "attendre" in code


def test_the_closer_rests_on_one_evidence():
    """TWO STATEMENTS BECAME ONE, and that is a consequence and not a tidy-up.
    The first closed what Airflow declared over, the second the orphans Airflow
    never heard of -- two cases on two pieces of evidence, which is why they
    were two statements. Without dag_run there is one piece of evidence left:
    nobody closed this row and it is old enough that nobody will. Two
    statements would now leave a reader wondering which one closed a row."""
    code = code_of(CLOSE)
    assert code.count("UPDATE fd_pipeline_runs") == 1
    assert code.count("AS motif") == 1      # and it still says why


def test_the_acceptance_of_s3_asks_the_same_question():
    """Two readings of one rule must not disagree: that is what left an
    operator with a red check and no way out."""
    code = code_of(ROOT / "sql" / "checks" / "s3_acceptance.sql")
    criterion = code.split("N7 aucune execution que personne ne peut fermer")[1]
    criterion = criterion.split("UNION ALL")[0]
    assert "interval '1 hour'" in criterion
    assert "interval '5 minutes'" not in criterion
    assert "dag_run" not in criterion


def test_the_closure_is_a_failure_and_never_a_success():
    """The run did not reach close_run, so nothing recorded its counters.
    Calling that a success would put a run with no volumes among the healthy
    ones and let the guard count a window as sound on the strength of it."""
    text = CLOSE.read_text(encoding="utf-8")
    assert "SET state      = 'failed'" in text
    assert "'success'" not in text.split("UPDATE fd_pipeline_runs")[1]


def test_a_duration_nobody_measured_is_null_and_not_invented():
    """ended_at USED TO TAKE AIRFLOW'S end_date, with now() as a fallback only.
    With no end_date to be had, now() is the moment somebody tidied up and not
    the moment the run stopped: written into duration_s it would measure our
    own delay, in a column every reader takes for a duration. NULL states what
    is true, and `duration_s IS NULL` is how a reader tells these rows from the
    ones runs.abandon() closed at the instant of failure, where now() really
    was the end. ended_at keeps now(): when we closed it is a fact."""
    code = code_of(CLOSE)
    assert "duration_s = NULL" in code
    assert "coalesce(d.end_date" not in code
    assert "ended_at   = now()" in code


def test_the_check_writes_nothing():
    """A file under sql/checks/ that modified anything would be a trap: one
    reads a check to find out, not to change."""
    text = CHECK.read_text(encoding="utf-8")
    code = "\n".join(line for line in text.splitlines()
                     if not line.lstrip().startswith("--"))
    for verb in ("UPDATE", "DELETE", "INSERT", "TRUNCATE"):
        assert verb not in code.upper(), verb


def test_no_check_in_the_repository_writes_to_a_project_table():
    """The same rule for every check, not only this one. CREATE TEMP TABLE is
    allowed -- the acceptance files materialise their rows in one -- but no
    fd_ table may be written from sql/checks/."""
    offenders = []
    for path in sorted((ROOT / "sql" / "checks").glob("*.sql")):
        for number, line in enumerate(path.read_text(encoding="utf-8")
                                      .splitlines(), 1):
            if line.lstrip().startswith("--"):
                continue
            if re.search(r"\b(UPDATE|DELETE\s+FROM|INSERT\s+INTO|TRUNCATE|DROP)\b"
                         r"[^;]*\bfd_\w+", line, re.IGNORECASE):
                offenders.append("%s:%d" % (path.name, number))
    assert not offenders, "ecriture dans un controle : %s" % ", ".join(offenders)


def test_the_closure_is_replayable():
    """Running it twice must change nothing the second time: the rows it
    touched are no longer in 'running'. Stated in the file, because somebody
    will wonder before running it."""
    assert "REPLAYABLE" in CLOSE.read_text(encoding="utf-8")


def test_the_runner_lists_before_it_closes():
    """-Apply, and not the other way round: looking at a journal must not
    change it."""
    script = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    branch = script[script.index("    'abandoned' {"):
                    script.index("    'verdicts' {")]
    assert "checks/abandoned_runs.sql" in branch
    assert "if (-not $Apply)" in branch
    listing = branch.index("checks/abandoned_runs.sql")
    closing = branch.index("maintenance/030_close_abandoned_runs.sql")
    assert listing < closing
