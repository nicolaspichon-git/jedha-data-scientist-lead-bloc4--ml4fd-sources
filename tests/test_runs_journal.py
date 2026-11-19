"""A run nobody closed, and the two files that find it and close it.

Covers: EC12, EC14
Lot: N9
Competency: C4, C5

WHY A RUN LEFT IN 'running' IS WORSE THAN AN UNTIDY TABLE. The guard of M1
counts state = 'success' and state = 'failed' to decide whether a window is
healthy enough to train on. A row in 'running' is in NEITHER, so it is not a
failure the guard can see -- it is a run that silently never happened. One such
row was found in the real journal by the acceptance criteria of S3, left there
when the real-time DAG still closed its failures with a task carrying
trigger_rule="one_failed", which cannot be scheduled while the RUN itself is
being killed.

These tests read the SQL rather than running it -- PostgreSQL is not in the
authoring environment -- and check the decisions: the verdict comes from
Airflow and not from an age, the closure is a failure and not a success, a
check does not write, and the journal keeps the time the run actually stopped.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CHECK = ROOT / "sql" / "checks" / "abandoned_runs.sql"
CLOSE = ROOT / "sql" / "maintenance" / "030_close_abandoned_runs.sql"


def test_the_verdict_comes_from_airflow_and_not_from_an_age():
    """AN AGE WOULD HAVE TO BE WRONG for one DAG or the other: five minutes is
    generous for a run of the minute DAG and absurd for a historical load that
    takes twenty. The project and Airflow share one database, so dag_run is
    right there -- "Airflow says this run is over" is a fact, not a guess.

    AN AGE SURVIVES IN ONE PLACE ONLY, added in Q6: a run Airflow never heard
    of. tools/ingest_once.py and the digest backfill open runs of their own,
    and for those there is no verdict to have and no other signal -- the old
    inner join simply hid them, so `abandoned` reported nothing to close while
    the acceptance of S3 counted one.
    """
    for path in (CHECK, CLOSE):
        text = path.read_text(encoding="utf-8")
        assert "dag_run" in text, path.name
        assert "d.state <> 'running'" in text, path.name
        # The age, where it is allowed, sits beside the absence of a dag_run
        # row and nowhere else.
        for match in re.finditer(r"interval '([^']+)'", text):
            window = text[max(0, match.start() - 400):match.end()]
            assert ("NOT EXISTS" in window or "d.run_id IS NULL" in window), \
                "%s : un age sans absence de dag_run" % path.name


@pytest.mark.parametrize("path", [CHECK, CLOSE])
def test_a_run_airflow_still_reports_as_running_is_spared(path):
    """The one case that must never be touched: a live execution."""
    assert "r.state = 'running'" in path.read_text(encoding="utf-8")


def test_the_listing_shows_every_open_run_and_not_only_the_closable_ones():
    """THE DEFECT OF 17 NOVEMBER: `abandoned` reported nothing to close while
    the acceptance of S3 failed on one open run. The inner join hid the two
    cases an operator most needs to see -- a run with no dag_run row, and a run
    Airflow itself holds open."""
    text = CHECK.read_text(encoding="utf-8")
    assert "LEFT   JOIN dag_run" in text or "LEFT JOIN dag_run" in text
    assert "aucune ligne dag_run" in text
    assert "intouchable" in text          # and it says so, rather than hiding
    decision = text[text.index("SELECT r.run_id"):]
    assert "d.state <> 'running'" not in decision.split("CASE")[0]


def test_the_closer_handles_a_run_airflow_never_heard_of():
    """Nothing else can close it: tools/ingest_once.py and the digest backfill
    open runs of their own."""
    text = CLOSE.read_text(encoding="utf-8")
    assert text.count("UPDATE fd_pipeline_runs") == 2
    assert "NOT EXISTS (SELECT 1 FROM dag_run d" in text
    # and each statement says which evidence closed the run
    assert text.count("AS motif") == 2


def test_the_acceptance_of_s3_asks_the_same_question():
    """Two readings of one rule must not disagree: that is what left an
    operator with a red check and no way out."""
    text = (ROOT / "sql" / "checks" / "s3_acceptance.sql").read_text(
        encoding="utf-8")
    criterion = text.split("N7 aucune execution que personne ne peut fermer")[1]
    criterion = criterion.split("UNION ALL")[0]
    assert "d.state <> 'running'" in criterion
    assert "NOT EXISTS" in criterion
    assert "interval '1 hour'" in criterion
    assert "interval '5 minutes'" not in criterion


def test_the_closure_is_a_failure_and_never_a_success():
    """The run did not reach close_run, so nothing recorded its counters.
    Calling that a success would put a run with no volumes among the healthy
    ones and let the guard count a window as sound on the strength of it."""
    text = CLOSE.read_text(encoding="utf-8")
    assert "SET state      = 'failed'" in text
    assert "'success'" not in text.split("UPDATE fd_pipeline_runs")[1]


def test_the_journal_keeps_when_the_run_actually_stopped():
    """ended_at takes Airflow's end_date when there is one. Measured to now(),
    the duration would grow with the delay before somebody tidied up."""
    text = CLOSE.read_text(encoding="utf-8")
    assert "coalesce(d.end_date, now())" in text
    assert text.count("coalesce(d.end_date, now())") == 2   # ended_at AND duration


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


@pytest.mark.parametrize("path", [CHECK, CLOSE])
def test_a_missing_airflow_table_names_itself(path):
    """Without the guard the file fails on "relation dag_run does not exist",
    which reads as a defect of the project rather than an uninitialised
    Airflow."""
    text = path.read_text(encoding="utf-8")
    assert "to_regclass('public.dag_run')" in text
    assert "RAISE EXCEPTION" in text
    assert "USING HINT" in text


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
