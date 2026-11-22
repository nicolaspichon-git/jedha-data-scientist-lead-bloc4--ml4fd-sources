"""S5 / Q6: the journal of the purges, and the acceptance of S5.

Block: B3, B4.
Lot: B3-Q6, B4-L0.
Competency: B3-C4, B3-C5.
Covers: B3-EC14, B3-EC16, B3-EC18, B3-EC19, B3-EC23, B3-EC27.

"UNE DUREE NON APPLIQUEE N'EST PAS UNE POLITIQUE" (D1, annexe B). governance.yml
claims that personal data leaves at ninety days; what turns that claim into a
processing one can show is the record that it happened -- including the Sundays
that found nothing, which is the ordinary case and the first thing a log loses.

These tests hold five decisions:

  - the journal is written AFTER the deletion, and by execute() itself, so no
    caller can delete without journalling;
  - one row per store and per execution, including the stores that had nothing
    to remove;
  - it is append-only for the role that purges: the process that destroys the
    data cannot rewrite the record of what it destroyed;
  - no dry_run column, against the plan: a dry run writes nothing at all, and a
    column that could only ever be false would invite recording them;
  - the acceptance asks a privilege rather than a consequence -- "the purge did
    not overflow" is something the session CANNOT do, not something measured
    afterwards.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")


ROOT = Path(__file__).resolve().parents[1]
DDL = ROOT / "sql" / "ddl" / "130_purges.sql"
RECORD = ROOT / "sql" / "purge" / "record_purge.sql"
ACCEPT = ROOT / "sql" / "checks" / "s5_acceptance.sql"
CHECKER = ROOT / "tools" / "check_s5.py"


def _sql(path):
    return "\n".join(line for line in path.read_text(encoding="utf-8").splitlines()
                     if not line.strip().startswith("--"))


# --- the table ------------------------------------------------------------------

def test_the_table_is_declared_and_replayable():
    text = _sql(DDL)
    assert "CREATE TABLE IF NOT EXISTS fd_purges" in text


def test_one_row_carries_the_decision_and_what_it_meant():
    """as_of is the instant the purge was FOR, cutoff what it meant for this
    store. Both, because the first is the decision and the second its effect."""
    text = _sql(DDL)
    for column in ("run_id", "ran_at", "as_of", "cutoff", "before_date",
                   "store", "n_batches", "n_deleted", "n_refused", "n_resumed",
                   "share"):
        assert re.search(r"^\s+%s\s" % column, text, re.M), column


def test_there_is_no_dry_run_column():
    """AGAINST THE PLAN OF S5, which listed one. A dry run writes nothing --
    the same principle as `abandoned` and `digests`, where looking at a journal
    must not change it. A column that could only ever be false would invite
    someone to record dry runs, and "the purge ran" would stop meaning
    anything."""
    assert "dry_run" not in _sql(DDL)
    assert "dry_run" not in _sql(RECORD)


def test_a_hand_run_is_recognisable_rather_than_disguised():
    """run_id is nullable: a purge run by hand has no Airflow run, and an
    invented identifier would hide that."""
    text = _sql(DDL)
    line = [row for row in text.splitlines()
            if re.search(r"^\s+run_id\s", row)][0]
    assert "NOT NULL" not in line


def test_the_journal_is_append_only_for_the_role_that_purges():
    text = _sql(DDL)
    assert "GRANT SELECT, INSERT ON fd_purges TO fd_purge" in text
    assert re.search(r"REVOKE UPDATE, DELETE, TRUNCATE ON fd_purges\s+FROM fd_purge",
                     text)
    assert "GRANT SELECT ON fd_purges TO fd_report" in text


def test_the_sequence_is_granted_too():
    """A bigserial without its sequence is an INSERT that fails on the role
    that holds every other privilege it needs."""
    assert "GRANT USAGE, SELECT ON SEQUENCE fd_purges_purge_id_seq TO fd_purge" \
        in _sql(DDL)


@pytest.mark.parametrize("constraint", [
    "fd_purges_rows_need_batches",
    "fd_purges_cutoff_before_as_of",
    "fd_purges_override_is_the_cutoff",
])
def test_the_table_refuses_a_row_that_contradicts_itself(constraint):
    assert constraint in _sql(DDL)


def test_the_cutoff_cannot_be_later_than_the_instant_it_came_from():
    """That would delete rows younger than the retention."""
    assert "CHECK (cutoff <= as_of)" in _sql(DDL)


# --- who writes it, and when -------------------------------------------------------

def test_the_journal_is_written_by_execute_and_not_by_its_callers():
    """So that no caller can delete without journalling."""
    source = (ROOT / "ml4fd" / "purge.py").read_text(encoding="utf-8")
    body = source.split("def execute(")[1].split("\ndef ")[0]
    assert "journal(" in body
    for tool in ("purge_run.py", "purge_plan.py"):
        text = (ROOT / "tools" / tool).read_text(encoding="utf-8")
        assert "record_purge" not in text and "purge.journal" not in text


def test_the_dry_run_writes_nothing():
    source = (ROOT / "tools" / "purge_plan.py").read_text(encoding="utf-8")
    assert "execute" not in source.replace("execute_file", "")


def test_it_is_written_after_the_deletion():
    """Recording first would claim a purge that may not happen, and nothing
    would correct the claim. Recording after can leave rows deleted without a
    line -- which the acceptance catches by recomputing the oldest row."""
    source = (ROOT / "ml4fd" / "purge.py").read_text(encoding="utf-8")
    body = source.split("def execute(")[1].split("\ndef ")[0]
    assert body.index("purge_batch(") < body.index("journal(")


def test_every_store_that_deletes_is_journalled_even_with_nothing_to_remove():
    """A Sunday that found nothing is the ordinary case, and it is the proof
    that the purge RUNS."""
    source = (ROOT / "ml4fd" / "purge.py").read_text(encoding="utf-8")
    body = source.split("def execute(")[1].split("\ndef ")[0]
    loop = body.split("for store_report in report[\"stores\"]:")[-1]
    # the only thing that skips a store is having no delete file at all
    assert "if store_report[\"store\"] not in DELETE_SQL" in loop
    assert "n_deleted" not in loop        # no "skip when zero"


def test_the_recorded_share_is_what_was_removed_and_not_what_was_planned():
    source = (ROOT / "ml4fd" / "purge.py").read_text(encoding="utf-8")
    body = source.split("def journal(")[1].split("\ndef ")[0]
    assert "deleted / total" in body


def test_the_dag_hands_over_its_run_id():
    body = (ROOT / "airflow" / "dags" / "dag_purge.py").read_text(
        encoding="utf-8")
    assert "run_id=opened[\"run_id\"]" in body


# --- the acceptance ------------------------------------------------------------------

def test_the_decisive_criterion_asks_a_privilege_and_not_a_consequence():
    """"THE PURGE DID NOT OVERFLOW" IS NOT MEASURED HERE, it is something the
    session cannot do. A criterion that reads a privilege cannot be satisfied
    by luck."""
    text = _sql(ACCEPT)
    assert "information_schema.table_privileges" in text
    assert "grantee = 'fd_purge'" in text
    for table in ("fd_transactions", "fd_ground_truth", "fd_scores",
                  "fd_frauds", "fd_reports"):
        assert table in text


def test_the_acceptance_does_not_compare_the_digest_of_the_exploited_layer():
    """THE PLAN ASKED FOR EXACTLY THAT, and it cannot hold: the stream writes a
    transaction every minute, so the digest of fd_transactions legitimately
    differs between any two instants. A criterion that fails for a good reason
    is a criterion somebody switches off."""
    text = _sql(ACCEPT)
    assert "md5" not in text


def test_the_boundary_is_read_from_what_was_applied_and_not_restated():
    """The durations are in governance.yml; what this file asks is whether
    anything survived the cutoff a purge actually used."""
    text = _sql(ACCEPT)
    assert "fd_purges" in text and "max(cutoff)" in text
    assert "90" not in text


def test_a_batch_without_a_fingerprint_is_a_criterion_and_not_a_figure():
    """It is unpurgeable, and the acceptance says so before a Sunday does."""
    text = _sql(ACCEPT)
    assert "sans empreinte relevee" in text


def test_the_evidence_must_outlive_the_data():
    text = _sql(ACCEPT)
    assert "empreinte conservee pour chaque lot purge" in text


def test_nothing_recorded_yet_is_reported_and_not_passed():
    """At ninety days of retention with batches loaded this month, the first
    purge with anything to remove is months away. A red check for that is a
    check somebody switches off -- and a green one would read as compliance."""
    text = _sql(ACCEPT)
    assert text.count("'mesure' ELSE 'oui' END") == 2
    assert "aucune purge enregistree" in text


def test_an_execution_is_identified_by_what_the_caller_passed():
    """NOT BY ran_at, which defaults to now() and is therefore shared only as
    long as the two inserts sit in one transaction -- a coupling nobody
    declared, and one my own verification broke on the first real run. as_of is
    passed once per purge and is identical for every store of it."""
    text = _sql(ACCEPT)
    assert "count(DISTINCT (coalesce(run_id, \'\'), as_of))" in text
    assert "ran_at)) AS executions" not in text


def test_the_acceptance_raises_so_that_psql_exits_non_zero():
    """psql exits 0 whatever a verdict column says: the defect that let
    `fd-manager check` walk past seven failures."""
    text = _sql(ACCEPT)
    assert "CREATE TEMP TABLE acceptance" in text
    assert "RAISE EXCEPTION" in text
    assert "verdict LIKE '%ECHEC%'" in text


# --- the checker ----------------------------------------------------------------------

def test_the_checker_asks_what_sql_cannot():
    source = CHECKER.read_text(encoding="utf-8")
    assert "config.load()" in source
    assert "dag_purge.py" in source
    assert "fd_datasets" in source


def test_the_checker_compares_the_declaration_with_what_was_applied():
    """NEITHER SIDE CAN ANSWER THIS ALONE: the SQL cannot read governance.yml,
    and governance.yml cannot know what ran. This is the whole of EC18."""
    source = CHECKER.read_text(encoding="utf-8")
    assert "raw_retention_days" in source
    assert "(as_of - cutoff).days" in source


def test_the_checker_holds_the_dag_to_the_declaration():
    source = CHECKER.read_text(encoding="utf-8")
    assert "purge_schedule" in source
    assert "catchup=False" in source
    assert '"before" not in code' in source


def test_the_checker_strips_the_prose_before_grepping_the_dag():
    """The module explains at length what it must never do; a grep over the
    prose would find the paragraph forbidding it. The fifth time this project
    pays for that mistake."""
    source = CHECKER.read_text(encoding="utf-8")
    assert "def _code(" in source
    assert "ast.unparse" in source


def test_the_checker_does_not_spell_the_dag_directory_itself():
    """tests/test_dag_paths.py holds this repo-wide; named here because this
    file is where the fourth copy was written by reflex."""
    source = CHECKER.read_text(encoding="utf-8")
    assert "config.dags_dir" in source
    assert "airflow/dags" not in source


def test_the_checker_verifies_the_files_the_catalogue_names():
    """fd_datasets describes two parquet files per version. A catalogue
    describing files that are gone is the M8 defect, and the purge is the one
    process that could cause it."""
    source = CHECKER.read_text(encoding="utf-8")
    assert "train_path" in source and "test_path" in source
    assert "absents du disque" in source


def test_the_checker_does_not_claim_a_rule_nobody_applies():
    """The five-version rule left the purged stores for `kept`: measured, these
    files carry no identifying field. A checker still asking for it would be
    asking for code this policy never required."""
    source = CHECKER.read_text(encoding="utf-8")
    assert "datasets_versions" not in source


def test_the_checker_exits_non_zero_on_a_failure():
    source = CHECKER.read_text(encoding="utf-8")
    assert "return 1 if failed else 0" in source


# --- fd-manager ------------------------------------------------------------------------

def test_both_halves_of_s5_are_in_the_check():
    text = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    block = text.split("    'check' {")[1].split("\r\n    }\r\n")[0]
    assert "s5_acceptance.sql" in block
    assert "check_s5.py" in block
    # and each behind its own Invoke-Step, so a failure names which half
    assert block.count("Invoke-Step 'criteres d''acceptation de S5'") == 1
    assert block.count("Invoke-Step 'declaration, DAG et fichiers de S5'") == 1


def test_the_acceptance_task_covers_s5_too():
    text = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    block = text.split("    'accept' {")[1].split("\r\n    }\r\n")[0]
    assert "s5_acceptance.sql" in block and "check_s5.py" in block


def test_every_process_of_the_project_is_in_the_check():
    """S1 to S5, both halves of each where there are two. Read from the script
    rather than remembered."""
    text = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    block = text.split("    'check' {")[1].split("\r\n    }\r\n")[0]
    for process in ("s1", "s2", "s3", "s4", "s5"):
        assert "%s_acceptance.sql" % process in block, process
    for checker in ("check_s2.py", "check_s3.py", "check_s4.py", "check_s5.py"):
        assert checker in block, checker


def test_a_forget_is_not_read_as_a_retention_purge():
    """FOUND ON A SOUND STACK, 18 November 2026. Four targeted forgets (Q7)
    made S5's acceptance fail twice: their cutoff -- the forgotten batch's last
    row -- was read as a boundary, so 556,560 rows "survived" it; and each
    journalled one store, read as a purge that forgot the other. A forget is
    batch_id IS NOT NULL; every retention criterion reads batch_id IS NULL."""
    text = _sql(ACCEPT)
    for cte in ("applied AS (", "runs_journalled AS ("):
        body = text.split(cte)[1].split("\n),")[0]
        assert "batch_id IS NULL" in body, cte
    assert "batch_id IS NOT NULL" in text.split("forgets AS (")[1].split("\n),")[0]
    source = CHECKER.read_text(encoding="utf-8")
    assert "FROM fd_purges WHERE batch_id IS NULL" in source
