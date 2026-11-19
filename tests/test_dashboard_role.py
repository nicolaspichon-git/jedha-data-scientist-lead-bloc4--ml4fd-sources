"""R0: the role the dashboard logs in as reads three tables and nothing else.

Covers: EC19
Lot: R0
Competency: C2, C5

THE ONLY COMPONENT OPEN TO A BROWSER. It must be UNABLE to write, to see the
raw zone or to learn the stack's secrets, not merely refrain from it.

Proven on PostgreSQL 16, connected as fd_dashboard WITH ITS PASSWORD over TCP
under scram-sha-256 (as the postgres:16 image does): a wrong password is
refused; the three tables read; the raw zone, the quarantine's payload, the
verdicts and the run journal are refused; rewriting a report digest, inserting
a report, acknowledging an alert and deleting a transaction are refused; SET
ROLE to fd_report, fd_ingest or the owner is refused; CREATE in public is
refused. A TEMPORARY table is allowed -- PUBLIC holds TEMP on the database --
which opens no data and is recorded in the plan of S6 rather than revoked from
every role.

These tests hold the FILES, since the database is not reachable from pytest:
what the role is declared to be, and that no secret was written into them.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DDL = ROOT / "sql" / "ddl" / "140_dashboard_role.sql"
PASSWORD_SQL = ROOT / "sql" / "roles" / "dashboard_password.sql"
MANAGER = ROOT / "fd-manager.ps1"
READ = {"fd_reports", "fd_frauds", "fd_transactions",      # R2, R3
        "fd_pipeline_runs", "fd_ops_state",                   # R6
        "fd_ops_notifications",                               # R9
        "fd_ground_truth", "fd_scores"}                       # R10
# R6: the quarantine BY COLUMN, for the volume invariant -- never payload,
# never reason_detail.
# R10: the catalogue BY COLUMN -- the reference, never the paths.
DATASET_COLUMNS = {"recipe_digest", "version", "created_at", "base_rate_test",
                   "test_recall", "test_precision", "test_alerts_per_day",
                   "test_threshold", "measured_at"}
QUARANTINE_COLUMNS = {"batch_id", "line_no",                # R6
                      "reason_class", "reason_code", "rejected_at"}  # R7


def _code(path: Path) -> str:
    return "\n".join(line.split("--")[0]
                     for line in path.read_text(encoding="utf-8").splitlines())


def test_the_role_logs_in_and_is_nothing_more():
    code = _code(DDL).upper()
    for attribute in ("LOGIN", "NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE",
                      "NOINHERIT", "NOREPLICATION", "NOBYPASSRLS"):
        assert attribute in code, attribute


def test_it_is_member_of_no_role():
    """fd_report PRODUCES the morning report and holds INSERT and UPDATE on
    fd_reports: a dashboard that inherited it could rewrite the digest that
    proves a document was not altered."""
    code = _code(DDL)
    assert not re.search(r"GRANT\s+fd_\w+\s+TO\s+fd_dashboard", code, re.I)
    assert not re.search(r"IN\s+ROLE", code, re.I)


def test_it_reads_exactly_the_tables_of_its_pages():
    code = _code(DDL)
    tables = re.findall(r"GRANT\s+SELECT\s+ON\s+(.+?)\s+TO\s+fd_dashboard", code,
                        re.I | re.S)
    assert len(tables) == 1, tables
    assert {t.strip() for t in tables[0].split(",")} == READ
    every = re.findall(r"GRANT\s+(.+?)\s+ON\s+", code, re.I | re.S)
    assert all(g.strip().upper().startswith("SELECT") for g in every), every


def test_the_quarantine_is_granted_by_column_and_never_its_payload():
    """pipeline_health.sql counts the quarantined lines of each batch, and that
    is all R6 needs. payload is the raw rejected record -- the raw zone's
    personal data -- and reason_detail may quote a raw value."""
    code = _code(DDL)
    granted = re.findall(r"GRANT\s+SELECT\s*\(([^)]*)\)\s*ON\s+fd_quarantine\s+TO\s+"
                         r"fd_dashboard", code, re.I | re.S)
    assert len(granted) == 1
    assert {c.strip() for c in granted[0].split(",")} == QUARANTINE_COLUMNS
    # And the table is closed FIRST on every replay, so a column added by hand
    # does not survive `init` -- proven on PostgreSQL with payload.
    assert code.index("REVOKE ALL ON fd_raw_events, fd_quarantine") < \
        code.index("GRANT SELECT (batch_id, line_no")


def test_the_catalogue_is_granted_by_column_and_never_its_paths():
    """R10 compares with what M5 measured. The dashboard opens no dataset: the
    Parquet paths and the feature list are not its business."""
    code = _code(DDL)
    granted = re.findall(r"GRANT\s+SELECT\s*\(([^)]*)\)\s*ON\s+fd_datasets\s+TO\s+"
                         r"fd_dashboard", code, re.I | re.S)
    assert len(granted) == 1
    columns = {c.strip() for c in granted[0].split(",")}
    assert columns == DATASET_COLUMNS
    assert not columns & {"train_path", "test_path", "features"}


def test_the_rest_is_revoked_in_writing():
    """Nothing grants these today. The REVOKE makes the intent part of the
    schema, so the acceptance has something to read rather than an absence to
    infer."""
    code = _code(DDL)
    revoked = " ".join(re.findall(r"REVOKE\s+ALL\s+ON\s+(.+?)\s+FROM", code,
                                  re.I | re.S))
    for table in ("fd_raw_events", "fd_quarantine", "fd_ground_truth",
                  "fd_scores", "fd_datasets", "fd_purges"):
        assert table in revoked, table


def test_no_secret_is_written_into_any_versioned_sql_file():
    """A versioned file is not a place for a password. The role is created
    without one, and so cannot log in until init applies the .env value."""
    for path in (ROOT / "sql").rglob("*.sql"):
        assert not re.search(r"PASSWORD\s+'", path.read_text(encoding="utf-8"),
                             re.I), path


def test_the_password_arrives_as_a_quoted_psql_variable():
    """:'name' makes psql quote the value as a literal: it can never be read
    as SQL, whatever it contains."""
    text = PASSWORD_SQL.read_text(encoding="utf-8")
    assert "PASSWORD :'dashboard_password'" in text
    assert r"\if :{?dashboard_password}" in text, "no value must mean no change"


def test_the_password_file_lives_outside_the_ddl():
    """sql/ddl is replayed by anything that builds a schema; this file needs a
    secret it must not find in the repository."""
    assert PASSWORD_SQL.parent.name == "roles"
    assert not (ROOT / "sql" / "ddl" / PASSWORD_SQL.name).exists()


def test_setup_adds_the_line_to_an_env_made_before_s6():
    """A replace-only loop -- the way the pepper is written -- would leave
    every existing .env without a dashboard password, silently."""
    text = MANAGER.read_text(encoding="utf-8")
    block = text[text.index("THE DASHBOARD'S PASSWORD"):]
    assert "$lines += \"FD_DASHBOARD_PASSWORD=$d\"" in block
    assert "New-Pepper" in block, "a cryptographic generator, not Get-Random"


def test_init_refuses_a_value_of_the_wrong_form():
    """64 hexadecimal characters: nothing PowerShell would mangle on a native
    command line, nothing psql would mistake."""
    text = MANAGER.read_text(encoding="utf-8")
    assert "'^[0-9a-f]{64}$'" in text
    assert "/work/sql/roles/dashboard_password.sql" in text
