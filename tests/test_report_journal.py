"""The journal of the reports, and the mornings it proves are missing.

Block: B3, B4.
Lot: B3-P3, B4-L0.
Competency: B3-C5.
Covers: B3-EC14, B3-EC23.

A WRONG REPORT IS SEEN; AN ABSENT ONE IS NOT. The analyst who finds nothing in
his inbox concludes there was nothing to report. fd_reports turns that silence
into a question, and sql/checks/s4_absence.sql asks it.

The table, its five constraints, the upsert and the absence check were executed
against a real PostgreSQL 16 while P3 was written: every constraint was offered
a row that breaks it and refused it by name, a day written twice left one row,
and the check found exactly the two gaps planted in four days of reports --- one
BETWEEN two reports and one AFTER the last. What cannot be re-run here is read
here.
"""
from __future__ import annotations

import datetime as dt
import re
import types
from pathlib import Path

import pytest

from ml4fd import report

ROOT = Path(__file__).resolve().parents[1]
DDL = ROOT / "sql" / "ddl" / "110_reports.sql"
UPSERT = ROOT / "sql" / "reports" / "record_report.sql"
ABSENCE = ROOT / "sql" / "checks" / "s4_absence.sql"


def _code(path: Path) -> str:
    return "\n".join(line for line in path.read_text(encoding="utf-8").splitlines()
                     if not line.lstrip().startswith("--"))


# --- the table ----------------------------------------------------------------

def test_the_primary_key_is_the_covered_day():
    """THE IDEMPOTENCE. A replay of the 3rd of November replaces the row of the
    3rd of November; it cannot add a second."""
    assert "report_date    date         PRIMARY KEY" in _code(DDL)


@pytest.mark.parametrize("constraint", [
    "fd_reports_alerts_within_payments",
    "fd_reports_pending_within_payments",
    "fd_reports_version_with_alerts",
    "fd_reports_threshold_with_alerts",
    "fd_reports_digest_is_sha256",
])
def test_the_constraint_is_declared(constraint):
    """Each was offered a row that breaks it, against PostgreSQL 16, and each
    refused it naming itself."""
    assert constraint in _code(DDL)


def test_a_day_with_alerts_must_name_the_model_that_raised_them():
    """Without this a row could claim alerts produced by nothing -- and the
    equality runs BOTH ways, so a version on a day with no alert is refused
    too."""
    assert "CHECK ((n_alerts = 0) = (model_version IS NULL))" in _code(DDL)


def test_the_digest_is_held_to_the_shape_of_a_sha256():
    """A truncated or upper-case digest would never match the file, and the
    mismatch would read as a corrupted report rather than a bad write."""
    assert "'^[0-9a-f]{64}$'" in _code(DDL)


def test_the_reporting_role_may_write_its_own_journal_and_nothing_else():
    """The property of fd_report is that it has NO ACCESS TO THE RAW ZONE --
    the raw payloads still carry names, addresses and card numbers -- and not
    that it writes nothing. Measured against PostgreSQL 16: it inserts here,
    and "permission denied" on fd_transactions and on a DELETE here."""
    code = _code(DDL)
    assert "GRANT SELECT, INSERT, UPDATE ON fd_reports TO fd_report;" in code
    assert "REVOKE DELETE, TRUNCATE ON fd_reports FROM fd_report;" in code


# --- the upsert ---------------------------------------------------------------

def test_a_replay_replaces_and_does_not_duplicate():
    assert "ON CONFLICT (report_date) DO UPDATE" in _code(UPSERT)


def test_the_replay_refreshes_every_column():
    """A column left out of the DO UPDATE would keep the value of the FIRST
    run for ever, and the row would describe two different documents at once."""
    code = _code(UPSERT)
    inserted = re.search(r"INSERT INTO fd_reports \(([^)]*)\)", code,
                         re.S).group(1)
    columns = {name.strip() for name in inserted.split(",")}
    refreshed = set(re.findall(r"(\w+)\s*=\s*excluded\.", code))
    # report_date is the conflict target and cannot be refreshed by itself.
    assert columns - refreshed == {"report_date"}


def test_the_row_produce_returns_fits_the_upsert_exactly():
    """A name that drifts between the dict and the SQL fails with "no value
    found for parameter", which names the parameter and not the file. The two
    are compared here instead."""
    placeholders = set(re.findall(r"%\((\w+)\)s", _code(UPSERT)))
    produced = {"report_date", "knowledge_date", "path", "content_digest",
                "n_payments", "n_alerts", "n_verdicts", "n_pending",
                "model_version", "threshold"}
    assert placeholders == produced


def test_produced_at_is_the_databases_and_not_the_callers():
    """The one column a replay legitimately changes, and the server's clock is
    the one the rest of the journal is timestamped with."""
    code = _code(UPSERT)
    assert "produced_at" in code
    assert "%(produced_at)s" not in code
    assert "now()" in code


# --- the absence check --------------------------------------------------------

def test_the_check_starts_at_the_first_report_and_not_thirty_days_ago():
    """THE DIFFERENCE BETWEEN A CONTROL AND AN ALARM. A pipeline three days old
    would otherwise list twenty-seven missing mornings nobody ever expected, and
    a check that fails on reality teaches its reader to ignore it -- the lesson
    of criterion 13 of S3, paid for once already."""
    code = _code(ABSENCE)
    assert "greatest(min(report_date), current_date - 30)" in code


def test_an_empty_table_reports_nothing():
    """Before the first report ever produced, no morning is missing. "No report
    has ever been produced" is a different finding and belongs to P5."""
    assert "HAVING count(*) > 0" in _code(ABSENCE)


def test_the_check_stops_at_yesterday():
    """Today's report is not due until tomorrow morning."""
    assert "current_date - 1" in _code(ABSENCE)


def test_the_check_finds_a_hole_after_the_last_report_too():
    """The gap that matters more: the DAG stopped, and has been stopped since.
    Walking to yesterday rather than to the last report is what catches it."""
    code = _code(ABSENCE)
    assert "last_day" in code
    assert "max(report_date)" not in code       # NOT bounded by the last report


def test_the_check_writes_nothing():
    code = _code(ABSENCE).upper()
    for verb in ("INSERT", "UPDATE", "DELETE", "TRUNCATE"):
        assert verb not in code, verb


# --- write, THEN record -------------------------------------------------------

class _Fake:
    def __init__(self):
        self.order = []

    def commit(self):
        self.order.append("commit")

    def cursor(self):
        raise AssertionError("produce() ne doit pas requeter ici")


def test_the_document_is_written_before_the_row_is_recorded(monkeypatch,
                                                            tmp_path):
    """THE ORDER, and it is N5's turned around: recording first would leave the
    table claiming a document nobody wrote, and the absence check -- which reads
    that table -- would report a healthy morning. A crash between the two leaves
    a document with no row, which the check DOES catch. One failure is silent,
    the other loud."""
    fake = _Fake()
    settings = types.SimpleNamespace(report_root=str(tmp_path),
                                     alert_capacity_per_day=60)
    window = report.Window(
        start=dt.datetime(2026, 11, 16, tzinfo=dt.timezone.utc),
        end=dt.datetime(2026, 11, 17, tzinfo=dt.timezone.utc),
        knowledge_at=dt.datetime(2026, 11, 17, 6, tzinfo=dt.timezone.utc),
        zone="UTC")

    monkeypatch.setattr(report, "gather", lambda *a: {
        "window": window,
        "volumes": {"total": {"transactions": 0, "amount": 0, "cardholders": 0,
                              "median_amount": None}, "category": [], "hour": []},
        "alerts": [], "verdicts": {"verdicts": 0},
        "pending": {"pending": 0, "without_verdict_row": 0,
                    "last_due_at": None},
        "reference": None, "capacity": 60})
    monkeypatch.setattr(report, "chain_health", lambda *a: None)
    monkeypatch.setattr(report, "write",
                        lambda *a: (fake.order.append("write")
                                    or tmp_path / "2026-11-16.md"))
    monkeypatch.setattr(report, "record",
                        lambda conn, row: fake.order.append("record"))

    report.produce(fake, settings, window)
    assert fake.order == ["write", "record", "commit"]


def test_a_dry_run_writes_nothing_at_all(monkeypatch,
                                                           tmp_path):
    """So looking at a morning does not make the absence check believe it was
    produced."""
    fake = _Fake()
    settings = types.SimpleNamespace(report_root=str(tmp_path),
                                     alert_capacity_per_day=60)
    window = report.Window(
        start=dt.datetime(2026, 11, 16, tzinfo=dt.timezone.utc),
        end=dt.datetime(2026, 11, 17, tzinfo=dt.timezone.utc),
        knowledge_at=dt.datetime(2026, 11, 17, 6, tzinfo=dt.timezone.utc),
        zone="UTC")
    monkeypatch.setattr(report, "gather", lambda *a: {
        "window": window,
        "volumes": {"total": {"transactions": 0, "amount": 0, "cardholders": 0,
                              "median_amount": None}, "category": [], "hour": []},
        "alerts": [], "verdicts": {"verdicts": 0},
        "pending": {"pending": 0, "without_verdict_row": 0,
                    "last_due_at": None},
        "reference": None, "capacity": 60})
    monkeypatch.setattr(report, "chain_health", lambda *a: None)
    monkeypatch.setattr(report, "write",
                        lambda *a: (fake.order.append("write")
                                    or tmp_path / "2026-11-16.md"))
    monkeypatch.setattr(report, "record",
                        lambda conn, row: fake.order.append("record"))

    text = report.preview(fake, settings, window)
    assert fake.order == []          # NEITHER write NOR record
    assert isinstance(text, str) and text


def test_producing_cannot_skip_the_table_any_more():
    """THE DEFECT THE ACCEPTANCE OF S4 CAUGHT on 17 November: journal=False
    skipped fd_reports and WROTE THE FILE ANYWAY, so a dry run overwrote the
    document whose sha256 the table vouches for. The switch is gone: the
    document and its row go together, and preview() is how one looks."""
    import inspect
    assert "journal" not in inspect.signature(report.produce).parameters
    source = (ROOT / "ml4fd" / "report.py").read_text(encoding="utf-8")
    body = source.split("def preview(")[1].split("\ndef ")[0]
    code = body.split('"""')[2]              # past the docstring
    for forbidden in ("write(", "record("):
        assert forbidden not in code, "preview() %s" % forbidden


def test_the_runner_offers_the_dry_run_and_it_writes_nothing():
    body = (ROOT / "tools" / "daily_report.py").read_text(encoding="utf-8")
    assert "--dry-run" in body
    assert "report.preview(" in body
    assert "journal=" not in body
    # and it says where the document WOULD have gone
    assert "il serait ecrit dans" in body
