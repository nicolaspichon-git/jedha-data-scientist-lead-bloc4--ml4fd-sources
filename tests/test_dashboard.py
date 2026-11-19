"""R1: the reads behind the dashboard -- delegations, not second answers.

Covers: EE03, EC14, EC17
Lot: R1
Competency: C4

The dashboard decides nothing. What is tested here is mostly that it ASKS the
definitions other processes already use, rather than answering on its own, and
that everything it reads lies inside what R0 granted fd_dashboard.
"""
from __future__ import annotations

import datetime as dt
import os
import re
import types
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import dashboard, notify, report  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PARIS = "Europe/Paris"


def settings(tmp_path: Path, capacity: int = 60):
    return types.SimpleNamespace(report_root=str(tmp_path), report_timezone=PARIS,
                                 alert_capacity_per_day=capacity)


class Cursor:
    def __init__(self, row): self.row = row
    def __enter__(self): return self
    def __exit__(self, *exc): return False
    def execute(self, sql, params=None): self.sql = sql
    def fetchone(self): return self.row


class Conn:
    def __init__(self, row=("fd_dashboard", False)): self.row = row
    def cursor(self): return Cursor(self.row)


# --- who it runs as -------------------------------------------------------------

def test_it_serves_as_fd_dashboard():
    assert dashboard.assert_identity(Conn()) == "fd_dashboard"


def test_it_refuses_to_serve_as_a_superuser():
    """The defect this guards: the service reusing x-airflow-common in
    docker-compose.yml, and so the OWNER's credentials."""
    with pytest.raises(dashboard.WrongIdentity) as refused:
        dashboard.assert_identity(Conn(("aiafraud", True)))
    assert "x-airflow-common" in str(refused.value)


def test_it_refuses_to_serve_as_any_other_role():
    with pytest.raises(dashboard.WrongIdentity):
        dashboard.assert_identity(Conn(("fd_report", False)))


def test_it_never_sets_a_role_that_could_be_reset():
    source = (ROOT / "ml4fd" / "dashboard.py").read_text(encoding="utf-8")
    assert "db.connect(None, settings)" in source


# --- the day ------------------------------------------------------------------

def test_today_is_the_day_of_the_declared_zone(tmp_path):
    """23:30 UTC on the 16th is 00:30 in Paris on the 17th. A page that counted
    in UTC would not find the alerts the document of the same day lists."""
    late = dt.datetime(2026, 11, 16, 23, 30, tzinfo=dt.timezone.utc)
    assert dashboard.today(settings(tmp_path), late) == dt.date(2026, 11, 17)


def test_the_window_runs_to_the_next_local_midnight(tmp_path):
    """Wall-clock arithmetic: 23 hours on the spring change, 25 on the autumn
    one -- the calendar day, which is what a report covers."""
    s = settings(tmp_path)
    for day, hours in ((dt.date(2026, 3, 29), 23), (dt.date(2026, 10, 25), 25),
                       (dt.date(2026, 11, 16), 24)):
        w = report.window_for(s, day)
        assert w.start.hour == 0 and w.end.hour == 0 and w.day == day
        real = w.end.astimezone(dt.timezone.utc) - w.start.astimezone(dt.timezone.utc)
        assert real == dt.timedelta(hours=hours), (day, real)


def test_knowledge_defaults_to_the_end_of_the_day(tmp_path):
    w = report.window_for(settings(tmp_path), dt.date(2026, 11, 16))
    assert w.knowledge_at == w.end


# --- the document ---------------------------------------------------------------

def _row(tmp_path, text, day=dt.date(2026, 11, 16), path=None):
    target = report.path_for(settings(tmp_path), day)
    if text is not None:
        target.write_text(text, encoding="utf-8")
    return {"report_date": day, "path": str(path or target),
            "content_digest": report.digest("# Rapport\n")}


def test_a_document_that_matches_its_digest(tmp_path):
    m = dashboard.morning(settings(tmp_path), _row(tmp_path, "# Rapport\n"))
    assert m.check.conforms and m.check.text == "# Rapport\n"
    assert not m.relocated


def test_a_document_that_was_altered_is_said_so_and_still_shown(tmp_path):
    """The reader must be able to see WHAT differs; the page says it first."""
    m = dashboard.morning(settings(tmp_path), _row(tmp_path, "# Rapport modifie\n"))
    assert m.check.verdict == report.DIFFERENTE
    assert m.check.text == "# Rapport modifie\n"


def test_a_missing_document_is_absent_not_an_exception(tmp_path):
    m = dashboard.morning(settings(tmp_path), _row(tmp_path, None))
    assert m.check.verdict == report.ABSENT and m.check.text is None


def test_the_recorded_path_is_compared_and_not_followed(tmp_path):
    """fd_reports.path is a path of the Airflow container. Following it would
    work by coincidence while both mount reports/ at the same place."""
    elsewhere = tmp_path / "ailleurs" / "2026-11-16.md"
    m = dashboard.morning(settings(tmp_path),
                          _row(tmp_path, "# Rapport\n", path=elsewhere))
    assert m.check.conforms, "the document was read where path_for() says"
    assert m.relocated, "and the difference of mounts is reported"


def test_the_digest_rule_is_the_producers():
    """check_s4.py computed it with hashlib on its own. A check written with
    another function agrees with the producer only until either changes."""
    for relative in ("tools/check_s4.py", "ml4fd/dashboard.py"):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert "hashlib" not in text, relative
        assert "check_document(" in text, relative


# --- one definition of the day --------------------------------------------------

def test_the_day_window_is_built_in_one_place_only():
    """It was written three times; the dashboard would have been the fourth."""
    offenders = []
    for path in list((ROOT / "ml4fd").glob("*.py")) + \
            list((ROOT / "tools").glob("*.py")) + \
            list((ROOT / "airflow" / "dags").glob("*.py")):
        if path.name == "report.py":
            continue
        text = path.read_text(encoding="utf-8")
        if "report.Window(" in text:
            offenders.append(path.name)
    assert offenders == [], offenders


# --- the alerts -----------------------------------------------------------------

def test_the_alerts_are_the_reports_own_query_over_the_reports_own_day(
        tmp_path, monkeypatch):
    seen = {}

    def fake(conn, window):
        seen["window"] = window
        return [{"txn_id": "T1", "amt": 400, "notified": True,
                 "client_key": "deadbeef", "first": "Jane"}]

    monkeypatch.setattr(report, "alerts", fake)
    rows = dashboard.frauds_of_day(Conn(), settings(tmp_path), dt.date(2026, 11, 16))
    w = seen["window"]
    assert w.day == dt.date(2026, 11, 16) and w.start.hour == 0
    assert "client_key" not in rows[0] and "first" not in rows[0]
    assert rows[0]["txn_id"] == "T1"


def test_the_shown_fields_carry_nothing_personal():
    """A whitelist: a column added to the query tomorrow must be DECIDED onto
    the page. client_key is a pseudonym the analyst has no use for (EC17)."""
    shown = set(dashboard.FRAUD_FIELDS)
    assert not shown & set(notify.FORBIDDEN_FIELDS)
    assert "client_key" not in shown


def test_the_capacity_is_n5s_rolling_count(monkeypatch, tmp_path):
    monkeypatch.setattr(notify, "window", lambda conn: {
        "notified_24h": 60, "pending": 7, "pending_failed": 0, "total": 90})
    monkeypatch.setattr(notify, "budget", lambda conn, capacity: 0)
    c = dashboard.capacity(Conn(), settings(tmp_path))
    assert c["reached"] and c["pending"] == 7 and c["capacity"] == 60


def test_the_head_of_the_page_counts_what_it_shows():
    s = dashboard.summary_of([{"notified": True}, {"notified": False},
                              {"notified": True}])
    assert s == {"alerts": 3, "notified": 2, "not_notified": 1}


# --- everything read lies inside what R0 granted --------------------------------

def _granted() -> tuple[set[str], dict[str, set[str]]]:
    """(tables granted whole, {table: columns granted one by one})."""
    ddl = (ROOT / "sql" / "ddl" / "140_dashboard_role.sql").read_text(encoding="utf-8")
    code = "\n".join(line.split("--")[0] for line in ddl.splitlines())
    tables = re.search(r"GRANT\s+SELECT\s+ON\s+(.+?)\s+TO\s+fd_dashboard", code,
                       re.I | re.S).group(1)
    columns = {table: {c.strip() for c in cols.split(",")}
               for cols, table in re.findall(
                   r"GRANT\s+SELECT\s*\(([^)]*)\)\s*ON\s+(\w+)\s+TO\s+fd_dashboard",
                   code, re.I)}
    return {t.strip() for t in tables.split(",")}, columns


def test_every_query_the_dashboard_runs_stays_inside_its_grants():
    """THE TEST THAT TIES R0 TO THE PAGES. A page that reached for another
    table -- or another COLUMN of the quarantine -- would fail in the container
    with `permission denied`, found by whoever opens it first. Here it fails
    before."""
    from ml4fd import health
    whole, by_column = _granted()
    files = [dashboard.REPORTS_SQL, dashboard.ABSENCE_SQL, dashboard.RUNS_SQL,
             dashboard.REJECTS_SQL, dashboard.REJECTS_TOTAL_SQL,
             dashboard.STREAM_RATE_SQL,
             report.FRAUDS_SQL, notify.WINDOW_SQL,
             health.FACTS_SQL, health.STATE_SQL,
             dashboard.OPS_SQL,                                    # R9
             report.VERDICTS_SQL, report.REFERENCE_SQL,            # R10
             dashboard.PERF_BY_MODEL_SQL, dashboard.PERF_BY_DAY_SQL]
    # Every column of a table granted BY COLUMN, read from its DDL: in a query
    # that reads such a table, the columns NOT granted must not appear at all
    # -- aliased or bare (`payload`, not only `q.payload`).
    ddl = (ROOT / "sql" / "ddl" / "020_quarantine.sql").read_text(encoding="utf-8")
    table_columns = {"fd_quarantine": set(re.findall(
        r"^\s{4}(\w+)\s+\w", ddl.split("CREATE TABLE")[1].split(");")[0], re.M))}
    # fd_datasets: the columns of its CREATE TABLE and those R10 ADDs.
    ddl = (ROOT / "sql" / "ddl" / "070_datasets.sql").read_text(encoding="utf-8")
    table_columns["fd_datasets"] = set(re.findall(
        r"^\s{4}(\w+)\s+\w", ddl.split("CREATE TABLE")[1].split(");")[0], re.M)
    ) | set(re.findall(r"ADD COLUMN IF NOT EXISTS (\w+)", ddl))
    assert {"train_path", "test_recall"} <= table_columns["fd_datasets"]
    for relative in files:
        sql = (ROOT / "sql" / relative).read_text(encoding="utf-8")
        code = "\n".join(line.split("--")[0] for line in sql.splitlines())
        used = set(re.findall(r"\b(fd_[a-z_]+)\b", code))
        assert used <= whole | set(by_column), "%s reads %s" % (
            relative, used - whole - set(by_column))
        for table, allowed in by_column.items():
            if table in used:
                forbidden = table_columns[table] - allowed
                named = set(re.findall(r"\b(\w+)\b", code)) & forbidden
                assert not named, "%s names %s.%s" % (relative, table, named)
            for alias in re.findall(r"\b%s\s+(?:AS\s+)?(\w+)" % table, code, re.I):
                touched = set(re.findall(r"\b%s\.(\w+)" % alias, code))
                assert touched <= allowed, "%s reads %s.%s" % (
                    relative, table, touched - allowed)


def test_the_dashboard_module_writes_nothing_and_needs_no_streamlit():
    source = (ROOT / "ml4fd" / "dashboard.py").read_text(encoding="utf-8")
    assert "import streamlit" not in source
    assert not re.search(r"\b(INSERT|UPDATE|DELETE|TRUNCATE)\b", source)
