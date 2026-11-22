"""R3, R8, R11: the frauds page -- the alerts, and their notification.

Block: B3, B4.
Lot: B3-R3, B4-L0.
Competency: B3-C4.
Covers: B3-EE03, B3-EC17.

The DAY shown is a calendar day in the declared zone, like the report's; the
CAPACITY is N5's rolling 24 hours. A capacity reached is a decision of the
notifier, not an outage, and the page must say which.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import os
import runpy
import sys
import types
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import config, dashboard, notify, report  # noqa: E402

from test_dashboard_morning import FakeStreamlit, Stop  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "dashboard"
DAY = dt.date(2026, 11, 16)
UTC = dt.timezone.utc


def settings():
    return types.SimpleNamespace(report_root="/tmp", report_timezone="Europe/Paris",
                                 alert_capacity_per_day=60, alert_channel="file")


def alert(txn="T1", notified=True, error=None, loss=372.0):
    return {"txn_id": txn, "txn_ts": dt.datetime(2026, 11, 16, 22, 41, 7, tzinfo=UTC),
            "merchant": "fraud_Kub", "category": "shopping_net", "amt": 1234.5,
            "score": 0.97, "threshold": 0.93, "model_version": "v7",
            "raised_at": None, "notified": notified,
            "notified_at": (dt.datetime(2026, 11, 17, 6, 2, 9, tzinfo=UTC)
                            if notified else None),
            "channel": "file" if notified else None, "attempts": 1,
            "last_error": error, "f_tx_count": 12, "f_avg_amount": 54.2,
            "times_usual": 22.8, "expected_loss": loss,
            "client_key": "deadbeef"}


@pytest.fixture
def state(monkeypatch):
    s = {"rows": [], "cap": {"notified_24h": 3, "pending": 0, "pending_failed": 0,
                             "total": 3}, "remaining": 57, "window": None}

    def rows(conn, window):
        s["window"] = window
        return s["rows"]

    monkeypatch.setattr(report, "alerts", rows)
    monkeypatch.setattr(notify, "window", lambda conn: s["cap"])
    monkeypatch.setattr(notify, "budget", lambda conn, c: s["remaining"])
    return s


def levels(view):
    return [n.level for n in view["notices"]]


# --- what the page shows --------------------------------------------------------

def test_the_rows_are_the_reports_over_the_reports_day(state):
    state["rows"] = [alert()]
    view = dashboard.frauds_view(None, settings(), DAY)
    assert state["window"].day == DAY and state["window"].start.hour == 0
    assert len(view["table"]) == 1 and levels(view) == []


def test_the_default_day_is_today_in_paris_not_in_utc(state):
    late = dt.datetime(2026, 11, 16, 23, 30, tzinfo=UTC)          # 00:30 in Paris
    view = dashboard.frauds_view(None, settings(), now=late)
    assert view["first"] == view["last"] == dt.date(2026, 11, 17)


def test_cells_are_written_as_the_report_writes_them(state):
    state["rows"] = [alert()]
    row = dashboard.frauds_view(None, settings(), DAY)["table"][0]
    assert row["Date et heure"] == "2026-11-16 23:41:07", "22:41 UTC is 23:41 in Paris"
    assert row["Montant"] == report._amount(1234.5)
    assert row["Score"] == report._ratio(0.97)
    assert row["Notifiée le"] == "2026-11-17 07:02:09", "06:02 UTC is 07:02 in Paris"
    assert row["× habituel"] == "22,8"


def test_the_table_holds_nothing_personal(state):
    """client_key arrives from the fake query; FRAUD_FIELDS stops it."""
    state["rows"] = [alert()]
    row = dashboard.frauds_view(None, settings(), DAY)["table"][0]
    assert "deadbeef" not in row.values()
    labels = {name for name, _ in dashboard.FRAUD_COLUMNS}
    assert labels <= set(dashboard.FRAUD_FIELDS), "every column passed the whitelist"


def test_the_report_order_is_kept(state):
    state["rows"] = [alert("T9", loss=900.0), alert("T1", loss=10.0)]
    table = dashboard.frauds_view(None, settings(), DAY)["table"]
    assert [r["Transaction"] for r in table] == ["T9", "T1"]


def test_a_reached_capacity_is_a_decision_not_an_outage(state):
    state["cap"]["notified_24h"], state["remaining"] = 60, 0
    view = dashboard.frauds_view(None, settings(), DAY)
    warning = [n for n in view["notices"] if n.level == dashboard.WARNING]
    assert warning and "pas une panne" in warning[0].text
    assert "24 heures glissantes" in warning[0].text


def test_failed_sends_come_first(state):
    state["cap"]["pending_failed"], state["cap"]["notified_24h"] = 2, 60
    state["remaining"] = 0
    state["rows"] = [alert(notified=False, error="canal indisponible")]
    view = dashboard.frauds_view(None, settings(), DAY)
    assert levels(view) == [dashboard.ERROR, dashboard.WARNING]
    assert "2 alertes" in view["notices"][0].text
    assert "toutes journées confondues" in view["notices"][0].text, (
        "the count is global: a failure can be outside the period shown")
    assert view["table"][0]["Dernière erreur"] == "canal indisponible"


def test_a_quiet_day_says_so(state):
    view = dashboard.frauds_view(None, settings(), DAY)
    assert levels(view) == [dashboard.INFO] and "2026-11-16" in view["notices"][0].text
    assert view["table"] == []


def test_the_two_clocks_are_named(state):
    state["rows"] = [alert(), alert("T2", notified=False)]
    view = dashboard.frauds_view(None, settings(), DAY)
    figures = dict(view["figures"])
    assert figures["Alertes de la période"] == "2" and figures["Non notifiées"] == "1"
    assert figures["Notifiées sur 24 h glissantes"] == "3 / 60"
    assert "et non sur la période affichée" in view["capacity_note"]
    assert "« file »" in view["capacity_note"], "the DECLARED channel is named"


# --- the page, executed ---------------------------------------------------------

class AlertsStreamlit(FakeStreamlit):
    def date_input(self, label, value=None, **kwargs):
        self._record("date_input", label, value=value, **kwargs)
        return value


@pytest.fixture
def page(monkeypatch):
    fake = AlertsStreamlit()
    monkeypatch.setitem(sys.modules, "streamlit", fake)
    monkeypatch.syspath_prepend(str(APP))
    sys.modules.pop("common", None)
    monkeypatch.setattr(config, "load", lambda *a, **k: settings())

    @contextlib.contextmanager
    def connected(s):
        yield object()

    monkeypatch.setattr(dashboard, "connect", connected)

    def run(view):
        seen = {}

        def build(conn, s, first, last):
            seen["first"], seen["last"] = first, last
            return view
        monkeypatch.setattr(dashboard, "frauds_view", build)
        try:
            runpy.run_path(str(APP / "pages" / "frauds.py"))
        except Stop:
            pass
        return fake.calls, seen
    return run


def _view(notices, table):
    return {"first": DAY, "last": DAY, "notices": notices,
            "title": "Alertes le 2026-11-16",
            "figures": [("Alertes de la période", "1")] * 4, "table": table,
            "capacity_note": "glissantes"}


def test_the_page_shows_the_notices_before_the_table(page):
    calls, _ = page(_view([dashboard.Notice(dashboard.ERROR, "echec")],
                          [{"Transaction": "T1"}]))
    names = [c[0] for c in calls]
    assert names.index("error") < names.index("dataframe")
    assert names.index("caption") < names.index("dataframe")
    assert names.count("metric") == 4


def test_the_selected_period_is_what_the_view_is_asked_for(page):
    calls, seen = page(_view([], []))
    assert isinstance(seen["first"], dt.date) and seen["first"] == seen["last"]
    picker = [c for c in calls if c[0] == "date_input"][0]
    assert isinstance(picker[2]["value"], tuple), "a range picker, not a day picker"


# --- the notification, as the page reports it (R11) -----------------------------

def test_an_alert_not_yet_notified_says_non_and_names_no_channel(state):
    """"non" and not "n/a": not having been notified is a state of the alert.
    And the column « Canal » is the one the SEND recorded, so it is empty."""
    state["rows"] = [alert(notified=False)]
    row = dashboard.frauds_view(None, settings(), DAY)["table"][0]
    assert row["Notifiée le"] == "non" and row["Canal"] == ""


def test_the_instant_of_the_notification_is_read_from_the_query():
    """The figures count `notified`; the table shows WHEN. Over a period, an
    alert raised on the 3rd and notified on the 5th read alike without it."""
    assert "notified_at" in dashboard.FRAUD_FIELDS
    assert ("notified_at", "Notifiée le") in dashboard.FRAUD_COLUMNS
    assert "notified" in dashboard.FRAUD_FIELDS, "still counted, no longer shown"
    code = (ROOT / "sql" / "reports" / "daily_frauds.sql").read_text(encoding="utf-8")
    assert "a.notified_at," in code


# --- the period (R8) -----------------------------------------------------------

def test_a_period_is_one_window_from_first_midnight_to_after_last(state):
    last = DAY + dt.timedelta(days=6)
    view = dashboard.frauds_view(None, settings(), DAY, last)
    w = state["window"]
    assert w.start.date() == DAY and w.start.hour == 0
    assert w.end.date() == last + dt.timedelta(days=1) and w.end.hour == 0
    assert view["title"] == "Alertes du 2026-11-16 au 2026-11-22"


def test_a_period_across_the_clock_change_keeps_whole_days(state):
    """25 October 2026: Paris leaves summer time; the window is still whole days."""
    dashboard.frauds_view(None, settings(), dt.date(2026, 10, 24), dt.date(2026, 10, 26))
    w = state["window"]
    assert (w.start.hour, w.end.hour) == (0, 0)
    # Same-zone subtraction is wall-clock in Python: measure the real span in UTC.
    assert w.end.astimezone(UTC) - w.start.astimezone(UTC) == dt.timedelta(hours=73)


def test_a_too_long_period_keeps_its_end_and_says_so(state):
    first = DAY - dt.timedelta(days=400)
    view = dashboard.frauds_view(None, settings(), first, DAY)
    assert view["last"] == DAY
    assert (view["last"] - view["first"]).days + 1 == dashboard.MAX_PERIOD_DAYS
    assert levels(view)[0] == dashboard.INFO and "ramenée" in view["notices"][0].text


@pytest.mark.parametrize("chosen, expected", [
    (None, (DAY, DAY)),
    (dt.date(2026, 11, 3), (dt.date(2026, 11, 3),) * 2),
    ((dt.date(2026, 11, 3),), (dt.date(2026, 11, 3),) * 2),       # second click awaited
    ((dt.date(2026, 11, 9), dt.date(2026, 11, 3)), (dt.date(2026, 11, 3), dt.date(2026, 11, 9))),
    ((), (DAY, DAY)),
])
def test_period_of_reads_every_state_of_the_picker(chosen, expected):
    assert dashboard.period_of(chosen, DAY) == expected


def test_a_quiet_day_draws_no_empty_table(page):
    calls, _ = page(_view([dashboard.Notice(dashboard.INFO, "aucune")], []))
    assert "dataframe" not in [c[0] for c in calls]


def test_no_page_offers_anything_to_write():
    """No acknowledgement, no comment, no re-send. The CALLS are matched, not
    the words: `format=` contains "form", which a substring test took for a
    form (2026-11-17)."""
    import re
    writing = re.compile(r"\bst\.(button|form|form_submit_button|text_input|"
                         r"text_area|data_editor|file_uploader|number_input|"
                         r"checkbox|toggle)\s*\(")
    for path in (APP / "pages").glob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert not writing.search(source), path.name
        assert not re.search(r"\b(INSERT|UPDATE|DELETE)\b", source), path.name


def test_the_entry_point_declares_the_pages_that_exist(monkeypatch):
    """The menu, in its order, and a file behind every entry.

    A page file that app.py does not declare is unreachable: it is reported
    here rather than left to be noticed by whoever looks for it in the menu.
    """
    fake = FakeStreamlit()
    monkeypatch.setitem(sys.modules, "streamlit", fake)
    runpy.run_path(str(APP / "app.py"))
    declared = [c[1][0] for c in fake.calls if c[0] == "Page"]
    # In menu order, sections included: Analyse, Exploitation, Suivi.
    # THE ORDER IS PINNED, so reordering the menu is a deliberate edit of two
    # files rather than one. operations before ops since 2026-11-20: a reader
    # meets the controls before the alerts they raise. decisions after frauds
    # since 2026-11-21: Analyse answers what was decided about payments, Suivi
    # answers how the model and the ingestion are doing.
    assert declared == ["pages/report.py", "pages/frauds.py",       
                        "pages/operations.py", "pages/ops.py",
                        "pages/rejects.py", "pages/monitoring.py",
                        "pages/performances.py", "pages/decisions.py"]
    assert all((APP / p).is_file() for p in declared)
    on_disk = {"pages/%s" % path.name for path in (APP / "pages").glob("*.py")}
    assert on_disk == set(declared), (
        "une page n'est pas au menu : la declarer ou la retirer")
