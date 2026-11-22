"""R2: the morning page -- notices before the document, figures from the row.

Block: B3, B4.
Lot: B3-R2, B4-L0.
Competency: B3-C4.
Covers: B3-EE03, B3-EC14.

Streamlit cannot be installed where this is developed. So the page is tested
twice over: what it shows is decided by ml4fd/dashboard.morning_view(), held
here directly; and the page SCRIPT is executed against a fake `streamlit`
module that records every call -- which proves it runs, and in what order it
displays.
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

from ml4fd import config, dashboard, report  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "dashboard"
DAY = dt.date(2026, 11, 16)
UTC = dt.timezone.utc


def settings(tmp_path):
    return types.SimpleNamespace(report_root=str(tmp_path),
                                 report_timezone="Europe/Paris",
                                 alert_capacity_per_day=60)


def row(tmp_path, day=DAY, text="# Rapport\n", recorded_text="# Rapport\n",
        alerts=2, path=None):
    target = report.path_for(settings(tmp_path), day)
    if text is not None:
        target.write_text(text, encoding="utf-8")
    return {"report_date": day, "path": str(path or target),
            "content_digest": report.digest(recorded_text),
            "knowledge_date": dt.datetime(2026, 11, 17, 5, 0, tzinfo=UTC),
            "produced_at": dt.datetime(2026, 11, 17, 5, 0, 3, tzinfo=UTC),
            "n_payments": 7200, "n_alerts": alerts, "n_verdicts": 0,
            "n_pending": 7200,
            "model_version": "v7" if alerts else None,
            "threshold": 0.93 if alerts else None}


@pytest.fixture
def series(monkeypatch):
    state = {"rows": [], "missing": []}
    monkeypatch.setattr(dashboard, "reports", lambda conn: state["rows"])
    monkeypatch.setattr(dashboard, "missing_mornings", lambda conn: [
        {"journee_sans_rapport": d} for d in state["missing"]])
    return state


def levels(view):
    return [n.level for n in view["notices"]]


# --- what the page shows --------------------------------------------------------

def test_a_conforming_report_says_so_before_its_text(series, tmp_path):
    series["rows"] = [row(tmp_path)]
    view = dashboard.morning_view(None, settings(tmp_path))
    assert levels(view) == [dashboard.SUCCESS]
    assert view["document"] == "# Rapport\n"


def test_an_altered_document_is_flagged_first_and_still_readable(series, tmp_path):
    series["rows"] = [row(tmp_path, text="# Rapport retouche\n")]
    view = dashboard.morning_view(None, settings(tmp_path))
    assert levels(view)[0] == dashboard.ERROR
    assert "NE CORRESPOND PAS" in view["notices"][0].text
    assert view["document"] == "# Rapport retouche\n"


def test_a_missing_file_shows_the_figures_and_no_text(series, tmp_path):
    series["rows"] = [row(tmp_path, text=None)]
    view = dashboard.morning_view(None, settings(tmp_path))
    assert levels(view) == [dashboard.ERROR]
    assert view["document"] is None and view["figures"]


def test_the_figures_come_from_the_row_never_from_the_text(series, tmp_path):
    """The S4 plan's decision: a reworded sentence cannot change a number."""
    series["rows"] = [row(tmp_path, text="12 alertes hier\n",
                          recorded_text="12 alertes hier\n", alerts=2)]
    figures = dict(dashboard.morning_view(None, settings(tmp_path))["figures"])
    assert figures["Alertes"] == "2"
    # The report's own formatter -- a NARROW no-break space (U+202F), which a
    # test that copied "7 200" by hand got wrong. The page and the document
    # agree to the character because they share the function, not a string.
    assert figures["Paiements"] == report._count(7200) == "7\u202f200"


def test_instants_are_shown_in_the_declared_zone(series, tmp_path):
    """PostgreSQL returns UTC in the postgres image: a 06:00 report would read
    05:00."""
    series["rows"] = [row(tmp_path)]
    figures = dict(dashboard.morning_view(None, settings(tmp_path))["figures"])
    assert figures["Produit le"].startswith("2026-11-17 06:00")


def test_a_day_without_alerts_names_no_model(series, tmp_path):
    series["rows"] = [row(tmp_path, alerts=0)]
    figures = dict(dashboard.morning_view(None, settings(tmp_path))["figures"])
    assert "aucun" in figures["Modèle"] and figures["Seuil"] == "--"


def test_missing_mornings_come_before_everything(series, tmp_path):
    series["rows"] = [row(tmp_path)]
    series["missing"] = [dt.date(2026, 11, 14), dt.date(2026, 11, 15)]
    view = dashboard.morning_view(None, settings(tmp_path))
    assert levels(view)[0] == dashboard.WARNING
    assert "2 matins" in view["notices"][0].text
    assert "2026-11-14" in view["notices"][0].text


def test_a_relocated_document_is_reported(series, tmp_path):
    series["rows"] = [row(tmp_path, path="/opt/ailleurs/2026-11-16.md")]
    view = dashboard.morning_view(None, settings(tmp_path))
    assert levels(view) == [dashboard.SUCCESS, dashboard.WARNING]
    assert "ne montent pas reports/" in view["notices"][1].text


def test_the_most_recent_report_is_the_default(series, tmp_path):
    older = row(tmp_path, day=dt.date(2026, 11, 15))
    series["rows"] = [row(tmp_path), older]
    assert dashboard.morning_view(None, settings(tmp_path))["selected"] == DAY
    chosen = dashboard.morning_view(None, settings(tmp_path), dt.date(2026, 11, 15))
    assert chosen["selected"] == dt.date(2026, 11, 15)


def test_a_day_with_no_report_is_not_invented(series, tmp_path):
    series["rows"] = [row(tmp_path)]
    view = dashboard.morning_view(None, settings(tmp_path), dt.date(2020, 1, 1))
    assert view["selected"] == DAY


def test_no_report_at_all_is_said_plainly(series, tmp_path):
    view = dashboard.morning_view(None, settings(tmp_path))
    assert levels(view) == [dashboard.INFO] and "dag_daily" in view["notices"][0].text
    assert view["document"] is None and view["dates"] == []


# --- the page, executed ---------------------------------------------------------

class Stop(Exception):
    pass


class FakeStreamlit(types.ModuleType):
    """Records every call, in order. Enough of the API for these pages."""

    def __init__(self):
        super().__init__("streamlit")
        self.calls, self.session_state = [], {}

    def _record(self, name, *args, **kwargs):
        self.calls.append((name, args, kwargs))

    def cache_resource(self, fn):
        return fn

    def stop(self):
        self._record("stop")
        raise Stop

    def selectbox(self, label, options, index=0, **kwargs):
        self._record("selectbox", label, options, index=index, **kwargs)
        return options[index]

    def navigation(self, pages, **kwargs):
        self._record("navigation", pages, **kwargs)
        outer = self

        class Navigation:
            def run(self): outer._record("run")
        return Navigation()

    def columns(self, n):
        self._record("columns", n)
        outer = self

        class Column:
            def __enter__(self): return self
            def __exit__(self, *exc): return False
            def metric(self, *a, **k): outer._record("metric", *a, **k)
        return [Column() for _ in range(n)]

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        return lambda *a, **k: self._record(name, *a, **k)


@pytest.fixture
def page(monkeypatch, tmp_path):
    """Run dashboard/pages/rapport.py against FakeStreamlit."""
    fake = FakeStreamlit()
    monkeypatch.setitem(sys.modules, "streamlit", fake)
    monkeypatch.syspath_prepend(str(APP))
    sys.modules.pop("common", None)
    monkeypatch.setattr(config, "load", lambda *a, **k: settings(tmp_path))

    @contextlib.contextmanager
    def connected(s):
        yield object()

    monkeypatch.setattr(dashboard, "connect", connected)

    def run(view=None, connect=None):
        if connect is not None:
            monkeypatch.setattr(dashboard, "connect", connect)
        if view is not None:
            monkeypatch.setattr(dashboard, "morning_view", lambda c, s, d: view)
        try:
            runpy.run_path(str(APP / "pages" / "report.py"))
        except Stop:
            pass
        return fake.calls
    return run


def _view(notices, document="# Rapport\n"):
    return {"dates": [DAY], "selected": DAY, "notices": notices,
            "figures": [("Paiements", "7 200")] * 8, "document": document,
            "title": "Rapport du 2026-11-16"}


def test_the_page_shows_every_notice_before_the_document(page):
    calls = page(_view([dashboard.Notice(dashboard.ERROR, "empreinte"),
                        dashboard.Notice(dashboard.WARNING, "montage")]))
    names = [c[0] for c in calls]
    assert names.index("error") < names.index("markdown")
    assert names.index("warning") < names.index("markdown")
    assert names.index("selectbox") < names.index("error")
    assert names.count("metric") == 8


def test_the_document_is_never_rendered_as_html(page):
    calls = page(_view([], document="<script>alert(1)</script>"))
    for name, args, kwargs in calls:
        assert not kwargs.get("unsafe_allow_html"), name
    for path in APP.rglob("*.py"):
        assert "unsafe_allow_html" not in path.read_text(encoding="utf-8").replace(
            "WITHOUT unsafe_allow_html", ""), path


def test_with_no_report_the_page_neither_selects_nor_renders(page):
    calls = page({"dates": [], "selected": None, "figures": [], "document": None,
                  "title": None,
                  "notices": [dashboard.Notice(dashboard.INFO, "aucun")]})
    names = [c[0] for c in calls]
    assert "selectbox" not in names and "markdown" not in names and "info" in names


def test_a_wrong_identity_stops_the_page_before_any_read(page, monkeypatch):
    read = []
    monkeypatch.setattr(dashboard, "morning_view",
                        lambda *a: read.append(1) or _view([]))

    @contextlib.contextmanager
    def superuser(s):
        raise dashboard.WrongIdentity("connecte en SUPER-UTILISATEUR")
        yield  # pragma: no cover

    calls = page(connect=superuser)
    assert ("error", ("connecte en SUPER-UTILISATEUR",), {}) in calls
    assert calls[-1][0] == "stop" and read == []


def test_an_unreachable_database_is_a_message_not_a_traceback(page):
    @contextlib.contextmanager
    def down(s):
        raise OSError("connection to server at postgres failed")
        yield  # pragma: no cover

    calls = page(connect=down)
    errors = [c for c in calls if c[0] == "error"]
    assert errors and "La base est inaccessible" in errors[0][1][0]


def test_the_entry_point_declares_the_page_and_reads_nothing(monkeypatch):
    fake = FakeStreamlit()
    monkeypatch.setitem(sys.modules, "streamlit", fake)
    runpy.run_path(str(APP / "app.py"))
    pages = [c for c in fake.calls if c[0] == "Page"]
    assert pages and pages[0][1][0] == "pages/report.py", "the default page"
    assert (APP / pages[0][1][0]).is_file(), "the declared path exists"
    source = (APP / "app.py").read_text(encoding="utf-8")
    assert "dashboard" not in source.split('"""')[2], "the entry point reads no data"
