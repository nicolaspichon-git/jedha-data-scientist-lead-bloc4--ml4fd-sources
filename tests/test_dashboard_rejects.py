"""R7: the rejections page -- F07.3, counts only, a closed vocabulary.

Covers: EC15, EC22
Lot: R7
Competency: C4

fd_dashboard reads five columns of the quarantine. Neither payload -- the raw
rejected record -- nor reason_detail, which for a duplicate IS the trans_num.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import os
import re
import runpy
import sys
import types
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import checks, config, dashboard, health, report  # noqa: E402

from test_dashboard_morning import FakeStreamlit, Stop  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "dashboard"
DAY = dt.date(2026, 11, 17)


def settings():
    return types.SimpleNamespace(report_root="/tmp", report_timezone="Europe/Paris",
                                 alert_capacity_per_day=60,
                                 ops_max_rejection_share=0.05)


def _code(relative):
    return "\n".join(l.split("--")[0] for l in
                     (ROOT / "sql" / relative).read_text(encoding="utf-8").splitlines())


# --- the vocabulary -------------------------------------------------------------

def test_every_reason_code_has_a_label():
    """The vocabulary is closed in checks.py; a code added there without a
    label here would reach the page as a bare constant."""
    codes = {v for k, v in vars(checks).items()
             if k.isupper() and isinstance(v, str) and k == v}
    assert codes == set(checks.REASON_LABELS), (
        codes ^ set(checks.REASON_LABELS))
    assert set(checks.CLASS_LABELS) == {checks.FIELD, checks.RECORD}


# --- what the page shows --------------------------------------------------------

@pytest.fixture
def state(monkeypatch):
    s = {"rows": [], "total": {"motifs": 0, "lignes": 0, "lots": 0},
         "stream": {"lues": 0, "rejetees": 0}, "params": {}}

    def rows(conn, relative, params):
        s["params"][relative] = params
        return {dashboard.REJECTS_SQL: s["rows"],
                dashboard.REJECTS_TOTAL_SQL: [s["total"]],
                dashboard.STREAM_RATE_SQL: [s["stream"]]}[relative]

    monkeypatch.setattr(report, "_rows", rows)
    return s


def levels(view):
    return [n.level for n in view["notices"]]


def test_reasons_are_labelled_and_the_day_is_the_reports(state):
    state["rows"] = [{"reason_class": "field", "reason_code": "AMT_UNPARSABLE",
                      "motifs": 3, "lignes": 3, "lots": 2}]
    view = dashboard.rejects_view(None, settings(), DAY)
    row = view["table"][0]
    assert row["Classe"] == "champ" and row["Motif"] == "montant illisible"
    params = state["params"][dashboard.REJECTS_SQL]
    assert params["window_start"].hour == 0
    assert params["window_start"].date() == DAY
    assert str(params["window_start"].tzinfo) == "Europe/Paris"


def test_the_total_is_counted_not_summed(state):
    """One line rejected for two reasons: two rows of the breakdown, one line."""
    state["rows"] = [
        {"reason_class": "field", "reason_code": "AMT_UNPARSABLE",
         "motifs": 1, "lignes": 1, "lots": 1},
        {"reason_class": "field", "reason_code": "SCHEMA_MISSING_COLUMN",
         "motifs": 1, "lignes": 1, "lots": 1}]
    state["total"] = {"motifs": 2, "lignes": 1, "lots": 1}
    figures = dict(dashboard.rejects_view(None, settings(), DAY)["figures"])
    assert figures["Lignes rejetées"] == "1" and figures["Motifs relevés"] == "2"


def test_a_stream_rate_above_the_limit_is_flagged(state):
    state["rows"] = [{"reason_class": "record", "reason_code": "CLOCK_IMPLAUSIBLE",
                      "motifs": 40, "lignes": 40, "lots": 30}]
    state["stream"] = {"lues": 500, "rejetees": 40}
    view = dashboard.rejects_view(None, settings(), DAY)
    assert levels(view) == [dashboard.WARNING]
    assert "pas été scorées" in view["notices"][0].text
    assert dict(view["figures"])["Taux de rejet du flux"] == report._share(0.08)


def test_the_rate_is_the_streams(state):
    dashboard.rejects_view(None, settings(), DAY)
    assert state["params"][dashboard.STREAM_RATE_SQL]["dag_id"] == health.WATCHED_DAG


def test_no_reading_is_not_a_zero_rate(state):
    view = dashboard.rejects_view(None, settings(), DAY)
    assert dict(view["figures"])["Taux de rejet du flux"] == "aucune lecture"
    assert levels(view) == [dashboard.INFO]


def test_an_unlabelled_code_is_said_not_hidden(state):
    state["rows"] = [{"reason_class": "field", "reason_code": "NEW_CODE",
                      "motifs": 1, "lignes": 1, "lots": 1}]
    view = dashboard.rejects_view(None, settings(), DAY)
    assert "NEW_CODE" in view["notices"][0].text
    assert view["table"][0]["Motif"] == "NEW_CODE"


# --- the SQL --------------------------------------------------------------------

def test_no_rejects_query_names_payload_or_reason_detail():
    for relative in (dashboard.REJECTS_SQL, dashboard.REJECTS_TOTAL_SQL):
        code = _code(relative)
        assert "payload" not in code and "reason_detail" not in code, relative


def test_distinct_lines_are_counted_as_pairs():
    """line_no restarts at 1 in every batch: distinct line_no alone would merge
    line 3 of two batches."""
    for relative in (dashboard.REJECTS_SQL, dashboard.REJECTS_TOTAL_SQL):
        assert "count(DISTINCT (batch_id, line_no))" in _code(relative)


def test_the_stream_rate_joins_nothing():
    code = _code(dashboard.STREAM_RATE_SQL)
    assert "JOIN" not in code.upper()
    assert set(re.findall(r"\bfd_\w+", code)) == {"fd_pipeline_runs"}


# --- the page, executed ---------------------------------------------------------

class DateStreamlit(FakeStreamlit):
    def date_input(self, label, value=None, **kwargs):
        self._record("date_input", label, value=value, **kwargs)
        return value


@pytest.fixture
def page(monkeypatch):
    fake = DateStreamlit()
    monkeypatch.setitem(sys.modules, "streamlit", fake)
    monkeypatch.syspath_prepend(str(APP))
    sys.modules.pop("common", None)
    monkeypatch.setattr(config, "load", lambda *a, **k: settings())

    @contextlib.contextmanager
    def connected(s):
        yield object()

    monkeypatch.setattr(dashboard, "connect", connected)

    def run(view):
        monkeypatch.setattr(dashboard, "rejects_view", lambda c, s, f, l: view)
        try:
            runpy.run_path(str(APP / "pages" / "rejects.py"))
        except Stop:
            pass
        return [c[0] for c in fake.calls]
    return run


def test_the_page_shows_notices_then_figures_then_table(page):
    names = page({"first": DAY, "last": DAY, "notices": [dashboard.Notice(dashboard.WARNING, "w")],
                  "figures": [("a", "1")] * 4, "note": "n", "title": "t",
                  "table": [{"Code": "AMT_UNPARSABLE"}]})
    assert names.index("warning") < names.index("metric") < names.index("dataframe")


def test_a_quiet_day_draws_no_empty_table(page):
    names = page({"first": DAY, "last": DAY, "notices": [dashboard.Notice(dashboard.INFO, "i")],
                  "figures": [("a", "1")] * 4, "note": "n", "title": "t",
                  "table": []})
    assert "dataframe" not in names


def test_a_period_of_rejections_is_one_window(state):
    last = DAY + dt.timedelta(days=2)
    view = dashboard.rejects_view(None, settings(), DAY, last)
    params = state["params"][dashboard.REJECTS_SQL]
    assert params["window_start"].date() == DAY
    assert params["window_end"].date() == last + dt.timedelta(days=1)
    assert view["title"] == "Rejets du %s au %s" % (DAY.isoformat(), last.isoformat())
    assert "lues sur la période" not in view["notices"][0].text   # quiet: info only
