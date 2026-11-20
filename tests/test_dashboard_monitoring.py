"""R11: the monitoring page -- the one reader fd_monitoring did not have.

Covers: EC45, EC49
Lot: L4
Competency: C6

WHAT THESE TESTS ARE FOR. On 2026-11-20 a search for fd_monitoring returned
its DDL, its insert, the task that calls it, and no reader at all: the history
EC45 asks to keep was kept and never shown, and the report EC49 asks for was a
terminal printing figures. The page below is that reader, and the cases worth
guarding are the ones a live run almost never produces -- no measurement at
all, a limit that moved mid-series, a request recorded for a run that then
failed.

AND THE PAGE MUST DECIDE NOTHING. Each row of fd_monitoring already carries the
state monitor.py decided and the limit it compared against. One test below
holds that property in place: recomputing a verdict here would need the
DIRECTION of each comparison -- a share alerts above its limit, a recall below
it -- which is the policy written a second time, in a display module.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import os
import runpy
import sys
import types
from decimal import Decimal
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import config, dashboard, monitor, report  # noqa: E402

from test_dashboard_morning import FakeStreamlit, Stop  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "dashboard"
DAY = dt.date(2026, 11, 20)
UTC = dt.timezone.utc


def settings():
    return types.SimpleNamespace(report_timezone="Europe/Paris")


def measurement(**over):
    row = {"control": "input_drift", "metric": "drifted_columns_share",
           "value": Decimal("0.60000000"), "threshold": Decimal("0.70000000"),
           "state": "ok", "detail": "2 of 3 columns drifted",
           "measured_at": dt.datetime(2026, 11, 20, 15, 40, tzinfo=UTC),
           "model_version": "4", "n_rows": 75, "n_labelled": None,
           "tool": "evidently 0.7.23",
           "window_start": dt.datetime(2026, 11, 20, 15, 25, tzinfo=UTC),
           "window_end": dt.datetime(2026, 11, 20, 15, 40, tzinfo=UTC),
           "recorded_state": "ok", "recorded_since": None}
    row.update(over)
    return row


@pytest.fixture
def state(monkeypatch):
    s = {"latest": [measurement()], "series": [], "columns": [],
         "requests": [], "params": {}}

    def rows(conn, relative, params):
        s["params"][relative] = params
        return {dashboard.MONITORING_LATEST_SQL: s["latest"],
                dashboard.MONITORING_SERIES_SQL: s["series"],
                dashboard.MONITORING_COLUMNS_SQL: s["columns"],
                dashboard.RETRAIN_REQUESTS_SQL: s["requests"]}[relative]

    monkeypatch.setattr(report, "_rows", rows)
    return s


def levels(view):
    return [notice.level for notice in view["notices"]]


def view(first=DAY, last=DAY):
    return dashboard.monitoring_view(None, settings(), first, last)


# --- the default period ------------------------------------------------------

def test_the_default_period_is_a_week_ending_today(state):
    out = dashboard.monitoring_view(
        None, settings(), now=dt.datetime(2026, 11, 20, 18, tzinfo=UTC))
    assert out["last"] == DAY
    assert (out["last"] - out["first"]).days \
        == dashboard.MONITORING_DEFAULT_DAYS - 1


def test_the_state_and_the_columns_are_read_without_a_period(state):
    """Where each control stands NOW does not depend on the dates chosen, and
    neither do the per-column figures of the last run. Passing the window to
    them would have made "where are we" mean "where were we on the first of
    the month"."""
    view()
    assert state["params"][dashboard.MONITORING_LATEST_SQL] == {}
    assert state["params"][dashboard.MONITORING_COLUMNS_SQL] == {}
    assert "window_start" in state["params"][dashboard.MONITORING_SERIES_SQL]


# --- no measurement at all ---------------------------------------------------

def test_an_empty_table_is_explained_with_its_remedy(state):
    """The likeliest state of a fresh install, and the one a jury is most
    likely to meet: the DAG arrives paused."""
    state["latest"] = []
    out = view()
    assert levels(out) == [dashboard.INFO]
    assert "monitor -Apply" in out["notices"][0].text
    assert out["state"] == []


def test_nothing_in_alert_is_said_so(state):
    out = view()
    assert dashboard.SUCCESS in levels(out)


# --- an alert ----------------------------------------------------------------

def test_an_alert_names_the_control_its_figure_and_its_limit(state):
    state["latest"] = [measurement(state="alert",
                                   value=Decimal("1.00000000"),
                                   detail="3 of 3 columns drifted")]
    out = view()
    assert levels(out) == [dashboard.ERROR]
    text = out["notices"][0].text
    assert "Dérive des entrées" in text
    assert "3 of 3 columns drifted" in text
    assert dashboard.SUCCESS not in levels(out)


def test_the_row_carries_since_when_and_not_only_when_it_was_measured(state):
    """A control in alert for six hours and one that turned red a minute ago
    read identically without it. The instant comes from the measurement, the
    since from fd_ops_state -- two different questions."""
    state["latest"] = [measurement(
        state="alert", recorded_state="alert",
        recorded_since=dt.datetime(2026, 11, 20, 9, 34, 34, tzinfo=UTC))]
    row = view()["state"][0]
    assert row["Depuis"] != row["Mesuré à"]
    assert "09:34" in row["Depuis"] or "10:34" in row["Depuis"]


def test_an_unmeasurable_control_is_neither_ok_nor_an_alert(state):
    """A thin window records a reason rather than a verdict, and the page must
    not round it to either side."""
    state["latest"] = [measurement(state="unmeasurable", threshold=None,
                                   detail="37 lignes, plancher de 50")]
    out = view()
    assert out["state"][0]["État"] == "non mesurable"
    assert out["state"][0]["Limite"] == "—"
    assert dashboard.ERROR not in levels(out)


# --- the series, which is what EC45 is for -----------------------------------

def test_one_series_per_metric_and_not_one_per_control(state):
    """THE DEFECT THIS PINS. performance writes recall_flagged AND
    recall_alerted; data_quality writes rejection_share AND
    seconds_since_success. Grouped by control alone, two metrics landed in one
    frame and were drawn as one line -- a share between 0 and 1 plotted through
    a count of seconds."""
    state["series"] = [
        measurement(control="performance", metric="recall_flagged",
                    value=Decimal("0.85"), threshold=Decimal("0.60")),
        measurement(control="performance", metric="recall_alerted",
                    value=Decimal("0.80"), threshold=None)]
    out = view()
    assert len(out["series"]) == 2
    assert [serie["metric"] for serie in out["series"]] \
        == ["Rappel sur les seules alertées", "Rappel observé"]


def test_a_metric_with_no_limit_at_all_draws_no_limit(state):
    """recall_alerted carries no threshold, and recall_flagged carries none
    while the window holds too few labelled frauds to judge. The column is then
    null from end to end -- not a line, but a chart asked to plot nothing, and
    the error the page actually met."""
    state["series"] = [measurement(control="performance",
                                   metric="recall_alerted",
                                   value=Decimal("0.80"), threshold=None)]
    serie = view()["series"][0]
    assert serie["columns"] == ["Mesure"]
    assert "Limite" not in serie["points"][0]


def test_a_limit_known_on_part_of_the_series_is_still_drawn(state):
    """A control that was unmeasurable and then judged has a null limit on its
    first points and a real one after. Dropping the column because of the
    nulls would hide the limit exactly when it started to apply."""
    state["series"] = [
        measurement(control="performance", metric="recall_flagged",
                    value=Decimal("0.0"), threshold=None, state="unmeasurable"),
        measurement(control="performance", metric="recall_flagged",
                    value=Decimal("0.85"), threshold=Decimal("0.60"))]
    serie = view()["series"][0]
    assert serie["columns"] == ["Mesure", "Limite"]
    assert [point["Limite"] for point in serie["points"]] == [None, 0.60]


def test_one_series_per_control_and_never_one_chart_for_all(state):
    """A share between 0 and 1 beside a count of 864 on one axis is a chart
    nobody can read."""
    state["series"] = [
        measurement(measured_at=dt.datetime(2026, 11, 20, 14, tzinfo=UTC)),
        measurement(control="prediction_drift", metric="alerts_per_day",
                    value=Decimal("864.00000000"),
                    threshold=Decimal("151.78901400"))]
    out = view()
    assert [serie["control"] for serie in out["series"]] \
        == ["input_drift", "prediction_drift"]
    assert out["series"][1]["metric"] == "Alertes par jour"


def test_the_limit_of_each_point_is_the_one_that_was_in_force(state):
    """max_drifted_share went from 0.40 to 0.70 on 2026-11-20. Reading the
    limit from governance at display time would redraw the past against today's
    rule, and a series whose limit moves silently cannot be read at all."""
    state["series"] = [
        measurement(threshold=Decimal("0.40000000"),
                    measured_at=dt.datetime(2026, 11, 20, 9, tzinfo=UTC)),
        measurement(threshold=Decimal("0.70000000"),
                    measured_at=dt.datetime(2026, 11, 20, 16, tzinfo=UTC))]
    limits = [point["Limite"] for point in view()["series"][0]["points"]]
    assert limits == [0.40, 0.70]


def test_a_missing_value_stays_a_gap_and_never_a_zero(state):
    """fd_monitoring.value is NOT NULL today, so this guards a path the
    database cannot currently produce -- deliberately: the day a metric records
    an absence rather than a figure, a zero plotted in its place would read as
    a measurement, and the series would show a cliff that never happened.

    Limite is absent rather than None here: this series carries no limit at
    all, so the column is dropped. See the test above."""
    state["series"] = [measurement(value=None, threshold=None)]
    point = view()["series"][0]["points"][0]
    assert point["Mesure"] is None
    assert "Limite" not in point


# --- the per-column figures, which is what found the bug ---------------------

def test_the_per_column_figures_name_the_column_that_moved(state):
    """"3 of 5 columns drifted" is true and says nothing about WHICH. These
    rows said it: f_tx_count at 6.857 against a limit of 0.100, while amt --
    the only column the injection touched -- sat under its own."""
    state["columns"] = [
        {"column_name": "f_tx_count", "value": Decimal("6.85742100"),
         "threshold": Decimal("0.10000000"), "detail": "method psi",
         "measured_at": None, "n_rows": 75},
        {"column_name": "amt", "value": Decimal("0.07315100"),
         "threshold": Decimal("0.10000000"), "detail": "method psi",
         "measured_at": None, "n_rows": 75}]
    table = view()["columns"]
    assert [row["Colonne"] for row in table] == ["f_tx_count", "amt"]
    assert table[0]["Statistique"] == "6.8574"


def test_the_page_does_not_call_a_column_fine(state):
    """The state of those rows is always ok, because the policy judges the
    share and records the columns. Showing that state would read as a verdict
    on the column, which the tool never gave."""
    state["columns"] = [{"column_name": "amt", "value": Decimal("0.07"),
                         "threshold": Decimal("0.10"), "detail": "method psi",
                         "measured_at": None, "n_rows": 75}]
    assert "État" not in view()["columns"][0]


# --- the retraining requests -------------------------------------------------

def test_a_request_is_shown_as_asked_for_and_not_as_a_model(state):
    """The record follows the trigger, so a run that failed afterwards leaves a
    request with nothing behind it -- which happened at 14:42 on 2026-11-20,
    when the guard refused. The note says so rather than letting the table
    imply a model."""
    state["requests"] = [
        {"request_id": 2, "requested_at": dt.datetime(2026, 11, 20, 14, 42,
                                                      tzinfo=UTC),
         "run_id": "manual__2026-11-20T14:42", "controls": "input_drift",
         "reason": "input_drift in alert since 14:40:15"}]
    out = view()
    assert out["requests"][0]["Contrôles"] == "input_drift"
    assert "non qu'un modèle en est sorti" in out["note"]


# --- the page holds no policy ------------------------------------------------

def test_the_view_computes_no_verdict_of_its_own():
    """The stored state is what speaks. A comparison here would need the
    direction of each control, which is the policy declared twice."""
    import inspect

    body = inspect.getsource(dashboard.monitoring_view)
    for forbidden in ("> threshold", "< threshold", ">= threshold",
                      "<= threshold", "monitoring_max", "min_recall"):
        assert forbidden not in body


def test_every_metric_the_monitor_can_write_has_a_french_label():
    """A cross-declaration: monitor.py names the metrics, this map translates
    them, and a metric added on one side without the other would reach a French
    page as an underscored English identifier."""
    written = set()
    body = (ROOT / "ml4fd" / "monitor.py").read_text(encoding="utf-8")
    import re

    for found in re.finditer(r'metric="([a-z_]+)"', body):
        written.add(found.group(1))
    assert written, "no metric found: the check has stopped checking"
    assert written <= set(dashboard.METRIC_LABELS), \
        sorted(written - set(dashboard.METRIC_LABELS))


def test_every_control_of_the_policy_has_a_french_label():
    assert set(config.CONTROLS) == set(dashboard.MONITORING_LABELS)


def test_the_states_of_the_table_are_the_states_of_the_policy():
    """fd_monitoring's CHECK allows three; the page must know all three or
    print a raw one on the day a thin window arrives."""
    assert set(dashboard.MONITORING_STATE_LABELS) == {
        monitor.OK, monitor.ALERT, monitor.UNMEASURABLE}


# --- the page, executed ------------------------------------------------------

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

    def run(built):
        seen = {}

        def build(conn, s, first, last):
            seen["period"] = (first, last)
            return built
        monkeypatch.setattr(dashboard, "monitoring_view", build)
        try:
            runpy.run_path(str(APP / "pages" / "monitoring.py"))
        except Stop:
            pass
        return [call[0] for call in fake.calls], seen
    return run


def _built(**over):
    built = {"notices": [dashboard.Notice(dashboard.ERROR, "e")],
             "state": [{"Contrôle": "x"}], "series": [], "columns": [],
             "requests": [], "title": "t", "note": "n",
             "first": DAY, "last": DAY}
    built.update(over)
    return built


def test_the_page_shows_the_notices_before_the_state(page):
    names, seen = page(_built())
    assert names.index("error") < names.index("dataframe")
    first, last = seen["period"]
    assert (last - first).days == dashboard.MONITORING_DEFAULT_DAYS - 1


def test_an_empty_section_draws_nothing(page):
    """A chart over no point and an empty table are worse than an absence: they
    read as "measured, and nothing happened"."""
    names, _ = page(_built(state=[], series=[], columns=[], requests=[]))
    assert "line_chart" not in names
    assert "dataframe" not in names


def test_each_series_draws_its_own_chart(page):
    names, _ = page(_built(series=[
        {"control": "a", "label": "A", "metric": "m", "columns": ["Mesure"],
         "points": [{"x": 1}]},
        {"control": "b", "label": "B", "metric": "m",
         "columns": ["Mesure", "Limite"], "points": [{"x": 1}]}]))
    assert names.count("line_chart") == 2


def test_the_page_draws_the_columns_the_view_named(page):
    """The page must not name ["Mesure", "Limite"] itself: a metric with no
    threshold would then be charted against a column its frame does not hold,
    which is the error this lot was opened by."""
    body = (APP / "pages" / "monitoring.py").read_text(encoding="utf-8")
    assert 'y=serie["columns"]' in body
    assert 'y=["Mesure", "Limite"]' not in body
