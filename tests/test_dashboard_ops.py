"""R9: the operations-channel page -- what the channel was told, and when.

Block: B3, B4.
Lot: B3-R9, B4-L0.
Competency: B3-C4.
Covers: B3-EC14, B3-EC17.

The page reads fd_ops_notifications, the journal notify_ops writes after each
message sent. A silence is a fact about the controls, not about the channel,
and the page must say so.
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

from ml4fd import config, dashboard, health, report  # noqa: E402

from test_dashboard_morning import FakeStreamlit, Stop  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "dashboard"
DAY = dt.date(2026, 11, 17)
UTC = dt.timezone.utc


def settings():
    return types.SimpleNamespace(report_timezone="Europe/Paris", ops_channel="file")


def sent(control="freshness", state="alert", at=None, lasted=None, run="r1"):
    return {"sent_at": at or dt.datetime(2026, 11, 17, 8, 0, tzinfo=UTC),
            "control": control, "state": state, "detail": "constat",
            "channel": "file", "run_id": run, "incident_seconds": lasted}


@pytest.fixture
def state(monkeypatch):
    s = {"sent": [], "recorded": [], "params": None}

    def rows(conn, relative, params):
        assert relative == dashboard.OPS_SQL
        s["params"] = params
        return s["sent"]

    monkeypatch.setattr(report, "_rows", rows)
    monkeypatch.setattr(health, "stored_rows", lambda conn: s["recorded"])
    return s


def levels(view):
    return [n.level for n in view["notices"]]


def test_a_quiet_period_says_silence_is_not_a_mute_channel(state):
    view = dashboard.ops_view(None, settings(), DAY)
    assert levels(view) == [dashboard.INFO]
    assert "pas que le canal est muet" in view["notices"][0].text
    assert view["table"] == [] and dict(view["figures"])["Messages envoyés"] == "0"


def test_a_control_still_red_comes_first_even_if_silent_in_the_period(state):
    state["recorded"] = [{"control": "freshness", "state": "alert",
                          "since": dt.datetime(2026, 11, 10, 7, 0, tzinfo=UTC),
                          "detail": "aucune réussite depuis 7 jours"}]
    view = dashboard.ops_view(None, settings(), DAY)
    assert levels(view) == [dashboard.ERROR, dashboard.INFO]
    # Read from the table that holds it: the label is the page's to choose.
    assert dashboard.CONTROL_LABELS["freshness"] in view["notices"][0].text
    assert "2026-11-10" in view["notices"][0].text


def test_the_figures_count_both_directions_and_the_longest_incident(state):
    state["sent"] = [sent(state="ok", lasted=7380.0), sent(), sent("empty_run", "ok", lasted=60.0)]
    figures = dict(dashboard.ops_view(None, settings(), DAY)["figures"])
    assert figures["Messages envoyés"] == "3"
    assert figures["Passages en alerte"] == "1" and figures["Retours au vert"] == "2"
    assert figures["Incident le plus long"] == "2 h 03 min"


def test_rows_are_labelled_and_shown_in_the_declared_zone(state):
    state["sent"] = [sent(state="ok", lasted=90.0, run=None)]
    row = dashboard.ops_view(None, settings(), DAY)["table"][0]
    assert row["Contrôle"] == dashboard.CONTROL_LABELS["freshness"]
    assert row["Devenu"] == "ok"
    assert row["Envoyé le"].startswith("2026-11-17 09:00"), "08:00 UTC is 09:00 in Paris"
    assert row["Durée de l'incident"] == "1 min"
    assert row["Exécution"] == "--"


def test_the_period_is_the_reports_window(state):
    view = dashboard.ops_view(None, settings(), DAY, DAY + dt.timedelta(days=6))
    assert state["params"]["window_start"].date() == DAY
    assert state["params"]["window_end"].date() == DAY + dt.timedelta(days=7)
    assert view["title"] == "Messages du %s au %s" % (DAY, DAY + dt.timedelta(days=6))
    assert "« file »" in view["note"]


@pytest.mark.parametrize("seconds, text", [
    (None, "--"), (20, "moins d'une minute"), (59 * 60, "59 min"),
    (3600 + 5 * 60, "1 h 05 min"), (26 * 3600, "1 j 2 h")])
def test_lengths_are_said_in_the_largest_units(seconds, text):
    assert dashboard._lasted(seconds) == text


# --- the SQL --------------------------------------------------------------------

def _code(relative):
    return "\n".join(line.split("--")[0] for line in
                     (ROOT / "sql" / relative).read_text(encoding="utf-8").splitlines())


def test_the_length_is_read_before_the_period_is_filtered():
    """lag() in the CTE, the window in the outer WHERE: an incident begun
    before the first day keeps its length."""
    code = _code(dashboard.OPS_SQL)
    assert code.index("lag(sent_at)") < code.index("%(window_start)s")
    assert set(re.findall(r"\bfd_\w+", code)) == {"fd_ops_notifications"}


def test_the_journal_is_declared_kept_with_a_reason():
    import yaml
    kept = yaml.safe_load((ROOT / "governance.yml").read_text(encoding="utf-8")
                          )["retention"]["kept"]
    assert "fd_ops_notifications" in kept


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
        seen = {}

        def build(conn, s, first, last):
            seen["period"] = (first, last)
            return view
        monkeypatch.setattr(dashboard, "ops_view", build)
        try:
            runpy.run_path(str(APP / "pages" / "ops.py"))
        except Stop:
            pass
        return [c[0] for c in fake.calls], seen
    return run


def test_the_page_shows_notices_then_figures_then_table(page):
    names, seen = page({"notices": [dashboard.Notice(dashboard.ERROR, "rouge")],
                        "figures": [("a", "1")] * 4, "note": "n", "title": "t",
                        "table": [{"Contrôle": "un contrôle"}]})
    assert names.index("error") < names.index("metric") < names.index("dataframe")
    assert seen["period"][0] == seen["period"][1]


def test_a_quiet_period_draws_no_empty_table(page):
    names, _ = page({"notices": [dashboard.Notice(dashboard.INFO, "i")],
                     "figures": [("a", "1")] * 4, "note": "n", "title": "t",
                     "table": []})
    assert "dataframe" not in names
