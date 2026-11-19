"""R6: the operations page -- N9's four controls, not a second set.

Covers: EC14
Lot: R6
Competency: C4

health.check() is what raise_and_notify_ops runs every minute. The page calls
it, so it shows what the control task would say at that instant -- same facts,
same judgements, same declared limits. Proven on PostgreSQL 16 as fd_dashboard,
with its password: the controls' query runs (as_of NULL included), the
recorded state reads, the quarantine yields batch_id and line_no, and payload,
reason_detail, `SELECT *` and any write are refused.
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
UTC = dt.timezone.utc


def settings():
    return types.SimpleNamespace(report_root="/tmp", report_timezone="Europe/Paris",
                                 alert_capacity_per_day=60, ops_window_minutes=60,
                                 ops_freshness_minutes=10, ops_max_empty_share=0.10,
                                 ops_max_rejection_share=0.05)


def checked(red=(), transitions=(), since=95.0):
    controls = [{"control": c, "state": health.ALERT if c in red else health.OK,
                 "detail": "constat %s" % c} for c in health.CONTROLS]
    return {"dag_id": health.WATCHED_DAG, "run_id": None, "window_minutes": 60,
            "facts": {"seconds_since_success": since, "runs_ended": 58,
                      "runs_failed": 2, "runs_empty": 1, "lines_read": 290,
                      "lines_rejected": 3, "batches_breaking_invariant": 0},
            "controls": controls,
            "transitions": [{"control": c, "state": health.ALERT,
                             "detail": "x"} for c in transitions],
            "in_alert": len(red)}


@pytest.fixture
def state(monkeypatch):
    s = {"checked": checked(), "stored": [], "runs": [], "asked": None}

    def check(conn, settings, dag_id, run_id=None):
        s["asked"] = dag_id
        return s["checked"]

    monkeypatch.setattr(health, "check", check)
    monkeypatch.setattr(health, "stored_rows", lambda conn: s["stored"])
    monkeypatch.setattr(dashboard, "recent_runs", lambda conn: s["runs"])
    return s


def levels(view):
    return [n.level for n in view["notices"]]


# --- what the page shows --------------------------------------------------------

def test_it_asks_n9s_own_check_about_the_watched_dag(state):
    dashboard.operations_view(None, settings())
    assert state["asked"] == health.WATCHED_DAG == "dag_ingest"


def test_a_green_chain_says_so_once(state):
    view = dashboard.operations_view(None, settings())
    assert levels(view) == [dashboard.SUCCESS]
    assert "60 dernières minutes" in view["notices"][0].text


def test_each_red_control_is_named_first(state):
    state["checked"] = checked(red=("freshness", "volume_invariant"))
    view = dashboard.operations_view(None, settings())
    assert levels(view) == [dashboard.ERROR, dashboard.ERROR]
    # THE LABELS ARE THE PAGE'S TO CHOOSE, so they are read from the table
    # that holds them rather than copied here: a renamed control must not look
    # like a broken page.
    assert view["notices"][0].text.startswith(dashboard.CONTROL_LABELS["freshness"])
    assert view["notices"][1].text.startswith(dashboard.CONTROL_LABELS["volume_invariant"])


def test_an_unannounced_change_points_at_the_dag(state):
    """A transition not yet recorded means the next run will send it -- and if
    the message persists, nothing is running."""
    state["checked"] = checked(red=("empty_run",), transitions=("empty_run",))
    view = dashboard.operations_view(None, settings())
    assert levels(view) == [dashboard.ERROR, dashboard.WARNING]
    assert "ne tourne plus" in view["notices"][1].text


def test_the_four_controls_carry_their_declared_limits(state):
    rows = dashboard.operations_view(None, settings())["controls"]
    assert [r["Contrôle"] for r in rows] == [
        dashboard.CONTROL_LABELS[name] for name in
        ("freshness", "empty_run", "rejection_rate", "volume_invariant")]
    limits = [r["Seuil déclaré"] for r in rows]
    assert limits[0] == "10 min sans réussite"
    assert limits[1].startswith(report._share(0.10, 0))
    assert limits[2].startswith(report._share(0.05, 0))


def test_since_when_comes_from_the_recorded_state(state):
    when = dt.datetime(2026, 11, 17, 21, 3, tzinfo=UTC)
    state["stored"] = [{"control": "freshness", "state": "alert", "since": when,
                        "detail": "x"}]
    rows = dashboard.operations_view(None, settings())["controls"]
    assert rows[0]["Dernier état notifié"] == "alert"
    assert rows[0]["Depuis"].startswith("2026-11-17 22:03"), "in Paris"
    assert rows[1]["Dernier état notifié"] == "jamais"


@pytest.mark.parametrize("seconds, said", [
    (None, "aucune réussite enregistrée"), (20, "il y a moins d'une minute"),
    (95, "il y a 1 min"), (7_500, "il y a 2 h 05 min")])
def test_the_last_success_reads_as_a_duration(state, seconds, said):
    state["checked"] = checked(since=seconds)
    figures = dict(dashboard.operations_view(None, settings())["figures"])
    assert figures["Dernière réussite"] == said


def test_the_runs_are_shown_in_paris_time(state):
    state["runs"] = [{"run_id": "scheduled__x", "state": "failed",
                      "started_at": dt.datetime(2026, 11, 17, 21, 3, tzinfo=UTC),
                      "ended_at": None, "seconds": None, "n_read": None,
                      "n_valid": None, "n_rejected": None}]
    run = dashboard.operations_view(None, settings())["runs"][0]
    assert run["Début"].startswith("2026-11-17 22:03") and run["Durée (s)"] == "--"
    assert run["État"] == "failed"


# --- the page, executed ---------------------------------------------------------

@pytest.fixture
def page(monkeypatch):
    fake = FakeStreamlit()
    monkeypatch.setitem(sys.modules, "streamlit", fake)
    monkeypatch.syspath_prepend(str(APP))
    sys.modules.pop("common", None)
    monkeypatch.setattr(config, "load", lambda *a, **k: settings())

    @contextlib.contextmanager
    def connected(s):
        yield object()

    monkeypatch.setattr(dashboard, "connect", connected)

    def run(view):
        monkeypatch.setattr(dashboard, "operations_view", lambda c, s: view)
        try:
            runpy.run_path(str(APP / "pages" / "operations.py"))
        except Stop:
            pass
        return [c[0] for c in fake.calls]
    return run


def test_the_page_shows_the_notices_before_the_tables(page):
    names = page({"notices": [dashboard.Notice(dashboard.ERROR, "rouge")],
                  "figures": [("a", "1")] * 6, "note": "n",
                  "controls": [{"Contrôle": "un contrôle"}],
                  "runs": [{"Exécution": "x"}]})
    assert names.index("error") < names.index("dataframe")
    assert names.count("metric") == 6 and names.count("dataframe") == 2


def test_no_runs_draws_no_empty_table(page):
    names = page({"notices": [], "figures": [("a", "1")], "note": "n",
                  "controls": [{"Contrôle": "un contrôle"}], "runs": []})
    assert names.count("dataframe") == 1


# --- one name for the watched DAG ---------------------------------------------

def test_every_place_that_names_the_watched_dag_names_the_same_one():
    """It was spelled five times; the dashboard would have been the sixth."""
    dag = (ROOT / "airflow" / "dags" / "dag_ingest.py").read_text(encoding="utf-8")
    assert re.search(r'^DAG_ID = "%s"$' % health.WATCHED_DAG, dag, re.M)
    ops = (ROOT / "tools" / "ops_check.py").read_text(encoding="utf-8")
    assert 'default="%s"' % health.WATCHED_DAG in ops
    rep = (ROOT / "ml4fd" / "report.py").read_text(encoding="utf-8")
    assert '"dag_ingest"' not in rep and "health.WATCHED_DAG" in rep
