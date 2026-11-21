"""R10: the performances page, and the reference of M5 kept in the database.

Covers: EE04, EI02, EI03, EC23
Lot: R10
Competency: C2, C4

The page is section 3 of the report over a period, compared with what M5
measured -- which fit_model now copies into fd_datasets beside run.json, so a
dashboard that does not mount the datasets can compare at all.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import inspect
import json
import os
import re
import runpy
import sys
import types
from decimal import Decimal
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import config, dashboard, db, registry, report  # noqa: E402

from test_dashboard_morning import FakeStreamlit, Stop  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "dashboard"
DAY = dt.date(2026, 11, 17)
UTC = dt.timezone.utc


def settings():
    return types.SimpleNamespace(report_timezone="Europe/Paris", verdict_delay_days=30)


def seen(**over):
    row = {"verdicts": 200, "frauds": 10, "alerts": 12, "true_positives": 8,
           "false_positives": 4, "false_negatives": 2, "true_negatives": 186,
           "recall_observed": Decimal("0.8000"), "precision_observed": Decimal("0.6667"),
           "base_rate_observed": Decimal("0.050000"),
           "payments_from": dt.date(2026, 9, 20), "payments_to": dt.date(2026, 10, 17),
           "payment_days": 28}
    row.update(over)
    return row


REF = {"version": "8813b096e2e4", "recall": 0.7512, "precision": 0.4, "alerts_per_day": 41.2,
       "threshold": 0.93, "measured_at": None, "base_rate_test": 0.0058}


@pytest.fixture
def state(monkeypatch):
    s = {"seen": seen(), "ref": REF, "models": [], "days": [], "params": {}}

    def rows(conn, relative, params):
        s["params"][relative] = params
        return {dashboard.PERF_BY_MODEL_SQL: s["models"],
                dashboard.PERF_BY_DAY_SQL: s["days"]}[relative]

    def verdicts(conn, window):
        s["window"] = window
        return s["seen"]

    monkeypatch.setattr(report, "_rows", rows)
    monkeypatch.setattr(report, "verdicts", verdicts)
    monkeypatch.setattr(report, "reference", lambda conn, st: s["ref"])
    return s


def levels(view):
    return [n.level for n in view["notices"]]


# --- the page -------------------------------------------------------------------

def test_the_default_period_is_a_month_ending_today(state):
    view = dashboard.performances_view(None, settings(),
                                      now=dt.datetime(2026, 11, 17, 12, tzinfo=UTC))
    assert view["last"] == DAY and (view["last"] - view["first"]).days == 29
    assert state["window"].start.date() == view["first"]


def reference_column(row: dict) -> str:
    """The name of the reference column, found and never spelled out.

    THE THIRD TEST OF THE EVENING TO BREAK ON A SPELLING. This assertion read
    "Mesuré en M5 (test)", then "Mesuré à l’entraînement (test)" with a
    typographic apostrophe, then the same with a straight one -- three edits to
    a test whose subject is that the observed figure and the reference sit side
    by side, which is true whatever the column is called. The property is that
    there are exactly two value columns beside the indicator; the wording of
    the second belongs to whoever writes the page.
    """
    others = [key for key in row if key not in ("Indicateur", "Observé")]
    assert len(others) == 1, sorted(row)
    return others[0]


def test_observed_and_reference_sit_side_by_side(state):
    rows = {r["Indicateur"]: r for r in
            dashboard.performances_view(None, settings(), DAY, DAY)["comparison"]}
    rappel = rows["Fraudes retrouvées (rappel)"]
    column = reference_column(rappel)
    assert rappel["Observé"] == report._percent(0.8)
    assert rappel[column] == report._percent(0.7512)
    assert rows["Part de fraudes"][column] == report._share(0.0058, 3)


def test_no_reference_is_said_with_its_remedy(state):
    state["ref"] = None
    view = dashboard.performances_view(None, settings(), DAY, DAY)
    assert levels(view)[0] == dashboard.WARNING
    assert "fd-manager.ps1 reference" in view["notices"][0].text
    first = view["comparison"][0]
    assert first[reference_column(first)] == "indisponible"


def test_no_verdict_yet_is_explained_by_the_delay(state):
    state["seen"] = seen(verdicts=0, frauds=0, recall_observed=None,
                         precision_observed=None, payments_from=None, payments_to=None)
    view = dashboard.performances_view(None, settings(), DAY, DAY)
    assert levels(view) == [dashboard.INFO] and "30 jours" in view["notices"][0].text


def test_a_day_without_fraud_is_a_gap_not_a_zero(state):
    state["days"] = [{"day": DAY, "recall_observed": None,
                      "precision_observed": Decimal("0.5")}]
    point = dashboard.performances_view(None, settings(), DAY, DAY)["series"][0]
    assert point["Rappel (%)"] is None and point["Précision (%)"] == 50.0
    assert point["Jour"] == "2026-11-17"


def test_the_scope_says_which_payments_the_verdicts_judge(state):
    note = dashboard.performances_view(None, settings(), DAY, DAY)["note"]
    assert "2026-09-20" in note and "2026-10-17" in note
    assert "8813b096e2e4" in note


def test_several_versions_are_announced_and_detailed(state):
    model = {"model_version": "3", "dataset_version": "8813b096e2e4", "verdicts": 5,
             "frauds": 1, "recall_observed": Decimal("1"), "test_recall": Decimal("0.75"),
             "precision_observed": None, "test_precision": Decimal("0.4")}
    state["models"] = [model, {**model, "model_version": "4", "dataset_version": None}]
    view = dashboard.performances_view(None, settings(), DAY, DAY)
    assert any("2 versions" in n.text for n in view["notices"])
    assert view["by_model"][1]["Recette"] == "inconnue"
    assert view["by_model"][0]["Précision observée"] == "non mesurable"


def test_the_day_series_is_cut_in_the_declared_zone(state):
    dashboard.performances_view(None, settings(), DAY, DAY)
    assert state["params"][dashboard.PERF_BY_DAY_SQL]["zone"] == "Europe/Paris"


# --- the SQL --------------------------------------------------------------------

def _code(relative):
    return "\n".join(line.split("--")[0] for line in
                     (ROOT / "sql" / relative).read_text(encoding="utf-8").splitlines())


def test_by_model_aggregates_before_joining_the_catalogue():
    code = _code(dashboard.PERF_BY_MODEL_SQL)
    assert code.index("GROUP BY") < code.index("LEFT JOIN fd_datasets")
    assert "s.flagged" in code, "each version is judged on its own decision"


def test_the_day_series_uses_the_reports_definition_of_alerted():
    day = _code(dashboard.PERF_BY_DAY_SQL)
    section3 = _code(report.VERDICTS_SQL)
    rule = "EXISTS (SELECT 1 FROM fd_frauds a WHERE a.txn_id = g.txn_id)"
    assert rule in day and rule in section3
    assert "%(zone)s::text" in day


# --- the reference, in the database (R10) ---------------------------------------

PAYLOAD = {"written_at": "2026-11-10T03:00:00+00:00",
           "dataset": {"recipe_digest": "a" * 64, "version": "a" * 12},
           "metrics": {"recall": 0.75, "precision": 0.4, "alerts_per_day": 41.2},
           "threshold": {"value": 0.93}}


def test_the_reference_is_the_manifests_five_figures():
    figures = registry.reference_of(PAYLOAD)
    assert figures == {"recipe_digest": "a" * 64, "recall": 0.75, "precision": 0.4,
                       "alerts_per_day": 41.2, "threshold": 0.93,
                       "measured_at": "2026-11-10T03:00:00+00:00"}
    assert registry.reference_of({**PAYLOAD, "threshold": None})["threshold"] is None


def test_a_manifest_whose_recipe_is_unregistered_is_refused(monkeypatch):
    monkeypatch.setattr(db, "execute_file", lambda conn, rel, **p: 0)
    with pytest.raises(LookupError, match="absente de fd_datasets"):
        registry.record_reference(None, PAYLOAD)


def test_the_writer_writes_the_reference_columns_only():
    code = _code(registry.REFERENCE_SQL)
    assigned = set(re.findall(r"(\w+)\s*=\s*%\(", code)) - {"recipe_digest"}
    assert assigned == {"test_recall", "test_precision", "test_alerts_per_day",
                        "test_threshold", "measured_at"}
    ddl = (ROOT / "sql" / "ddl" / "070_datasets.sql").read_text(encoding="utf-8")
    grant = re.search(r"GRANT UPDATE \(([^)]*)\)\s*ON fd_datasets TO fd_ingest", ddl)
    assert {c.strip() for c in grant.group(1).split(",")} == assigned


def test_fit_model_records_the_reference_from_the_manifest_it_wrote():
    source = (ROOT / "airflow" / "dags" / "dag_train.py").read_text(encoding="utf-8")
    assert source.index("registry.write_manifest(payload") < \
        source.index("registry.record_reference(conn, payload)")


class _Cursor:
    def __init__(self, row): self.row = row
    def __enter__(self): return self
    def __exit__(self, *exc): return False
    def execute(self, sql, params=None):
        self.description = [types.SimpleNamespace(name=n) for n in self.row] if self.row else []
    def fetchall(self): return [tuple(self.row.values())] if self.row else []


class _Conn:
    def __init__(self, row): self.row = row
    def cursor(self): return _Cursor(self.row)


def test_report_reference_reads_the_database_and_converts_decimals():
    row = {"version": "a" * 12, "base_rate_test": Decimal("0.005800"),
           "test_recall": Decimal("0.751200"), "test_precision": Decimal("0.400000"),
           "test_alerts_per_day": Decimal("41.2000"), "test_threshold": None,
           "measured_at": None}
    ref = report.reference(_Conn(row), settings())
    assert ref["recall"] == 0.7512 and isinstance(ref["recall"], float)
    assert ref["threshold"] is None and ref["base_rate_test"] == 0.0058
    assert "run.json" not in inspect.getsource(report.reference).split('"""')[2]


def test_a_recipe_without_figures_is_no_reference():
    row = {"version": "a" * 12, "base_rate_test": Decimal("0.0058"),
           "test_recall": None, "test_precision": None, "test_alerts_per_day": None,
           "test_threshold": None, "measured_at": None}
    assert report.reference(_Conn(row), settings()) is None
    assert report.reference(_Conn(None), settings()) is None


def test_the_backfill_skips_what_it_cannot_vouch_for(monkeypatch, tmp_path):
    import importlib.util
    spec = importlib.util.spec_from_file_location("backfill_reference",
                                                  ROOT / "tools" / "backfill_reference.py")
    tool = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tool)
    good = tmp_path / "good" / "run.json"
    good.parent.mkdir()
    good.write_text(json.dumps(PAYLOAD), encoding="utf-8")
    other = tmp_path / "other" / "run.json"
    other.parent.mkdir()
    other.write_text(json.dumps({**PAYLOAD, "dataset": {"recipe_digest": "b" * 64}}),
                     encoding="utf-8")
    rows = [("a" * 64, "good"), ("c" * 64, "other"), ("d" * 64, "missing")]

    class Cur:
        def __enter__(self): return self
        def __exit__(self, *e): return False
        def execute(self, sql): pass
        def fetchall(self): return rows

    class Conn:
        def cursor(self): return Cur()

    @contextlib.contextmanager
    def connected(role, s):
        assert role == "fd_ingest"
        yield Conn()

    from ml4fd import score
    written = []
    monkeypatch.setattr(config, "load", lambda *a, **k: object())
    monkeypatch.setattr(db, "connect", connected)
    monkeypatch.setattr(score, "manifest_path", lambda s, v: tmp_path / v / "run.json")
    monkeypatch.setattr(registry, "record_reference", lambda c, p: written.append(p))
    assert tool.main([]) == 1, "two manifests skipped: the exit code says so"
    assert [p["dataset"]["recipe_digest"] for p in written] == ["a" * 64]


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
        seen_ = {}

        def build(conn, s, first, last):
            seen_["period"] = (first, last)
            return view
        monkeypatch.setattr(dashboard, "performances_view", build)
        try:
            runpy.run_path(str(APP / "pages" / "performances.py"))
        except Stop:
            pass
        return [c[0] for c in fake.calls], seen_
    return run


def _view(series, by_model):
    return {"notices": [dashboard.Notice(dashboard.WARNING, "w")],
            "figures": [("a", "1")] * 4, "note": "n", "title": "t",
            "comparison": [{"Indicateur": "x"}], "matrix": [{"": "y"}],
            "series": series, "by_model": by_model}


def test_the_page_shows_notices_figures_then_the_comparison(page):
    names, seen_ = page(_view([{"Jour": "2026-11-17"}], [{"Version du modèle": "3"}]))
    assert names.index("warning") < names.index("metric") < names.index("dataframe")
    assert "line_chart" in names and names.count("dataframe") == 3
    first, last = seen_["period"]
    assert (last - first).days == dashboard.PERF_DEFAULT_DAYS - 1


def test_no_series_draws_no_empty_chart(page):
    names, _ = page(_view([], []))
    assert "line_chart" not in names and names.count("dataframe") == 2
