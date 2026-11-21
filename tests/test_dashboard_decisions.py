"""L5: the decisions page -- the reader fd_scoring_decisions did not have.

Covers: EC16, EC19
Lot: L5
Competency: C3

WHAT THESE TESTS ARE FOR. On 2026-11-21 a search for fd_scoring_decisions
returned its DDL, two grants and no reader: the view existed so the two
journals could be read as one, and nothing read it. EC19 asks that each call
and its answer be persisted; a journal nobody can read is a claim nobody can
check.

THE CASES WORTH GUARDING are the ones a live run almost never produces: a
period with no decision at all, a period where the service answered the
pipeline and no third party, and the one divergence that means something --
two model versions, one per path.
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

from ml4fd import dashboard, report  # noqa: E402

from test_dashboard_morning import FakeStreamlit  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "dashboard"
DAY = dt.date(2026, 11, 21)
UTC = dt.timezone.utc


def settings():
    return types.SimpleNamespace(report_timezone="Europe/Paris")


def grouped(source="pipeline", **over):
    row = {"source": source, "decisions": 3731, "flagged": 89,
           "first_at": dt.datetime(2026, 11, 21, 6, 0, tzinfo=UTC),
           "last_at": dt.datetime(2026, 11, 21, 10, 30, tzinfo=UTC),
           "versions": 1, "version_list": "2"}
    row.update(over)
    return row


def call(**over):
    row = {"call_id": 1,
           "decided_at": dt.datetime(2026, 11, 21, 9, 30, tzinfo=UTC),
           "model_version": "2", "recipe_digest": "93d16ede49b5a1b2",
           "score": Decimal("0.00429507"), "threshold": Decimal("0.86000000"),
           "flagged": False}
    row.update(over)
    return row


@pytest.fixture
def state(monkeypatch):
    s = {"by_source": [grouped()], "calls": [], "params": {}}

    def rows(conn, relative, params):
        s["params"][relative] = params
        return {dashboard.DECISIONS_BY_SOURCE_SQL: s["by_source"],
                dashboard.DECISIONS_THIRD_PARTY_SQL: s["calls"]}[relative]

    monkeypatch.setattr(report, "_rows", rows)
    return s


def view(first=DAY, last=DAY):
    return dashboard.decisions_view(None, settings(), first, last)


def levels(out):
    return [notice.level for notice in out["notices"]]


def messages(out):
    return " ".join(notice.text for notice in out["notices"])


# --- the view is read, and the tables are not --------------------------------

def test_the_queries_read_the_view_and_never_the_tables():
    """fd_dashboard has no privilege on fd_scores nor on
    fd_scoring_service_calls -- 190 revokes them. A query naming a table would
    fail in production and pass here, so it is the SQL that is checked."""
    for name in (dashboard.DECISIONS_BY_SOURCE_SQL,
                 dashboard.DECISIONS_THIRD_PARTY_SQL):
        text = (ROOT / "sql" / name).read_text(encoding="utf-8")
        code = "\n".join(line.split("--")[0] for line in text.splitlines())
        assert "fd_scoring_decisions" in code
        assert "fd_scores" not in code
        assert "fd_scoring_service_calls" not in code


def test_the_page_shows_no_field_of_the_caller():
    """The view does not carry client_key, merchant, amt nor the history
    features, and the page must not pretend to: a decision page is not a
    payment page."""
    code = (ROOT / "ml4fd" / "dashboard.py").read_text(encoding="utf-8")
    block = code[code.index("def decisions_view("):]
    for field in ("client_key", "merchant", "f_tx_count", "f_avg_amount",
                  "f_home_distance_km"):
        assert field not in block, field


# --- what the period does ----------------------------------------------------

def test_the_default_period_is_today(state):
    out = dashboard.decisions_view(
        None, settings(), now=dt.datetime(2026, 11, 21, 18, tzinfo=UTC))
    assert out["first"] == out["last"] == DAY


def test_both_queries_get_the_same_window(state):
    view()
    window = state["params"][dashboard.DECISIONS_BY_SOURCE_SQL]
    listed = state["params"][dashboard.DECISIONS_THIRD_PARTY_SQL]
    assert listed["window_start"] == window["window_start"]
    assert listed["window_end"] == window["window_end"]
    assert listed["listed"] == dashboard.DECISIONS_LISTED


def test_a_period_too_long_is_clipped_and_said(state):
    out = view(first=DAY - dt.timedelta(days=400), last=DAY)
    assert dashboard.INFO in levels(out)
    assert "ramenée" in messages(out)


# --- the three cases a live run almost never produces ------------------------

def test_no_decision_at_all_is_said_and_not_a_blank_page(state):
    state["by_source"] = []
    out = view()
    assert dashboard.INFO in levels(out)
    assert "Aucune décision" in messages(out)
    assert out["table"] == [] and out["calls"] == []
    assert out["figures"][0][1] == "0"


def test_no_third_party_call_is_information_and_not_a_defect(state):
    """The service answers the pipeline on /score/batch whether or not anybody
    else calls it. A page reading "0" without a word would look broken."""
    out = view()
    assert levels(out) == [dashboard.INFO]
    assert "Aucun appel tiers" in messages(out)
    assert out["figures"][2] == ("Par une requête tierce", "0")


def test_two_versions_one_per_path_is_a_warning(state):
    """Since the decoupling the pipeline's decisions ARE the service's answers,
    so a version served to a third party that the pipeline never recorded means
    the two paths did not see the same champion -- or that a promotion fell
    between them, which is what the message says."""
    state["by_source"] = [grouped(version_list="2"),
                          grouped(source="tiers", decisions=1, flagged=0,
                                  version_list="3")]
    out = view()
    assert dashboard.WARNING in levels(out)
    assert "promotion" in messages(out)


def test_the_same_version_on_both_paths_raises_nothing(state):
    state["by_source"] = [grouped(version_list="2"),
                          grouped(source="tiers", decisions=1, flagged=0,
                                  version_list="2")]
    assert levels(view()) == []


# --- what the tables hold ----------------------------------------------------

def test_the_two_paths_are_counted_separately_and_together(state):
    state["by_source"] = [grouped(decisions=3731, flagged=89),
                          grouped(source="tiers", decisions=4, flagged=1)]
    out = view()
    figures = dict(out["figures"])
    # report._count separates thousands with a NARROW NO-BREAK SPACE, not a
    # plain one: written here as the escape so the expectation cannot be
    # "corrected" by an editor that normalises whitespace.
    assert figures["Décisions servies"] == "3\u202f735"
    assert figures["Par le pipeline d'ingestion"] == "3\u202f731"
    assert figures["Par une requête tierce"] == "4"
    assert figures["Signalées"] == "90"


def test_each_path_is_named_in_words(state):
    state["by_source"] = [grouped(), grouped(source="tiers", decisions=1)]
    paths = [row["Source"] for row in view()["table"]]
    assert paths == [dashboard.SOURCE_LABELS["pipeline"],
                     dashboard.SOURCE_LABELS["tiers"]]


def test_an_unknown_source_is_shown_as_itself(state):
    """A third value in the view's source column would be a schema change; the
    page must show it rather than hide it behind a missing label."""
    state["by_source"] = [grouped(source="autre")]
    assert view()["table"][0]["Source"] == "autre"


def test_a_call_is_listed_with_its_figures_and_its_model(state):
    state["calls"] = [call()]
    row = view()["calls"][0]
    assert row["Requêtes"] == 1
    assert row["Modèle"] == "2"
    assert row["Recette"] == "93d16ede49b5"
    assert row["Score"] == "0.0043"
    assert row["Seuil"] == "0.86"
    assert row["Signalée"] == "non"


def test_a_flagged_call_reads_oui(state):
    state["calls"] = [call(flagged=True, score=Decimal("0.91000000"))]
    assert view()["calls"][0]["Signalée"] == "oui"


def test_the_instants_are_shown_in_the_declared_zone(state):
    """PostgreSQL hands timestamps back in the session's zone, UTC in the
    image: 09:30 UTC must read 10:30 in Europe/Paris."""
    state["calls"] = [call()]
    assert "10:30" in view()["calls"][0]["Décidé le"]


def test_the_note_says_why_there_are_two_journals(state):
    """A reader meeting two sources must find the foreign key argument on the
    page and not in the DDL."""
    note = view()["note"]
    assert "fd_scoring_decisions" in note
    assert "clé étrangère" in note


# --- the page itself ---------------------------------------------------------

def test_the_page_runs_and_displays_what_the_view_returns(state, monkeypatch):
    state["by_source"] = [grouped(), grouped(source="tiers", decisions=1)]
    state["calls"] = [call()]
    fake = FakeStreamlit()
    monkeypatch.setitem(sys.modules, "streamlit", fake)
    monkeypatch.syspath_prepend(str(APP))
    # The page opens its own connection as fd_dashboard, and psycopg is
    # stubbed here: the connection is replaced rather than faked deeper, so
    # what is under test stays the page's display and not the driver.
    import common
    monkeypatch.setattr(common, "connection",
                        lambda: contextlib.nullcontext(None))
    monkeypatch.setattr(common, "settings", settings)
    runpy.run_path(str(APP / "pages" / "decisions.py"))
    named = [c[0] for c in fake.calls]
    assert "title" in named and "dataframe" in named
    assert not [c for c in fake.calls if c[0] == "exception"]

def test_the_page_defines_what_signalee_means(state):
    """THE QUESTION THAT PROMPTED THIS. A reader met a column counting
    "Signalées" and had no way to learn what it counted. Three notions are
    distinct and the word names only the first."""
    note = view()["flagged_note"]
    assert "seuil de décision" in note
    assert "n'est pas « notifiée »" in note
    assert "plafond" in note


def test_the_note_says_that_a_third_party_flag_raises_nothing(state):
    """The asymmetry a jury can probe: the same word, two consequences. A
    flagged pipeline decision raises an alert; a flagged third-party call does
    not, because the service has no write outside its own journal."""
    note = view()["flagged_note"]
    assert "fd_frauds" in note
    assert "n'alerte pas" in note


def test_the_definition_reaches_the_page_and_not_only_the_view(state):
    """A note the view returns and the page drops is a note nobody reads."""
    code = (ROOT / "dashboard" / "pages" / "decisions.py").read_text(
        encoding="utf-8")
    assert 'view["flagged_note"]' in code
