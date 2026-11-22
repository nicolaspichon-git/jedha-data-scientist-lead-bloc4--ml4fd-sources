"""B4-L5: the scoring service's logic, exercised without a web framework.

Block: B4.
Lot: B4-L5.
Competency: B4-C2, B4-C3.
Covers: B4-EC15, B4-EC16, B4-EC19, B4-EC20, B4-EC21.

WHAT THESE TESTS ARE FOR. The refusals are the branches a live call almost
never produces -- a caller sending a naive timestamp, an amount above what the
pipeline would accept, a champion whose recipe the feature query no longer
produces -- so they would not be exercised at all if they were not exercised
here.

AND THE SERVICE MUST BE EXERCISABLE WITHOUT fastapi, which is installed in the
api image and nowhere else: the suite installs psycopg, pandas, requests and
PyYAML. ml4fd/serve.py therefore holds every decision and
scoring_service/app.py only
routes. One test below holds that property in place.
"""
from __future__ import annotations

import ast
import datetime as dt
import os
import re
from decimal import Decimal
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import datasets, serve  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
UTC = dt.timezone.utc
PEPPER = "x" * 64
MAX_AMOUNT = 50000.0


def request(**over):
    payload = {"cc_num": "4532112345678901",
               "txn_ts": "2026-11-20T15:40:00+01:00",
               "merchant": "fraud_Kirlin and Sons",
               "category": "grocery_pos",
               "amt": "120.50",
               "merch_lat": "48.8566", "merch_long": "2.3522",
               "home_lat": "48.8584", "home_long": "2.2945"}
    payload.update(over)
    return payload


# --- the refusals, which is what B4-EC20 asks for -------------------------------

def test_a_complete_request_has_no_problem():
    assert serve.problems(request(), MAX_AMOUNT) == []


def test_every_required_field_is_required():
    for name in serve.REQUIRED:
        found = serve.problems(request(**{name: None}), MAX_AMOUNT)
        assert found, name
        assert name in found[0]


def test_a_blank_string_is_an_absence_and_not_a_value():
    """A caller sending "" for a merchant has not sent a merchant, and the
    pipeline's own validation would quarantine the row."""
    assert "merchant" in serve.problems(request(merchant="   "),
                                        MAX_AMOUNT)[0]


def test_all_the_problems_are_reported_and_not_only_the_first():
    """A caller who fixes one field and is then told about the next has to
    guess how many rounds are left -- the same choice guard.verdict makes."""
    found = serve.problems(request(amt="-5", merch_lat="200"), MAX_AMOUNT)
    assert len(found) == 2


def test_an_amount_the_pipeline_would_reject_is_refused_here_too():
    """A service that accepted what validation quarantines would score rows the
    model was never fitted on and answer with a straight face."""
    found = serve.problems(request(amt="50000.01"), MAX_AMOUNT)
    assert len(found) == 1
    assert "maximum accepte" in found[0] and "50000" in found[0]


def test_a_negative_or_zero_amount_is_refused():
    for bad in ("-1", "0", "0.00"):
        assert "strictement positif" in serve.problems(
            request(amt=bad), MAX_AMOUNT)[0], bad


def test_a_coordinate_outside_the_globe_is_refused():
    assert "hors de l'intervalle" in serve.problems(
        request(home_lat="91"), MAX_AMOUNT)[0]
    assert "hors de l'intervalle" in serve.problems(
        request(merch_long="-181"), MAX_AMOUNT)[0]


def test_the_problem_names_the_field_and_what_was_received():
    """"requete non conforme" names nothing. B4-EC20 asks for an explicit error,
    so the sentence carries the field, the value and the expectation."""
    found = serve.problems(request(amt="beaucoup"), MAX_AMOUNT)[0]
    assert "amt" in found and "beaucoup" in found


# --- the instant, and why a naive one cannot be accepted ---------------------

def test_a_timestamp_without_a_zone_is_refused():
    """The history features are computed with a strict bound on this instant,
    so a naive timestamp would be read in the session's zone -- the same
    request would get a different history depending on how the container is
    set."""
    assert serve.instant("2026-11-20T15:40:00") is None
    assert serve.instant(dt.datetime(2026, 11, 20, 15, 40)) is None
    assert "fuseau" in serve.problems(
        request(txn_ts="2026-11-20T15:40:00"), MAX_AMOUNT)[0]


def test_the_zulu_spelling_is_accepted():
    """ISO 8601 allows Z and callers use it; fromisoformat did not read it
    before 3.11, and the service must not depend on its runtime's version."""
    read = serve.instant("2026-11-20T14:40:00Z")
    assert read is not None and read.utcoffset() == dt.timedelta(0)


def test_a_timestamp_that_is_not_one_is_refused():
    for bad in ("hier", "2026-13-45T00:00:00+00:00", 17, None):
        assert serve.instant(bad) is None, bad


# --- what is kept of the caller, and what is not -----------------------------

def test_the_card_number_never_leaves_the_request():
    call = serve.anonymous(request(), PEPPER)
    assert "cc_num" not in call
    assert "4532112345678901" not in repr(call)
    assert len(call["client_key"]) == 32


def test_the_home_location_becomes_a_distance():
    """A distance describes a circle around the merchant, not a point -- the
    minimisation B4-EC17 allows."""
    call = serve.anonymous(request(), PEPPER)
    for field in ("home_lat", "home_long", "merch_lat", "merch_long"):
        assert field not in call
    assert 4 < float(call["f_home_distance_km"]) < 6


def test_the_pseudonym_is_the_pipeline_s_own():
    """A second implementation here would be a second definition of the
    pseudonym, and the first mismatch would be invisible: a call and a
    transaction of the same cardholder would stop joining."""
    from ml4fd import transform

    call = serve.anonymous(request(), PEPPER)
    assert call["client_key"] == transform.client_key("4532112345678901",
                                                      PEPPER)


def test_the_amount_is_kept_to_the_cent_and_never_a_float():
    call = serve.anonymous(request(amt="120.509"), PEPPER)
    assert call["amt"] == Decimal("120.51")
    assert isinstance(call["amt"], Decimal)


def test_the_recorded_columns_hold_no_identifying_field():
    """The enforcement is the schema's, not this module's: the table has no
    column for a card number or a location. Read here so a column added later
    meets this test."""
    ddl = (ROOT / "sql" / "ddl" / "190_scoring_service_calls.sql").read_text(
        encoding="utf-8")
    body = ddl[ddl.index("CREATE TABLE"):ddl.index(");")]
    for forbidden in ("cc_num", "card", "home_lat", "home_long",
                      "first", "last", "street", "dob"):
        assert forbidden not in body, forbidden


# --- the decision, and the rounding that keeps it storable -------------------

class FakeScorer:
    features = ("amt", "f_tx_count", "f_avg_amount", "f_home_distance_km",
                "category")
    threshold = 0.93
    model_version = "4"
    recipe_digest = "93d16ede49b5"

    def __init__(self, raw=0.95):
        self.raw = raw

    def scores(self, frame):
        self.frame = frame
        return [self.raw]

    def flagged(self, value):
        from ml4fd import score

        return round(value, score.SCORE_SCALE) >= self.threshold


def row(**over):
    out = {"amt": Decimal("120.50"), "f_tx_count": 1403,
           "f_avg_amount": Decimal("67.61"),
           "f_home_distance_km": Decimal("5.12"), "category": "grocery_pos",
           "txn_hour": 15, "txn_weekday": 5}
    out.update(over)
    return out


def test_the_frame_carries_exactly_the_champion_s_features_in_order():
    """frame[list(features)] is what the model is handed, so the ORDER is the
    model's. A frame carrying them in another order would score against the
    wrong features without raising."""
    scorer = FakeScorer()
    frame = serve.frame_of(row(), scorer)
    assert list(frame.columns) == list(scorer.features)
    assert len(frame) == 1


def test_a_feature_the_query_does_not_produce_is_a_refusal():
    """A recipe changed without the query following it must raise rather than
    score. The message names the file to change."""
    scorer = FakeScorer()
    scorer.features = scorer.features + ("f_new",)
    with pytest.raises(ValueError, match="features_for_call"):
        serve.frame_of(row(), scorer)


def test_the_amputated_recipe_in_service_is_scored_without_the_clock():
    """governance drops txn_hour and txn_weekday, and the query produces them
    anyway: the service builds what the MANIFEST asks, so the extra columns are
    simply not selected."""
    scorer = FakeScorer()
    frame = serve.frame_of(row(), scorer)
    assert "txn_hour" not in frame.columns


def test_the_score_is_rounded_before_the_decision_is_taken():
    """MEASURED ON THE BATCH PATH (ml4fd/score.py): a raw score of 0.929999996
    against a threshold of 0.93 is False in Python and True once rounded to
    eight decimals. fd_scoring_service_calls carries CHECK (flagged = (score >= threshold))
    over a numeric(9,8), so the INSERT would fail after the caller had been
    answered."""
    decision = serve.answer(FakeScorer(raw=0.9299999960), row())
    assert decision["score"] == 0.93
    assert decision["flagged"] is True


def test_the_decision_carries_what_answered_it():
    decision = serve.answer(FakeScorer(), row())
    assert decision["model_version"] == "4"
    assert decision["recipe_digest"] == "93d16ede49b5"
    assert decision["threshold"] == 0.93


# --- the sentence, which is what B4-EC15 asks for -------------------------------

def test_the_decision_is_also_a_sentence():
    """"Restituer la sortie du modele sous une forme exploitable par une
    technologie d'assistance": a score of 0.94 beside a threshold of 0.93 is a
    comparison the reader has to make, and a screen reader announces this."""
    said = serve.sentence(serve.answer(FakeScorer(), row()), row())
    assert said.startswith("Transaction signalee pour verification.")
    assert "0.9500" in said and "0.9300" in said
    assert "version 4" in said
    assert said.endswith(".")


def test_an_accepted_transaction_says_so_rather_than_saying_nothing():
    said = serve.sentence(serve.answer(FakeScorer(raw=0.10), row()), row())
    assert "acceptee sans signalement" in said


def test_a_first_transaction_is_described_and_not_shown_as_zero():
    """fd_client_features ruling 6: a first transaction has tx_count = 0 and
    avg_amount NULL. "0 transaction(s) anterieure(s)" reads as a measurement;
    the absence is what is true."""
    said = serve.sentence(serve.answer(FakeScorer(), row(f_tx_count=0)),
                          row(f_tx_count=0))
    assert "aucune transaction anterieure" in said


def test_the_figures_stay_beside_the_sentence_and_are_not_replaced_by_it():
    """A caller that needs the number must not have to parse French."""
    decision = serve.answer(FakeScorer(), row())
    assert set(decision) >= {"score", "threshold", "flagged",
                             "model_version", "recipe_digest"}


# --- the health endpoint, which is what B4-EC21 asks for ------------------------

def test_the_health_check_announces_the_version_it_serves():
    """A service that answers "ok" without saying what it serves cannot be
    distinguished from one that has served last month's model since a
    promotion nobody noticed."""
    said = serve.health(FakeScorer())
    assert said["state"] == "ok"
    assert said["model_version"] == "4"
    assert said["recipe_digest"] == "93d16ede49b5"


def test_no_champion_is_degraded_and_not_down():
    """An ordinary state before the first promotion, and a different problem
    from a process that has stopped."""
    said = serve.health(None, problem="aucune version ne porte l'alias")
    assert said["state"] == "degraded"
    assert said["model_version"] is None
    assert "alias" in said["detail"]


def test_the_health_check_announces_the_code_it_runs():
    """L6: the model and the code move independently -- the alias chooses one,
    the deployment the other -- so /health names both, degraded or not."""
    assert serve.health(FakeScorer(), revision="3e17977")["revision"] == "3e17977"
    assert serve.health(None, revision="3e17977")["revision"] == "3e17977"


def test_an_unknown_revision_says_so():
    """A build that was not given its commit must not look like one that was."""
    assert serve.health(FakeScorer())["revision"] == "inconnue"


# --- the module stays installable and the SQL stays single-sourced -----------

def test_the_logic_imports_no_web_framework_at_load_time():
    """fastapi lives in the api image and nowhere else. A module-level import
    here would make the suite -- and the integration chain -- unable to even
    collect."""
    tree = ast.parse((ROOT / "ml4fd" / "serve.py").read_text(encoding="utf-8"))
    for node in tree.body:
        names = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        assert not any(name.startswith(("fastapi", "uvicorn", "pydantic"))
                       for name in names), names


def test_the_calendar_features_are_computed_where_the_batch_path_computes_them():
    """A CROSS-DECLARATION. Computing txn_hour in Python would be a second
    definition of a feature the batch path already defines in SQL, and
    sql/checks/reconcile_features.sql exists because this project pays for such
    divergences. Both files must use the same extract()."""
    served = (ROOT / "sql" / "scoring_service" / "features_for_call.sql").read_text(
        encoding="utf-8")
    batch = (ROOT / "sql" / "scoring" / "rows_to_score.sql").read_text(
        encoding="utf-8")
    for expression in ("extract(hour", "extract(isodow"):
        assert expression in served and expression in batch
    body = (ROOT / "ml4fd" / "serve.py").read_text(encoding="utf-8")
    for forbidden in (".hour", ".isoweekday", ".weekday"):
        assert forbidden not in body, forbidden


def test_the_feature_query_produces_every_feature_of_the_recipe():
    """The select list against datasets.FEATURES: a recipe gaining a feature
    the service cannot build would be found here rather than at the first
    call."""
    served = (ROOT / "sql" / "scoring_service" / "features_for_call.sql").read_text(
        encoding="utf-8")
    selected = set(re.findall(r"AS\s+([a-z_]+)", served))
    assert set(datasets.FEATURES) <= selected, \
        sorted(set(datasets.FEATURES) - selected)


def test_the_recorded_call_names_every_column_the_table_requires():
    """A NOT NULL column absent from the insert fails at the first call, after
    the caller has been answered -- the worst moment to find out."""
    insert = (ROOT / "sql" / "scoring_service" / "record_call.sql").read_text(
        encoding="utf-8")
    named = set(re.findall(r"%\((\w+)\)s", insert))
    ddl = (ROOT / "sql" / "ddl" / "190_scoring_service_calls.sql").read_text(
        encoding="utf-8")
    body = ddl[ddl.index("CREATE TABLE"):ddl.index(");")]
    required = {line.split()[0] for line in
                (raw.strip() for raw in body.splitlines())
                if "NOT NULL" in line and not line.startswith(("--", "CONSTRAINT"))}
    required -= {"call_id", "called_at"}        # both defaulted
    assert required <= named, sorted(required - named)


# --- B4-L5: the batch the pipeline sends ----------------------------------------

def batch_rows(n=3):
    return [dict(row(), txn_id="t%d" % i) for i in range(n)]


class CountingScorer(FakeScorer):
    """Records the frame it was handed, and scores every row."""

    def scores(self, frame):
        self.frame = frame
        return [self.raw] * len(frame)


def test_one_frame_for_the_whole_batch_and_one_call_to_the_model():
    """A batch of five rows is one predict_proba, not five: the point of the
    batch entry point is that the pipeline stops paying a round trip per row."""
    scorer = CountingScorer()
    answer = serve.batch_answer(scorer, batch_rows(5))
    assert len(scorer.frame) == 5
    assert list(scorer.frame.columns) == list(scorer.features)
    assert len(answer["decisions"]) == 5


def test_the_model_identity_is_reported_once_for_the_batch():
    """fd_scores records model_version on every row and they must all agree: a
    batch answered by two models would be a lie in that journal."""
    answer = serve.batch_answer(CountingScorer(), batch_rows(3))
    assert answer["model_version"] == "4"
    assert answer["recipe_digest"] == "93d16ede49b5"
    assert answer["threshold"] == 0.93
    for decision in answer["decisions"]:
        assert set(decision) == {"txn_id", "score", "flagged"}


def test_each_decision_carries_the_txn_id_it_answers():
    answer = serve.batch_answer(CountingScorer(), batch_rows(3))
    assert [d["txn_id"] for d in answer["decisions"]] == ["t0", "t1", "t2"]


def test_the_batch_uses_the_same_rounding_as_the_single_call():
    """The score is rounded to the scale it will be stored at BEFORE the
    comparison, so this answer and the CHECK on fd_scores cannot differ."""
    answer = serve.batch_answer(CountingScorer(raw=0.9299999960),
                                batch_rows(1))
    assert answer["decisions"][0]["score"] == 0.93
    assert answer["decisions"][0]["flagged"] is True


def test_an_empty_batch_is_refused():
    with pytest.raises(ValueError, match="no row to score"):
        serve.batch_answer(CountingScorer(), [])


def test_a_batch_over_the_declared_maximum_is_refused():
    """A batch half scored is worse than a batch refused: fd_scores would carry
    the half, and nothing downstream would say so."""
    with pytest.raises(ValueError, match="half scored"):
        serve.batch_answer(CountingScorer(),
                           batch_rows(serve.MAX_BATCH_ROWS + 1))


def test_a_row_missing_a_feature_of_the_recipe_is_refused():
    rows = batch_rows(2)
    del rows[1]["f_avg_amount"]
    with pytest.raises(ValueError, match="f_avg_amount"):
        serve.batch_answer(CountingScorer(), rows)


def test_the_batch_path_journals_nothing():
    """THE DECISION OF THE LOT. The pipeline records its own decisions in
    fd_scores, where the foreign key proves the row exists and the stored
    features make it re-derivable. Recording them again here would create a
    second set of figures that could disagree with the first."""
    import inspect

    body = inspect.getsource(serve.batch_answer)
    assert "record(" not in body
    assert serve.RECORD_SQL not in body
    app = (ROOT / "scoring_service" / "app.py").read_text(encoding="utf-8")
    endpoint = app[app.index("def post_score_batch"):]
    assert "serve.record(" not in endpoint
    assert "conn" not in endpoint, "the batch path opens no connection at all"
