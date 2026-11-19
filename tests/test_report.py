"""Assembling and rendering the morning report.

Covers: EE04, EI04, EC14, EC17
Lot: P2
Competency: C5

THE PROPERTY WORTH TESTING IS THAT THE DOCUMENT IS A FUNCTION OF ITS DATA.
Two runs on the same window must produce the same bytes, or comparing two
mornings -- which is what an analyst does -- compares the renderer's mood as
much as the data. The decision that makes it absolute rather than modulo a line
is that the document carries NO production timestamp; this file holds it to that.

render() is a pure function of a dict, so everything below runs without a
database. The four queries were executed against a real PostgreSQL 16 in P1 and
the gathering is exercised here against a fake connection, for the same reason
as in N5 and N9: what is worth testing is what this module decides.
"""
from __future__ import annotations

import datetime as dt
import decimal
import types

import pytest

from ml4fd import report

PARIS = dt.timezone(dt.timedelta(hours=1), "CET")
START = dt.datetime(2026, 11, 16, 0, 0, tzinfo=PARIS)
END = dt.datetime(2026, 11, 17, 0, 0, tzinfo=PARIS)
KNOWN_AT = dt.datetime(2026, 11, 17, 6, 0, tzinfo=PARIS)


def _window(**over):
    base = dict(start=START, end=END, knowledge_at=KNOWN_AT,
                zone="Europe/Paris")
    base.update(over)
    return report.Window(**base)


def _settings(root="/tmp/reports"):
    return types.SimpleNamespace(
        report_root=root, report_timezone="Europe/Paris",
        alert_capacity_per_day=60, realtime_schedule="* * * * *",
        calls_per_run=5, ops_freshness_minutes=10, ops_max_empty_share=0.10,
        ops_max_rejection_share=0.05, ops_window_minutes=60,
        observed_per_day=lambda: 7200)


def _alert(rank=1, score="0.97", amt="1204.50", usual="71.40", notified=True):
    return {"txn_id": "abc:%d" % rank,
            "txn_ts": dt.datetime(2026, 11, 16, 23, 41, tzinfo=PARIS),
            "merchant": "fraud_Dach-Borer", "category": "shopping_net",
            "amt": decimal.Decimal(amt), "score": decimal.Decimal(score),
            "threshold": decimal.Decimal("0.89"), "model_version": "3",
            "raised_at": dt.datetime(2026, 11, 16, 23, 41, tzinfo=PARIS),
            "notified": notified, "channel": "file", "attempts": 1,
            "f_tx_count": 38,
            "f_avg_amount": decimal.Decimal(usual) if usual else None,
            "times_usual": decimal.Decimal("16.9") if usual else None,
            "expected_loss": decimal.Decimal("1168.37")}


def _data(**over):
    base = {
        "window": _window(),
        "volumes": {"total": {"bucket": "total", "transactions": 7200,
                              "amount": decimal.Decimal("499582.14"),
                              "cardholders": 911,
                              "median_amount": decimal.Decimal("42.18")},
                    "category": [{"bucket": "grocery_pos", "transactions": 1204,
                                  "amount": decimal.Decimal("88431.02")}],
                    "hour": [{"bucket": "22", "transactions": 612},
                             {"bucket": "08", "transactions": 201}]},
        "alerts": [_alert()],
        "verdicts": {"verdicts": 7200, "frauds": 29, "alerts": 42,
                     "true_positives": 22, "false_positives": 20,
                     "false_negatives": 7, "true_negatives": 7151,
                     "recall_observed": decimal.Decimal("0.7586"),
                     "precision_observed": decimal.Decimal("0.5238"),
                     "base_rate_observed": decimal.Decimal("0.004028"),
                     "payments_from": dt.date(2026, 10, 17),
                     "payments_to": dt.date(2026, 10, 17), "payment_days": 1},
        "pending": {"transactions": 7200, "pending": 7200,
                    "without_verdict_row": 0,
                    "first_due_at": dt.datetime(2026, 12, 16, tzinfo=PARIS),
                    "last_due_at": dt.datetime(2026, 12, 16, 23, 59,
                                               tzinfo=PARIS)},
        "reference": {"version": "8813b096e2e4", "recall": 0.751219512195122,
                      "precision": 0.5292096219931272,
                      "alerts_per_day": 41.61, "base_rate_test": 0.004072},
        "capacity": 60,
    }
    base.update(over)
    return base


# --- the window -----------------------------------------------------------

def test_a_naive_window_is_refused():
    """A window without a zone cannot say which calendar day it covers, and
    two runs meant to produce the same document would differ by the offset of
    whoever ran them."""
    with pytest.raises(ValueError, match="naive"):
        _window(start=dt.datetime(2026, 11, 16))


def test_a_window_that_ends_before_it_starts_is_refused():
    with pytest.raises(ValueError, match="ends before"):
        _window(start=END, end=START)


def test_a_knowledge_date_inside_the_window_is_refused():
    """The report would be asked what was known before the day it covers was
    over -- which is how a section 3 comes to show half a day of verdicts."""
    with pytest.raises(ValueError, match="precedes the end"):
        _window(knowledge_at=START + dt.timedelta(hours=1))


def test_the_covered_day_is_read_at_the_start_of_the_window():
    """THE OFF-BY-ONE OF EVERY DAILY REPORT. A half-open window ending at
    midnight would name the NEXT day if read at its end."""
    assert _window().day == dt.date(2026, 11, 16)


# --- the document is a function of its data -------------------------------

def test_two_renders_of_the_same_data_are_byte_identical():
    """The criterion of P2, and absolute rather than modulo a line because the
    document carries no production timestamp."""
    first = report.render(_data(), _settings())
    second = report.render(_data(), _settings())
    assert first == second
    assert report.digest(first) == report.digest(second)


def test_the_document_carries_no_production_timestamp():
    """produced_at belongs to fd_reports (P3). In the body it would change the
    bytes and the digest on every run, and a diff between two mornings would
    never be empty."""
    text = report.render(_data(), _settings())
    assert "ne d\u00e9pend pas de l'heure" in text   # the provenance, not the hour
    today = dt.datetime.now(dt.timezone.utc)
    for stamp in (today.strftime("%H:%M"), today.strftime("%Y-%m-%dT%H")):
        assert stamp not in text


def test_the_sentence_g2_cannot_be_answered_without_is_there_and_figured():
    """G2 asks each morning for "yesterday's frauds". Taken at its word the
    request is impossible, and omitting the sentence would let a reader take
    "frauds confirmed: 0" for a complete column."""
    text = report.render(_data(), _settings())
    assert "attendent encore leur verdict" in text
    assert "7 200 transactions" in text
    assert "2026-12-16" in text
    assert "conteste" in text


def test_each_section_dates_the_payments_it_covers():
    """THE CENTRAL TRAP OF S4, SAID IN THE READER'S LANGUAGE. A report read on
    the wrong timeline shows either a permanent zero or month-old frauds under
    the heading "yesterday".

    The module reasons in "axes" -- the payment axis and the knowledge axis --
    and that is the right abstraction in a docstring and the wrong words in a
    document: an analyst does not read "axe de la connaissance". Two DATES
    thirty days apart say the same thing and need no glossary, so every title
    carries the period it covers and section 3 carries both.
    """
    text = report.render(_data(), _settings())
    assert "## 1. Paiements du 2026-11-16" in text
    assert "## 2. Alertes sur les paiements du 2026-11-16" in text
    assert "## 3. Verdicts re\u00e7us le 2026-11-16 sur les paiements du 2026-10-17" in text
    # And the designer's vocabulary stays OUT of the document.
    assert "axe du paiement" not in text
    assert "axe de la connaissance" not in text


def test_section_three_explains_the_offset_in_plain_words():
    """So the reader cannot mistake "verdicts of yesterday" for "payments of
    yesterday". The explanation is the business fact -- a verdict arrives when
    the cardholder disputes -- and not the delay's technical name."""
    text = report.render(_data(), _settings())
    assert "2026-10-17" in text
    assert "quand le porteur conteste" in text
    assert "non sur ceux de la section 1" in text
    assert "d\u00e9lai de verdict" not in text


def test_the_order_of_the_alerts_is_written_in_the_document():
    """A piege nomme dans le plan: an analyst who reviews the first 38 of a
    list of 60 must know what he sorted on."""
    text = report.render(_data(), _settings())
    assert "de la perte attendue la plus forte \u00e0 la plus faible" in text
    # And the formula is spelled out, not left as a symbol to decode.
    assert "le score multipli\u00e9 par le montant" in text


def test_the_score_range_of_the_day_is_printed():
    """So the prediction of P0 -- that the order will follow the amount,
    the score band above the threshold being narrow -- can be denied on
    figures after a few weeks instead of argued."""
    text = report.render(_data(), _settings())
    assert "Scores de la journ\u00e9e" in text


def test_an_unmeasurable_recall_never_renders_as_zero():
    """"No fraud to catch" and "caught none of the frauds" are opposite
    findings. The queries return null for both ratios on a day with no fraud."""
    data = _data()
    data["verdicts"] = dict(data["verdicts"], frauds=0, true_positives=0,
                            false_negatives=0, recall_observed=None,
                            precision_observed=None)
    text = report.render(data, _settings())
    assert "non mesurable" in text
    assert "par le mod\u00e8le : **0,0 %" not in text


def test_the_two_fraud_shares_sit_beside_the_two_measurements():
    """A recall compared across two different base rates compares nothing --
    and "taux de base" is the statistician's name for it, not the analyst's.
    The document says "part de fraudes" and gives both figures."""
    text = report.render(_data(), _settings())
    assert "des paiements concern\u00e9s" in text
    assert "part de fraudes" in text
    assert "taux de base" not in text


def test_the_measurements_are_percentages_and_not_ratios():
    """0,7586 asks a reader to convert; 75,9 pour cent does not. The ratios
    stay in the queries and in fd_reports, where code compares them."""
    text = report.render(_data(), _settings())
    assert "75,9 %" in text          # recall 0.7586
    assert "52,4 %" in text          # precision 0.5238
    assert "0,7586" not in text


def test_the_header_speaks_the_readers_language():
    """"Date de connaissance" is the designer's phrase for it. An analyst reads
    "informations arretees au"."""
    text = report.render(_data(), _settings())
    assert "Informations arr\u00eat\u00e9es au" in text
    assert "Journ\u00e9e couverte" in text
    assert "Date de connaissance" not in text


def test_the_alert_columns_name_what_they_show():
    """"Historique", "Habituel" and "Rapport" name table fields. An analyst
    reads what they mean."""
    text = report.render(_data(), _settings())
    assert "Paiements pass\u00e9s" in text
    assert "Montant habituel" in text
    assert "\u00c9cart \u00e0 l'habitude" in text


def test_the_verdicts_are_received_and_do_not_fall():
    """"Un verdict tombe" is this project's own verb."""
    text = report.render(_data(), _settings())
    assert "Verdicts re\u00e7us" in text
    assert "tomb\u00e9s : " not in text


def test_the_health_section_says_who_it_is_for():
    """Its details repeat the controls' own messages, which are written for an
    operator and stay so. Saying it is better than pretending otherwise."""
    data = _data()
    data["health"] = _health()
    assert "s'adresse \u00e0 l'exploitant" in report.render(data, _settings())


def test_the_two_measurements_are_glossed_before_being_named():
    """"Rappel" and "precision" are terms of art. The document says what they
    measure and puts the term in italics after it, which teaches a reader
    without assuming he already knows."""
    text = report.render(_data(), _settings())
    assert "Fraudes retrouv\u00e9es par le mod\u00e8le" in text
    assert "*(rappel)*" in text
    assert "Alertes qui \u00e9taient de vraies fraudes" in text
    assert "*(pr\u00e9cision)*" in text


def test_a_missing_reference_degrades_and_does_not_remove_the_section():
    """A morning report that refuses to exist because a comparison is missing
    has failed at its one job."""
    text = report.render(_data(reference=None), _settings())
    assert "R\u00e9f\u00e9rence de M5 : **indisponible**" in text
    assert "Fraudes retrouv\u00e9es par le mod\u00e8le" in text


def test_a_column_added_for_the_dashboard_does_not_move_the_document():
    """daily_frauds.sql serves the document AND the pages, so a column added
    for a page must leave the document byte-identical -- its digest in
    fd_reports vouches for it. notified_at was added for S6/R11."""
    before = report.render(_data(), _settings())
    widened = _alert()
    widened["notified_at"] = dt.datetime(2026, 11, 17, 6, 2, tzinfo=PARIS)
    after = report.render(_data(alerts=[widened]), _settings())
    assert after == before
    assert report.digest(after) == report.digest(before)


def test_a_day_without_alerts_still_renders():
    text = report.render(_data(alerts=[]), _settings())
    assert "Aucune alerte sur cette journ\u00e9e." in text
    assert "aucune alerte, donc aucune version" in text


def test_a_cardholder_first_transaction_reads_as_such():
    """f_avg_amount is null for a first transaction: "premiere" and not a ratio
    of zero."""
    data = _data(alerts=[_alert(usual=None)])
    assert "premi\u00e8re" in report.render(data, _settings())


def test_an_unnotified_alert_is_visible_in_the_table():
    """An alert raised but never delivered is the failure the capacity policy
    and the channel exist around; the analyst must see it as such."""
    data = _data(alerts=[_alert(notified=False)])
    assert "| NON |" in report.render(data, _settings())


def test_a_missing_verdict_row_is_announced_as_a_finding():
    """dag_ingest_verdicts runs nightly. A non-zero count means it did not, and
    the report cannot then say when the verdict falls -- so it says THAT."""
    data = _data()
    data["pending"] = dict(data["pending"], without_verdict_row=120)
    text = report.render(data, _settings())
    assert "n'a pas tourn\u00e9" in text


def test_no_identifying_field_can_reach_the_document():
    """fd_transactions holds none, so the document cannot carry one -- and that
    is an argument. This reads the rendered text."""
    text = report.render(_data(), _settings()).lower()
    for word in ("cc_num", "street", "dob", "client_key"):
        assert word not in text


# --- section 4 -------------------------------------------------------------

def _health():
    return {"facts": {"window_minutes": 1440, "seconds_since_success": 61.0,
                      "runs_ended": 1436, "runs_failed": 4, "runs_empty": 4,
                      "lines_read": 7183, "lines_rejected": 12,
                      "batches_breaking_invariant": 0,
                      "empty_share": 0.002786, "rejection_share": 0.001671},
            "controls": [{"control": "freshness", "state": "ok",
                          "detail": "derniere execution reussie il y a 1.0 min"},
                         {"control": "volume_invariant", "state": "alert",
                          "detail": "2 lot(s) ou n_read differe"}],
            "in_alert": 1, "expected_runs": 1440,
            "expected_transactions": 7200}


def test_the_health_section_renders_the_controls_and_their_state():
    """It is the fourth section, added in P0 and coming from N9: the operations
    channel carries only what cannot wait a day, and the daily set had to go
    somewhere."""
    data = _data()
    data["health"] = _health()
    text = report.render(data, _settings())
    assert "## 4. Bon fonctionnement du 2026-11-16" in text
    # The identifiers of health.CONTROLS are the right names in a module and
    # the wrong ones in a document.
    assert "| Fra\u00eecheur de l'ingestion | ok |" in text
    assert "| Concordance des compteurs | ALERTE |" in text
    assert "freshness" not in text
    assert "volume_invariant" not in text
    assert "1 440 attendues" in text


def test_the_document_renders_without_the_health_section():
    """A report whose health query failed must still be a report."""
    text = report.render(_data(), _settings())
    assert "## 4." not in text
    assert "## 3." in text


# --- writing ---------------------------------------------------------------

def test_the_file_is_named_by_the_day_it_covers(tmp_path):
    assert report.path_for(_settings(tmp_path), dt.date(2026, 11, 16)) \
        == tmp_path / "2026-11-16.md"


def test_the_directory_is_created_and_not_assumed(tmp_path):
    """28 real observations and a whole dataset have been lost to directories
    that were not mounted. A report nobody can find has not been produced."""
    settings = _settings(tmp_path / "deep" / "reports")
    written = report.write(settings, dt.date(2026, 11, 16), "# essai\n")
    assert written.read_text(encoding="utf-8") == "# essai\n"


def test_the_digest_is_over_the_bytes_of_the_document(tmp_path):
    """fd_reports stores it (P3) so a file on disk can be checked against the
    row describing it -- the reasoning of run.json beside a dataset in M8."""
    text = report.render(_data(), _settings(tmp_path))
    written = report.write(_settings(tmp_path), dt.date(2026, 11, 16), text)
    import hashlib
    assert report.digest(text) == hashlib.sha256(
        written.read_bytes()).hexdigest()


def test_the_document_ends_with_a_single_newline(tmp_path):
    """So two consecutive days diff on their content and not on whitespace."""
    text = report.render(_data(), _settings(tmp_path))
    assert text.endswith("\n") and not text.endswith("\n\n")


# --- gathering, against a fake connection ----------------------------------

class _Fake:
    """A connection answering the report queries by a MARKER IN THEIR TEXT.

    By the text and not by the file name: _rows() hands the cursor the file's
    CONTENTS, so a fake keyed on "daily_pending" matches nothing -- which is
    how the first version of the last test in this file failed.
    """

    def __init__(self, rows):
        self.rows = rows
        self.asked = []

    def cursor(self):
        fake = self

        class _Cursor:
            description = ()

            def __enter__(self): return self
            def __exit__(self, *exc): return False

            def execute(self, sql, params=None):
                for name, (names, data) in fake.rows.items():
                    if name in sql:
                        fake.asked.append((name, params))
                        self.description = tuple(
                            types.SimpleNamespace(name=n) for n in names)
                        self._data = data
                        return
                raise AssertionError("requete inattendue")

            def fetchall(self):
                return self._data

        return _Cursor()


def test_the_volumes_are_split_on_the_axis_the_query_returns():
    """GROUPING SETS gives one result set; the three readings are separated by
    the axis column, so the total and the breakdowns cannot disagree."""
    fake = _Fake({"GROUPING SETS": (
        ("axis", "bucket", "transactions", "amount", "cardholders",
         "median_amount"),
        [("total", "total", 7200, 10, 911, 42),
         ("category", "grocery_pos", 1204, 5, 400, 40),
         ("hour", "22", 612, 3, 300, 41)])})
    out = report.volumes(fake, _window())
    assert out["total"]["transactions"] == 7200
    assert [row["bucket"] for row in out["category"]] == ["grocery_pos"]
    assert [row["bucket"] for row in out["hour"]] == ["22"]


def test_an_empty_day_still_gives_a_total():
    """A window with no payment is what a Sunday outage looks like, and the
    document must render it rather than fail on a missing key."""
    fake = _Fake({"GROUPING SETS": (("axis", "bucket", "transactions",
                                     "amount", "cardholders",
                                     "median_amount"), [])})
    out = report.volumes(fake, _window())
    assert out["total"]["transactions"] == 0


def test_the_window_travels_as_parameters_and_the_zone_only_where_needed():
    fake = _Fake({"GROUPING SETS": (("axis", "bucket", "transactions",
                                     "amount", "cardholders",
                                     "median_amount"), [])})
    report.volumes(fake, _window())
    _, params = fake.asked[0]
    assert params["window_start"] == START and params["window_end"] == END
    assert params["zone"] == "Europe/Paris"
    assert "knowledge_at" not in params


def test_the_pending_query_is_the_only_one_told_the_knowledge_date():
    """"Known" is label_ts <= knowledge_at, and the row exists from the night
    of the payment with a label_ts in the future: the filter is the
    withholding."""
    assert "knowledge_at" in _window().params(knowledge=True)
    assert "knowledge_at" not in _window().params()


def test_the_rows_are_named_by_the_cursor_and_not_by_position():
    """Four queries with a dozen columns between them: by position, an edit to
    a SELECT shifts a rejected count into a read count. The defect to_alert()
    was guarded against in N5."""
    fake = _Fake({"without_verdict_row": (("transactions", "pending",
                                     "without_verdict_row", "first_due_at",
                                     "last_due_at"),
                                    [(7200, 7200, 0, None, None)])})
    out = report.pending(fake, _window())
    assert out["pending"] == 7200
    assert out["without_verdict_row"] == 0


def test_an_untranslated_control_is_shown_and_not_hidden():
    """A control nobody translated must still be visible: the identifier falls
    through unchanged rather than disappearing from the table."""
    data = _data()
    data["health"] = dict(_health(), controls=[
        {"control": "nouveau_controle", "state": "ok", "detail": "d"}])
    assert "| nouveau_controle | ok |" in report.render(data, _settings())
