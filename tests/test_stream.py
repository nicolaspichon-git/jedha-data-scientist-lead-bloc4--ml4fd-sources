"""Collecting the stream. No network, no database: the decode, the text
contract, the clock bound and the failure handling are all pure.

Block: B3, B4.
Lot: B4-L0.
Competency: ?.
Covers: ?.

The payload used throughout is a REAL response, observed on 2026-11-16."""
import json

import pytest

from ml4fd import checks, stream

# A real body, with the names replaced: the shape is what matters here, and a
# test file in the repository has no business carrying someone's address.
COLUMNS = ["cc_num", "merchant", "category", "amt", "first", "last", "gender",
           "street", "city", "state", "zip", "lat", "long", "city_pop", "job",
           "dob", "trans_num", "merch_lat", "merch_long", "is_fraud",
           "current_time"]
VALUES = [4777065439639721, "fraud_Schmeler and Price", "shopping_net", 106.96,
          "A", "B", "M", "1 rue", "Oaks", "PA", 19456, 40.1334, -75.4536, 737,
          "Job", "1987-04-24", "6f867781597c53009e68655f5cab70aa", 39.675388,
          -76.184247, 0, 1794792372826]

# The longest card measured in the source. 2**53 = 9007199254740992, so this
# one does NOT fit a float64.
LONG_CARD = 4992346398065154184


def body(columns=None, values=None, index=436602, rows=None):
    """A double-encoded body, as the API really sends."""
    inner = {"columns": list(columns or COLUMNS),
             "index": [index],
             "data": rows if rows is not None else [list(values or VALUES)]}
    return json.dumps(json.dumps(inner))


# --- the double decode ----------------------------------------------------

def test_a_double_encoded_body_decodes():
    """Measured on 57 responses: always a JSON string holding JSON."""
    columns, values = stream.decode(body())
    assert "cc_num" in columns
    assert len(columns) == len(values)


def test_a_single_decode_would_have_given_characters():
    """The defect this guards against: json.loads once yields a string, and
    indexing it returns characters instead of fields."""
    once = json.loads(body())
    assert isinstance(once, str)


def test_a_single_encoded_body_is_accepted_not_refused():
    """Not what the API does today. If its shape changed, a single decode would
    be correct, and refusing it would be the wrong answer."""
    inner = json.dumps({"columns": COLUMNS, "index": [1], "data": [VALUES]})
    columns, values = stream.decode(inner)
    assert len(columns) == len(values)


def test_a_body_that_is_not_json_is_refused():
    with pytest.raises(ValueError, match="not JSON"):
        stream.decode("<html>503 Service Unavailable</html>")


# --- the alignment check, THE one that matters ----------------------------

def test_misaligned_columns_and_values_are_refused():
    """A misalignment would put an amount in the category column and nothing
    would raise. It is refused rather than truncated."""
    with pytest.raises(ValueError, match="refused rather than truncated"):
        stream.decode(body(columns=COLUMNS[:-1]))


def test_a_payload_with_two_rows_is_refused():
    with pytest.raises(ValueError, match="2 row"):
        stream.decode(body(rows=[VALUES, VALUES]))


def test_a_payload_without_columns_is_refused():
    with pytest.raises(ValueError, match="no columns/data pair"):
        stream.decode(json.dumps(json.dumps({"data": [VALUES]})))


# --- every value is text --------------------------------------------------

def test_every_value_comes_out_as_text():
    """S1's contract for the CSV, applied to the stream: both sources reach the
    raw zone in the same shape."""
    row = stream.to_row(*stream.decode(body()))
    assert all(isinstance(v, str) for v in row.values())


def test_a_nineteen_digit_card_crosses_intact():
    """THE criterion of this lot. 7 of the 924 cards exceed 2**53; through a
    float, 4992346398065154184 becomes 4992346398065154048 -- a different
    client_key, therefore a different cardholder, therefore history features
    computed against an empty past."""
    values = list(VALUES)
    values[COLUMNS.index("cc_num")] = LONG_CARD
    row = stream.to_row(*stream.decode(body(values=values)))
    assert row["cc_num"] == "4992346398065154184"
    assert int(row["cc_num"]) == LONG_CARD
    assert LONG_CARD > 2 ** 53          # the premise of the test itself


def test_the_float_path_would_have_lost_digits():
    """Stated as a test so the risk is not taken on trust."""
    assert int(float(LONG_CARD)) != LONG_CARD
    assert int(float(LONG_CARD)) == 4992346398065154048


def test_an_amount_keeps_its_decimals_as_text():
    row = stream.to_row(*stream.decode(body()))
    assert row["amt"] == "106.96"


def test_the_source_index_is_carried_as_a_field():
    row = stream.to_row(*stream.decode(body(index=436602)))
    assert row["index"] == "436602"


# --- the Reading ----------------------------------------------------------

def reading(server_ms=1794792372826, our_ms=1794792367000, latency=5.8,
            values=None):
    return stream.read_reading(
        body(values=values or _with_time(server_ms)), our_ms, latency)


def _with_time(server_ms):
    values = list(VALUES)
    values[COLUMNS.index("current_time")] = server_ms
    return values


def test_current_time_is_read_as_milliseconds():
    """Read as seconds it is out of range; as microseconds it lands in 1970."""
    assert reading().server_time().year == 2026
    assert reading().server_time().isoformat().startswith("2026-11-16T01:26")


def test_the_clock_gap_is_the_server_stamp_less_ours():
    assert reading(server_ms=1_000_000, our_ms=994_000).clock_gap_s == 6.0


def test_a_response_without_current_time_is_refused():
    values = list(VALUES)
    values[COLUMNS.index("current_time")] = None
    with pytest.raises(ValueError, match="no current_time"):
        stream.read_reading(body(values=values), 0, 0.0)


def test_a_current_time_that_is_not_an_integer_is_refused():
    values = list(VALUES)
    values[COLUMNS.index("current_time")] = "hier"
    with pytest.raises(ValueError, match="not an integer"):
        stream.read_reading(body(values=values), 0, 0.0)


# --- the clock bound, now in checks.py -----------------------------------

def clock_rejects(server_ms, our_ms, latency, tolerance=120):
    r = reading(server_ms=server_ms, our_ms=our_ms, latency=latency)
    return checks.check_clock(r.row, 1, r.our_ms, r.latency_s, tolerance)


def test_a_gap_of_the_order_of_the_latency_is_plausible():
    """Measured median in N0: +5.1 s, which IS the latency -- the server stamps
    when it answers, we when we call."""
    assert clock_rejects(1_000_000, 994_200, 5.8) == []


def test_the_measured_818_second_gap_is_refused():
    """The real anomaly of 2026-11-16, call 55 of 60."""
    rejects = clock_rejects(1_818_400, 1_000_000, 7.5)
    assert len(rejects) == 1
    assert rejects[0].reason_code == checks.CLOCK_IMPLAUSIBLE
    assert "818 s after our call" in rejects[0].reason_detail


def test_a_timestamp_before_our_call_is_refused():
    rejects = clock_rejects(1_000_000, 1_300_000, 5.0)
    assert rejects and "before our call" in rejects[0].reason_detail


def test_the_bound_does_not_attribute_a_cause():
    """The 818 s gap was our own suspended process, not the server. The remedy
    is the same either way, so the reason names neither."""
    detail = clock_rejects(1_818_400, 1_000_000, 7.5)[0].reason_detail.lower()
    assert "server" not in detail and "suspend" not in detail


def test_the_rule_lives_in_one_place_only():
    """stream.py carried its own copy for an hour. One rule, one place."""
    assert not hasattr(stream, "clock_implausible")


# --- validation under the stream profile ---------------------------------

def test_a_valid_reading_is_accepted():
    accepted, items = stream.validate([reading()], 50_000, 120)
    assert len(accepted) == 1 and items == []


def test_the_absent_trans_date_trans_time_is_not_a_defect_here():
    """It is mandatory on the CSV and absent by design on the stream. The
    validation is parameterised by the SOURCE, not by the absence."""
    accepted, _ = stream.validate([reading()], 50_000, 120)
    assert accepted
    assert "trans_date_trans_time" not in accepted[0].row


def test_the_same_trans_num_twice_is_two_events_not_a_duplicate():
    """Two draws of the same historical row are two real transactions. The CSV
    profile would reject the second."""
    accepted, items = stream.validate([reading(), reading()], 50_000, 120)
    assert len(accepted) == 2 and items == []


def test_an_implausible_clock_sends_the_reading_to_quarantine():
    r = reading(server_ms=1_818_400, our_ms=1_000_000, latency=7.5)
    accepted, items = stream.validate([r], 50_000, 120)
    assert accepted == []
    codes = [x.reason_code for _, _, rejects in items for x in rejects]
    assert checks.CLOCK_IMPLAUSIBLE in codes


def test_an_implausible_amount_sends_the_reading_to_quarantine():
    values = list(VALUES)
    values[COLUMNS.index("amt")] = 99_999_999
    r = stream.read_reading(body(values=values), 1794792367000, 5.8)
    accepted, items = stream.validate([r], 50_000, 120)
    assert accepted == []
    codes = [x.reason_code for _, _, rejects in items for x in rejects]
    assert checks.AMT_IMPLAUSIBLE in codes


def test_the_reasons_point_at_the_rank_that_caused_them():
    good, bad = reading(), reading(server_ms=1_818_400, our_ms=1_000_000,
                                   latency=7.5)
    accepted, items = stream.validate([good, bad], 50_000, 120)
    assert len(accepted) == 1
    assert [rank for rank, _, _ in items] == [2]


def test_the_items_are_the_shape_quarantine_write_consumes():
    """(line_no, payload, rejects) -- the writer S1 already has, unmodified."""
    _, items = stream.validate(
        [reading(server_ms=1_818_400, our_ms=1_000_000, latency=7.5)],
        50_000, 120)
    rank, payload, rejects = items[0]
    assert isinstance(rank, int)
    assert "row" in payload and "trans_num" in payload
    assert rejects and hasattr(rejects[0], "reason_code")


def test_the_ranks_are_the_same_in_all_three_tables():
    """The raw zone, the quarantine and fd_transactions must speak about the
    same record by the same number."""
    readings = [reading(), reading()]
    assert [rank for rank, _ in stream.valid_records(readings)] == [1, 2]


# --- polling, and the failures it returns ---------------------------------

class FakeSession:
    """Answers a scripted sequence. Counts its calls."""

    class Response:
        def __init__(self, status, text=""):
            self.status_code, self.text = status, text

    def __init__(self, script):
        self.script, self.calls = list(script), 0

    def get(self, url, timeout=None):
        self.calls += 1
        status = self.script.pop(0) if self.script else 200
        if status == 200:
            return self.Response(200, body())
        return self.Response(status)


def test_a_clean_poll_returns_its_readings():
    readings, failures = stream.poll("http://x", 3,
                                     session=FakeSession([200, 200, 200]))
    assert len(readings) == 3 and failures == []


def test_a_502_is_counted_and_does_not_stop_the_poll():
    """Measured: 5 % of calls fail. A collector that raised would stop five
    times a day on a stream that is merely flaky."""
    readings, failures = stream.poll("http://x", 3,
                                     session=FakeSession([200, 502, 200]))
    assert len(readings) == 2
    assert len(failures) == 1 and failures[0].status == 502


def test_a_502_is_not_retried_in_the_same_run():
    """It costs 25 s; retrying it would eat the whole minute."""
    session = FakeSession([502, 200])
    stream.poll("http://x", 2, session=session)
    assert session.calls == 2


def test_a_429_is_retried_at_once():
    """It costs 0.23 s, the cheapest thing that can happen."""
    session = FakeSession([429, 200])
    readings, failures = stream.poll("http://x", 1, session=session)
    assert session.calls == 2
    assert len(readings) == 1 and failures == []


def test_a_persistent_429_is_returned_as_a_failure():
    session = FakeSession([429, 429])
    readings, failures = stream.poll("http://x", 1, session=session)
    assert readings == [] and len(failures) == 1
    assert failures[0].refused


def test_failures_are_returned_not_dropped():
    """EC14: a poll that quietly returned three rows where four were asked is
    exactly the absence nobody notices."""
    readings, failures = stream.poll(
        "http://x", 4, session=FakeSession([200, 502, 500, 200]))
    assert len(readings) + len(failures) == 4


def test_a_body_that_does_not_decode_is_a_failure_not_a_crash():
    class Broken(FakeSession):
        def get(self, url, timeout=None):
            self.calls += 1
            return self.Response(200, "{}")

    readings, failures = stream.poll("http://x", 1, session=Broken([]))
    assert readings == [] and len(failures) == 1
    assert failures[0].status == 200          # the HTTP call worked
    assert "columns/data" in failures[0].error


# --- what goes to the raw zone -------------------------------------------

def test_the_payload_carries_the_row_and_the_provenance():
    payload = reading().as_payload()
    assert set(payload) == {"row", "source_index", "trans_num", "server_ms",
                            "our_ms", "latency_ms"}
    assert payload["row"]["cc_num"] == "4777065439639721"


def test_the_payload_is_json_serialisable():
    """It is written to a jsonb column."""
    assert json.loads(json.dumps(reading().as_payload()))


def test_the_payload_keeps_the_nominative_fields():
    """The raw zone holds what arrived. Minimisation happens in L3, under a role
    that cannot read this table."""
    payload = reading().as_payload()
    assert "first" in payload["row"] and "street" in payload["row"]


# --- the event identity, which is not the record's own -------------------

def test_two_draws_of_the_same_source_row_get_distinct_ids():
    """THE defect this guards against. trans_num names the SOURCE ROW, and the
    same row comes back: keyed on it alone, the second draw would collide with
    fd_transactions' primary key and be lost -- silently, since
    ON CONFLICT DO NOTHING is what makes a replay idempotent."""
    from ml4fd import transform

    first = {"trans_num": "abc", "current_time": "1794792372826",
             "cc_num": "4777065439639721", "merchant": "m",
             "category": "shopping_net", "amt": "106.96", "is_fraud": "0"}
    second = dict(first, current_time="1794792400000")
    pepper = "x" * 32
    a = transform.to_transaction(first, pepper, checks.STREAM_PROFILE)
    b = transform.to_transaction(second, pepper, checks.STREAM_PROFILE)
    assert a["txn_id"] != b["txn_id"]
    assert a["txn_ts"] != b["txn_ts"]


def test_the_csv_profile_still_keys_on_trans_num_alone():
    """S1 is unchanged: one line, one transaction."""
    record = {"trans_num": "abc", "trans_date_trans_time": "2020-06-21 12:14:25"}
    assert checks.CSV_PROFILE.event_id(record) == "abc"


def test_the_event_id_is_readable_not_hashed():
    """An operator must be able to see which source row was drawn and when. The
    value is not a secret and does not need to be short."""
    record = {"trans_num": "abc", "current_time": "1794792372826"}
    assert checks.STREAM_PROFILE.event_id(record) == "abc:1794792372826"


def test_transform_and_truth_derive_the_same_id():
    """The foreign key from fd_ground_truth to fd_transactions is on txn_id, so
    two derivations would break every verdict of the stream."""
    import datetime as dt

    from ml4fd import transform, truth

    record = {"trans_num": "abc", "current_time": "1794792372826",
              "cc_num": "4777065439639721", "merchant": "m",
              "category": "shopping_net", "amt": "106.96", "is_fraud": "0"}
    typed = transform.to_transaction(record, "x" * 32, checks.STREAM_PROFILE)
    verdict = truth.to_ground_truth(record, dt.timedelta(days=30), "stream",
                                    checks.STREAM_PROFILE)
    assert typed["txn_id"] == verdict["txn_id"]
    assert typed["txn_ts"] == verdict["txn_ts"]


def test_a_stream_record_no_longer_raises_on_its_timestamp():
    """Hard-coded to trans_date_trans_time, to_transaction raised on every
    stream record."""
    from ml4fd import transform

    record = {"trans_num": "abc", "current_time": "1794792372826",
              "cc_num": "4777065439639721", "merchant": "m",
              "category": "shopping_net", "amt": "106.96", "is_fraud": "0"}
    typed = transform.to_transaction(record, "x" * 32, checks.STREAM_PROFILE)
    assert typed["txn_ts"].year == 2026


def test_the_error_names_the_field_it_could_not_read():
    from ml4fd import transform

    with pytest.raises(ValueError, match="current_time"):
        transform.to_transaction({"trans_num": "a", "current_time": "hier",
                                  "cc_num": "1", "merchant": "m",
                                  "category": "c", "amt": "1"},
                                 "x" * 32, checks.STREAM_PROFILE)


# --- les deux portes de la validation doivent rendre le meme verdict ----------

def _with_amount(amount):
    values = list(VALUES)
    values[COLUMNS.index("amt")] = amount
    values[COLUMNS.index("current_time")] = 1794792372826
    return values


def test_the_two_validation_doors_agree():
    """THE test that keeps the two adapters honest.

    validate() works on Readings held in memory, validate_raw() on the
    envelopes the raw zone gives back -- two shapes, one rule. N7's first draft
    had the DAG call checks.validate_batch alone, which dropped the clock bound
    while the manual runner kept it: the pipeline validated differently
    depending on which door the record came through.
    """
    readings = [reading(), reading(values=_with_amount("beaucoup"))]

    accepted, items = stream.validate(readings, 5000.0, 120.0)
    envelopes = [(rank, r.as_payload())
                 for rank, r in enumerate(readings, start=1)]
    raw_accepted, raw_items = stream.validate_raw(envelopes, 5000.0, 120.0)

    assert len(accepted) == len(raw_accepted)
    assert len(items) == len(raw_items)
    # The same lines, with the same coded reasons, in the same order.
    assert [line for line, _, _ in items] == [line for line, _, _ in raw_items]
    for (_, _, left), (_, _, right) in zip(items, raw_items):
        assert [r.reason_code for r in left] == [r.reason_code for r in right]


def test_the_raw_door_applies_the_clock_bound():
    """Not merely the field rules: a reading whose clock is implausible must be
    quarantined whichever door it came through."""
    envelope = reading().as_payload()
    # Our clock half an hour behind the server's: beyond any sane tolerance.
    envelope["our_ms"] = envelope["server_ms"] - 1_800_000
    _, items = stream.validate_raw([(1, envelope)], 5000.0, 120.0)
    assert items
    assert any(r.reason_code == checks.CLOCK_IMPLAUSIBLE
               for _, _, rejects in items for r in rejects)


def test_the_raw_door_stores_the_envelope_with_the_reason():
    """As validate() does: a quarantined line reads without joining anything."""
    envelope = reading(values=_with_amount("beaucoup")).as_payload()
    _, items = stream.validate_raw([(7, envelope)], 5000.0, 120.0)
    assert items[0][0] == 7
    assert items[0][1] is envelope
