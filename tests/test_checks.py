"""The two validation classes, and the trap that separates them.

Block: B3, B4.
Lot: B4-L0.
Competency: ?.
Covers: ?.
"""

from ml4fd import checks

# A record taken from the source, which passes every check.
GOOD = {
    "source_line": "2",
    "trans_date_trans_time": "2020-06-21 12:14:53",
    "cc_num": "2291163933867244",
    "merchant": "fraud_Kirlin and Sons",
    "category": "personal_care",
    "amt": "276.31",
    "dob": "1968-03-19",
    "trans_num": "a1b2c3d4e5f6",
    "unix_time": "1371816893",
    "lat": "33.9659", "long": "-80.9355",
    "merch_lat": "33.986391", "merch_long": "-81.200714",
    "is_fraud": "1",
}
MAX_AMOUNT = 50000.0


def codes(rejects):
    return {r.reason_code for r in rejects}


def altered(**changes):
    record = dict(GOOD)
    record.update(changes)
    return record


# --- EC20: one field at a time -------------------------------------------

def test_a_sound_record_yields_no_field_reject():
    assert checks.check_fields(GOOD, 1, MAX_AMOUNT) == []


def test_an_empty_mandatory_field_is_rejected():
    r = checks.check_fields(altered(merchant=""), 1, MAX_AMOUNT)
    assert checks.MISSING_MANDATORY in codes(r)
    assert all(x.reason_class == checks.FIELD for x in r)


def test_a_missing_column_is_told_apart_from_an_empty_one():
    record = dict(GOOD)
    del record["category"]
    assert checks.SCHEMA_MISSING_COLUMN in codes(
        checks.check_fields(record, 1, MAX_AMOUNT))


def test_an_unparsable_amount_is_rejected():
    assert checks.AMT_UNPARSABLE in codes(
        checks.check_fields(altered(amt="12,34"), 1, MAX_AMOUNT))


def test_a_negative_amount_is_rejected():
    assert checks.AMT_NEGATIVE in codes(
        checks.check_fields(altered(amt="-1.00"), 1, MAX_AMOUNT))


def test_an_amount_above_the_bound_is_rejected():
    assert checks.AMT_IMPLAUSIBLE in codes(
        checks.check_fields(altered(amt="99999.00"), 1, MAX_AMOUNT))


def test_the_largest_real_amount_is_accepted():
    """Measured maximum of the source: 22,768.11. The bound must not reject it."""
    assert checks.check_fields(altered(amt="22768.11"), 1, MAX_AMOUNT) == []


def test_an_unparsable_timestamp_is_rejected():
    assert checks.TS_UNPARSABLE in codes(
        checks.check_fields(altered(trans_date_trans_time="21/06/2020"), 1, MAX_AMOUNT))


def test_a_label_outside_zero_and_one_is_rejected():
    assert checks.LABEL_INVALID in codes(
        checks.check_fields(altered(is_fraud="2"), 1, MAX_AMOUNT))


def test_a_non_numeric_cc_num_is_rejected():
    assert checks.CC_NOT_NUMERIC in codes(
        checks.check_fields(altered(cc_num="4306-7238-8424"), 1, MAX_AMOUNT))


def test_a_rejected_cc_num_is_never_logged_in_full():
    r = checks.check_fields(altered(cc_num="4306x238842430893"), 1, MAX_AMOUNT)
    detail = next(x.reason_detail for x in r if x.reason_code == checks.CC_NOT_NUMERIC)
    assert "4306x238842430893" not in detail
    assert detail.endswith("...")


def test_a_coordinate_out_of_range_is_rejected():
    assert checks.COORD_OUT_OF_RANGE in codes(
        checks.check_fields(altered(merch_lat="91.0"), 1, MAX_AMOUNT))


def test_an_unparsable_coordinate_is_rejected():
    assert checks.COORD_UNPARSABLE in codes(
        checks.check_fields(altered(long="east"), 1, MAX_AMOUNT))


def test_every_failure_is_reported_not_only_the_first():
    r = checks.check_fields(altered(amt="-1", is_fraud="7", merchant=""), 1, MAX_AMOUNT)
    assert {checks.AMT_NEGATIVE, checks.LABEL_INVALID,
            checks.MISSING_MANDATORY} <= codes(r)


# --- EC21: between fields of one record -----------------------------------

def test_a_sound_record_yields_no_record_reject():
    assert checks.check_record(GOOD, 1) == []


def test_luhn_accepts_every_length_found_in_the_source():
    """11 to 19 digits, all Luhn-valid: a trait of the synthetic generator."""
    for number in ("2291163933867244", "4306723884243089331"):
        assert checks.luhn_ok(number), number


def test_a_broken_checksum_is_rejected_as_a_record_defect():
    broken = "2291163933867245"            # last digit changed
    assert not checks.luhn_ok(broken)
    r = checks.check_record(altered(cc_num=broken), 1)
    assert checks.CC_LUHN_INVALID in codes(r)
    assert all(x.reason_class == checks.RECORD for x in r)


def test_a_birth_date_after_the_transaction_is_rejected():
    assert checks.DOB_AFTER_TXN in codes(
        checks.check_record(altered(dob="2021-01-01"), 1))


def test_the_time_gap_is_not_rejected_by_default():
    """THE trap: the gap is systematic over this source, so a per-record rule
    would quarantine all 555,719 rows."""
    assert checks.check_record(GOOD, 1) == []
    assert checks.check_record(GOOD, 1, reject_time_gap=False) == []


def test_the_time_gap_is_rejected_only_when_asked():
    r = checks.check_record(GOOD, 1, reject_time_gap=True)
    assert checks.TIME_COLUMNS_DIVERGE in codes(r)


# --- profiling: what decides between the two readings ---------------------

def test_a_uniform_gap_is_called_systematic():
    p = checks.profile_gap([GOOD] * 100, systematic_share=0.99)
    assert p["verdict"] == "systematic"
    assert p["dominant_gap_days"] == 2557
    assert p["dominant_share"] == 1.0


def test_a_scattered_gap_is_called_sporadic():
    records = [GOOD] * 50 + [altered(unix_time=str(1371816893 + 86400 * k))
                             for k in range(1, 51)]
    assert checks.profile_gap(records, systematic_share=0.99)["verdict"] == "sporadic"


def test_matching_columns_are_called_absent():
    """unix_time consistent with trans_date_trans_time: nothing to rule on."""
    aligned = altered(unix_time="1592741693")      # 2020-06-21 12:14:53 UTC
    p = checks.profile_gap([aligned] * 20, systematic_share=0.99)
    assert p["verdict"] == "absent"
    assert p["dominant_gap_days"] == 0


def test_profiling_an_empty_source_does_not_crash():
    assert checks.profile_gap([], systematic_share=0.99)["verdict"] == "absent"


# --- batch level: what one record cannot judge ----------------------------

def test_a_duplicate_transaction_id_is_rejected():
    records = [(1, GOOD), (2, altered())]          # same trans_num
    out = list(checks.validate_batch(records, MAX_AMOUNT))
    assert codes(out[0][2]) == set()
    assert checks.DUPLICATE_TXN_ID in codes(out[1][2])


def test_distinct_transaction_ids_pass():
    records = [(1, GOOD), (2, altered(trans_num="ffffffff"))]
    out = list(checks.validate_batch(records, MAX_AMOUNT))
    assert all(not rejects for _, _, rejects in out)


def test_the_counters_close():
    records = [(1, GOOD), (2, altered(trans_num="b2", amt="-5")),
               (3, altered(trans_num="c3"))]
    out = list(checks.validate_batch(records, MAX_AMOUNT))
    n_read = len(out)
    n_rejected = sum(1 for _, _, rejects in out if rejects)
    n_valid = n_read - n_rejected
    assert n_read == n_valid + n_rejected == 3
    assert n_rejected == 1
