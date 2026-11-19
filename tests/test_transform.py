"""Typing and pseudonymisation: the properties the exploited layer relies on."""
from decimal import Decimal

import pytest

from ml4fd import transform

PEPPER = "a1" * 32                      # 64 hex characters, as fd-manager.ps1 emits
OTHER_PEPPER = "b2" * 32

GOOD = {
    "trans_date_trans_time": "2020-06-21 12:14:53",
    "cc_num": "2291163933867244",
    "merchant": "fraud_Kirlin and Sons",
    "category": "personal_care",
    "amt": "276.31",
    "merch_lat": "33.986391",
    "merch_long": "-81.200714",
    "trans_num": "a1b2c3d4e5f6",
    # Identity, which must not come out the other side:
    "first": "Jeff", "last": "Elliott", "street": "351 Darlene Green",
    "city": "Columbia", "state": "SC", "zip": "29209",
    "lat": "33.9659", "long": "-80.9355", "job": "Mechanical engineer",
    "dob": "1968-03-19", "gender": "M", "is_fraud": "1",
}
IDENTIFYING = ("first", "last", "street", "city", "state", "zip",
               "lat", "long", "job", "dob", "cc_num")


def altered(**changes):
    record = dict(GOOD)
    record.update(changes)
    return record


# --- EC17: identity does not reach the exploited layer --------------------

def test_no_identifying_field_comes_out_of_typing():
    out = transform.to_transaction(GOOD, PEPPER)
    for field in IDENTIFYING:
        assert field not in out, field


def test_gender_does_not_come_out_either():
    """Not listed in governance.yml, but personal data all the same."""
    assert "gender" not in transform.to_transaction(GOOD, PEPPER)


def test_the_output_holds_exactly_the_schema_columns():
    out = transform.to_transaction(GOOD, PEPPER)
    assert set(out) == set(transform.OUTPUT_COLUMNS)


def test_the_cardholder_number_appears_nowhere_in_the_output():
    out = transform.to_transaction(GOOD, PEPPER)
    rendered = repr(out)
    assert GOOD["cc_num"] not in rendered


# --- the pseudonymisation itself ------------------------------------------

def test_the_same_number_always_yields_the_same_key():
    """Otherwise a cardholder's history breaks apart and the features are void."""
    a = transform.client_key(GOOD["cc_num"], PEPPER)
    b = transform.client_key(GOOD["cc_num"], PEPPER)
    assert a == b


def test_two_numbers_yield_two_keys():
    a = transform.client_key("2291163933867244", PEPPER)
    b = transform.client_key("2291163933867245", PEPPER)
    assert a != b


def test_changing_the_pepper_changes_every_key():
    assert transform.client_key(GOOD["cc_num"], PEPPER) != \
           transform.client_key(GOOD["cc_num"], OTHER_PEPPER)


def test_the_key_is_128_bits_of_hexadecimal():
    key = transform.client_key(GOOD["cc_num"], PEPPER)
    assert len(key) == 32
    assert all(c in "0123456789abcdef" for c in key)


def test_a_short_pepper_is_refused():
    """924 distinct numbers: a weak pepper is inverted by enumeration."""
    with pytest.raises(ValueError, match="pepper too short"):
        transform.client_key(GOOD["cc_num"], "tooshort")


def test_an_empty_number_is_refused():
    with pytest.raises(ValueError, match="empty cc_num"):
        transform.client_key("", PEPPER)


def test_a_19_digit_number_is_pseudonymised_whole():
    """Read as a float it would have been rounded well before this point."""
    long_number = "4306723884243089331"
    a = transform.client_key(long_number, PEPPER)
    b = transform.client_key(long_number[:-1] + "0", PEPPER)
    assert a != b


# --- typing ---------------------------------------------------------------

def test_the_amount_is_a_decimal_with_two_places():
    out = transform.to_transaction(GOOD, PEPPER)
    assert isinstance(out["amt"], Decimal)
    assert out["amt"] == Decimal("276.31")
    assert out["amt"].as_tuple().exponent == -2


def test_a_one_decimal_amount_is_widened_not_rounded():
    """55,452 amounts in the source carry a single decimal."""
    out = transform.to_transaction(altered(amt="276.3"), PEPPER)
    assert out["amt"] == Decimal("276.30")
    assert out["amt"].as_tuple().exponent == -2


def test_the_amount_is_never_a_float():
    """A float total would not reconcile to the cent between stores."""
    out = transform.to_transaction(GOOD, PEPPER)
    assert not isinstance(out["amt"], float)


def test_the_timestamp_is_timezone_aware():
    out = transform.to_transaction(GOOD, PEPPER)
    assert out["txn_ts"].tzinfo is not None
    assert out["txn_ts"].utcoffset().total_seconds() == 0


def test_an_unreadable_timestamp_is_refused():
    with pytest.raises(ValueError, match="unreadable timestamp"):
        transform.to_transaction(altered(trans_date_trans_time="21/06/2020"), PEPPER)


def test_the_merchant_coordinates_keep_six_decimals():
    out = transform.to_transaction(GOOD, PEPPER)
    assert out["merch_lat"] == Decimal("33.986391")
    assert out["merch_long"] == Decimal("-81.200714")


def test_a_missing_coordinate_becomes_none_not_zero():
    """Zero is a place off the coast of Africa; absence is not a location."""
    out = transform.to_transaction(altered(merch_lat="", merch_long=""), PEPPER)
    assert out["merch_lat"] is None
    assert out["merch_long"] is None


# --- EC17 by minimisation: the distance, not the address ------------------

def test_a_known_distance_is_computed_correctly():
    """Paris to New York, 5,837 km by the great circle."""
    km = transform.home_distance_km("48.8566", "2.3522", "40.7128", "-74.0060")
    assert Decimal("5830") < km < Decimal("5845")


def test_the_distance_from_a_point_to_itself_is_zero():
    assert transform.home_distance_km("33.9659", "-80.9355",
                                      "33.9659", "-80.9355") == Decimal("0.00")


def test_a_missing_coordinate_gives_no_distance_rather_than_zero():
    """Zero would read as "the merchant is at the cardholder's door"."""
    assert transform.home_distance_km("33.9659", "", "33.98", "-81.20") is None
    assert transform.home_distance_km(None, None, "33.98", "-81.20") is None
    assert transform.home_distance_km("33.9659", "-80.9355", "", "") is None


def test_an_unparsable_coordinate_gives_no_distance():
    assert transform.home_distance_km("north", "-80.9355", "33.98", "-81.20") is None


def test_the_distance_is_symmetric():
    a = transform.home_distance_km("33.9659", "-80.9355", "40.7128", "-74.0060")
    b = transform.home_distance_km("40.7128", "-74.0060", "33.9659", "-80.9355")
    assert a == b


def test_the_distance_is_a_decimal_with_two_places():
    km = transform.home_distance_km("33.9659", "-80.9355", "33.986391", "-81.200714")
    assert isinstance(km, Decimal)
    assert km.as_tuple().exponent == -2


def test_typing_carries_the_distance_but_not_the_home():
    """The whole point: the signal survives, the address does not."""
    out = transform.to_transaction(GOOD, PEPPER)
    assert out["f_home_distance_km"] == Decimal("24.56")
    assert "lat" not in out and "long" not in out


def test_a_record_without_home_coordinates_still_types():
    out = transform.to_transaction(altered(lat="", long=""), PEPPER)
    assert out["f_home_distance_km"] is None
    assert out["txn_id"] == GOOD["trans_num"]


def test_surrounding_spaces_are_stripped():
    out = transform.to_transaction(altered(merchant="  shop  ",
                                           trans_num=" tx1 "), PEPPER)
    assert out["merchant"] == "shop"
    assert out["txn_id"] == "tx1"
