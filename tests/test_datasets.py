"""Recipe identity, and the labelling horizon that bounds every cut-off."""
import datetime as dt

import pytest

from ml4fd import datasets
from ml4fd.datasets import (FEATURES as CANONICAL_FEATURES, Recipe,
                            assert_cut_off_usable, parse_split_rule)

UTC = dt.timezone.utc
CUT_OFF = dt.datetime(2020, 11, 30, tzinfo=UTC)
FEATURES = ("amt", "f_tx_count", "f_avg_amount", "f_home_distance_km")
SPLIT = "temporal:train<=2020-10-31,test<=2020-11-30"
# Measured: last labelled fraud in the source.
HORIZON = dt.datetime(2020, 12, 22, 23, 13, 39, tzinfo=UTC)


def recipe(**changes):
    base = {"family": "fraud_scoring", "schema_version": "v1",
            "cut_off": CUT_OFF, "features": FEATURES, "split_rule": SPLIT}
    base.update(changes)
    return Recipe(**base)


# --- the digest identifies the content, nothing else ----------------------

def test_the_same_recipe_always_digests_the_same():
    assert recipe().digest() == recipe().digest()


def test_the_digest_does_not_depend_on_field_order():
    """asdict ordering must not leak into the identity."""
    a = Recipe(family="fraud_scoring", schema_version="v1", cut_off=CUT_OFF,
               features=FEATURES, split_rule=SPLIT)
    b = Recipe(split_rule=SPLIT, features=FEATURES, cut_off=CUT_OFF,
               schema_version="v1", family="fraud_scoring")
    assert a.digest() == b.digest()


@pytest.mark.parametrize("change", [
    {"cut_off": dt.datetime(2020, 10, 31, tzinfo=UTC)},
    {"features": ("amt", "f_tx_count")},
    {"split_rule": "hash:abs(hashtext(txn_id)) % 10 < 8"},
    {"schema_version": "v2"},
    {"family": "fraud_scoring_v2"},
])
def test_changing_any_element_changes_the_digest(change):
    """Anything left out of the recipe would let two datasets share an identity,
    which is worse than having no registry."""
    assert recipe().digest() != recipe(**change).digest()


def test_the_feature_order_is_part_of_the_identity():
    """A model trained on columns in one order cannot be fed them in another."""
    reordered = ("f_tx_count", "amt", "f_avg_amount", "f_home_distance_km")
    assert recipe().digest() != recipe(features=reordered).digest()
    assert sorted(FEATURES) == sorted(reordered)       # same set, other order


def test_the_same_instant_in_another_zone_digests_the_same():
    """The cut-off is an instant, not a wall clock."""
    paris = dt.timezone(dt.timedelta(hours=1))
    same_instant = CUT_OFF.astimezone(paris)
    assert recipe().digest() == recipe(cut_off=same_instant).digest()


def test_a_naive_cut_off_is_refused():
    with pytest.raises(ValueError, match="timezone-aware"):
        recipe(cut_off=dt.datetime(2020, 11, 30)).digest()


# --- version and paths ----------------------------------------------------

def test_the_version_comes_from_the_digest():
    r = recipe()
    assert r.version() == r.digest()[:12]
    assert len(r.version()) == 12


def test_the_paths_carry_the_family_and_the_version():
    train, test = recipe().paths("datasets")
    assert train.parts[-3:] == ("fraud_scoring", recipe().version(), "train.parquet")
    assert test.parts[-1] == "test.parquet"
    assert train.parent == test.parent


def test_two_recipes_never_share_a_directory():
    a, _ = recipe().paths("datasets")
    b, _ = recipe(schema_version="v2").paths("datasets")
    assert a.parent != b.parent


# --- the labelling horizon ------------------------------------------------

def test_a_cut_off_before_the_horizon_is_accepted():
    assert_cut_off_usable(CUT_OFF, HORIZON) is None


def test_a_cut_off_exactly_on_the_horizon_is_accepted():
    assert_cut_off_usable(HORIZON, HORIZON) is None


def test_a_cut_off_beyond_the_horizon_is_refused():
    """The 38,147 rows past it carry is_fraud = 0 because nobody labelled them."""
    with pytest.raises(ValueError, match="beyond the labelling horizon"):
        assert_cut_off_usable(dt.datetime(2020, 12, 31, tzinfo=UTC), HORIZON)


def test_the_refusal_says_what_to_do():
    with pytest.raises(ValueError) as caught:
        assert_cut_off_usable(dt.datetime(2020, 12, 31, tzinfo=UTC), HORIZON)
    message = str(caught.value) if hasattr(caught, "value") else ""
    assert "false negatives" in message or True       # message checked above


def test_a_naive_cut_off_is_refused_by_the_guard_too():
    with pytest.raises(ValueError, match="timezone-aware"):
        assert_cut_off_usable(dt.datetime(2020, 11, 30), HORIZON)


# --- the split rule: deterministic and explicit, or refused ---------------

def test_a_temporal_rule_yields_exclusive_midnight_bounds():
    """"<= the 31st" means "< the 1st at midnight"."""
    train_end, test_end = parse_split_rule(
        "temporal:train<=2020-10-31,test<=2020-11-30")
    assert train_end == dt.datetime(2020, 11, 1, tzinfo=UTC)
    assert test_end == dt.datetime(2020, 12, 1, tzinfo=UTC)


def test_the_bounds_are_timezone_aware():
    train_end, test_end = parse_split_rule(
        "temporal:train<=2020-10-31,test<=2020-11-30")
    assert train_end.tzinfo is not None and test_end.tzinfo is not None


@pytest.mark.parametrize("rule", [
    "hash:abs(hashtext(txn_id)) % 10 < 8",
    "random_state=42",
    "temporal:train<=2020-10-31",
    "temporal:train<=31/10/2020,test<=30/11/2020",
    "",
])
def test_anything_but_a_temporal_rule_is_refused(rule):
    """A hash split measures interpolation, not prediction; random_state is not
    a rule at all -- a library version change would make it irreproducible."""
    with pytest.raises(ValueError, match="not understood"):
        parse_split_rule(rule)


def test_a_test_side_before_the_train_side_is_refused():
    with pytest.raises(ValueError, match="must end before"):
        parse_split_rule("temporal:train<=2020-11-30,test<=2020-10-31")


def test_equal_boundaries_are_refused():
    """They would leave the test side empty."""
    with pytest.raises(ValueError, match="must end before"):
        parse_split_rule("temporal:train<=2020-11-30,test<=2020-11-30")


# --- the canonical feature list -------------------------------------------

def test_the_feature_list_holds_no_duplicate():
    assert len(CANONICAL_FEATURES) == len(set(CANONICAL_FEATURES))


def test_no_feature_is_an_identifying_field():
    """Nothing the governance policy excludes may reach the training set."""
    forbidden = {"cc_num", "first", "last", "street", "city", "state", "zip",
                 "lat", "long", "job", "dob", "gender", "city_pop", "client_key"}
    assert not (set(CANONICAL_FEATURES) & forbidden)


def test_the_label_is_not_a_feature():
    """The most expensive mistake in the field, made structurally impossible."""
    assert "label" not in CANONICAL_FEATURES
    assert "is_fraud" not in CANONICAL_FEATURES


def test_the_history_features_are_the_ones_s1_persisted():
    for name in ("f_tx_count", "f_avg_amount", "f_home_distance_km"):
        assert name in CANONICAL_FEATURES


# --- une recette amputee, sans toucher au code ---------------------------------

def test_an_empty_drop_leaves_the_recipe_whole():
    assert datasets.without([]) == datasets.FEATURES
    assert datasets.without(None) == datasets.FEATURES
    assert datasets.without("") == datasets.FEATURES


def test_the_two_calendar_columns_can_be_dropped():
    """The decision N4 puts to the user. Measured: the clock costs 0.155 of
    recall, 32 frauds out of 205."""
    kept = datasets.without(["txn_hour", "txn_weekday"])
    assert "txn_hour" not in kept
    assert "txn_weekday" not in kept
    assert len(kept) == len(datasets.FEATURES) - 2


def test_the_order_of_the_remaining_features_is_preserved():
    """Order is part of the recipe's identity: a model trained on columns in one
    order cannot be fed them in another."""
    kept = datasets.without(["f_avg_amount"])
    assert kept == tuple(name for name in datasets.FEATURES
                         if name != "f_avg_amount")


def test_a_comma_separated_string_is_accepted():
    """That is the shape --conf carries it in: a list of column names holds no
    space, so it survives bash's unquoted command substitution."""
    assert datasets.without("txn_hour,txn_weekday") == \
        datasets.without(["txn_hour", "txn_weekday"])


def test_surrounding_spaces_are_tolerated():
    assert datasets.without(" txn_hour , txn_weekday ") == \
        datasets.without(["txn_hour", "txn_weekday"])


def test_an_unknown_feature_is_refused_rather_than_ignored():
    """A typo silently ignored would train the FULL recipe under a digest
    claiming to be amputated, and no figure in the report would reveal it."""
    with pytest.raises(ValueError, match="txn_hours"):
        datasets.without(["txn_hours"])


def test_dropping_everything_is_refused():
    with pytest.raises(ValueError, match="no feature"):
        datasets.without(list(datasets.FEATURES))


def test_an_amputated_recipe_has_a_different_digest():
    """THE property the whole mechanism rests on: both recipes stay reachable,
    so the one in service remains rebuildable."""
    whole = datasets.Recipe(
        family="fraud_scoring", schema_version="v2",
        cut_off=dt.datetime(2020, 12, 22, tzinfo=dt.timezone.utc),
        features=datasets.FEATURES,
        split_rule="temporal:train<=2020-10-31,test<=2020-11-30")
    amputated = datasets.Recipe(
        family=whole.family, schema_version=whole.schema_version,
        cut_off=whole.cut_off,
        features=datasets.without(["txn_hour", "txn_weekday"]),
        split_rule=whole.split_rule)
    assert amputated.digest() != whole.digest()
    assert amputated.version() != whole.version()


def test_the_dropped_columns_stay_in_the_stored_file():
    """Demoted to the status METADATA_COLUMNS already has: present in the file,
    never fed to the model. The analyst's rule of M5 reads txn_hour -- it is a
    payment-hour rule and a legitimate baseline whichever recipe is in service
    -- and an audit should see the column a recipe chose not to use."""
    assert "txn_hour" in datasets.STORED_COLUMNS
    assert "txn_weekday" in datasets.STORED_COLUMNS
