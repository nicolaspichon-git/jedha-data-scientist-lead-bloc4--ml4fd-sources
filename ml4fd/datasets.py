"""Dataset recipes and the registry that makes their construction idempotent.

Covers: EC05, EC23, EI10
Lot: M0
Competency: C1, C5

A dataset's identity is the DIGEST OF ITS RECIPE, not a version number. A version
is a label stuck on afterwards; a digest is what lets the same recipe find the
dataset it already produced instead of building a twin. One versions in order to
find again, not in order to archive.

The recipe holds everything that changes the data: the ordered feature list, the
cut-off, the split rule, the schema version and the family. Anything left out of
it would let two different datasets share an identity, which is worse than having
no registry at all.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import psycopg

log = logging.getLogger(__name__)

DIGEST_CHARS = 64
VERSION_CHARS = 12

# THE ordered feature list. Order is part of the recipe's identity: a model
# trained on columns in one order cannot be fed them in another.
#
# Every one of them is read AS STORED or derived from txn_ts alone. Nothing is
# recomputed from history here -- S1 persisted the history features on purpose
# (F04.1) and L4 proved the two computation modes agree, so training reads the
# same values scoring will.
FEATURES = (
    "amt",                   # numeric(12,2), never a float
    "f_tx_count",            # persisted by S1, strict bound
    "f_avg_amount",          # persisted by S1, NULL on a first transaction
    "f_home_distance_km",    # the minimisation of lat/long (EC17)
    "txn_hour",              # derived from txn_ts, no history
    "txn_weekday",           # derived from txn_ts, no history
    "category",              # categorical, encoded at training time
)

LABEL = "label"
SPLIT = "split"


def without(dropped, features: tuple[str, ...] = FEATURES) -> tuple[str, ...]:
    """The feature list minus the named columns, order preserved.

    WHY A RECIPE NEEDS THIS. N4 measured that txn_hour and txn_weekday mean the
    hour of the PAYMENT at training time and the hour of OUR CALL on the stream,
    where they are independent of the label by construction and cost 0.155 of
    recall. Deciding whether to drop them requires training a model without
    them and comparing -- so the feature list has to be a parameter of a run,
    not a constant of the code. Editing FEATURES would work once and destroy
    the ability to rebuild the recipe in service, which is exactly what the
    digest exists to protect.

    THE DROPPED COLUMNS STAY IN THE PARQUET, and that is deliberate rather than
    an oversight. They are demoted to the status METADATA_COLUMNS already has:
    present in the file, never fed to the model. Two reasons. The analyst's rule
    that M5 compares the model against reads txn_hour -- it is a payment-hour
    rule, and a legitimate baseline whichever recipe is in service -- and an
    audit of a run should be able to see the column a recipe chose not to use.
    What keeps the model from seeing them is this list, not their absence from
    the file.

    An unknown name is refused rather than ignored: a typo would otherwise
    train the full recipe under a digest that claims to be amputated, and no
    figure in the report would reveal it.

    Covers: EC05, EC23, EI04
    Lot: N4
    Competency: C1, C5
    """
    if isinstance(dropped, str):
        dropped = dropped.split(",")
    names = tuple(str(name).strip() for name in (dropped or ())
                  if str(name).strip())
    if not names:
        return tuple(features)

    unknown = [name for name in names if name not in features]
    if unknown:
        raise ValueError(
            "cannot drop %s: not in the recipe, whose features are %s. A name "
            "silently ignored here would train the full recipe under a digest "
            "claiming otherwise."
            % (", ".join(unknown), ", ".join(features)))

    kept = tuple(name for name in features if name not in names)
    if not kept:
        raise ValueError(
            "dropping %s leaves no feature: there would be nothing to train on"
            % ", ".join(names))
    return kept

# Stored in the Parquet but NEVER fed to the model. txn_ts earns its place in
# schema v2: M6 chooses the threshold on a temporal validation slice of the
# training period, and a slice cannot be cut without the time.
#
# They are listed apart from FEATURES precisely so that nothing can drift into
# the model by being present in the file.
METADATA_COLUMNS = ("txn_id", "txn_ts")
STORED_COLUMNS = METADATA_COLUMNS + FEATURES + (LABEL,)

# Deterministic and explicit, never random_state: a library version change would
# otherwise make the split irreproducible and the whole model audit worthless.
TEMPORAL_RULE = re.compile(
    r"^temporal:train<=(\d{4}-\d{2}-\d{2}),test<=(\d{4}-\d{2}-\d{2})$")


@dataclass(frozen=True)
class Recipe:
    """Everything that determines the content of a dataset."""
    family: str
    schema_version: str
    cut_off: dt.datetime
    features: tuple[str, ...]
    split_rule: str

    def canonical(self) -> str:
        """The form that gets hashed.

        Sorted keys and an explicit separator so that the same recipe hashes the
        same way whatever the insertion order. The feature list is NOT sorted:
        its order is part of the recipe, because a model trained on columns in
        one order cannot be fed them in another.
        """
        if self.cut_off.tzinfo is None:
            raise ValueError("cut_off must be timezone-aware")
        payload = {
            "family": self.family,
            "schema_version": self.schema_version,
            "cut_off": self.cut_off.astimezone(dt.timezone.utc).isoformat(),
            "features": list(self.features),
            "split_rule": self.split_rule,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=True)

    def digest(self) -> str:
        return hashlib.sha256(self.canonical().encode("utf-8")).hexdigest()

    def version(self) -> str:
        """A short, self-identifying directory name taken from the digest."""
        return self.digest()[:VERSION_CHARS]

    def paths(self, root: str | Path) -> tuple[Path, Path]:
        base = Path(root) / self.family / self.version()
        return base / "train.parquet", base / "test.parquet"


def parse_split_rule(rule: str) -> tuple[dt.datetime, dt.datetime]:
    """Read a split rule into the two EXCLUSIVE upper bounds it means.

    "temporal:train<=2020-10-31,test<=2020-11-30" means a training set of
    everything before 2020-11-01 and a test set of 2020-11-01 to 2020-12-01,
    both bounds exclusive at midnight UTC.

    The rule string is the recipe's identity, so it is never reconstructed from
    parsed parts: it is parsed strictly, and anything else is refused rather
    than interpreted. A rule nobody can parse would silently become a rule
    nobody applied.

    Covers: EE07
    Lot: M4
    Competency: C5
    """
    match = TEMPORAL_RULE.match(rule or "")
    if not match:
        raise ValueError(
            "split rule %r not understood. The only form supported is "
            "temporal:train<=YYYY-MM-DD,test<=YYYY-MM-DD -- a hash split "
            "measures interpolation, not prediction, and random_state is not a "
            "rule at all." % rule)

    train_day = dt.date.fromisoformat(match.group(1))
    test_day = dt.date.fromisoformat(match.group(2))
    if train_day >= test_day:
        raise ValueError(
            "the training set must end before the test set: train<=%s and "
            "test<=%s leave the test side empty or overlapping"
            % (train_day, test_day))

    # Exclusive bounds: "<= the 31st" is "< the 1st at midnight".
    train_end = dt.datetime.combine(train_day, dt.time.min,
                                    tzinfo=dt.timezone.utc) + dt.timedelta(days=1)
    test_end = dt.datetime.combine(test_day, dt.time.min,
                                   tzinfo=dt.timezone.utc) + dt.timedelta(days=1)
    return train_end, test_end


def assert_cut_off_usable(cut_off: dt.datetime, horizon: dt.datetime) -> None:
    """Refuse a cut-off beyond the labelling horizon.

    Past it, the rows carry is_fraud = 0 because nobody labelled them, not
    because they are not fraud. Training on them teaches the model that an
    unlabelled period is never fraudulent, and evaluating on them reports a
    flattering recall over a window holding nothing to find.

    Covers: EC14
    Lot: M1
    Competency: C5
    """
    if cut_off.tzinfo is None:
        raise ValueError("cut_off must be timezone-aware")
    if cut_off > horizon:
        raise ValueError(
            "cut-off %s is beyond the labelling horizon %s: the rows in between "
            "are false negatives, not negatives. Move the cut-off back, or "
            "extend labels.horizon once the labels actually exist."
            % (cut_off.isoformat(), horizon.isoformat()))


def find(conn: "psycopg.Connection", recipe: Recipe) -> dict | None:
    """The dataset this recipe already produced, if any."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT recipe_digest, family, version, cut_off, features, "
            "       split_rule, train_path, test_path, n_train, n_test, "
            "       n_train_pos, n_test_pos, base_rate_train, base_rate_test "
            "FROM   fd_datasets WHERE recipe_digest = %s", (recipe.digest(),))
        row = cur.fetchone()
    if row is None:
        return None
    keys = ("recipe_digest", "family", "version", "cut_off", "features",
            "split_rule", "train_path", "test_path", "n_train", "n_test",
            "n_train_pos", "n_test_pos", "base_rate_train", "base_rate_test")
    found = dict(zip(keys, row))
    log.info("recipe %s already registered as %s/%s",
             found["recipe_digest"][:12], found["family"], found["version"])
    return found


def register(conn: "psycopg.Connection", recipe: Recipe, root: str | Path,
             n_train: int, n_train_pos: int, n_test: int, n_test_pos: int
             ) -> dict:
    """Record the dataset and its base rates.

    The base rates are stored with it because a precision figure cannot be read
    without them. The CHECK constraints refuse a split whose train or test side
    holds no positive -- the December case, which no amount of row counting would
    have caught.

    Covers: EC23
    Lot: M0
    Competency: C1
    """
    train_path, test_path = recipe.paths(root)
    rate_train = n_train_pos / n_train if n_train else 0.0
    rate_test = n_test_pos / n_test if n_test else 0.0

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO fd_datasets "
            "(recipe_digest, family, version, cut_off, features, split_rule, "
            " schema_version, train_path, test_path, n_train, n_test, "
            " n_train_pos, n_test_pos, base_rate_train, base_rate_test) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (recipe_digest) DO NOTHING",
            (recipe.digest(), recipe.family, recipe.version(), recipe.cut_off,
             list(recipe.features), recipe.split_rule, recipe.schema_version,
             str(train_path), str(test_path), n_train, n_test,
             n_train_pos, n_test_pos, round(rate_train, 6), round(rate_test, 6)))
        inserted = cur.rowcount

    log.info("dataset %s/%s %s: train=%d (%d pos, %.4f %%) test=%d (%d pos, %.4f %%)",
             recipe.family, recipe.version(),
             "registered" if inserted else "already present",
             n_train, n_train_pos, 100 * rate_train,
             n_test, n_test_pos, 100 * rate_test)
    return {"recipe_digest": recipe.digest(), "version": recipe.version(),
            "train_path": str(train_path), "test_path": str(test_path),
            "base_rate_train": rate_train, "base_rate_test": rate_test}
