"""Building the training set, and registering what was built.

Covers: EC05, EC23, EI04
Lot: M3
Competency: C1, C2, C5

PostgreSQL to Parquet: the transfer between two stores of distinct natures that
EC05 asks for, and the reason the dataset is an ARTEFACT rather than a transient
dataframe. A training run whose data cannot be found again is not auditable.

The construction is idempotent through the recipe digest: a recipe that has
already produced a dataset finds it instead of building a twin.
"""
from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from ml4fd import datasets, db

if TYPE_CHECKING:
    import psycopg

log = logging.getLogger(__name__)

QUERY = "datasets/training_set.sql"


def fetch_rows(conn: "psycopg.Connection", recipe: datasets.Recipe,
               horizon: dt.datetime):
    """Run the query and return (columns, rows).

    THE HORIZON IS A PARAMETER NOW, because the query bounds the two sources
    separately: the historical corpus by labels.horizon and the stream by the
    cut-off. It used to be carried implicitly, the cut-off being clamped to the
    horizon before it ever got here.
    """
    from ml4fd import stream

    train_end, test_end = datasets.parse_split_rule(recipe.split_rule)
    sql = db.sql_path(QUERY).read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql, {"cut_off": recipe.cut_off,
                          "train_end": train_end,
                          "test_end": test_end,
                          "horizon": horizon,
                          "include_stream": recipe.include_stream,
                          # FROM THE INGESTION'S OWN CONSTANT, so a rename
                          # there cannot leave this query silently selecting
                          # nothing: ml4fd/stream.py writes that value into
                          # fd_raw_events.source.
                          "stream_source": stream.SOURCE})
        columns = [d.name for d in cur.description]
        return columns, cur.fetchall()


def build(conn: "psycopg.Connection", recipe: datasets.Recipe,
          root: str | Path, horizon: dt.datetime) -> dict:
    """Build the two Parquet files and register the dataset.

    Refuses a split whose train or test side holds no positive BEFORE writing
    anything. The CHECK constraints on fd_datasets would refuse it too, but
    after two files had been written -- and a message naming the empty side is
    more use than a constraint violation.

    Covers: EC05, EC23
    Lot: M3
    Competency: C2, C5
    """
    import pandas as pd

    # The horizon is checked by build_or_reuse(), which is the entry point:
    # checking it here against itself would be a tautology.
    columns, rows = fetch_rows(conn, recipe, horizon)
    if not rows:
        raise ValueError(
            "the query returned no row for cut-off %s: either the window holds "
            "no transaction, or no verdict was known at that date."
            % recipe.cut_off.isoformat())

    frame = pd.DataFrame(rows, columns=columns)
    expected = set(datasets.STORED_COLUMNS) | {datasets.SPLIT}
    missing = expected - set(frame.columns)
    if missing:
        raise ValueError("the query does not return %s: the recipe and "
                         "sql/datasets/training_set.sql have diverged"
                         % ", ".join(sorted(missing)))

    train = frame[frame[datasets.SPLIT] == "train"]
    test = frame[frame[datasets.SPLIT] == "test"]
    n_train, n_test = len(train), len(test)
    # THE SHARE, COUNTED RATHER THAN CLAIMED. from_stream is reported by the
    # query and dropped before the Parquet is written, since `keep` is
    # STORED_COLUMNS: the model never sees it, a reader of the manifest always
    # does. A retraining whose fresh share is 0.006 is mechanically fresh and
    # materially identical, and that is worth reading at a glance.
    n_train_stream = int(train["from_stream"].sum()) \
        if "from_stream" in train.columns else 0
    pos_train = int(train[datasets.LABEL].sum())
    pos_test = int(test[datasets.LABEL].sum())

    for side, n, pos in (("train", n_train, pos_train), ("test", n_test, pos_test)):
        if n == 0:
            raise ValueError(
                "the %s side of split rule %r is empty: move the boundary."
                % (side, recipe.split_rule))
        if pos == 0:
            raise ValueError(
                "the %s side holds %d row(s) and NO positive. A window without "
                "fraud teaches the model that this period is never fraudulent. "
                "On this source the labelling stops on 2020-12-22, so a boundary "
                "past it produces exactly this." % (side, n))

    # No txn_id can sit on both sides: the CASE assigns one split per row. Still
    # checked, because the query could be edited and the guarantee would go.
    overlap = set(train["txn_id"]) & set(test["txn_id"])
    if overlap:
        raise AssertionError("%d transaction(s) in both train and test, e.g. %s"
                             % (len(overlap), next(iter(overlap))))

    train_path, test_path = recipe.paths(root)
    train_path.parent.mkdir(parents=True, exist_ok=True)
    keep = list(datasets.STORED_COLUMNS)
    train[keep].to_parquet(train_path, index=False)
    test[keep].to_parquet(test_path, index=False)
    # The DIRECTORY, absolute: a relative path in a log is how the artefacts
    # ended up outside the mounted volume without anybody noticing.
    log.info("written under %s : train=%d rows (%d from the stream, %.3f %%), "
             "test=%d rows", train_path.parent.resolve(), n_train,
             n_train_stream, 100.0 * n_train_stream / n_train if n_train else 0.0,
             n_test)

    registered = datasets.register(conn, recipe, root,
                                   n_train, pos_train, n_test, pos_test)
    registered.update({"n_train": n_train, "n_test": n_test,
                       "n_train_pos": pos_train, "n_test_pos": pos_test,
                       "n_train_stream": n_train_stream})
    return registered


def build_or_reuse(conn: "psycopg.Connection", recipe: datasets.Recipe,
                   root: str | Path, label_horizon: dt.datetime) -> dict:
    """Find the dataset this recipe already produced, or build it.

    This is what makes the construction idempotent (EC23) and what gives
    versioning a purpose: one versions in order to find again.

    Covers: EC23
    Lot: M2
    Competency: C5
    """
    datasets.assert_cut_off_usable(recipe.cut_off, label_horizon,
                                   recipe.include_stream)

    found = datasets.find(conn, recipe)
    if found is not None:
        train_path, test_path = recipe.paths(root)
        if train_path.is_file() and test_path.is_file():
            log.info("recipe %s reused: no file rewritten",
                     recipe.digest()[:12])
            found["reused"] = True
            # NOT RECOUNTED, AND SAID SO. The stream share was counted when
            # this dataset was built and it is in that build's log; counting it
            # again here would mean running the query a reuse exists to avoid.
            # None reads as "not counted now", which is true; a 0 would read as
            # "no fresh row", which would not.
            found["n_train_stream"] = None
            return found
        log.warning("recipe %s is registered but its Parquet files are missing: "
                    "rebuilding", recipe.digest()[:12])

    built = build(conn, recipe, root, label_horizon)
    built["reused"] = False
    return built
