"""The one permutation that simulates the stream's clock.

Block: B3, B4.
Lot: B3-N4, B4-L0.
Competency: B3-C5.
Covers: B3-EI04, B3-EC24.

It lives in one module because two tools need it -- the N4 measurement and the
re-choice of the threshold -- and two copies of a permutation are two chances to
permute differently and then compare the results as if they were comparable.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

from ml4fd import clock

HOURS = [0, 1, 2, 22, 23, 8, 8, 14]
WEEKDAYS = [1, 2, 3, 4, 5, 6, 7, 1]


@pytest.fixture
def frame():
    return pd.DataFrame({"txn_hour": list(HOURS),
                         "txn_weekday": list(WEEKDAYS),
                         "amt": [10.0] * len(HOURS)})


def test_the_permutation_actually_permutes(frame):
    """Assigning a sampled Series back lets pandas realign on the index, which
    undoes the permutation exactly -- verified on these eight rows. The
    measurement would then report that the clock costs nothing."""
    assert clock.permute(frame, 0)["txn_hour"].tolist() != HOURS


def test_the_hour_and_the_weekday_move_together(frame):
    """ONE permutation for both. The clock hands out a CONSISTENT (hour,
    weekday) pair -- it is the same instant -- so what must be destroyed is the
    link to the ROW, never the link between the two. Two independent
    permutations would measure a stream nobody has."""
    got = clock.permute(frame, 0)
    pairs = list(zip(got["txn_hour"].tolist(), got["txn_weekday"].tolist()))
    assert sorted(pairs) == sorted(zip(HOURS, WEEKDAYS))


def test_the_marginal_distributions_are_kept(frame):
    got = clock.permute(frame, 0)
    assert sorted(got["txn_hour"].tolist()) == sorted(HOURS)
    assert sorted(got["txn_weekday"].tolist()) == sorted(WEEKDAYS)


def test_the_other_columns_are_untouched(frame):
    assert clock.permute(frame, 0)["amt"].tolist() == frame["amt"].tolist()


def test_the_original_frame_survives(frame):
    """Scored twenty-six times in the measurement; writing through a view would
    corrupt every scenario after the first, in order, without raising."""
    for draw in range(20):
        clock.permute(frame, draw)
    assert frame["txn_hour"].tolist() == HOURS


def test_each_draw_differs_and_the_set_replays(frame):
    first = [tuple(clock.permute(frame, d)["txn_hour"]) for d in range(6)]
    second = [tuple(clock.permute(frame, d)["txn_hour"]) for d in range(6)]
    assert first == second                    # seeded, so comparable
    assert len(set(first)) == len(first)      # and no draw repeats another


def test_a_frame_without_the_columns_is_returned_unharmed():
    """An amputated recipe carries neither column: there is nothing to permute,
    and that must be a no-op rather than a KeyError."""
    frame = pd.DataFrame({"amt": [1.0, 2.0], "category": ["a", "b"]})
    got = clock.permute(frame, 0)
    assert got["amt"].tolist() == [1.0, 2.0]


def test_present_reports_which_columns_a_recipe_uses():
    assert clock.present(("amt", "txn_hour", "txn_weekday")) == \
        ("txn_hour", "txn_weekday")
    assert clock.present(("amt", "txn_hour")) == ("txn_hour",)
    assert clock.present(("amt", "category")) == ()


def test_both_tools_take_the_same_permutation():
    """THE reason this module exists. If the measurement and the re-choice of
    the threshold permuted differently, the bar and the threshold would be
    measured on two different simulated streams."""
    measure = Path(__file__).resolve().parents[1] / "tools"
    for name in ("measure_hour_effect", "rechoose_threshold"):
        body = (measure / ("%s.py" % name)).read_text(encoding="utf-8")
        assert "clock" in body, name
        # No local copy of the permutation left behind in either tool.
        assert "default_rng(SHUFFLE_SEED" not in body, name
        assert "CLOCK_COLUMNS" not in body, name

    spec = importlib.util.spec_from_file_location(
        "mhe", measure / "measure_hour_effect.py")
    mhe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mhe)
    frame = pd.DataFrame({"txn_hour": list(HOURS),
                          "txn_weekday": list(WEEKDAYS),
                          "amt": [10.0] * len(HOURS),
                          "label": [0] * len(HOURS)})
    from_tool = [got for name, got in mhe.scenarios(frame, [], 3)
                 if name == "clock"]
    direct = [clock.permute(frame, draw) for draw in range(3)]
    for got, want in zip(from_tool, direct):
        assert got["txn_hour"].tolist() == want["txn_hour"].tolist()
        assert got["txn_weekday"].tolist() == want["txn_weekday"].tolist()
