"""The scenarios N4 scores, and the one that is easy to get silently wrong.

Block: B3, B4.
Lot: B3-N4, B4-L0.
Competency: B3-C5.
Covers: B3-EI04, B3-EC24.

The arithmetic of the measurement is threshold.point_at, already covered. What
is tested here is the construction of the frames to score -- because a
permutation that does not permute would report that the hour is harmless.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd
import pytest

MODULE = Path(__file__).resolve().parents[1] / "tools" / "measure_hour_effect.py"


def _module():
    spec = importlib.util.spec_from_file_location("measure_hour_effect", MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mhe = _module()

HOURS = [0, 1, 2, 22, 23, 8, 8, 14]
# Distinct on purpose: a constant weekday would make every permutation of it
# look correct, so the test of the paired permutation would prove nothing.
WEEKDAYS = [1, 2, 3, 4, 5, 6, 7, 1]


@pytest.fixture
def frame():
    return pd.DataFrame({"txn_hour": list(HOURS),
                         "txn_weekday": list(WEEKDAYS),
                         "amt": [10.0] * len(HOURS),
                         "label": [0, 0, 1, 1, 0, 0, 0, 1]})


def _named(frame, hours):
    return dict(mhe.scenarios(frame, hours))


def test_the_scenarios_are_named_and_complete(frame):
    got = _named(frame, [8, 22])
    assert sorted(got) == ["as-is", "clock", "hour=08", "hour=22", "shuffled"]


def test_the_permutation_actually_permutes(frame):
    """THE test of this file.

    Assigning the sampled Series back instead of its values lets pandas realign
    on the index, which undoes the permutation exactly -- verified on these
    eight rows. The measurement would then show shuffled == as-is and the
    feature would look harmless.
    """
    shuffled = _named(frame, [])["shuffled"]["txn_hour"].tolist()
    assert shuffled != HOURS


def test_the_permutation_keeps_the_marginal_distribution(frame):
    """Shuffling must destroy the association with the label and nothing else:
    a different set of hours would measure a different question."""
    shuffled = _named(frame, [])["shuffled"]["txn_hour"].tolist()
    assert sorted(shuffled) == sorted(HOURS)


def test_the_permutation_replays(frame):
    """Seeded, so two runs of the measurement are comparable."""
    first = _named(frame, [])["shuffled"]["txn_hour"].tolist()
    second = _named(frame.copy(), [])["shuffled"]["txn_hour"].tolist()
    assert first == second


def test_the_clock_scenario_breaks_both_calendar_columns(frame):
    """txn_weekday comes from our clock too, and it is constant for a whole day
    of batches. Permuting the hour alone understates what the stream does, so
    "shuffled" is an upper bound and "clock" is the bar."""
    clock = _named(frame, [])["clock"]
    assert clock["txn_hour"].tolist() != HOURS
    assert sorted(clock["txn_hour"].tolist()) == sorted(HOURS)
    assert sorted(clock["txn_weekday"].tolist()) == sorted(WEEKDAYS)


def test_the_clock_keeps_the_hour_and_the_weekday_together(frame):
    """ONE permutation for both columns, not two seeds. The clock hands out a
    CONSISTENT (hour, weekday) pair -- it is the same instant -- so what must be
    destroyed is the link to the ROW, never the link between the two. Two
    independent permutations would measure a stream nobody has."""
    clock = _named(frame, [])["clock"]
    original = list(zip(HOURS, WEEKDAYS))
    got = list(zip(clock["txn_hour"].tolist(), clock["txn_weekday"].tolist()))
    assert sorted(got) == sorted(original)


def test_the_hour_only_scenario_leaves_the_weekday_alone(frame):
    """It is kept so the loss can be decomposed: how much is the hour, how much
    is the pair."""
    shuffled = _named(frame, [])["shuffled"]
    assert shuffled["txn_weekday"].tolist() == WEEKDAYS


def test_a_forced_hour_is_forced_everywhere(frame):
    forced = _named(frame, [8, 22])["hour=22"]
    assert set(forced["txn_hour"]) == {22}


def test_the_other_columns_are_untouched(frame):
    """Only the hour may move: a scenario that also changed the amount would
    measure two things at once."""
    for name, got in mhe.scenarios(frame, [8]):
        assert got["amt"].tolist() == frame["amt"].tolist(), name
        assert got["label"].tolist() == frame["label"].tolist(), name
        if name != "clock":        # clock moves the weekday, by design
            assert got["txn_weekday"].tolist() == WEEKDAYS, name


def test_the_original_frame_survives_every_scenario(frame):
    """The frame is scored 26 times; a scenario writing through a view would
    corrupt every scenario after it, in order, without raising."""
    before = frame["txn_hour"].tolist()
    for _ in mhe.scenarios(frame, list(range(24))):
        pass
    assert frame["txn_hour"].tolist() == before


def test_as_is_is_the_reference_and_not_a_copy_of_a_scenario(frame):
    got = _named(frame, [8])
    assert got["as-is"]["txn_hour"].tolist() == HOURS


def test_latest_run_takes_the_most_recent(tmp_path):
    import os
    import time
    for name, age in (("old", 100), ("new", 0)):
        folder = tmp_path / name
        folder.mkdir()
        manifest = folder / "run.json"
        manifest.write_text("{}", encoding="utf-8")
        stamp = time.time() - age
        os.utime(manifest, (stamp, stamp))
    assert mhe.latest_run(tmp_path).parent.name == "new"


def test_latest_run_names_the_remedy_when_nothing_is_trained(tmp_path):
    with pytest.raises(FileNotFoundError, match="fd-manager.ps1 train"):
        mhe.latest_run(tmp_path)


# --- une permutation est un tirage, pas une barre ------------------------------

def _all(frame, hours, repeats):
    out = {}
    for name, got in mhe.scenarios(frame, hours, repeats):
        out.setdefault(name, []).append(got)
    return out


def test_the_permuted_readings_are_repeated(frame):
    """A single permutation is one draw. Changing the generator between two runs
    of this tool -- same seed -- moved the permuted recall by 0.015 and the
    permuted precision by 0.070, about two sigma on 205 positives. The decision
    turns on gaps of that size, so the bar is a mean with a spread."""
    got = _all(frame, [], 4)
    assert len(got["shuffled"]) == 4
    assert len(got["clock"]) == 4
    assert len(got["as-is"]) == 1          # deterministic, drawn once


def test_the_forced_hours_are_not_repeated(frame):
    """Forcing an hour is deterministic: repeating it would score the same frame
    four times and report a spread of zero as if it meant something."""
    got = _all(frame, [8, 22], 4)
    assert len(got["hour=08"]) == 1
    assert len(got["hour=22"]) == 1


def test_no_draw_repeats_another(frame):
    orders = [tuple(got["txn_hour"]) for got in _all(frame, [], 5)["clock"]]
    assert len(set(orders)) == len(orders)


def test_the_whole_set_of_draws_replays(frame):
    """Seeded from the project seed plus the draw number, so two runs of the
    measurement are comparable."""
    first = [tuple(got["txn_hour"]) for got in _all(frame, [], 3)["clock"]]
    second = [tuple(got["txn_hour"])
              for got in _all(frame.copy(), [], 3)["clock"]]
    assert first == second


def test_a_repeat_count_below_one_still_draws_once(frame):
    """A measurement that produced no permuted reading at all would leave the
    verdict with no bar, and render would fail on a missing key."""
    for repeats in (0, -3):
        assert len(_all(frame, [], repeats)["clock"]) == 1


def test_the_report_separates_the_spread_of_a_draw_from_the_error_on_the_mean():
    """Two different quantities, and conflating them cost a wrong conclusion.

    The range of 20 draws says where ONE draw lands (about +/-0.04 here); the
    MEAN of those draws is determined twenty times better, sd/sqrt(n). Quoting
    the range as the resolution of the bar would wave a real difference through
    as "within the noise".
    """
    import types

    from ml4fd import threshold as th

    def point(tp, fp, fn, n=50349):
        return th.Point(threshold=0.93, true_positives=tp, false_positives=fp,
                        false_negatives=fn, true_negatives=n - tp - fp - fn,
                        amount_missed=0.0, cost_false_alerts=fp * 4.0)

    rows = [("as-is", point(161, 108, 44))]
    for tp in (122, 126, 129, 133, 138):        # a deliberate spread
        rows.append(("clock", point(tp, 90, 205 - tp)))
        rows.append(("shuffled", point(tp, 90, 205 - tp)))
    rows.append(("hour=22", point(140, 414, 65)))

    body = mhe.render(rows, {"dataset": {"version": "x"},
                             "threshold": {"value": 0.93, "policy": "capacity"}},
                      0.93, 50349,
                      types.SimpleNamespace(alert_capacity_per_day=60))

    assert "Etendue d'UN tirage" in body
    assert "Incertitude sur LA BARRE" in body
    assert "appariee" in body
