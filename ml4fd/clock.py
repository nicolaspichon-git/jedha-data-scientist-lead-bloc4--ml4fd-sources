"""Simulating, on historical data, the clock the stream actually hands us.

Block: B3, B4.
Lot: B3-N4, B4-L0.
Competency: B3-C5.
Covers: B3-EI04, B3-EC24.

WHAT THIS IS FOR. The model was trained on the hour of the PAYMENT. On the
stream, current_time is the instant our own call returned, and the simulator
draws a row at random from 2020 and stamps it with that instant -- measured on
the N0 campaign: indices spread over 16,227..553,065 with 29 of 56 gaps
positive, a coin flip. So on the stream the calendar features are independent of
the row BY CONSTRUCTION, while in training they are anything but.

Permuting them across rows reproduces that independence on data whose labels we
have. It is the only way to measure what the model really does on the stream
without waiting for a month of traffic.

ONE PERMUTATION FOR BOTH COLUMNS, not one per column. The clock hands out a
CONSISTENT (hour, weekday) pair -- it is the same instant -- so what has to be
destroyed is the link between that pair and the ROW, never the link between the
hour and the weekday. Two independent permutations would measure a stream
nobody has.

BY INDEX POSITION, not by pandas index label. Assigning a sampled Series back
lets pandas realign on the index, which undoes the permutation exactly:
verified on eight rows, the column comes back in its original order. The
measurement would then report that the clock costs nothing, and that is a wrong
answer rather than an error.

This lives in one module because two tools need it -- the measurement of N4 and
the re-choice of the threshold -- and two copies of a permutation are two
chances to permute differently and compare the results as if they were
comparable.
"""
from __future__ import annotations

SEED = 20261116
COLUMNS = ("txn_hour", "txn_weekday")


def permute(frame, draw: int = 0):
    """A copy of the frame with its calendar columns permuted together.

    draw indexes the permutation: seeded from SEED + draw, so a set of draws
    replays and no draw repeats another. A permutation is ONE draw -- the N4
    measurement saw a single draw move the permuted precision by 0.070, about
    two sigma -- so callers take several.
    """
    import numpy as np

    out = frame.copy()
    positions = np.random.default_rng(SEED + draw).permutation(len(frame))
    for column in COLUMNS:
        if column in frame.columns:
            out[column] = frame[column].to_numpy()[positions]
    return out


def present(features) -> tuple[str, ...]:
    """Which calendar columns a recipe actually uses.

    Empty means the recipe carries none of them, so the stream's distribution
    is the training distribution and there is nothing to permute -- which is
    exactly what an amputated recipe is for.
    """
    return tuple(name for name in COLUMNS if name in tuple(features))
