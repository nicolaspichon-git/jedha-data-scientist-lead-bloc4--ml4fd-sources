"""The report of the threshold re-choice, rendered without running the sweep.

Block: B3, B4.
Lot: B3-N4, B4-L0.
Competency: B3-C5.
Covers: B3-EE08, B3-EI04, B3-EC09.

The sweep needs sklearn and a probe fit of several minutes. render() needs
neither, and render() is where the tool failed: it raised NameError AFTER the
work, and the whole measurement went with it. So it is exercised here on its
own, which is the test that would have caught it.
"""
from __future__ import annotations

import importlib.util
import types
from pathlib import Path

from ml4fd import paired, threshold as th

MODULE = (Path(__file__).resolve().parents[1] / "tools"
          / "rechoose_threshold.py")


def _module():
    spec = importlib.util.spec_from_file_location("rechoose_threshold", MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


rt = _module()

N_VALIDATION = 68710
N_TEST = 50349


def _point(threshold, tp, fp, fn, n=N_VALIDATION):
    return th.Point(threshold=threshold, true_positives=tp, false_positives=fp,
                    false_negatives=fn, true_negatives=n - tp - fp - fn,
                    amount_missed=0.0, cost_false_alerts=fp * 4.0)


def _choice(threshold, tp, fp, fn):
    point = _point(threshold, tp, fp, fn)
    return th.ThresholdChoice(policy="capacity", chosen=point, by_cost=point,
                              by_capacity=point, n_rows=N_VALIDATION,
                              chosen_on="la validation, pendule rompue",
                              capacity=60)


def _at_proposed(proposed):
    """Every draw read at the SINGLE proposed threshold -- what render takes,
    and not each draw at its own choice, which would describe a moving
    threshold as if it were the one proposed."""
    return [_point(proposed, 190, 210, 86), _point(proposed, 188, 205, 88),
            _point(proposed, 192, 215, 84)]


def _settings():
    return types.SimpleNamespace(threshold_policy="capacity",
                                 alert_capacity_per_day=60,
                                 cost_false_alert=4.0)


def _payload():
    return {"dataset": {"version": "de96c967f7e6"},
            "threshold": {"value": 0.93, "policy": "capacity"}}


def _render(proposed=0.89, tables=None):
    # The measured draws of 16 November 2026.
    tally = {0.88: 5, 0.89: 4, 0.90: 1}
    if tables is None:
        tables = [paired.Paired(both=120, only_first=1, only_second=14,
                                neither=70, first_name="seuil 0.93",
                                second_name="seuil %.2f" % proposed)]
    return rt.render(_payload(), 0.93, proposed, tally,
                     _at_proposed(proposed), tables,
                     N_VALIDATION, N_TEST, "2020-10-01..2020-10-31",
                     _settings())


def test_the_report_renders_at_all():
    """THE test. render() is a module-level function, so a name bound inside
    main() is not in scope for it -- and the failure came only after the probe
    fit and ten sweeps had run."""
    assert _render().startswith("# B3-N4")


def test_the_report_names_both_thresholds():
    body = _render()
    assert "0.93" in body
    assert "0.89" in body


def test_the_report_says_the_figures_are_read_at_the_proposed_threshold():
    """Each draw at its own choice would describe a threshold that moves."""
    assert "au seuil propos" in _render()


def test_a_tie_between_draws_goes_to_the_HIGHER_threshold():
    """threshold.choose already declares it -- fewer alerts for the same money
    is strictly better for the analyst. My first aggregation broke ties by
    FREQUENCY, which on the measured draws gave 0.88 where the convention gives
    0.89: 58.6 alerts a day with a draw at 59.9, one draw from breaking the
    ceiling the policy exists to respect."""
    body = MODULE.read_text(encoding="utf-8")
    assert "key=lambda value: (abs(value - middle), -value)" in body
    assert "-tally[" not in body


def test_the_report_carries_the_paired_table():
    body = _render()
    assert "desaccords" in body.lower() or "Desaccords" in body


def test_the_report_states_the_direction_of_the_change():
    assert "abaisse" in _render(proposed=0.88)
    assert "releve" in _render(proposed=0.97)


def test_a_threshold_that_does_not_move_is_a_result_and_said_so():
    body = _render(proposed=0.93)
    assert "retombe sur le seuil" in body


def test_the_report_refuses_to_write_the_threshold_into_governance():
    """The operating threshold lives in the model version's manifest, which only
    S2 writes. Two sources of truth is one too many."""
    body = _render()
    assert "governance.yml" in body
    assert "deux sources de verite" in body


def test_the_report_reconducts_the_hypothesis_of_m6_explicitly():
    """The threshold is chosen on a probe and applied to the model fitted on
    everything. That is an assumption, not a result."""
    assert "hypothese" in _render().lower()


def test_the_report_demands_a_rechosen_threshold_on_both_sides():
    assert "deux cotes" in _render()


def test_the_figures_are_written_before_the_report():
    """The lesson of ingest_once, applied where I had failed to apply it: a
    defect in the report must never destroy the work."""
    body = MODULE.read_text(encoding="utf-8")
    figures = body.index('figures.write_text')
    rendering = body.index("report = render(")
    assert figures < rendering


def test_the_rendering_failure_is_caught_and_names_the_figures():
    body = MODULE.read_text(encoding="utf-8")
    assert "le RAPPORT n'a pas pu etre produit" in body
    assert "compte rendu, pas du travail" in body
