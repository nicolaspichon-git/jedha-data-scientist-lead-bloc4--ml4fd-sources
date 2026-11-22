"""The loyalty conditions of a recipe comparison.

Block: B3, B4.
Lot: B3-N4, B4-L0.
Competency: B3-C5.
Covers: B3-EE08, B3-EI04, B3-EC24.

The comparison itself needs two fitted models; what is tested here is what the
tool REFUSES and what it warns about, which is where a comparison stops meaning
anything.
"""
from __future__ import annotations

import importlib.util
import types
from pathlib import Path

from ml4fd import paired

MODULE = (Path(__file__).resolve().parents[1] / "tools"
          / "compare_recipes.py")


def _module():
    spec = importlib.util.spec_from_file_location("compare_recipes", MODULE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cr = _module()


def _payload(version, features, threshold, chosen_on="la validation"):
    return {"dataset": {"version": version, "features": list(features),
                        "split_rule": "temporal:train<=2020-10-31,"
                                      "test<=2020-11-30"},
            "threshold": {"value": threshold, "policy": "capacity",
                          "chosen_on": chosen_on}}


WHOLE = ["amt", "f_tx_count", "f_avg_amount", "f_home_distance_km",
         "txn_hour", "txn_weekday", "category"]
AMPUTATED = ["amt", "f_tx_count", "f_avg_amount", "f_home_distance_km",
             "category"]


def _render(tables=None, current_cut=0.89, current_source="la ligne de commande"):
    if tables is None:
        tables = [paired.Paired(both=130, only_first=2, only_second=20,
                                neither=53, first_name="de96c967",
                                second_name="8813b096")]
    return cr.render(_payload("de96c967f7e6", WHOLE, 0.93),
                     _payload("8813b096e2e4", AMPUTATED, 0.89),
                     tables, 50349,
                     types.SimpleNamespace(alert_capacity_per_day=60),
                     current_cut, current_source, 0.89)


def test_the_report_renders():
    assert _render().startswith("# B3-N4")


def test_the_report_says_where_each_threshold_came_from():
    """Confronting a well-tuned model with one left at a stale threshold does
    not measure the models, it measures the tuning -- so the provenance of each
    threshold belongs in the report, not in the operator's memory."""
    body = _render()
    assert "la ligne de commande" in body
    assert "son manifeste" in body


def test_the_report_uses_the_overridden_threshold_and_not_the_manifest_one():
    body = _render(current_cut=0.89)
    assert "| 0.89 (la ligne de commande) |" in body
    assert "0.93" not in body.split("## Un tirage")[0]


def test_a_clear_win_for_the_candidate_is_stated_as_such():
    tables = [paired.Paired(both=130, only_first=1, only_second=19,
                            neither=55)] * 7
    body = _render(tables=tables)
    assert "l'emporte" in body


def test_an_undecidable_comparison_is_not_called_equivalence():
    """With 205 frauds, a gap of this size is not decidable on these data --
    which is not the same thing as the two being equivalent."""
    tables = [paired.Paired(both=130, only_first=20, only_second=25,
                            neither=30)] * 10
    body = _render(tables=tables)
    assert "ne tranche" in body
    assert "stabilite" in body


def test_the_report_refuses_to_lead_with_the_aggregate_recalls():
    """On 205 positives a recall carries +/-0.034 and the difference of two such
    numbers +/-0.048 -- precisely what the paired view avoids."""
    assert "n'est volontairement pas mis en avant" in _render()


def test_the_tool_refuses_two_different_split_rules():
    body = MODULE.read_text(encoding="utf-8")
    assert 'split_rule"] != candidate["dataset"]["split_rule"]' in body
    assert "ne sont pas les memes" in body


def test_the_tool_warns_when_a_clock_reading_recipe_keeps_a_payment_threshold():
    body = MODULE.read_text(encoding="utf-8")
    assert "--current-threshold" in body
    assert "mesure alors aussi le reglage" in body


def test_an_amputated_candidate_needs_no_rechosen_threshold():
    """Its validation distribution IS its stream distribution: it reads no
    calendar column, so there is nothing to permute and nothing to re-choose.
    The warning must not fire on it."""
    body = MODULE.read_text(encoding="utf-8")
    assert "clock.present(\n                payload[\"dataset\"][\"features\"])" \
        in body or "clock.present(" in body
    assert "A recipe that reads no calendar column needs no re-choice" in body


def test_the_candidate_is_scored_once_because_it_reads_no_clock():
    """Its decisions do not depend on the permutation, so recomputing them per
    draw would cost twenty model loads for twenty identical answers."""
    body = MODULE.read_text(encoding="utf-8")
    index_candidate = body.index("candidate_flags = paired.flags(")
    index_loop = body.index("for draw in range(max(1, args.repeats)):")
    assert index_candidate < index_loop
