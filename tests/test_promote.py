"""L3: the gate between a model that is REGISTERED and one that is SERVED.

Covers: EC34, EC35, EC36   Lot: L3   Competency: C5

WHAT THESE TESTS ARE FOR. The refusals are the branches that matter, and a
live run almost never produces them: a retraining on the same data gives a
model close enough to the champion that the gate waves it through, every time.
So the cases worth guarding -- the candidate the champion beats, the one that
floods the morning queue, the comparison that cannot be asked -- would never be
exercised at all if they were not exercised here.

AND THE GATE MUST BE EXERCISABLE WITHOUT MLFLOW. mlflow is not a declared
dependency: the suite and the integration chain install psycopg, pandas,
requests and PyYAML and nothing more. ml4fd/promote.py therefore holds the
decision as a pure function and ml4fd/registry.py reads the alias off the
registered model's own table, so a fake client is enough here. One test below
holds that property in place.
"""
from __future__ import annotations

import ast
import os
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import datasets, config, paired, promote, registry, score  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]

POLICY = promote.Policy(significance=0.05, alert_capacity=60,
                        respects_capacity=True, first_unopposed=True)

CANDIDATE = promote.Side(version="8", digest="de96c967f7e6",
                         recall=0.631, alerts_per_day=41.6)
CHAMPION = promote.Side(version="7", recall=0.605)


def table(only_champion: int, only_candidate: int, both: int = 120,
          neither: int = 60, nested: bool = False) -> paired.Paired:
    """The champion is the FIRST side, the candidate the SECOND -- the order
    ml4fd/promote.decide documents, so difference > 0 favours the candidate."""
    return paired.Paired(both=both, only_first=only_champion,
                         only_second=only_candidate, neither=neither,
                         first_name="champion", second_name="candidat",
                         nested=nested)


# --- the alert ceiling --------------------------------------------------------

def test_a_candidate_over_the_alert_ceiling_is_refused():
    """Alerts nobody reads are not detections."""
    flooding = promote.Side(version="8", recall=0.9, alerts_per_day=140.0)
    decision = promote.decide(flooding, CHAMPION, table(3, 14), POLICY)
    assert not decision.promoted
    assert "capacite declaree de 60" in decision.reason


def test_the_ceiling_is_checked_before_the_comparison():
    """A candidate that WINS the paired test and still floods the queue is
    refused. The order of the checks is a decision, not an accident: the budget
    depends on no comparison, so it is asked first."""
    flooding = promote.Side(version="8", recall=0.95, alerts_per_day=200.0)
    winning = table(only_champion=1, only_candidate=30)
    assert winning.p_value <= POLICY.significance      # it does win
    assert not promote.decide(flooding, CHAMPION, winning, POLICY).promoted


def test_the_ceiling_can_be_waived_by_the_policy():
    lenient = promote.Policy(significance=0.05, alert_capacity=60,
                             respects_capacity=False, first_unopposed=True)
    flooding = promote.Side(version="8", recall=0.9, alerts_per_day=140.0)
    assert promote.decide(flooding, CHAMPION, table(8, 9), lenient).promoted


def test_an_unmeasured_volume_does_not_refuse_by_itself():
    """A missing figure is not a breach. fd_datasets may hold no volume for a
    recipe measured before R10, and refusing on that would refuse history."""
    silent = promote.Side(version="8", recall=0.6, alerts_per_day=None)
    assert promote.decide(silent, CHAMPION, table(8, 9), POLICY).promoted


# --- with no champion ---------------------------------------------------------

def test_the_first_model_is_promoted_unopposed():
    decision = promote.decide(CANDIDATE, None, None, POLICY)
    assert decision.promoted
    assert "sans opposition" in decision.reason


def test_the_first_model_is_refused_when_the_policy_forbids_it():
    strict = promote.Policy(significance=0.05, alert_capacity=60,
                            respects_capacity=True, first_unopposed=False)
    decision = promote.decide(CANDIDATE, None, None, strict)
    assert not decision.promoted


# --- the paired comparison ----------------------------------------------------

def test_a_champion_without_a_comparison_is_never_overtaken():
    """Promoting without comparing is the defect L3 removes, so the absence of
    a table is a refusal and not a shortcut."""
    decision = promote.decide(CANDIDATE, CHAMPION, None, POLICY)
    assert not decision.promoted
    assert "aucune comparaison appariee" in decision.reason


def test_a_nested_comparison_cannot_decide():
    """paired.Paired returns no p-value when the comparison is nested, and two
    different models are not nested: arriving here means the caller declared a
    nesting that does not hold."""
    nested = table(3, 14, nested=True)
    assert nested.p_value is None
    decision = promote.decide(CANDIDATE, CHAMPION, nested, POLICY)
    assert not decision.promoted
    assert "emboitee" in decision.reason


def test_a_candidate_the_champion_beats_significantly_is_refused():
    beaten = table(only_champion=14, only_candidate=3)
    assert beaten.p_value <= POLICY.significance
    decision = promote.decide(CANDIDATE, CHAMPION, beaten, POLICY)
    assert not decision.promoted
    assert "14 fraudes" in decision.reason and "contre 3" in decision.reason


def test_a_candidate_that_wins_significantly_is_promoted():
    winning = table(only_champion=3, only_candidate=14)
    assert winning.p_value <= POLICY.significance
    decision = promote.decide(CANDIDATE, CHAMPION, winning, POLICY)
    assert decision.promoted
    assert "14 fraudes" in decision.reason


def test_a_tie_promotes_the_newer_model():
    """THE ASYMMETRY, held in place. The gate refuses what the champion beats;
    it does not demand that the candidate win. A retraining on fresher data
    that merely holds the line is worth serving, and requiring significance in
    its favour would freeze the model for good."""
    even = table(only_champion=8, only_candidate=9)
    assert even.p_value > POLICY.significance
    decision = promote.decide(CANDIDATE, CHAMPION, even, POLICY)
    assert decision.promoted
    assert "aucune difference significative" in decision.reason


def test_a_near_miss_in_the_champions_favour_still_promotes():
    """Just above the level is not below it, and the boundary is where a >=
    written for a > would silently reverse every marginal decision.

    16 against 6 gives p = 0.0525: the champion is ahead by ten frauds and the
    test still does not reach 0.05, so the candidate is served. 13 against 4
    reads as a near miss to the eye and is NOT one -- p = 0.0490 -- which is
    why this fixture is a measured number and not an intuition."""
    marginal = table(only_champion=16, only_candidate=6)
    assert marginal.p_value > POLICY.significance
    assert promote.decide(CANDIDATE, CHAMPION, marginal, POLICY).promoted


# --- what the journal receives ------------------------------------------------

def test_the_journal_row_carries_the_policy_in_force():
    """governance.yml can be changed; a decision taken under the old numbers
    must stay re-derivable, so they are written into the row."""
    row = promote.decide(CANDIDATE, CHAMPION, table(8, 9), POLICY).row("run-1")
    assert row["alert_capacity"] == 60
    assert row["significance"] == 0.05
    assert row["run_id"] == "run-1"
    assert row["decision"] == "promoted"


def test_the_row_of_an_unopposed_decision_carries_no_comparison():
    """Mirrors the CHECK of sql/ddl/160_promotions.sql,
    fd_promotions_unopposed_carries_no_comparison: the two must agree, or one
    of them is wrong and the insert is what finds out."""
    row = promote.decide(CANDIDATE, None, None, POLICY).row()
    assert row["champion_before"] is None
    assert row["p_value"] is None
    assert row["only_candidate"] is None
    assert row["only_champion"] is None
    assert row["champion_recall"] is None


def test_the_comparison_reaches_the_row_whole():
    """The other CHECK of the same file, fd_promotions_comparison_is_whole:
    the two cells and the p-value travel together or not at all."""
    row = promote.decide(CANDIDATE, CHAMPION, table(14, 3), POLICY).row()
    assert row["only_champion"] == 14
    assert row["only_candidate"] == 3
    assert row["p_value"] is not None
    assert row["champion_before"] == "7"


def test_the_candidate_is_never_recorded_as_its_own_champion():
    """The last CHECK of that file. A gate asked to compare a version with
    itself is a caller's mistake, and the row would not survive the insert."""
    row = promote.decide(CANDIDATE, CHAMPION, table(8, 9), POLICY).row()
    assert row["champion_before"] != row["candidate_version"]


# --- the policy comes from governance -----------------------------------------

def test_the_policy_is_the_one_governance_declares():
    policy = promote.Policy.from_settings(config.load())
    assert policy.significance == 0.05
    assert policy.alert_capacity == 60
    assert policy.respects_capacity is True
    assert policy.first_unopposed is True


def test_the_alias_name_is_declared_and_not_hard_coded():
    assert config.load().champion_alias == registry.CHAMPION_ALIAS


# --- the gate stays installable ------------------------------------------------

@pytest.mark.parametrize("module", ["ml4fd/promote.py", "ml4fd/registry.py"])
def test_the_gate_imports_no_mlflow_at_load_time(module):
    """mlflow is not a declared dependency. A module-level import here would
    make the suite -- and the integration chain -- unable to even collect."""
    tree = ast.parse((ROOT / module).read_text(encoding="utf-8"))
    for node in tree.body:
        names = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        assert not any(n.startswith("mlflow") or n.startswith("sklearn")
                       for n in names), "%s imports %s at load time" % (
                           module, names)


def test_the_entry_point_only_reads_columns_the_splits_carry():
    """THE TEST THAT WOULD HAVE CAUGHT is_fraud.

    Every other test here hands decide() a table that is already built, so not
    one of them touches a column name. tools/promote.py is where the names are
    read, and it read frame["is_fraud"] -- which is what the payment API and
    fd_transactions call the label, while the Parquet splits call it label
    (datasets.LABEL). A KeyError, on the first real run, after a retraining.

    So the names are checked against the only list that says what a split
    holds. build.py writes exactly STORED_COLUMNS, deliberately including the
    features a recipe dropped, which is why scoring the champion on the
    candidate's split works at all.
    """
    source = (ROOT / "tools" / "promote.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    read = set()
    for node in ast.walk(tree):
        if (isinstance(node, ast.Subscript)
                and isinstance(node.value, ast.Name)
                and node.value.id == "frame"
                and isinstance(node.slice, ast.Constant)
                and isinstance(node.slice.value, str)):
            read.add(node.slice.value)
    assert read, "no column read found: the check has stopped checking"
    unknown = sorted(read - set(datasets.STORED_COLUMNS))
    assert not unknown, (
        "tools/promote.py reads %s, which no split carries. The columns are %s"
        % (unknown, list(datasets.STORED_COLUMNS)))


# --- resolving the champion ----------------------------------------------------

class _Model:
    def __init__(self, aliases):
        self.aliases = aliases


class _Client:
    """Just enough of MlflowClient for the three calls registry makes."""

    def __init__(self, aliases=None, versions=None):
        self._aliases = aliases or {}
        self._versions = versions or {}
        self.moved = []

    def get_registered_model(self, name):
        return _Model(dict(self._aliases))

    def get_model_version(self, name, number):
        return self._versions[str(number)]

    def set_registered_model_alias(self, name, alias, version):
        self.moved.append((name, alias, version))


def test_no_alias_means_no_champion():
    assert registry.champion(_Client(), "fraud_scoring", "champion") is None


def test_the_alias_is_read_off_the_registered_model():
    client = _Client(aliases={"champion": "7"}, versions={"7": "la version 7"})
    assert registry.champion(client, "fraud_scoring", "champion") == "la version 7"


def test_moving_the_alias_passes_the_version_as_text():
    """MLflow wants the version as a string; an int silently matches nothing."""
    client = _Client()
    registry.set_champion(client, 8, "fraud_scoring", "champion")
    assert client.moved == [("fraud_scoring", "champion", "8")]


def test_scoring_refuses_to_serve_when_nothing_is_promoted():
    """NO FALLBACK TO THE NEWEST VERSION, and this test is what holds that in
    place: a fallback would restore exactly the defect L3 removes."""
    with pytest.raises(LookupError) as raised:
        score.champion_version(_Client(), "fraud_scoring", "champion")
    message = str(raised.value)
    assert "registering is not promoting" in message
    assert "promote" in message


def test_scoring_resolves_the_promoted_version():
    client = _Client(aliases={"champion": "7"}, versions={"7": "la version 7"})
    assert score.champion_version(
        client, "fraud_scoring", "champion") == "la version 7"
