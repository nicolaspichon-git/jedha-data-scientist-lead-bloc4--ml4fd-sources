"""Q7: forgetting ONE batch by name, and refusing to.

Block: B3, B4.
Lot: B3-Q7, B4-L0.
Competency: B3-C2, B3-C4, B3-C5.
Covers: B3-EC18, B3-EC19, B3-EC23.

WHY THIS EXISTS. The retention purge cuts on an instant. Four interrupted loads
of the historical CSV and the load that finished arrived minutes apart, so no
instant separates them -- 2,222,876 rows of debris that no purge would ever
reach, and that `reset` would only remove along with everything else.

A tool that deletes a batch by name can delete the wrong one, so the tests here
are mostly about the refusals: a successful run, a run still open, rows already
in the exploited layer, a batch with no fingerprint, an identifier that holds
nothing. The deletion itself is the purge's own purge_batch(), already covered
by test_purge_run.py; what is tested here is that forget() calls it in the
purge's order and journals what it did.
"""
from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path

import pytest

os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import db, purge, runs  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
STORES = ROOT / "sql" / "checks" / "batch_stores.sql"
RUNS = ROOT / "sql" / "checks" / "batch_runs.sql"
BATCH = "16a003bd-f119-5898-8b3c-25ed1658942a"
NOW = dt.datetime(2026, 11, 17, 15, 30, tzinfo=dt.timezone.utc)
LAST = dt.datetime(2026, 11, 15, 22, 31, tzinfo=dt.timezone.utc)


def facts(raw=555_719, quarantine=0, txn=0, truth=0, runs=1, success=0,
          running=0, last_at=LAST, sources="fraudTest.csv"):
    """The two queries' answers, as the two roles see them."""
    return {purge.STORES_SQL: [(raw, quarantine, txn, truth, last_at, sources)],
            purge.BATCH_RUNS_SQL: [(runs, success, running)]}


@pytest.fixture
def answers(monkeypatch):
    """db.query_file and db.execute_file, recorded. Each SQL file answered
    separately, since the point of the split is that two roles read two
    different files."""
    state = {"facts": facts(), "written": [], "read": [], "marks": {}}

    def query(conn, rel, **params):
        state["read"].append(rel)
        return state["facts"][rel]

    monkeypatch.setattr(db, "query_file", query)
    # The purge marker, read from fd_pipeline_runs.digests by Q3's own reader.
    monkeypatch.setattr(runs, "purge_state", lambda conn, batch: state["marks"])
    monkeypatch.setattr(db, "execute_file",
                        lambda conn, rel, **p: state["written"].append((rel, p)))
    return state


class Conn:
    def commit(self): pass


# --- the refusals --------------------------------------------------------------

def test_debris_of_a_failed_load_may_go(answers):
    plan = purge.forget_plan(Conn(), BATCH)
    assert plan["forgettable"] and plan["refusals"] == []
    assert plan["raw_rows"] == 555_719


def test_a_successful_run_is_a_caution_and_not_a_refusal(answers):
    """"Success" means every task passed. For a duplicate load that means
    type_and_pseudonymize wrote nothing, load_transactions being idempotent --
    which is exactly what happened to c0dff73e on 2026-11-15. So an informed
    operator may lift it; a guard that cannot be lifted gets bypassed outside
    the tool, where nothing is journalled."""
    answers["facts"] = facts(success=1, runs=3)
    plan = purge.forget_plan(Conn(), BATCH)
    assert not plan["forgettable"]
    assert plan["forgettable_with_force"]
    assert any("REUSSI" in why for why in plan["cautions"])
    assert plan["refusals"] == []


def test_the_data_refusals_are_lifted_by_nothing(answers):
    """Transactions and verdicts point back at the batch, and its raw zone is
    what allows them to be replayed. There is no state of mind in which
    removing it is right."""
    for kind in ("txn", "truth"):
        answers["facts"] = facts(**{kind: 555_719, "success": 1})
        plan = purge.forget_plan(Conn(), BATCH)
        assert not plan["forgettable_with_force"]
        with pytest.raises(purge.ForgetRefused):
            purge.assert_forgettable(plan, force=True)


def test_a_running_run_is_not_liftable_either(answers):
    """A half-batch matches no recorded fingerprint ever again (Q1): no
    confirmation makes that acceptable."""
    answers["facts"] = facts(running=1)
    plan = purge.forget_plan(Conn(), BATCH)
    assert not plan["forgettable_with_force"]
    with pytest.raises(purge.ForgetRefused):
        purge.assert_forgettable(plan, force=True)


def test_a_lifted_caution_lets_the_deletion_through(answers, monkeypatch):
    monkeypatch.setattr(purge, "purge_batch",
                        lambda conn, store, batch_id, expected, now, chunk=None: {
                            "store": store, "batch_id": batch_id,
                            "deleted": expected, "expected": expected,
                            "refused": None})
    answers["facts"] = facts(success=1, runs=3)
    plan = purge.forget_plan(Conn(), BATCH)
    with pytest.raises(purge.ForgetRefused):
        purge.forget(Conn(), plan, NOW, confirm=True)            # no force
    summary = purge.forget(Conn(), plan, NOW, confirm=True, force=True)
    assert summary["deleted"] == 555_719


def test_a_run_that_finished_since_the_plan_stops_the_deletion(answers,
                                                               monkeypatch):
    """fd_purge cannot look at the exploited layer to find out what that run
    did, so the only honest answer is to stop."""
    monkeypatch.setattr(purge, "purge_batch",
                        lambda *a, **k: pytest.fail("deleted on a stale plan"))
    plan = purge.forget_plan(Conn(), BATCH)
    answers["facts"] = facts(success=1)          # one has closed since
    with pytest.raises(purge.ForgetRefused) as refused:
        purge.forget(Conn(), plan, NOW, confirm=True, force=True)
    assert "s'est terminee depuis le plan" in str(refused.value)


def test_a_run_still_open_is_refused_before_anything_is_deleted(answers):
    """Deleting under a task that is still writing would leave half a batch --
    and a half-batch matches no recorded fingerprint ever again (Q1)."""
    answers["facts"] = facts(running=1)
    plan = purge.forget_plan(Conn(), BATCH)
    assert not plan["forgettable"]
    assert any("EN COURS" in why for why in plan["refusals"])


def test_a_batch_that_reached_the_exploited_layer_is_refused(answers):
    """Its raw zone is what allows it to be replayed (EC24)."""
    answers["facts"] = facts(txn=555_719, success=1)
    plan = purge.forget_plan(Conn(), BATCH)
    assert any("couche exploitee" in why for why in plan["refusals"])


def test_a_batch_whose_verdicts_survive_is_refused(answers):
    """fd_ground_truth carries the batch as TEXT in its own `source` column, so
    a batch can own verdicts while owning no transaction at all. Inferred from
    the transactions, this fact would be missed."""
    answers["facts"] = facts(txn=0, truth=555_719)
    plan = purge.forget_plan(Conn(), BATCH)
    assert not plan["forgettable"]
    assert any("verdict" in why for why in plan["refusals"])


def test_a_batch_without_any_run_has_no_fingerprint_and_is_refused(answers):
    """purge_batch() refuses a batch with no recorded digest, and the remedy is
    one command: say it here rather than fail untitled further down."""
    answers["facts"] = facts(runs=0)
    plan = purge.forget_plan(Conn(), BATCH)
    assert any("empreinte" in why for why in plan["refusals"])
    assert any("digests" in why for why in plan["refusals"])


def test_an_identifier_that_holds_nothing_says_so(answers):
    answers["facts"] = facts(raw=0, quarantine=0, runs=0)
    plan = purge.forget_plan(Conn(), BATCH)
    assert any("errone" in why for why in plan["refusals"])


def test_a_batch_already_forgotten_is_told_so_and_not_left_guessing(answers):
    """THE REFUSAL OF 2026-11-17: "deja purge, ou identifiant errone" sent the
    reader to check both. The database knows which: the marker Q3 writes in
    fd_pipeline_runs.digests -> purge says what left and when, and fd_ingest
    may read it (fd_purges it may not)."""
    answers["facts"] = facts(raw=0, quarantine=0)
    answers["marks"] = {"fd_raw_events": {"started_at": "2026-11-17T17:00",
                                          "done_at": "2026-11-17T17:00",
                                          "deleted": 555_719,
                                          "expected": 555_719}}
    plan = purge.forget_plan(Conn(), BATCH)
    why = " ".join(plan["refusals"])
    assert "DEJA ETE OUBLIE" in why
    assert "555719" in why and "2026-11-17T17:00" in why
    assert "errone" not in why, "it no longer offers two causes at once"
    assert plan["purged"] and "fd_raw_events" in plan["purged"]


def test_an_interrupted_purge_is_not_reported_as_a_finished_one(answers):
    """A marker with no done_at means a purge started and did not come back."""
    answers["facts"] = facts(raw=0, quarantine=0)
    answers["marks"] = {"fd_quarantine": {"started_at": "2026-11-17T17:00",
                                          "expected": 7, "done_at": None,
                                          "deleted": None}}
    plan = purge.forget_plan(Conn(), BATCH)
    why = " ".join(plan["refusals"])
    assert "sans se terminer" in why
    assert "DEJA ETE OUBLIE" not in why


def test_looking_writes_nothing(answers):
    """forget_plan beside forget, like plan beside execute."""
    purge.forget_plan(Conn(), BATCH)
    assert answers["written"] == []


def test_the_plan_never_raises_it_reports(answers):
    answers["facts"] = facts(success=1)
    plan = purge.forget_plan(Conn(), BATCH)          # no exception
    with pytest.raises(purge.ForgetRefused):
        purge.assert_forgettable(plan)


# --- the deletion --------------------------------------------------------------

def test_nothing_is_deleted_without_confirmation(answers, monkeypatch):
    called = []
    monkeypatch.setattr(purge, "purge_batch",
                        lambda *a, **k: called.append(a))
    plan = purge.forget_plan(Conn(), BATCH)
    with pytest.raises(purge.PurgeRefused):
        purge.forget(Conn(), plan, NOW)              # confirm left out
    assert called == []


def test_a_refused_plan_deletes_nothing_even_with_confirmation(answers,
                                                               monkeypatch):
    called = []
    monkeypatch.setattr(purge, "purge_batch", lambda *a, **k: called.append(a))
    answers["facts"] = facts(success=1)
    plan = purge.forget_plan(Conn(), BATCH)
    with pytest.raises(purge.ForgetRefused):
        purge.forget(Conn(), plan, NOW, confirm=True)
    assert called == []


def test_the_stores_go_in_the_purges_own_order(answers, monkeypatch):
    """Quarantine before raw: the derived copy's permission rests on the raw
    fingerprint, which cannot be recomputed once raw is gone (Q3)."""
    seen = []

    def fake(conn, store, batch_id, expected, now, chunk=None):
        seen.append(store)
        return {"store": store, "batch_id": batch_id, "deleted": expected,
                "expected": expected, "refused": None}

    monkeypatch.setattr(purge, "purge_batch", fake)
    answers["facts"] = facts(raw=100, quarantine=7)
    plan = purge.forget_plan(Conn(), BATCH)
    purge.forget(Conn(), plan, NOW, confirm=True)
    assert seen == list(purge.PURGE_ORDER) == ["fd_quarantine", "fd_raw_events"]


def test_an_empty_store_is_not_visited(answers, monkeypatch):
    seen = []
    monkeypatch.setattr(purge, "purge_batch",
                        lambda conn, store, *a, **k: (
                            seen.append(store),
                            {"store": store, "deleted": 0, "expected": 0,
                             "refused": None})[1])
    answers["facts"] = facts(raw=100, quarantine=0)
    plan = purge.forget_plan(Conn(), BATCH)
    purge.forget(Conn(), plan, NOW, confirm=True)
    assert seen == ["fd_raw_events"]


def test_a_refusal_stops_the_following_stores(answers, monkeypatch):
    seen = []

    def fake(conn, store, batch_id, expected, now, chunk=None):
        seen.append(store)
        return {"store": store, "batch_id": batch_id, "deleted": 0,
                "expected": expected, "refused": "digest_mismatch",
                "why": "empreinte differente"}

    monkeypatch.setattr(purge, "purge_batch", fake)
    answers["facts"] = facts(raw=100, quarantine=7)
    plan = purge.forget_plan(Conn(), BATCH)
    summary = purge.forget(Conn(), plan, NOW, confirm=True)
    assert seen == ["fd_quarantine"]
    assert summary["refused"] and summary["deleted"] == 0


# --- the journal ---------------------------------------------------------------

def test_the_same_journal_carries_the_forget(answers, monkeypatch):
    """ONE answer to "what left the raw zone, and when". A second table would
    be the same fact in two places, and S5's acceptance would have to ask
    both."""
    monkeypatch.setattr(purge, "purge_batch",
                        lambda conn, store, batch_id, expected, now, chunk=None: {
                            "store": store, "batch_id": batch_id,
                            "deleted": expected, "expected": expected,
                            "refused": None})
    answers["facts"] = facts(raw=100, quarantine=0)
    plan = purge.forget_plan(Conn(), BATCH)
    purge.forget(Conn(), plan, NOW, confirm=True)

    rows = [p for rel, p in answers["written"] if rel == purge.RECORD_SQL]
    assert len(rows) == 1
    row = rows[0]
    assert row["batch_id"] == BATCH
    assert row["n_batches"] == 1 and row["n_deleted"] == 100
    assert row["share"] is None, "a named batch is not a share of a table"


def test_a_forget_records_the_boundary_as_an_override(answers, monkeypatch):
    """It deletes beyond the declared retention, and before_date exists to keep
    such a deletion recognisable for ever. The table's own CHECK demands
    before_date = cutoff."""
    monkeypatch.setattr(purge, "purge_batch",
                        lambda conn, store, batch_id, expected, now, chunk=None: {
                            "store": store, "batch_id": batch_id,
                            "deleted": expected, "expected": expected,
                            "refused": None})
    plan = purge.forget_plan(Conn(), BATCH)
    purge.forget(Conn(), plan, NOW, confirm=True)
    row = [p for rel, p in answers["written"] if rel == purge.RECORD_SQL][0]
    assert row["before_date"] == row["cutoff"] == LAST
    assert row["as_of"] == NOW
    assert row["cutoff"] <= row["as_of"]


def test_a_batch_newer_than_the_decision_still_gets_its_journal_line(
        answers, monkeypatch):
    """fd_purges_cutoff_before_as_of refused the row when the batch's last line
    was newer than `now` -- and the journal is written AFTER the deletion, so
    the lines would have gone with no record of them. A fixed as_of from a DAG
    run or a clock a second behind is enough to reach that."""
    monkeypatch.setattr(purge, "purge_batch",
                        lambda conn, store, batch_id, expected, now, chunk=None: {
                            "store": store, "batch_id": batch_id,
                            "deleted": expected, "expected": expected,
                            "refused": None})
    later = NOW + dt.timedelta(minutes=4)
    answers["facts"] = facts(last_at=later)
    plan = purge.forget_plan(Conn(), BATCH)
    purge.forget(Conn(), plan, NOW, confirm=True)
    row = [p for rel, p in answers["written"] if rel == purge.RECORD_SQL][0]
    assert row["cutoff"] == NOW, "the cutoff was left in the future"
    assert row["before_date"] == row["cutoff"], "the table demands they match"
    assert row["cutoff"] <= row["as_of"]


def test_the_retention_purge_still_names_no_batch():
    """The column is nullable for the purge, which removes whatever the cutoff
    caught; a batch_id there would invent one."""
    source = (ROOT / "ml4fd" / "purge.py").read_text(encoding="utf-8")
    journal = source[source.index("def journal("):source.index("def render_execution")]
    assert "batch_id=None" in journal


# --- the SQL -------------------------------------------------------------------

def _code(path):
    return "\n".join(line.split("--")[0] for line in
                     path.read_text(encoding="utf-8").splitlines())


def test_the_facts_are_counted_table_by_table_without_a_join():
    """batch_id is a key of none of these tables. raw_batches.sql counted after
    a join and reported 1,667,157 lines for 555,719; scalar subqueries make
    that impossible here."""
    code = _code(STORES)
    assert "JOIN" not in code.upper()
    for table in ("fd_raw_events", "fd_quarantine", "fd_transactions",
                  "fd_ground_truth"):
        assert re.search(r"FROM\s+%s\b" % table, code)


def test_the_two_files_follow_the_privilege_boundary():
    """fd_purge holds no grant on fd_transactions -- S5's acceptance reads that
    from the catalogue -- so the facts it may see have to be a file of their
    own, or the role that deletes cannot ask anything at all. That is what the
    InsufficientPrivilege of 2026-11-17 said."""
    runs_code = _code(RUNS)
    assert "fd_pipeline_runs" in runs_code
    for forbidden in ("fd_transactions", "fd_raw_events", "fd_quarantine",
                      "fd_ground_truth"):
        assert forbidden not in runs_code, (
            "%s is unreadable by fd_purge or irrelevant to a run: it does not "
            "belong in the file the deleting role reads" % forbidden)
    assert "fd_pipeline_runs" not in _code(STORES)


def test_the_run_refusals_have_one_definition():
    """Read twice, by two roles, a moment apart. Written twice they would
    drift, and the drift would be a deletion the plan had forbidden."""
    assert purge.run_refusals(1, 0, 0) == ([], [])
    assert purge.run_refusals(3, 1, 0) == ([], purge.run_refusals(3, 1, 0)[1])
    assert len(purge.run_refusals(3, 1, 0)[1]) == 1    # a success: a caution
    assert len(purge.run_refusals(2, 0, 1)[0]) == 1    # still running: refusal
    assert len(purge.run_refusals(0, 0, 0)[0]) == 1    # no run: no fingerprint


def test_the_state_is_read_again_under_the_deleting_role(answers, monkeypatch):
    """A run of this batch may start or close between the plan and the
    deletion: the plan was drawn in another connection, under another role."""
    monkeypatch.setattr(purge, "purge_batch",
                        lambda *a, **k: pytest.fail("deleted despite a change"))
    plan = purge.forget_plan(Conn(), BATCH)
    assert plan["forgettable"]
    answers["facts"] = facts(running=1)               # a run has started since
    with pytest.raises(purge.ForgetRefused) as refused:
        purge.forget(Conn(), plan, NOW, confirm=True)
    assert "l'etat a change" in str(refused.value)
    assert "EN COURS" in str(refused.value)


def test_the_recheck_reads_only_what_the_deleting_role_may_see(answers,
                                                               monkeypatch):
    """It must not reach for the stores file: fd_purge would be refused by
    PostgreSQL, which is the whole reason for the split."""
    monkeypatch.setattr(purge, "purge_batch",
                        lambda conn, store, batch_id, expected, now, chunk=None: {
                            "store": store, "batch_id": batch_id,
                            "deleted": expected, "expected": expected,
                            "refused": None})
    plan = purge.forget_plan(Conn(), BATCH)
    answers["read"].clear()
    purge.forget(Conn(), plan, NOW, confirm=True)
    assert answers["read"] == [purge.BATCH_RUNS_SQL]


def test_the_dry_run_reads_under_the_role_that_cannot_delete():
    text = (ROOT / "tools" / "forget_plan.py").read_text(encoding="utf-8")
    assert 'db.connect("fd_ingest"' in text
    assert 'db.connect("fd_report"' not in text, "fd_report cannot see the raw zone"
    assert "db.connect(purge.ROLE" not in text, "looking must not hold the right to destroy"


def test_the_runner_keeps_the_two_flags_apart():
    """--force says "delete"; --even-if-successful answers another question."""
    text = (ROOT / "tools" / "forget_run.py").read_text(encoding="utf-8")
    assert "--even-if-successful" in text
    assert "force=args.even_if_successful" in text
    assert "confirm=True" in text


def test_the_runner_separates_the_two_roles():
    text = (ROOT / "tools" / "forget_run.py").read_text(encoding="utf-8")
    assert 'db.connect("fd_ingest"' in text, "the plan is drawn by the reader"
    assert "db.connect(purge.ROLE" in text, "the deletion runs under fd_purge"
    assert text.index('db.connect("fd_ingest"') < text.index("db.connect(purge.ROLE")


def test_the_journal_insert_carries_the_batch():
    code = (ROOT / "sql" / "purge" / "record_purge.sql").read_text(encoding="utf-8")
    assert "%(batch_id)s" in code and "batch_id)" in code


def test_the_column_and_its_constraint_are_added_additively():
    """Replayed by `init` on an existing database: no reset, and ADD CONSTRAINT
    has no IF NOT EXISTS."""
    ddl = (ROOT / "sql" / "ddl" / "130_purges.sql").read_text(encoding="utf-8")
    assert "ADD COLUMN IF NOT EXISTS batch_id uuid" in ddl
    assert "fd_purges_forget_names_one_batch" in ddl
    assert "duplicate_object" in ddl


def test_the_dry_run_tool_cannot_delete():
    """Like purge_plan.py beside purge_run.py."""
    text = (ROOT / "tools" / "forget_plan.py").read_text(encoding="utf-8")
    assert "purge.forget(" not in text, "the dry run calls the deletion"
    assert "confirm=" not in text, "the dry run passes the confirmation"
    assert "force=" not in text
    assert "--force" not in text
    assert "fd_report" in text, "looking must not hold the right to destroy"
    assert "purge.ROLE" not in text


def test_the_destructive_tool_demands_the_confirmation():
    text = (ROOT / "tools" / "forget_run.py").read_text(encoding="utf-8")
    assert "--force" in text and "confirm=True" in text
    assert "purge.ROLE" in text


# --- the class of defect, not the instance -------------------------------------

def test_a_reused_batch_id_parameter_always_carries_its_cast():
    """THE DEFECT, 2026-11-17: `operator does not exist: text = uuid`.

    A named parameter reused in one statement is ONE parameter with ONE
    inferred type. PostgreSQL read $1 as uuid from `fd_raw_events.batch_id =
    $1`, and fd_ground_truth's `source = $1` -- a TEXT column -- then had no
    operator. Nothing in the file said which type was meant, so inference
    chose, and it chose for every use at once.

    The rule holds for every SQL file of the project and not only the one that
    broke: where the parameter appears more than once, each use says its type.
    sql/checks/batch_is_latest.sql carried the same latent trap.
    """
    uses = re.compile(r"%\(batch_id\)s(::[a-z]+)?")
    offenders = []
    for path in sorted((ROOT / "sql").rglob("*.sql")):
        found = uses.findall(path.read_text(encoding="utf-8"))
        if len(found) > 1 and not all(found):
            offenders.append("%s (%d use(s), %d cast)"
                             % (path.relative_to(ROOT), len(found),
                                len([f for f in found if f])))
    assert offenders == [], offenders
