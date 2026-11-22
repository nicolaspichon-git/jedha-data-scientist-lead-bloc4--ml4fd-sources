"""S5 / Q3: the purge itself. Rows leave the database here.

Block: B3, B4.
Lot: B3-Q3, B4-L0.
Competency: B3-C2, B3-C3.
Covers: B3-EE05, B3-EI11, B3-EC19, B3-EC26.

Everything in Q2 counted. This lot destroys, and these tests hold the five
decisions that separate it from an ordinary DELETE:

  - the set comes from the plan, so what the operator read is what happens;
  - the fingerprint is verified AGAIN, batch by batch, immediately before the
    rows go -- the plan may be minutes old, and a batch that gained a row in
    between must not be deleted on the strength of a stale report;
  - the deletion goes in committed slices, because 555 719 rows in one
    transaction holds a lock for minutes and inflates the log by 200 MB;
  - an interrupted purge is RESUMABLE: the slices commit as they go, so a
    killed process leaves a batch that no longer matches its fingerprint, and
    without the marker written beforehand the honest reading would be "refuse
    for ever" -- the very outcome Q1 exists to prevent;
  - the right to destroy belongs to a role of its own, and that role cannot
    reach the exploited layer at all.
"""
from __future__ import annotations

import datetime as dt
import re
import types
from pathlib import Path

import pytest

import os

# config.load() refuses to run without them. Set here rather than
# inherited from whichever test module happens to import first.
os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import db, purge, runs  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DELETE = ROOT / "sql" / "purge" / "010_raw_events.sql"
ROLE_SQL = ROOT / "sql" / "ddl" / "120_purge_role.sql"
PARIS = dt.timezone(dt.timedelta(hours=1), "CET")
NOW = dt.datetime(2026, 11, 22, 4, 0, tzinfo=PARIS)
A = "aaaaaaaa-0000-0000-0000-00000000000a"
B = "bbbbbbbb-0000-0000-0000-00000000000b"


def _sql(path):
    return "\n".join(line for line in path.read_text(encoding="utf-8").splitlines()
                     if not line.strip().startswith("--"))


class Conn:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


def _store(name, rows, deletable, total=1000):
    """One store as plan() reports it -- including the two fields the journal
    of Q6 reads: the cutoff it applied and the size it applied it to."""
    return {"store": name, "rows": rows, "deletable": deletable,
            "batch_ids": [entry["batch_id"] for entry in deletable],
            "refusals": [], "total_rows": total,
            "cutoff": "2026-08-24T04:00:00+01:00"}


def _report(stores=None, blocked=False):
    stores = stores if stores is not None else [
        _store("fd_raw_events", 10, [{"batch_id": A, "rows": 10}]),
        _store("fd_quarantine", 0, [])]
    return {"as_of": NOW.isoformat(), "before": None, "blocked": blocked,
            "blocking": [{"store": "fd_raw_events", "share": 0.9,
                          "max_share": 0.5, "rows": 900, "total_rows": 1000}]
            if blocked else [],
            "max_share": 0.5, "stores": stores, "rows": 10}


@pytest.fixture
def world(monkeypatch):
    """Every outside call recorded in order, and answered as we choose."""
    trace = []
    state = {"refusal": None, "rows": {A: 10, B: 10},
             "held": {"run_id": "runA", "purge": {}}}

    def recorded_digest(conn, batch_id):
        trace.append(("held", str(batch_id)))
        return state["held"]

    def refusal(conn, batch_id):
        trace.append(("verify", str(batch_id)))
        return state["refusal"]

    def mark_purging(conn, run_id, store, started_at, expected):
        trace.append(("mark_start", store, expected))

    def mark_purged(conn, run_id, store, done_at, deleted, expected,
                    started_at):
        trace.append(("mark_done", store, deleted))

    def execute_file(conn, relative, **params):
        if relative == purge.RECORD_SQL:
            # Q6: the journal goes through the same door as the deletes, and
            # taking it for one made every count here wrong.
            trace.append(("journal", params["store"], params["n_deleted"]))
            return 1
        trace.append(("delete", relative, params["batch_id"], params["chunk"]))
        left = state["rows"].get(params["batch_id"], 0)
        gone = min(left, params["chunk"])
        state["rows"][params["batch_id"]] = left - gone
        return gone

    monkeypatch.setattr(runs, "recorded_digest", recorded_digest)
    monkeypatch.setattr(runs, "refusal", refusal)
    monkeypatch.setattr(runs, "mark_purging", mark_purging)
    monkeypatch.setattr(runs, "mark_purged", mark_purged)
    monkeypatch.setattr(db, "execute_file", execute_file)
    return types.SimpleNamespace(trace=trace, state=state, conn=Conn())


# --- the file that deletes ------------------------------------------------------

def test_the_delete_carries_no_date_of_its_own():
    """ONE DEFINITION OF THE PURGEABLE SET. The cutoff lives in
    candidates_raw.sql; this file receives the batch it selected. A delete with
    a cutoff of its own would make the dry run a report about another set."""
    text = _sql(DELETE)
    assert "%(before)s" not in text
    assert "ingested_at" not in text
    assert "%(batch_id)s" in text and "%(chunk)s" in text


def test_the_delete_is_scoped_to_one_batch_and_one_slice():
    text = _sql(DELETE)
    assert re.search(r"WHERE\s+batch_id = %\(batch_id\)s", text)
    assert re.search(r"LIMIT\s+%\(chunk\)s", text)
    assert text.upper().count("DELETE") == 1


def test_the_slice_is_taken_on_the_primary_key_and_not_on_ctid():
    """ctid moves under VACUUM, which is exactly what runs during a long
    purge; raw_id is stable and indexed."""
    text = _sql(DELETE)
    assert "raw_id" in text and "ctid" not in text


def test_the_delete_touches_only_the_raw_zone():
    text = _sql(DELETE).upper()
    for other in ("FD_TRANSACTIONS", "FD_GROUND_TRUTH", "FD_SCORES",
                  "FD_ALERTS", "FD_QUARANTINE", "FD_PIPELINE_RUNS"):
        assert other not in text


def test_the_partitioning_limitation_is_written_down():
    """A table purged by date belongs partitioned by month, where the purge is
    a DROP of a partition. This one is not, and that is a decision to find in
    the file rather than in someone's memory."""
    assert "partition" in DELETE.read_text(encoding="utf-8").lower()


def test_every_store_that_deletes_is_a_store_the_plan_counts():
    assert set(purge.DELETE_SQL) <= set(purge.CANDIDATE_SQL)
    for relative in purge.DELETE_SQL.values():
        assert (ROOT / "sql" / relative).is_file()


# --- the role that may destroy ---------------------------------------------------

def test_the_purge_runs_under_a_role_of_its_own():
    assert purge.ROLE == "fd_purge"
    assert "CREATE ROLE fd_purge" in ROLE_SQL.read_text(encoding="utf-8")


def test_that_role_cannot_reach_the_exploited_layer():
    """"THE PURGE DID NOT OVERFLOW" IS NOT A CHECK HERE, IT IS A PRIVILEGE THE
    SESSION DOES NOT HOLD. Q6 measures the digest as well, because a property
    worth having is worth measuring -- but the first line of defence is that
    the grant does not exist."""
    granted = [line for line in _sql(ROLE_SQL).splitlines()
               if line.strip().upper().startswith("GRANT")]
    text = " ".join(granted)
    for forbidden in ("fd_transactions", "fd_ground_truth", "fd_scores",
                      "fd_frauds", "fd_reports", "fd_datasets"):
        assert forbidden not in text, "fd_purge was granted %s" % forbidden


def test_that_role_cannot_delete_the_evidence():
    """The fingerprints of Q1 are what remains of a purged batch. The process
    that deletes the data must not be able to delete the record of it."""
    text = _sql(ROLE_SQL)
    assert re.search(r"REVOKE[^;]*DELETE[^;]*ON fd_pipeline_runs\s+FROM fd_purge",
                     text)
    grants = [line for line in text.splitlines()
              if "fd_pipeline_runs" in line and line.strip().startswith("GRANT")]
    assert grants and "DELETE" not in grants[0]


def test_that_role_may_not_rewrite_what_it_does_not_delete():
    text = _sql(ROLE_SQL)
    assert re.search(r"REVOKE[^;]*UPDATE[^;]*ON fd_raw_events\s+FROM fd_purge",
                     text)


def test_the_role_is_replayable_like_the_rest_of_the_ddl():
    assert "IF NOT EXISTS" in _sql(ROLE_SQL)


def test_a_missing_role_is_named_with_its_remedy():
    message = str(db.RoleMissing)
    assert "RoleMissing" in message
    source = (ROOT / "ml4fd" / "db.py").read_text(encoding="utf-8")
    assert "UndefinedObject" in source
    assert "fd-manager.ps1 init" in source


# --- the order of operations ------------------------------------------------------

def test_the_fingerprint_is_verified_immediately_before_the_rows_go(world):
    """THE PLAN MAY BE MINUTES OLD. A batch that gained a row since must not be
    deleted on the strength of a stale report -- and a batch that LOST rows
    since is a batch something else touched."""
    purge.execute(world.conn, _settings(), _report(), NOW)
    verbs = [entry[0] for entry in world.trace]
    assert verbs.index("verify") < verbs.index("delete")
    assert verbs.index("mark_start") < verbs.index("delete")


def test_a_batch_refused_at_that_moment_is_not_touched(world):
    world.state["refusal"] = (runs.DIGEST_MISMATCH, "a bouge depuis le plan")
    summary = purge.execute(world.conn, _settings(), _report(), NOW)
    assert [entry for entry in world.trace if entry[0] == "delete"] == []
    assert summary["rows"] == 0
    assert summary["refused"][0]["refused"] == runs.DIGEST_MISMATCH
    assert "a bouge" in summary["refused"][0]["why"]


def test_a_batch_with_no_fingerprint_at_all_is_not_touched(world,
                                                           monkeypatch):
    monkeypatch.setattr(runs, "recorded_digest", lambda conn, batch: None)
    summary = purge.execute(world.conn, _settings(), _report(), NOW)
    assert summary["rows"] == 0
    assert summary["refused"][0]["refused"] == runs.NO_DIGEST


def test_only_the_batches_the_plan_named_are_deleted(world):
    """B exists and is purgeable, but the plan did not name it."""
    purge.execute(world.conn, _settings(), _report(), NOW)
    deleted = {entry[2] for entry in world.trace if entry[0] == "delete"}
    assert deleted == {A}


# --- the slices --------------------------------------------------------------------

def test_the_deletion_goes_in_slices_and_commits_between_them(world):
    """One DELETE over a historical batch is 555 719 rows and 200 MB of log in
    one transaction, with the lock to match."""
    world.state["rows"][A] = 25
    summary = purge.execute(world.conn, _settings(),
                            _report([_store("fd_raw_events", 25,
                                            [{"batch_id": A, "rows": 25}])]),
                            NOW, chunk=10)
    sizes = [entry[3] for entry in world.trace if entry[0] == "delete"]
    assert sizes == [10, 10, 10, 10]          # 10 + 10 + 5 + 0: the last ends it
    assert summary["rows"] == 25
    assert world.conn.commits >= 4            # one per slice, plus the marks


def test_the_loop_ends_on_the_empty_slice_and_not_on_a_guess(world):
    """A batch whose size is an exact multiple of the chunk must not stop one
    slice early, nor loop for ever."""
    world.state["rows"][A] = 20
    purge.execute(world.conn, _settings(),
                  _report([_store("fd_raw_events", 20,
                                  [{"batch_id": A, "rows": 20}])]),
                  NOW, chunk=10)
    assert len([e for e in world.trace if e[0] == "delete"]) == 3


def test_an_already_empty_batch_deletes_nothing_and_closes_cleanly(world):
    """IDEMPOTENCE. Running the purge again must remove nothing more."""
    world.state["rows"][A] = 0
    summary = purge.execute(world.conn, _settings(), _report(), NOW)
    assert summary["rows"] == 0
    assert summary["refused"] == []
    assert ("mark_done", "fd_raw_events", 0) in world.trace


# --- resumption ---------------------------------------------------------------------

def test_an_interrupted_purge_is_resumed_and_not_refused(world):
    """THE FAILURE THIS ANSWERS. The slices commit as they go, so a container
    killed half-way leaves rows that no longer match the fingerprint. Without
    the marker, the next run could not tell that remainder from tampering --
    and the batch would be unpurgeable for ever."""
    world.state["held"] = {"run_id": "runA", "purge": {
        "fd_raw_events": {"started_at": "2026-11-22T04:00:00+01:00",
                          "expected": 10, "done_at": None, "deleted": None}}}
    summary = purge.execute(world.conn, _settings(), _report(), NOW)
    assert ("verify", A) not in world.trace    # the mark is the authority
    assert summary["rows"] == 10
    assert summary["resumed"] == 1


def test_a_finished_purge_is_verified_again_like_any_other(world):
    """A marker that says done is history, not a licence."""
    world.state["held"] = {"run_id": "runA", "purge": {
        "fd_raw_events": {"started_at": "2026-11-01T04:00:00+01:00",
                          "expected": 10, "done_at": "2026-11-01T04:02:00+01:00",
                          "deleted": 10}}}
    purge.execute(world.conn, _settings(), _report(), NOW)
    assert ("verify", A) in world.trace


def test_the_marker_of_one_store_does_not_erase_the_other():
    """Neither trap: `||` alone would replace the whole purge object and lose
    the other store's mark; jsonb_set with a TWO-element path would write
    nothing at all on a run that has no purge key yet, because create_missing
    only creates the last element -- a silent no-op in the safety mechanism
    itself. Met for real on PostgreSQL 16."""
    source = (ROOT / "ml4fd" / "runs.py").read_text(encoding="utf-8")
    block = source.split("def _set_nested")[1].split("\ndef ")[0]
    statement = block.split('"""')[2]
    assert "jsonb_set" in statement and "jsonb_build_object" in statement
    # ONE element in the path: a top-level key is always creatable. The cast
    # is the third trap's (test_batch_digest.py), so the shape is what counts
    # here, not the exact spelling.
    import re
    assert re.search(r"ARRAY\[%s(::text)?\]", statement)
    assert not re.search(r"ARRAY\[%s(::text)?,\s*%s", statement)


def test_an_interrupted_purge_explains_the_mismatch_it_causes():
    """The dry run of Q2 reports a half-purged batch as a mismatch. The
    message must name the real cause, or the reader goes looking for an
    intruder."""
    source = (ROOT / "ml4fd" / "runs.py").read_text(encoding="utf-8")
    assert "interrompue" in source


# --- the guard --------------------------------------------------------------------

def test_a_blocked_plan_deletes_nothing(world):
    with pytest.raises(purge.PurgeRefused):
        purge.execute(world.conn, _settings(), _report(blocked=True), NOW)
    assert [entry for entry in world.trace if entry[0] == "delete"] == []


def test_the_confirmation_is_what_lets_it_through(world):
    summary = purge.execute(world.conn, _settings(), _report(blocked=True),
                            NOW, confirm=True)
    assert summary["rows"] == 10
    assert summary["confirmed"] is True


# --- a store the dry run counts and this lot does not delete ------------------------

def test_a_store_with_no_delete_is_named_rather_than_silently_skipped(world):
    """Every store the plan counts is deleted since Q4, so this holds the
    BEHAVIOUR rather than today's list: a store added to the dry run without a
    delete file must be named in the summary, or the plan's figures read as a
    promise. Invented here, since the real list no longer has such a store."""
    summary = purge.execute(world.conn, _settings(), _report([
        _store("fd_future_store", 20, [{"batch_id": A, "rows": 20}])]),
        NOW)
    assert summary["rows"] == 0
    ((skipped,)) = summary["skipped"]
    assert skipped["store"] == "fd_future_store" and skipped["rows"] == 20


def test_every_store_the_plan_counts_is_now_deleted():
    """Q4 closed the gap: the quarantine was counted by the dry run of Q2 and
    deleted by nothing."""
    assert set(purge.CANDIDATE_SQL) == set(purge.DELETE_SQL)


# --- what the operator reads ---------------------------------------------------------

def test_the_rendering_says_what_went_and_what_remains(world):
    text = purge.render_execution(
        purge.execute(world.conn, _settings(), _report(), NOW))
    assert "10 ligne(s) supprimee(s)" in text
    assert A in text
    assert "empreintes" in text.lower()      # what remains of a purged batch


def test_the_rendering_names_the_refusals(world):
    world.state["refusal"] = (runs.NO_DIGEST, "pas d'empreinte")
    text = purge.render_execution(
        purge.execute(world.conn, _settings(), _report(), NOW))
    assert "toujours en place" in text
    assert runs.NO_DIGEST in text


# --- the two runners ------------------------------------------------------------------

def test_the_dry_run_still_cannot_delete():
    source = (ROOT / "tools" / "purge_plan.py").read_text(encoding="utf-8")
    assert "purge.execute" not in source
    assert "--force" not in source


def test_the_runner_that_deletes_is_a_file_of_its_own():
    source = (ROOT / "tools" / "purge_run.py").read_text(encoding="utf-8")
    assert "purge.execute" in source
    assert "db.connect(purge.ROLE)" in source
    assert "--force" in source


def test_the_runner_shows_the_plan_it_is_about_to_carry_out():
    """The same object, in the same connection: no window in which the two
    could describe different sets."""
    source = (ROOT / "tools" / "purge_run.py").read_text(encoding="utf-8")
    plan_at = source.index("purge.plan(")
    render_at = source.index("purge.render(report)")
    execute_at = source.index("purge.execute(")
    assert plan_at < render_at < execute_at


def test_the_runner_separates_a_refusal_from_a_failure():
    """Exit 2 for "the guard stopped it" and 1 for "it could not run": an
    operator -- and a DAG -- must be able to tell them apart."""
    source = (ROOT / "tools" / "purge_run.py").read_text(encoding="utf-8")
    assert "return 2" in source.split("PurgeRefused")[1]
    assert "return 1" in source.split("RoleMissing")[1]


def test_the_task_dispatches_to_the_right_runner():
    text = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    block = text.split("    'purge' {")[1].split("\r\n    }")[0]
    assert "purge_run.py" in block and "purge_plan.py" in block
    assert "$Apply" in block
    assert "'--before'; $purgeArgs += $Before" in block
    assert "--force" in block


def test_the_switches_are_declared():
    text = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    for declared in ("[string]$Before", "[switch]$Verify", "[switch]$Force"):
        assert declared in text



# --- Q4: the quarantine, and the order the two stores go in -------------------

QUARANTINE = ROOT / "sql" / "purge" / "020_quarantine.sql"


def test_the_quarantine_delete_has_the_same_shape_as_the_raw_one():
    """Same reasons, so the same form: no date of its own, one batch, one
    slice, on the primary key."""
    text = _sql(QUARANTINE)
    assert "%(before)s" not in text and "rejected_at" not in text
    assert "%(batch_id)s" in text and "%(chunk)s" in text
    assert re.search(r"LIMIT\s+%\(chunk\)s", text)
    assert "quarantine_id" in text and "ctid" not in text
    assert text.upper().count("DELETE") == 1


def test_the_quarantine_delete_touches_only_the_quarantine():
    text = _sql(QUARANTINE).upper()
    for other in ("FD_RAW_EVENTS", "FD_TRANSACTIONS", "FD_GROUND_TRUTH",
                  "FD_SCORES", "FD_ALERTS", "FD_PIPELINE_RUNS"):
        assert other not in text


def test_the_derived_copy_is_purged_before_the_original():
    """THE INTERACTION THAT WOULD STRAND IT. A quarantined row derives from a
    raw line of its batch, and what permits its deletion is the batch's RAW
    fingerprint -- which cannot be recomputed once the raw rows are gone.
    Purging the raw zone first would leave the quarantine refused for ever as
    a mismatch against a digest nothing can reproduce."""
    assert purge.PURGE_ORDER == ("fd_quarantine", "fd_raw_events")
    assert set(purge.PURGE_ORDER) == set(purge.DELETE_SQL)


def test_execute_follows_that_order_whatever_order_the_plan_lists(world):
    """The plan lists the stores in the order of CANDIDATE_SQL, which is the
    other one. The purge must not inherit it."""
    world.state["rows"] = {A: 10}
    purge.execute(world.conn, _settings(), _report([
        _store("fd_raw_events", 10, [{"batch_id": A, "rows": 10}]),
        _store("fd_quarantine", 10, [{"batch_id": A, "rows": 10}])]),
        NOW)
    order = [entry[1] for entry in world.trace if entry[0] == "delete"]
    # The property, not a count of slices: every quarantine delete precedes
    # every raw one. (Here the fake shares one row counter per batch, so the
    # raw store finds the batch already empty -- which is the real behaviour
    # too once the quarantine of a batch is gone.)
    last_quarantine = max(i for i, f in enumerate(order) if "020_" in f)
    first_raw = min(i for i, f in enumerate(order) if "010_" in f)
    assert order[0] == "purge/020_quarantine.sql"
    assert last_quarantine < first_raw


def test_a_batch_whose_raw_zone_is_gone_still_lets_its_quarantine_go(world):
    """The state PURGE_ORDER makes rare and a manual step can still produce.
    The recorded fingerprint can no longer be matched against anything, so the
    mark is the authority -- otherwise those rows, which hold the same personal
    data as the raw zone, would be kept for ever by the very mechanism meant to
    protect them."""
    world.state["held"] = {"run_id": "runA", "purge": {
        "fd_raw_events": {"started_at": "2026-11-15T04:00:00+01:00",
                          "expected": 10, "done_at": "2026-11-15T04:01:00+01:00",
                          "deleted": 10}}}
    summary = purge.execute(world.conn, _settings(), _report([
        _store("fd_quarantine", 10, [{"batch_id": A, "rows": 10}])]),
        NOW)
    assert ("verify", A) not in world.trace
    assert summary["rows"] == 10
    assert summary["outcomes"][0]["raw_gone"] is True


def test_the_raw_zone_itself_is_never_exempted_by_its_own_mark(world):
    """The exemption is for the DERIVED store only. Applied to fd_raw_events it
    would skip the verification of the very batch being deleted."""
    world.state["held"] = {"run_id": "runA", "purge": {
        "fd_raw_events": {"started_at": "2026-11-15T04:00:00+01:00",
                          "expected": 10, "done_at": "2026-11-15T04:01:00+01:00",
                          "deleted": 10}}}
    purge.execute(world.conn, _settings(), _report([
        _store("fd_raw_events", 10, [{"batch_id": A, "rows": 10}])]),
        NOW)
    assert ("verify", A) in world.trace


def test_a_finished_purge_explains_a_mismatch_for_good():
    """Every purged batch is in permanent mismatch -- by design, since its rows
    are gone. The message must say so, or the dry run of Q2 and the acceptance
    of Q6 report tampering on every batch the purge ever removed."""
    source = (ROOT / "ml4fd" / "runs.py").read_text(encoding="utf-8")
    block = source.split("def refusal")[1]
    assert "a ete purge" in block
    assert "ce qui subsiste" in block


def test_the_journal_is_not_among_the_stores_the_purge_knows():
    """Q4's finding: nothing to empty, and its rows hold the fingerprints that
    are all that remains of a purged batch."""
    assert "fd_pipeline_runs" not in purge.CANDIDATE_SQL
    assert "fd_pipeline_runs" not in purge.DELETE_SQL
    assert "fd_pipeline_runs" not in purge.PURGE_ORDER

def _settings(**over):
    values = {"purge_max_share": 0.5}
    values.update(over)
    return types.SimpleNamespace(**values)
