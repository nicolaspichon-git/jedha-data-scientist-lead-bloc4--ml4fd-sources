"""S5 / Q2: the dry run counts what would go, and nothing goes.

Covers: EC26, EI13, EC16, EC19
Lot: Q2
Competency: C5

S5 is the only process of the project that DESTROYS. Everywhere else a mistake
is repaired by a replay; here it is permanent, and that asymmetry is what these
tests are about. They hold five decisions, none of which a passing happy path
would show:

  - the dry run writes nothing, and reads no clock;
  - WHOLE batches, because a half-purged batch is a batch no fingerprint
    describes any more -- unverifiable for ever;
  - a batch with no recorded fingerprint is refused, named, and counted apart;
  - the share the guard rests on is what WOULD BE DELETED, not what was
    considered -- the two differ exactly when there are refusals;
  - plan() never raises and assert_allowed() does, so an operator can always
    look at the figures of a purge that is being refused.
"""
from __future__ import annotations

import ast
import datetime as dt
import os
import re
import types
from pathlib import Path

import pytest

# config.load() refuses to run without them, and the two cadence tests below
# read the REAL governance. Set here rather than inherited from whichever
# other test module happens to import first: a test that passes only when the
# whole suite runs in one order is a test that will mislead someone.
os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")
os.environ.setdefault("POSTGRES_DB", "test_db")

from ml4fd import db, purge, runs  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PURGE_SQL = ROOT / "sql" / "purge"
PARIS = dt.timezone(dt.timedelta(hours=1), "CET")
AS_OF = dt.datetime(2026, 11, 17, 4, 17, tzinfo=PARIS)

A = "aaaaaaaa-0000-0000-0000-00000000000a"
B = "bbbbbbbb-0000-0000-0000-00000000000b"


def _settings(**over):
    values = {"raw_retention_days": 90, "quarantine_retention_days": 90,
              "purge_max_share": 0.5,
              "purge_schedule": "0 4 * * 0", "report_timezone": "Europe/Paris"}
    values.update(over)
    return types.SimpleNamespace(**values)


def _candidate_row(batch_id=A, source="csv", rows=10, first=None, last=None,
                   n_bytes=1024, total=1000, digest="d1"):
    first = first or dt.datetime(2026, 8, 1, 10, tzinfo=PARIS)
    last = last or dt.datetime(2026, 8, 1, 11, tzinfo=PARIS)
    return (batch_id, source, rows, first, last, n_bytes, total, digest)


@pytest.fixture
def answers(monkeypatch):
    """db.query_file answered per SQL file, and the parameters recorded."""
    book = {purge.CANDIDATE_SQL["fd_raw_events"]: [],
            purge.CANDIDATE_SQL["fd_quarantine"]: [],
            # The verifying path reads the purge marks through
            # runs.purge_state(); no row means no mark, which is the ordinary
            # case. A test that wants a mark replaces this entry.
            "checks/batch_digest_recorded.sql": [],
            "_calls": []}

    def fake(conn, relative, **params):
        book["_calls"].append((relative, params))
        value = book[relative]
        return value(params) if callable(value) else value

    monkeypatch.setattr(db, "query_file", fake)
    # The migration guard of plan() asks the catalogue, which these fake
    # connections cannot answer. It has tests of its own in
    # tests/test_migration_guard.py, including that plan() asks it FIRST.
    monkeypatch.setattr(runs, "assert_digests", lambda conn: None)
    return book


def _plan(answers, settings=None, **kwargs):
    return purge.plan(object(), settings or _settings(), as_of=AS_OF, **kwargs)


def _sql(path):
    """A SQL file without its comment lines: prose may say now() freely."""
    return "\n".join(line for line in path.read_text(encoding="utf-8").splitlines()
                     if not line.strip().startswith("--"))


# --- the files read, not guessed ---------------------------------------------

def test_the_three_files_exist_and_are_the_ones_the_module_names():
    named = set(purge.CANDIDATE_SQL.values())
    for relative in named:
        assert (ROOT / "sql" / relative).is_file(), relative
    named |= set(purge.DELETE_SQL.values())          # Q3
    named.add(purge.RECORD_SQL)                     # Q6
    on_disk = {"purge/%s" % path.name for path in PURGE_SQL.glob("*.sql")}
    assert on_disk == named, (
        "a file in sql/purge/ that the module does not name, or the reverse: "
        "%s" % (on_disk ^ named))


READERS = [ROOT / "sql" / relative
           for relative in purge.CANDIDATE_SQL.values()]


@pytest.mark.parametrize("path", READERS, ids=lambda p: p.name)
def test_the_dry_run_writes_nothing(path):
    text = _sql(path).upper()
    for verb in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALTER", "DROP",
                 "CREATE", "GRANT"):
        assert verb not in text, "%s contains %s" % (path.name, verb)


@pytest.mark.parametrize("path", sorted(PURGE_SQL.glob("*.sql")),
                         ids=lambda p: p.name)
def test_no_file_of_the_purge_reads_the_clock(path):
    """THE TRAP S4 ALREADY FELL INTO ONCE. A boundary from now() cannot be
    replayed and cannot be demonstrated on a chosen date; worse, a purge whose
    boundary moves between the dry run and the delete does not delete what was
    shown."""
    text = _sql(path).lower()
    for clock in ("now()", "current_date", "current_timestamp", "localtimestamp"):
        assert clock not in text, "%s reads %s" % (path.name, clock)


@pytest.mark.parametrize("path", READERS, ids=lambda p: p.name)
def test_every_reading_file_takes_its_boundary_as_a_parameter(path):
    """And the file that DELETES must not: its set comes from these."""
    assert "%(before)s" in _sql(path)


@pytest.mark.parametrize("path", [PURGE_SQL / "candidates_raw.sql",
                                  PURGE_SQL / "candidates_quarantine.sql"],
                         ids=lambda p: p.name)
def test_the_criterion_is_on_the_whole_batch(path):
    """A HALF-PURGED BATCH IS UNVERIFIABLE FOR EVER: the fingerprint of Q1
    covers a batch's rows together, so the remainder would match no recorded
    digest and be indistinguishable from a batch someone tampered with. The
    predicate therefore bears on the batch's LAST row, after the grouping."""
    text = _sql(path)
    # On the batch's LAST row -- in a WHERE or in a join's ON, the property is
    # the same -- and never on the row's own timestamp, which is what would
    # cut a batch in half.
    assert re.search(r"(WHERE|ON)\s+b\.last_at\s*<\s*%\(before\)s", text), path.name
    for row_level in ("ingested_at <", "rejected_at <"):
        assert row_level not in text, "%s coupe un lot en deux" % path.name
    assert "GROUP  BY batch_id" in text or "GROUP BY batch_id" in text
    # and the predicate comes after the aggregation, not inside it
    assert text.index("GROUP") < text.index("%(before)s")


def test_a_future_delete_cannot_carry_its_own_date_predicate():
    """SET FOR Q3, where the delete lands. One definition of the set: the
    delete takes its batch_id from the candidates file. A delete with a date
    predicate of its own would make the dry run a report about another set --
    worse than no dry run at all."""
    for path in PURGE_SQL.glob("*.sql"):
        text = _sql(path).upper()
        if "DELETE" in text:
            assert "%(BEFORE)S" not in text, (
                "%s deletes AND carries a date predicate: it must take the "
                "batch_id the candidates file selected" % path.name)



def test_a_store_with_nothing_to_purge_still_reports_its_size(answers):
    """"0 LIGNES SUR 0" READ AS AN EMPTY TABLE, and the table was not empty:
    the total travelled with the candidates, so a store beyond nothing returned
    no row and no total. Seen in the first real run of the DAG."""
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [
        (None, None, None, None, None, None, 555719, None)]
    raw = _plan(answers)["stores"][0]
    assert raw["rows"] == 0 and raw["batches"] == 0
    assert raw["total_rows"] == 555719
    assert raw["share"] == 0.0


def test_the_row_that_carries_only_a_total_is_not_a_candidate(answers):
    """It has no batch_id: counting it would invent a batch out of a count."""
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [
        (None, None, None, None, None, None, 100, None),
        _candidate_row(rows=10, total=100)]
    raw = _plan(answers)["stores"][0]
    assert raw["batches"] == 1
    assert raw["batch_ids"] == [A]


# --- the boundaries -----------------------------------------------------------

def test_each_store_is_purged_up_to_its_own_declared_duration():
    limits = purge.cutoffs(_settings(), AS_OF)
    assert limits["fd_raw_events"] == AS_OF - dt.timedelta(days=90)
    assert limits["fd_quarantine"] == AS_OF - dt.timedelta(days=90)
    # And no boundary at all for the run journal: Q4 keeps it (governance.yml,
    # retention.kept), so the purge has no business computing one.
    assert set(limits) == {"fd_raw_events", "fd_quarantine"}


def test_the_override_moves_every_store_at_once():
    """At 90 days and three batches loaded yesterday the purge would delete
    nothing before February 2027. A destructive process nobody can try is one
    that is discovered in production."""
    before = dt.datetime(2026, 11, 1, tzinfo=PARIS)
    limits = purge.cutoffs(_settings(), AS_OF, before=before)
    assert set(limits.values()) == {before}
    assert set(limits) == set(purge.CANDIDATE_SQL)


@pytest.mark.parametrize("field", ["as_of", "before"])
def test_a_naive_instant_is_refused(field):
    naive = dt.datetime(2026, 11, 1, 4, 17)
    kwargs = {"as_of": AS_OF, field: naive}
    with pytest.raises(ValueError, match="timezone-aware"):
        purge.cutoffs(_settings(), **kwargs)


def test_the_declared_cadence_is_weekly_and_on_a_sunday():
    """WEEKLY AND NOT DAILY, for the opposite of the usual reason: a purge at
    90 days has no daily urgency, and a destructive task that runs rarely is a
    task whose failure is noticed.

    READ FROM governance.yml AND NOT FROM THE FIXTURE ABOVE. A test that holds
    its own fixture holds nothing: the cadence could be changed in the
    declaration and this would still pass -- the defect already met twice in
    this project.
    """
    from ml4fd import config
    minute, hour, dom, month, dow = config.load().purge_schedule.split()
    assert (dom, month) == ("*", "*")
    assert dow == "0", "not weekly on a Sunday: %r" % dow
    assert hour != "*" and minute != "*"


def test_the_purge_does_not_meet_the_verdict_writer():
    """THE COLLISION THIS CADENCE AVOIDS. The purge deletes batches of the raw
    zone; dag_ingest_verdicts READS the raw zone to write the verdicts that
    fell. At the same hour the purge would take batches out from under it."""
    verdicts = (ROOT / "airflow" / "dags" / "dag_ingest_verdicts.py").read_text(
        encoding="utf-8")
    other = re.search(r'schedule="([^"]+)"', verdicts).group(1)
    from ml4fd import config
    mine = config.load().purge_schedule
    assert mine.split()[:2] != other.split()[:2], (
        "the purge and the verdict writer share an hour: %r" % mine)


# --- what is refused ----------------------------------------------------------

def test_a_batch_with_a_recorded_fingerprint_is_deletable(answers):
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [_candidate_row()]
    report = _plan(answers)
    raw = report["stores"][0]
    assert raw["rows"] == 10 and raw["batches"] == 1
    assert raw["refused_batches"] == 0
    assert raw["batch_ids"] == [A]


def test_a_batch_with_no_fingerprint_is_refused_and_named(answers):
    """THE STRUCTURAL GUARANTEE OF S5: what a batch held cannot stop being
    knowable before it stops existing."""
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [_candidate_row(digest=None)]
    report = _plan(answers)
    raw = report["stores"][0]
    assert raw["rows"] == 0 and raw["batches"] == 0
    assert raw["refused_rows"] == 10
    ((refusal,)) = raw["refusals"]
    assert refusal["code"] == runs.NO_DIGEST
    assert refusal["batch_id"] == A
    assert "digests -Apply" in refusal["why"]          # the way out is in it
    # and it is NOT handed to Q3 as something to delete
    assert raw["batch_ids"] == []


def test_a_fingerprint_that_no_longer_matches_is_refused_under_verify(
        answers, monkeypatch):
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [_candidate_row()]
    monkeypatch.setattr(runs, "refusal",
                        lambda conn, batch: (runs.DIGEST_MISMATCH, "a bouge"))
    report = _plan(answers, verify=True)
    raw = report["stores"][0]
    assert raw["rows"] == 0
    assert raw["refusals"][0]["code"] == runs.DIGEST_MISMATCH
    assert report["verified"] is True


def test_the_expensive_half_is_not_asked_by_default(answers, monkeypatch):
    """An md5 over every row of each candidate batch -- 555 thousand rows and
    some 200 MB for a historical one. Not the default here, and never skipped
    where it decides: Q3 asks it batch by batch before deleting."""
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [_candidate_row()]
    called = []
    monkeypatch.setattr(runs, "refusal",
                        lambda conn, batch: called.append(batch))
    report = _plan(answers)
    assert called == []
    assert report["verified"] is False
    assert report["stores"][0]["rows"] == 10


def test_a_batch_without_a_fingerprint_is_never_verified(answers, monkeypatch):
    """No point recomputing a digest to compare it with nothing, and
    runs.refusal() would read the journal a second time for the same answer."""
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [_candidate_row(digest=None)]
    monkeypatch.setattr(runs, "refusal",
                        lambda conn, batch: pytest.fail("verified anyway"))
    assert _plan(answers, verify=True)["stores"][0]["refused_rows"] == 10



# --- the one rule both paths read ---------------------------------------------
#
# FOUND BY RUNNING IT. The executor knew that a purge explains a mismatch; the
# verifying dry run did not. So a verifying plan -- which is what the weekly
# DAG and the acceptance run -- refused what the purge would have accepted, and
# the quarantined rows of a batch whose raw zone was already purged stayed in
# the database for ever. The same personal data the retention limit exists to
# remove, kept by the mechanism meant to remove it.

DONE = {"started_at": "2026-11-15T04:00:00+01:00", "expected": 10,
        "done_at": "2026-11-15T04:01:00+01:00", "deleted": 10}
STARTED = {"started_at": "2026-11-15T04:00:00+01:00", "expected": 10,
           "done_at": None, "deleted": None}


@pytest.mark.parametrize("marks,store,expected", [
    ({}, "fd_raw_events", None),
    ({}, "fd_quarantine", None),
    ({"fd_raw_events": STARTED}, "fd_raw_events", "resuming"),
    ({"fd_quarantine": STARTED}, "fd_quarantine", "resuming"),
    ({"fd_raw_events": DONE}, "fd_quarantine", "raw_gone"),
    # the raw zone is never exempted by its own finished mark: that would skip
    # the verification of the very rows being deleted
    ({"fd_raw_events": DONE}, "fd_raw_events", None),
    ({"fd_quarantine": DONE}, "fd_raw_events", None),
    # an unfinished purge of THIS store wins over anything else
    ({"fd_raw_events": DONE, "fd_quarantine": STARTED}, "fd_quarantine",
     "resuming"),
])
def test_when_the_fingerprint_check_does_not_apply(marks, store, expected):
    assert purge.exempt(marks, store) == expected


def test_the_verifying_plan_applies_the_same_exemption(answers, monkeypatch):
    """THE DEFECT ITSELF. Without this, a verifying plan refuses the quarantine
    of every batch the purge has already emptied."""
    answers[purge.CANDIDATE_SQL["fd_quarantine"]] = [
        _candidate_row(batch_id=A, rows=12, total=100)]
    monkeypatch.setattr(runs, "purge_state",
                        lambda conn, batch: {"fd_raw_events": DONE})
    monkeypatch.setattr(runs, "refusal",
                        lambda conn, batch: pytest.fail("verified anyway"))
    quarantine = _plan(answers, verify=True)["stores"][1]
    assert quarantine["rows"] == 12
    assert quarantine["refusals"] == []


def test_the_verifying_plan_still_offers_an_interrupted_purge(answers,
                                                              monkeypatch):
    """Else the resumption could never happen through the verifying path."""
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [_candidate_row(rows=10)]
    monkeypatch.setattr(runs, "purge_state",
                        lambda conn, batch: {"fd_raw_events": STARTED})
    monkeypatch.setattr(runs, "refusal",
                        lambda conn, batch: pytest.fail("verified anyway"))
    assert _plan(answers, verify=True)["stores"][0]["rows"] == 10


def test_the_verifying_plan_still_refuses_a_batch_nothing_explains(answers,
                                                                   monkeypatch):
    """The exemption is narrow: with no mark at all, a mismatch is a mismatch."""
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [_candidate_row(rows=10)]
    monkeypatch.setattr(runs, "purge_state", lambda conn, batch: {})
    monkeypatch.setattr(runs, "refusal",
                        lambda conn, batch: (runs.DIGEST_MISMATCH, "a bouge"))
    store = _plan(answers, verify=True)["stores"][0]
    assert store["rows"] == 0
    assert store["refusals"][0]["code"] == runs.DIGEST_MISMATCH


def test_both_paths_read_the_same_function():
    """Written twice, they disagreed. There is one copy, and these are the two
    callers."""
    source = (ROOT / "ml4fd" / "purge.py").read_text(encoding="utf-8")
    assert source.count("def exempt(") == 1
    judge = source.split("def _judge")[1].split("\ndef ")[0]
    batch = source.split("def purge_batch")[1].split("\ndef ")[0]
    assert "exempt(" in judge and "exempt(" in batch

# --- the share, and the guard --------------------------------------------------

def test_the_share_counts_what_would_be_deleted_and_not_what_was_considered(
        answers):
    """THE ARITHMETIC THE GUARD RESTS ON. One deletable batch of 10 rows and
    one refused batch of 90, out of 1000: the purge would remove 1 %, not
    10 %. A share computed on the candidates would block a purge that is not
    going to happen -- and, the other way round, hide one that is."""
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [
        _candidate_row(batch_id=A, rows=10, digest="d1", total=1000),
        _candidate_row(batch_id=B, rows=90, digest=None, total=1000)]
    raw = _plan(answers)["stores"][0]
    assert raw["rows"] == 10
    assert raw["refused_rows"] == 90
    assert raw["share"] == pytest.approx(0.01)


def test_an_empty_store_has_a_share_of_zero_and_blocks_nothing(answers):
    report = _plan(answers)
    assert [store["share"] for store in report["stores"]] == [0.0, 0.0]
    assert report["blocked"] is False


def test_the_guard_blocks_beyond_the_declared_share(answers):
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [
        _candidate_row(rows=501, total=1000)]
    report = _plan(answers)
    assert report["blocked"] is True
    assert report["blocking"][0]["store"] == "fd_raw_events"


def test_the_guard_lets_the_declared_share_itself_through(answers):
    """Strictly greater: a share declared at 0,5 allows exactly a half."""
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [
        _candidate_row(rows=500, total=1000)]
    assert _plan(answers)["blocked"] is False


def test_the_dry_run_never_raises_even_when_it_blocks(answers):
    """A dry run that refuses to show its figures is useless exactly when the
    figures matter."""
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [
        _candidate_row(rows=900, total=1000, digest=None)]
    answers[purge.CANDIDATE_SQL["fd_quarantine"]] = [
        _candidate_row(rows=900, total=1000)]
    report = _plan(answers)                     # no raise
    assert report["blocked"] is True
    assert report["refused_batches"] == 1


def test_assert_allowed_raises_and_names_the_store(answers):
    answers[purge.CANDIDATE_SQL["fd_quarantine"]] = [
        _candidate_row(rows=900, total=1000)]
    report = _plan(answers)
    with pytest.raises(purge.PurgeRefused, match="fd_quarantine"):
        purge.assert_allowed(report)


def test_assert_allowed_passes_a_plan_within_the_share(answers):
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [_candidate_row()]
    purge.assert_allowed(_plan(answers))


# --- the run journal is NOT part of the purge (Q4) ------------------------------

def test_the_run_journal_is_no_longer_counted_by_the_purge(answers):
    """MEASURED IN Q4: the table holds no personal field to empty, and its rows
    carry the fingerprints that are all that remains of a purged batch. So it
    is declared kept, and a destructive tool does not report a figure about it
    -- a number shown by a purge and never acted on is an invitation to finish
    the job."""
    report = _plan(answers)
    assert "journal" not in report
    assert [store["store"] for store in report["stores"]] \
        == ["fd_raw_events", "fd_quarantine"]
    assert not hasattr(purge, "runs_to_trim")
    assert not hasattr(purge, "RUNS_SQL")


# --- what the operator reads ---------------------------------------------------

def test_the_rendering_says_nothing_was_deleted_and_shows_the_figures(answers):
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [
        _candidate_row(rows=10, total=1000)]
    text = purge.render(_plan(answers))
    assert "rien n'est supprime" in text.lower()
    assert "fd_raw_events" in text and "fd_quarantine" in text
    assert "1,0 %" in text                      # the share, French decimals
    assert "journal d'executions n'est pas purge" in text
    assert "respectee" in text


def test_the_rendering_names_every_refusal_with_its_way_out(answers):
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [_candidate_row(digest=None)]
    text = purge.render(_plan(answers))
    assert A in text
    assert runs.NO_DIGEST in text
    assert "digests -Apply" in text


def test_the_rendering_of_a_blocked_plan_asks_for_a_confirmation(answers):
    answers[purge.CANDIDATE_SQL["fd_raw_events"]] = [
        _candidate_row(rows=900, total=1000)]
    text = purge.render(_plan(answers))
    assert "BLOQUEE" in text
    assert "confirmation" in text


def test_an_overridden_boundary_is_announced_as_a_demonstration(answers):
    text = purge.render(_plan(answers, before=dt.datetime(2026, 11, 1,
                                                          tzinfo=PARIS)))
    assert "SURCHARGEE" in text
    assert "demonstration" in text


def test_the_unverified_report_says_so(answers):
    assert "non recalcule" in purge.render(_plan(answers))


@pytest.mark.parametrize("value,expected", [
    (0, "0 o"), (2048, "2.0 ko"), (5 * 1024 ** 2, "5.0 Mo"),
    (3 * 1024 ** 3, "3.0 Go")])
def test_the_sizes_are_legible(value, expected):
    assert purge._bytes(value) == expected


def test_the_size_is_announced_as_approximate():
    """pg_column_size ignores the indexes and the TOAST compression, and the
    space returns to the table rather than to the operating system."""
    text = (PURGE_SQL / "candidates_raw.sql").read_text(encoding="utf-8")
    assert "APPROXIMATE" in text.upper()


# --- the runner cannot delete ---------------------------------------------------

def test_the_runner_offers_no_way_to_delete():
    """A dry run that is a MODE of a destructive command gets mixed up with it.
    The delete lands in Q3, behind its own task and its own flag."""
    source = (ROOT / "tools" / "purge_plan.py").read_text(encoding="utf-8")
    flags = re.findall(r'add_argument\("(--[a-z-]+)"', source)
    assert "--before" in flags and "--verify" in flags
    for forbidden in ("--apply", "--delete", "--force", "--yes"):
        assert forbidden not in flags


def test_the_runner_passes_an_aware_instant():
    """It reads the clock -- a human asking "what would go now" is entitled to
    -- but in the declared zone, and plan() refuses a naive one."""
    source = (ROOT / "tools" / "purge_plan.py").read_text(encoding="utf-8")
    assert "datetime.now(zone)" in source
    assert "report_timezone" in source


def test_the_task_exists_and_calls_the_planner():
    text = (ROOT / "fd-manager.ps1").read_text(encoding="utf-8")
    assert "'purge'," in text                   # in the ValidateSet
    assert "    'purge' {" in text              # and in the switch
    block = text.split("    'purge' {")[1].split("\r\n    }")[0]
    assert "purge_plan.py" in block
    # The dry run is what the task does WITHOUT -Apply: tested from the
    # dispatch itself, in tests/test_purge_run.py.
    assert "if ($Apply) { 'purge_run.py' } else { 'purge_plan.py' }" in block


# --- nothing in the module reads a clock ----------------------------------------

def test_the_module_never_reads_the_clock_itself():
    """The instant is the caller's: data_interval_end in the DAG of Q5."""
    tree = ast.parse((ROOT / "ml4fd" / "purge.py").read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr not in ("now", "today", "utcnow"), (
                "ml4fd/purge.py reads the clock at line %d" % node.lineno)
