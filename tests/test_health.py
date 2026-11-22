"""The four controls, and the one property that makes the channel usable.

Block: B3, B4.
Lot: B3-N9, B4-L0.
Competency: B3-C5.
Covers: B3-EC14, B3-EC17.

The controls are pure functions of a Facts record, so every verdict here is
exercised without a database: that split is the point of ml4fd/health.py and it
is what lets the thresholds be tested against the figures N0 measured rather
than against a mock.

The decisions worth testing are the ones this module makes -- which control
fires at which share, that a red control is announced ONCE and not 1,440 times,
what may travel in an operations payload, and that a send which fails leaves the
state alone so the next run retries. None of them is something PostgreSQL does.
"""
from __future__ import annotations

import datetime as dt
import json
import re
import types
from pathlib import Path

import pytest

from ml4fd import health

ROOT = Path(__file__).resolve().parents[1]

# The SELECT's column order. facts_of() unpacks BY POSITION, so a reordering of
# the query would land the rejected lines in the read lines and every share
# would be wrong while every number stayed plausible.
SQL_ORDER = ("seconds_since_success", "runs_ended", "runs_failed", "runs_empty",
             "lines_read", "lines_rejected", "batches_breaking_invariant")

# The declared limits of governance.yml, so the tests judge the SHIPPED values.
LIMIT_S, MAX_EMPTY, MAX_REJECT = 600.0, 0.10, 0.05

# The window facts_of() is handed. EXPLICIT instants and not a duration: the
# same query serves N9's rolling hour and S4's calendar day, and only the caller
# knows which it wants.
WINDOW_END = dt.datetime(2026, 11, 16, 17, 0, tzinfo=dt.timezone.utc)
WINDOW_START = WINDOW_END - dt.timedelta(minutes=60)


def _facts(**over):
    """A healthy hour at the declared cadence: 60 runs, one failed, 300 lines."""
    base = dict(dag_id="dag_ingest", window_minutes=60,
                seconds_since_success=30.0, runs_ended=60, runs_failed=1,
                runs_empty=0, lines_read=300, lines_rejected=2,
                batches_breaking_invariant=0)
    base.update(over)
    return health.Facts(**base)


def _settings(channel="log", ops_file="/tmp/ops.jsonl"):
    return types.SimpleNamespace(
        ops_channel=channel, ops_file=str(ops_file), ops_window_minutes=60,
        ops_freshness_minutes=10, ops_max_empty_share=MAX_EMPTY,
        ops_max_rejection_share=MAX_REJECT)


def _verdicts(facts):
    return health.controls(facts, LIMIT_S, MAX_EMPTY, MAX_REJECT)


def _by_name(facts):
    return {control.control: control for control in _verdicts(facts)}


# --- the four controls are the diagram's four ---------------------------------

def test_the_controls_are_the_four_of_diagram_3_3():
    """THE OMISSION THIS LOT REPAIRS. Diagram 3.3 names freshness, empty run,
    rejection rate and volume invariant; the plan of S3 listed five tasks and
    none was the stage carrying them. A control dropped from here must fail a
    test rather than wait for somebody to read the diagram again."""
    assert health.CONTROLS == ("freshness", "empty_run", "rejection_rate",
                               "volume_invariant")
    assert tuple(c.control for c in _verdicts(_facts())) == health.CONTROLS


def test_a_healthy_hour_lights_nothing():
    assert all(not control.alert for control in _verdicts(_facts()))


# --- freshness ----------------------------------------------------------------

def test_a_pipeline_that_never_succeeded_is_an_alert():
    control = _by_name(_facts(seconds_since_success=None))["freshness"]
    assert control.alert
    assert "jamais" in control.detail


def test_freshness_fires_past_the_declared_limit():
    control = _by_name(_facts(seconds_since_success=11 * 60))["freshness"]
    assert control.alert
    # Both figures in the sentence: what was observed and what was declared.
    assert "11" in control.detail and "10" in control.detail


def test_freshness_holds_just_inside_the_limit():
    assert not _by_name(_facts(seconds_since_success=9 * 60))["freshness"].alert


def test_freshness_looks_past_the_window():
    """A day's outage must not read as "no data for an hour". The query asks the
    whole journal for the last success, so the verdict can say how long."""
    control = _by_name(_facts(seconds_since_success=86_400))["freshness"]
    assert control.alert
    assert "1440" in control.detail.replace(" ", "")


# --- empty runs ---------------------------------------------------------------

def test_empty_runs_fire_above_the_declared_share():
    control = _by_name(_facts(runs_ended=60, runs_empty=7))["empty_run"]
    assert control.alert          # 11.7 % > 10 %
    assert "7" in control.detail


def test_a_few_empty_runs_do_not_fire():
    """N0 measured about 5 % of calls failing and a run makes five, so an
    entirely empty run is rare but not impossible. The control must tolerate
    what the measurement predicts."""
    assert not _by_name(_facts(runs_ended=60, runs_empty=5))["empty_run"].alert


def test_no_ended_run_defers_to_the_freshness():
    """Both controls firing on one outage would send two messages for one
    event, and the second says nothing the first did not."""
    control = _by_name(_facts(runs_ended=0, runs_empty=0))["empty_run"]
    assert not control.alert
    assert "fraicheur" in control.detail


# --- rejection rate -----------------------------------------------------------

def test_the_rejection_rate_fires_above_the_declared_share():
    control = _by_name(_facts(lines_read=300, lines_rejected=20))["rejection_rate"]
    assert control.alert          # 6.7 % > 5 %
    assert "scorees" in control.detail


def test_the_measured_floor_does_not_fire():
    """N0 saw one call in 57 (1.75 %) with an 818 s gap, which quarantines on
    the clock tolerance. The normal rate is NOT zero, and a threshold at zero
    would hold this control in permanent alert -- which is why config refuses
    a share of 0."""
    control = _by_name(_facts(lines_read=5700, lines_rejected=100))["rejection_rate"]
    assert not control.alert


def test_no_line_read_judges_nothing():
    control = _by_name(_facts(lines_read=0, lines_rejected=0))["rejection_rate"]
    assert not control.alert
    assert "rien a juger" in control.detail


# --- volume invariant ---------------------------------------------------------

def test_a_broken_invariant_is_an_alert():
    control = _by_name(_facts(batches_breaking_invariant=2))["volume_invariant"]
    assert control.alert
    assert "nulle part" in control.detail


def test_closing_counters_light_nothing():
    assert not _by_name(_facts())["volume_invariant"].alert


# --- the shares are computed, not guessed -------------------------------------

def test_the_shares_are_zero_rather_than_undefined_when_nothing_happened():
    """A division by zero inside a control would turn a quiet window into a
    red task -- the control failing the run it watches."""
    facts = _facts(runs_ended=0, runs_empty=0, lines_read=0, lines_rejected=0)
    assert facts.empty_share == 0.0
    assert facts.rejection_share == 0.0


def test_controls_refuses_to_return_a_different_set():
    """The order and the membership are asserted inside controls() itself, so a
    control removed in a hurry fails loudly here rather than silently there."""
    saved = health.freshness
    try:
        health.freshness = lambda facts, limit: health.Control(
            "fraicheur", health.OK, "renamed")
        with pytest.raises(AssertionError, match="diagram 3.3"):
            _verdicts(_facts())
    finally:
        health.freshness = saved


# --- THE property: on transition, never on condition --------------------------

def test_a_control_that_stays_red_is_announced_once_not_1440_times():
    """THE REASON fd_ops_state EXISTS, and the arithmetic that justifies the
    whole table: the DAG runs 1,440 times a day, so notifying on the CONDITION
    would send 1,440 identical messages and the channel would be ignored by the
    second day. N5's capacity policy does not help -- it caps the volume of a
    day's fraud alerts, which is a different failure."""
    facts = _facts(seconds_since_success=3600)       # freshness red, and stays
    stored, sent = {}, 0
    for _ in range(1440):
        changed = health.transitions(_verdicts(facts), stored)
        sent += len(changed)
        for change in changed:                       # send, THEN record
            stored[change.control] = change.state
    assert sent == 1
    assert stored == {"freshness": health.ALERT}


def test_the_return_to_green_is_announced_too():
    """An operator told that ingestion stopped and never told it resumed has to
    go and look, which is the pull model this channel exists to replace."""
    stored = {"freshness": health.ALERT}
    changed = health.transitions(_verdicts(_facts()), stored)
    assert [c.control for c in changed] == ["freshness"]
    assert changed[0].state == health.OK


def test_an_unrecorded_control_counts_as_ok():
    """What makes the first run of a healthy pipeline silent, and what keeps
    1,440 daily runs from writing to the table for nothing."""
    assert health.transitions(_verdicts(_facts()), {}) == []


def test_two_controls_changing_make_two_messages():
    facts = _facts(seconds_since_success=3600, batches_breaking_invariant=1)
    changed = health.transitions(_verdicts(facts), {})
    assert sorted(c.control for c in changed) == ["freshness", "volume_invariant"]


# --- what an operations payload may carry -------------------------------------

def _report(facts=None, transitions=None):
    facts = facts or _facts()
    verdicts = _verdicts(facts)
    changed = transitions if transitions is not None else health.transitions(
        verdicts, {})
    return {"dag_id": facts.dag_id, "run_id": "manual__2026-11-16T16:00:00",
            "window_minutes": facts.window_minutes, "facts": facts.as_dict(),
            "controls": [v.as_dict() for v in verdicts],
            "transitions": [c.as_dict() for c in changed],
            "in_alert": sum(1 for v in verdicts if v.alert)}


def test_the_payload_holds_only_the_declared_fields():
    report = _report(_facts(seconds_since_success=3600))
    payload = health.payload_of(
        health.Transition(*report["transitions"][0].values()), report)
    assert set(payload) == set(health.OPS_FIELDS)


def test_the_transition_travels_as_became_and_not_as_state():
    """FOUND BY THE WHITELIST REFUSING THE PAYLOAD, which is the whole reason
    the whitelist is a check and not a comment. N5's FORBIDDEN_FIELDS holds
    "state" -- the CARDHOLDER'S US state, one of the thirteen identifying
    columns of the source. A key named "state" would mean the control's state
    in the operations file and a person's address in the alert file, and a
    consumer reading both with one schema would read one as the other."""
    assert "state" in health.FORBIDDEN_FIELDS
    assert "state" not in health.OPS_FIELDS
    report = _report(_facts(seconds_since_success=3600))
    payload = health.payload_of(
        health.Transition("freshness", health.ALERT, "d"), report)
    assert payload["became"] == health.ALERT
    assert "state" not in payload


def test_no_transaction_travels_on_the_operations_channel():
    """NOT because these fields are personal -- they are not. Because an
    operations channel accumulating them would become a second, unmanaged copy
    of the alert stream, outside the capacity policy and outside the queue the
    analyst reads."""
    for name in ("txn_id", "amt", "merchant", "score", "threshold"):
        assert name in health.FORBIDDEN_FIELDS
    # And N5's whole list is inherited rather than retyped.
    from ml4fd import notify
    for name in notify.FORBIDDEN_FIELDS:
        assert name in health.FORBIDDEN_FIELDS

    report = _report(_facts(seconds_since_success=3600))
    payload = health.payload_of(
        health.Transition("freshness", health.ALERT, "d"), report)
    body = json.dumps(payload)
    for name in health.FORBIDDEN_FIELDS:
        assert '"%s"' % name not in body


def test_a_forbidden_field_smuggled_through_the_facts_is_refused():
    report = _report(_facts(seconds_since_success=3600))
    report["facts"]["txn_id"] = "abc:1794792372826"
    with pytest.raises(AssertionError, match="analyst"):
        health.payload_of(health.Transition("freshness", health.ALERT, "d"),
                          report)


def test_a_free_text_fact_is_refused():
    """The facts of this payload are counts. A free-text field here is exactly
    how a transaction would arrive -- in a "detail" nobody reviewed."""
    report = _report(_facts(seconds_since_success=3600))
    report["facts"]["note"] = "cc_num 4532..."
    with pytest.raises(AssertionError, match="counts"):
        health.payload_of(health.Transition("freshness", health.ALERT, "d"),
                          report)


# --- the channel --------------------------------------------------------------

def test_an_unknown_channel_is_refused_rather_than_defaulted():
    with pytest.raises(ValueError, match="ops.channel"):
        health.channel_of("pigeon")


def test_the_file_channel_writes_to_the_operations_path(tmp_path):
    """And nowhere near the analyst's. The two share one implementation of
    "append a JSON line" and nothing else."""
    ops = tmp_path / "deep" / "ops.jsonl"
    health.send_file({"control": "freshness"}, _settings(ops_file=ops))
    assert json.loads(ops.read_text(encoding="utf-8").strip()) == {
        "control": "freshness"}
    assert not (tmp_path / "alerts.jsonl").exists()


def test_the_journal_channel_separates_the_two_audiences(caplog):
    """One grep separates them even when both channels are the journal."""
    import logging
    with caplog.at_level(logging.ERROR, logger="ml4fd.health"):
        health.send_log({"control": "freshness"}, _settings())
    assert "EXPLOITATION" in caplog.text


# --- send, then record --------------------------------------------------------

class _Fake:
    """A connection answering the two queries this module runs."""

    def __init__(self, stored=None):
        self.stored = dict(stored or {})
        self.recorded = []
        self.journaled = []
        self.commits = 0
        self.order = []

    def commit(self):
        self.commits += 1
        self.order.append("commit")

    def cursor(self):
        """Only rolling_window() uses one: it asks the SERVER for its clock,
        because the container's and the server's can differ and the journal's
        timestamps are the server's."""
        fake = self

        class _Cursor:
            def __enter__(self): return self
            def __exit__(self, *exc): return False

            def execute(self, sql, params=None):
                fake.order.append("query:now")
                self.row = (WINDOW_START, WINDOW_END)

            def fetchone(self):
                return self.row

        return _Cursor()


class _patched:
    """Replace ml4fd.db's two entry points, and optionally break the channel.

    A context manager and not an autouse fixture: the restoration belongs to
    the same three lines as the patch.
    """

    def __init__(self, fake, facts=None, breaking=False):
        self.fake = fake
        self.facts = facts or _facts()
        self.breaking = breaking

    def __enter__(self):
        fake, facts = self.fake, self.facts
        self.saved = (health.db.query_file, health.db.execute_file,
                      health.CHANNELS["log"])

        def query_file(conn, relative, **params):
            conn.order.append("query:%s" % relative)
            if relative == health.STATE_SQL:
                return [(name, state, None, "")
                        for name, state in conn.stored.items()]
            if relative == health.FACTS_SQL:
                return [tuple(getattr(facts, name) for name in SQL_ORDER)]
            raise AssertionError("requete inattendue : %s" % relative)

        def execute_file(conn, relative, **params):
            conn.order.append("execute:%s" % relative)
            if relative == health.JOURNAL_SQL:
                conn.journaled.append(params)
                return 1
            assert relative == health.RECORD_SQL
            conn.recorded.append(params)
            return 1

        def send(payload, settings):
            fake.order.append("send:%s" % payload["control"])
            if self.breaking:
                raise OSError("read-only file system")

        health.db.query_file = query_file
        health.db.execute_file = execute_file
        health.CHANNELS["log"] = send
        return fake

    def __exit__(self, *exc):
        (health.db.query_file, health.db.execute_file,
         health.CHANNELS["log"]) = self.saved
        return False


def test_the_message_goes_out_before_the_state_is_recorded():
    """THE ORDER, and the reason is N5's turned around: recording first would
    mark the control as announced while nobody was told, and no later run would
    announce it -- there would no longer be a transition to see. That failure is
    silent and PERMANENT; a duplicate is neither."""
    fake = _Fake()
    with _patched(fake, facts=_facts(seconds_since_success=3600)):
        report = health.check(fake, _settings(), "dag_ingest", "r1")
        sent = health.notify_ops(fake, _settings(), report)
    assert sent["sent"] == 1
    assert fake.order.index("send:freshness") < fake.order.index(
        "execute:%s" % health.RECORD_SQL)


def test_a_failed_send_records_nothing_so_the_next_run_retries():
    fake = _Fake()
    with _patched(fake, facts=_facts(seconds_since_success=3600),
                  breaking=True):
        report = health.check(fake, _settings(), "dag_ingest", "r1")
        sent = health.notify_ops(fake, _settings(), report)
    assert (sent["sent"], sent["failed"]) == (0, 1)
    assert fake.recorded == [] and fake.journaled == []
    assert fake.commits == 0


def test_the_state_is_committed_per_transition():
    """A crash is then bounded to one repeated message instead of all of them."""
    fake = _Fake()
    facts = _facts(seconds_since_success=3600, batches_breaking_invariant=1)
    with _patched(fake, facts=facts):
        report = health.check(fake, _settings(), "dag_ingest", "r1")
        health.notify_ops(fake, _settings(), report)
    assert fake.commits == 2


def test_check_raises_on_nothing_a_control_found():
    """A red control is the output, not an error: failing the run on one would
    stop the ingestion the control exists to watch."""
    fake = _Fake()
    facts = _facts(seconds_since_success=86_400, runs_ended=60, runs_empty=60,
                   lines_read=10, lines_rejected=10,
                   batches_breaking_invariant=9)
    with _patched(fake, facts=facts):
        report = health.check(fake, _settings(), "dag_ingest", "r1")
    assert report["in_alert"] == 4


# --- the query and the reader agree on the column order -----------------------

def test_the_facts_are_read_in_the_order_the_query_returns_them():
    """facts_of() unpacks BY POSITION. A reordered SELECT would land the
    rejected lines in the read lines and every share would be wrong while every
    number stayed plausible -- the failure mode of to_alert() in N5, which is
    guarded the same way."""
    text = (ROOT / "sql" / "checks" / "pipeline_health.sql").read_text(
        encoding="utf-8")
    code = "\n".join(line for line in text.splitlines()
                     if not line.lstrip().startswith("--"))
    # The OUTER select only: the CTEs above it carry aliases of their own, and
    # reading those put "instant" at the head of the column order the first
    # time the query grew a CTE.
    outer = code[code.rindex("\nSELECT\n"):]
    assert tuple(re.findall(r"AS\s+(\w+)", outer)) == SQL_ORDER

    fake = _Fake()
    expected = _facts(seconds_since_success=611.0, runs_ended=59, runs_failed=3,
                      runs_empty=4, lines_read=281, lines_rejected=7,
                      batches_breaking_invariant=1)
    with _patched(fake, facts=expected):
        read = health.facts_of(fake, "dag_ingest", WINDOW_START, WINDOW_END)
    for name in SQL_ORDER:
        assert getattr(read, name) == getattr(expected, name), name


def test_the_report_is_json_serialisable():
    """It crosses an XCom boundary, which carries JSON and not Python objects --
    the defect of 16 November 2026, one task earlier in this same DAG."""
    fake = _Fake()
    with _patched(fake, facts=_facts(seconds_since_success=3600)):
        report = health.check(fake, _settings(), "dag_ingest", "r1")
    assert json.loads(json.dumps(report))["in_alert"] == 1


def test_the_rendering_is_one_line():
    fake = _Fake()
    with _patched(fake, facts=_facts(seconds_since_success=3600)):
        report = health.check(fake, _settings(), "dag_ingest", "r1")
    line = health.render(report)
    assert "\n" not in line
    assert "1/4" in line


def test_the_facts_of_a_payload_are_only_numbers():
    """FOUND BY payload_of() REFUSING ITS OWN REPORT. Facts.as_dict() carried
    dag_id, a string, among the counts -- and the rule that keeps free text out
    of this payload is the rule that keeps a transaction out of it. The DAG's
    name is identity, not a fact about health, and the payload carries it at the
    top level once."""
    facts = _facts().as_dict()
    assert "dag_id" not in facts
    for name, value in facts.items():
        assert value is None or isinstance(value, (int, float)), name


# --- one query, two windows ----------------------------------------------------

def test_the_window_is_the_callers_and_the_query_holds_no_clock():
    """FOUND WHILE IMPLEMENTING P2, and it was a defect of P0's own decision.
    The report reuses this query for its section 4, and the query anchored its
    window on now() -- so a replay of 3 November would have reported the health
    of today. The central trap of S4, in the file S4 was told to reuse.

    The bounds are parameters now, and the instant of reference is one too. The
    control task passes null for as_of and gets the DATABASE clock, which is the
    right one for a freshness reading; the report passes explicit instants and
    is replayable.
    """
    sql = (ROOT / "sql" / "checks" / "pipeline_health.sql").read_text(
        encoding="utf-8")
    code = "\n".join(line for line in sql.splitlines()
                     if not line.lstrip().startswith("--"))
    assert "%(window_start)s" in code and "%(window_end)s" in code
    assert "%(window_minutes)s" not in code
    # now() survives ONLY inside the coalesce that makes as_of optional.
    assert code.count("now()") == 1
    assert "coalesce(%(as_of)s::timestamptz, now())" in code


def test_the_window_in_the_facts_is_derived_from_the_bounds():
    """So a report covering a day reports 1 440 minutes without being told."""
    fake = _Fake()
    start = dt.datetime(2026, 11, 16, tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(days=1)
    with _patched(fake, facts=_facts()):
        read = health.facts_of(fake, "dag_ingest", start, end)
    assert read.window_minutes == 1440


def test_the_control_task_asks_the_server_for_its_clock():
    """And not the container's: the journal's timestamps are the server's."""
    fake = _Fake()
    with _patched(fake, facts=_facts(seconds_since_success=3600)):
        health.check(fake, _settings(), "dag_ingest", "r1")
    assert "query:now" in fake.order


# --- the journal of the channel (S6/R9) ---------------------------------------

def test_a_sent_message_is_journaled_with_its_state_in_one_transaction():
    """Record, journal, commit: a state without its journal row would be a
    message the dashboard never saw."""
    fake = _Fake()
    with _patched(fake, facts=_facts(seconds_since_success=3600)):
        report = health.check(fake, _settings(), "dag_ingest", "r1")
        health.notify_ops(fake, _settings(), report)
    steps = [s for s in fake.order if s.startswith(("send:", "execute:", "commit"))]
    assert steps == ["send:freshness", "execute:%s" % health.RECORD_SQL,
                     "execute:%s" % health.JOURNAL_SQL, "commit"]
    row = fake.journaled[0]
    assert (row["control"], row["state"], row["channel"]) == ("freshness", "alert", "log")
    assert (row["dag_id"], row["run_id"]) == ("dag_ingest", "r1")
    facts = json.loads(row["facts"])
    assert all(v is None or isinstance(v, (int, float)) for v in facts.values())


def test_the_journal_insert_casts_what_psycopg_leaves_untyped():
    code = (ROOT / "sql" / "ops" / "journal_notification.sql").read_text(encoding="utf-8")
    assert "%(facts)s::jsonb" in code and "%(window_minutes)s::integer" in code


def test_the_journal_is_append_only_for_its_writer():
    ddl = (ROOT / "sql" / "ddl" / "135_ops_notifications.sql").read_text(encoding="utf-8")
    assert "GRANT SELECT, INSERT ON fd_ops_notifications TO fd_ingest" in ddl
    assert "REVOKE UPDATE, DELETE, TRUNCATE ON fd_ops_notifications FROM fd_ingest" in ddl
    assert "fd_ops_notifications_notification_id_seq TO fd_ingest" in ddl
