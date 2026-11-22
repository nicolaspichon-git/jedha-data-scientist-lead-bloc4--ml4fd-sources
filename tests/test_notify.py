"""What a notification may carry, and what the capacity now enforces.

Block: B3, B4.
Lot: B3-N5, B4-L0.
Competency: B3-C5.
Covers: B3-EE09, B3-EI05, B3-EC09, B3-EC14, B3-EC17.

The channels and the lifecycle are exercised against a fake connection: the
decisions worth testing -- which fields leave the system, what order the send
and the record happen in, what the ceiling does -- are all decisions this module
makes, not things PostgreSQL does.
"""
from __future__ import annotations

import datetime as dt
import json
import types

import pytest

from ml4fd import notify

WHEN = dt.datetime(2026, 11, 16, 14, 0, tzinfo=dt.timezone.utc)


def _alert(txn_id="abc:1794792372826", score=0.97, attempts=0):
    return notify.Alert(
        txn_id=txn_id, score=score, threshold=0.89, model_version="2",
        raised_at=WHEN, attempts=attempts,
        txn_ts=WHEN - dt.timedelta(seconds=5),
        merchant="fraud_Dach-Borer", category="grocery_net", amt=371.94)


def _settings(channel="log", path="/tmp/alerts.jsonl", capacity=60):
    return types.SimpleNamespace(alert_channel=channel, alert_file=str(path),
                                 alert_capacity_per_day=capacity,
                                 alert_delivery="at-least-once",
                                 enforce_capacity=True)


class _Fake:
    """A connection that answers the three queries this module runs."""

    def __init__(self, notified_24h=0, pending=None, pending_failed=0):
        self.rows = list(pending if pending is not None else [])
        self.notified_24h = notified_24h
        self.pending_failed = pending_failed
        self.marked = []
        self.failures = []
        self.raised = 0
        self.commits = 0
        self.order = []          # every call, in order

    # --- what ml4fd.db does to a connection
    def commit(self):
        self.commits += 1
        self.order.append("commit")


class _patched:
    """Replace ml4fd.db's two entry points, and optionally one channel.

    A context manager and not an autouse fixture: the restoration then belongs
    to the same three lines as the patch, it reads at the point of use, and it
    does not depend on a fixture feature. A patch whose undo lives elsewhere is
    how one test starts depending on another's cleanup.
    """

    def __init__(self, fake, channel=None):
        self.fake = fake
        self.channel = channel

    def __enter__(self):
        fake = self.fake
        self.saved = (notify.db.query_file, notify.db.execute_file,
                      notify.CHANNELS["log"])

        def query_file(conn, relative, **params):
            conn.order.append("query:%s" % relative)
            if relative == notify.WINDOW_SQL:
                return [(conn.notified_24h, len(conn.rows),
                         conn.pending_failed,
                         len(conn.rows) + conn.notified_24h)]
            if relative == notify.PENDING_SQL:
                return [tuple(getattr(alert, name) for name in _ROW_ORDER)
                        for alert in conn.rows[:params["limit"]]]
            raise AssertionError("requete inattendue : %s" % relative)

        def execute_file(conn, relative, **params):
            conn.order.append("execute:%s" % relative)
            if relative == notify.RAISE_SQL:
                conn.raised += 1
                return conn.raised
            if relative == notify.MARK_SQL:
                conn.marked.append(params["txn_id"])
                conn.rows = [a for a in conn.rows
                             if a.txn_id != params["txn_id"]]
                return 1
            if relative == notify.FAILURE_SQL:
                conn.failures.append((params["txn_id"], params["reason"]))
                return 1
            raise AssertionError("requete inattendue : %s" % relative)

        notify.db.query_file = query_file
        notify.db.execute_file = execute_file
        if self.channel is not None:
            notify.CHANNELS["log"] = self.channel
        return fake

    def __exit__(self, *exception):
        (notify.db.query_file, notify.db.execute_file,
         notify.CHANNELS["log"]) = self.saved
        return False


# The SELECT order of sql/frauds/pending.sql. to_alert() builds by POSITION, so
# this list and that query must agree -- see the test that compares them.
_ROW_ORDER = ("txn_id", "score", "threshold", "model_version", "raised_at",
              "attempts", "txn_ts", "merchant", "category", "amt")


# --- what may leave the system ------------------------------------------------

def test_no_identifying_field_can_be_in_a_payload():
    """fd_transactions holds none (EC17), so this cannot happen by accident --
    the check is here for a future caller who adds a column."""
    payload = _alert().payload()
    for name in notify.FORBIDDEN_FIELDS:
        assert name not in payload, name


def test_the_pseudonym_never_travels():
    """client_key is an HMAC: a STABLE pseudonym over a cardholder's whole
    history. A set of them would let somebody group transactions by person
    without ever knowing a name."""
    assert "client_key" in notify.FORBIDDEN_FIELDS
    assert "client_key" not in notify.INTERNAL_FIELDS
    assert "client_key" not in notify.EXTERNAL_FIELDS


def test_a_field_set_on_the_object_is_still_not_selected():
    """The payload is built from a DECLARED list, not from the object's
    attributes. So a column added to the Alert tomorrow does not reach a channel
    until somebody puts it in EXTERNAL_FIELDS on purpose."""
    alert = _alert()
    object.__setattr__(alert, "client_key", "deadbeef")
    assert "client_key" not in alert.payload(external=False)
    assert "client_key" not in alert.payload(external=True)


def test_the_guard_fires_if_a_forbidden_field_ever_reaches_the_list():
    """And if somebody does put one in the list, the payload refuses to be
    built. Verified by doing it, not by trusting the list."""
    saved = notify.EXTERNAL_FIELDS
    notify.EXTERNAL_FIELDS = saved + ("client_key",)
    try:
        with pytest.raises(AssertionError, match="never leave"):
            _alert().payload(external=True)
    finally:
        notify.EXTERNAL_FIELDS = saved


def test_the_external_payload_carries_the_triage_facts():
    payload = _alert().payload(external=True)
    assert set(payload) == set(notify.EXTERNAL_FIELDS)
    assert payload["amt"] == 371.94
    assert payload["merchant"] == "fraud_Dach-Borer"
    assert payload["score"] == 0.97


def test_timestamps_are_serialised():
    """json.dumps would raise on a datetime, inside the channel, after the alert
    had been counted as attempted."""
    payload = _alert().payload()
    assert isinstance(payload["txn_ts"], str)
    json.dumps(payload)


def test_the_internal_payload_adds_only_non_personal_context():
    extra = set(notify.INTERNAL_FIELDS) - set(notify.EXTERNAL_FIELDS)
    assert extra == {"raised_at", "attempts"}


# --- the row order agrees with the query --------------------------------------

def test_the_alert_is_built_by_position_in_the_query_order():
    """to_alert() builds by position, so a reordered SELECT would put the amount
    in the category without raising."""
    from ml4fd import db

    body = db.sql_path(notify.PENDING_SQL).read_text(encoding="utf-8")
    selected = []
    for line in body.splitlines():
        line = line.strip()
        if line.startswith("--") or not line:
            continue
        if line.upper().startswith("FROM"):
            break
        for piece in line.replace("SELECT", "").split(","):
            name = piece.strip().split(".")[-1].strip()
            if name:
                selected.append(name)
    assert tuple(selected) == _ROW_ORDER


def test_the_query_does_not_select_the_pseudonym():
    from ml4fd import db

    body = db.sql_path(notify.PENDING_SQL).read_text(encoding="utf-8")
    body = "\n".join(line for line in body.splitlines()
                     if not line.strip().startswith("--"))
    assert "client_key" not in body


# --- the channels -------------------------------------------------------------

def test_an_unknown_channel_is_refused_not_defaulted():
    """A typo would otherwise send every alert to whichever channel happened to
    be first, and nothing in the log would say so."""
    with pytest.raises(ValueError, match="notifications.channel"):
        notify.channel_of("webhoook")


def test_the_file_channel_appends_one_json_line_per_alert(tmp_path):
    path = tmp_path / "deep" / "alerts.jsonl"
    settings = _settings(channel="file", path=path)
    notify.send_file(_alert("a").payload(), settings)
    notify.send_file(_alert("b").payload(), settings)
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["txn_id"] == "a"


def test_the_file_channel_creates_its_directory(tmp_path):
    """This project has already lost 28 real observations to a directory that
    was not mounted. An alert is worth more than an observation."""
    path = tmp_path / "not" / "there" / "alerts.jsonl"
    notify.send_file(_alert().payload(), _settings(channel="file", path=path))
    assert path.is_file()


# --- the lifecycle ------------------------------------------------------------

def test_a_successful_send_is_recorded_after_the_send():
    """THE order decision: send, then record. A crash between them re-sends on
    the next run; the reverse order loses a fraud notification in silence while
    the table claims it went out."""
    sent = []
    fake = _Fake(pending=[_alert("a")])
    with _patched(fake, lambda payload, settings: sent.append(payload)):
        report = notify.notify(fake, _settings(channel="log"))
    assert report["sent"] == 1
    assert fake.marked == ["a"]
    assert fake.order.index("execute:%s" % notify.MARK_SQL) > 0
    assert sent                                  # the channel ran


def test_each_send_is_committed_on_its_own():
    """That granularity bounds a crash to ONE duplicated notification instead
    of re-sending everything the run had already delivered."""
    fake = _Fake(pending=[_alert("a"), _alert("b"), _alert("c")])
    with _patched(fake, lambda payload, settings: None):
        notify.notify(fake, _settings(channel="log"))
    assert fake.commits == 3


def test_a_failed_send_leaves_the_alert_raised_and_records_why():
    def broken(payload, settings):
        raise OSError("read-only file system")

    fake = _Fake(pending=[_alert("a")])
    with _patched(fake, broken):
        report = notify.notify(fake, _settings(channel="log"))
    assert report["failed"] == 1
    assert report["sent"] == 0
    assert not fake.marked
    assert fake.failures[0][0] == "a"
    assert "read-only" in fake.failures[0][1]


def test_one_failure_does_not_stop_the_others():
    calls = []

    def flaky(payload, settings):
        calls.append(payload["txn_id"])
        if payload["txn_id"] == "a":
            raise OSError("boom")

    fake = _Fake(pending=[_alert("a"), _alert("b")])
    with _patched(fake, flaky):
        report = notify.notify(fake, _settings(channel="log"))
    assert calls == ["a", "b"]
    assert (report["failed"], report["sent"]) == (1, 1)


# --- the capacity, now enforced -----------------------------------------------

def test_the_ceiling_withholds_notification_and_keeps_the_alerts():
    """Until N5 nothing applied alert_capacity_per_day: the figure only ever
    picked a threshold. A bad retrain would have drowned the analyst."""
    fake = _Fake(notified_24h=60, pending=[_alert("a"), _alert("b")])
    with _patched(fake):
        report = notify.notify(fake, _settings(capacity=60))
    assert report["sent"] == 0
    assert report["withheld"] == 2
    assert not fake.marked                 # raised, never notified


def test_a_partial_budget_sends_what_fits_and_withholds_the_rest():
    fake = _Fake(notified_24h=58, pending=[_alert(str(i)) for i in range(5)])
    with _patched(fake, lambda payload, settings: None):
        report = notify.notify(fake, _settings(channel="log", capacity=60))
    assert report["sent"] == 2
    assert report["withheld"] == 3


def test_nothing_pending_costs_one_query():
    """The task runs 1,440 times a day for about forty notifications, so it
    must be cheap when there is nothing to do."""
    fake = _Fake(pending=[])
    with _patched(fake):
        report = notify.notify(fake, _settings())
    assert report["sent"] == 0
    assert fake.order == ["query:%s" % notify.WINDOW_SQL]


def test_the_report_is_one_line_per_run():
    """Not one per alert: 1,440 runs a day would make one stuck alert
    unreadable."""
    line = notify.render({"channel": "file", "sent": 3, "failed": 1,
                          "withheld": 2, "pending_failed": 4,
                          "notified_24h": 10, "capacity": 60})
    assert "\n" not in line
    assert "canal file" in line
    assert "13/60" in line


# --- the defect of 2026-11-17: Decimal from PostgreSQL ---------------------------

def test_an_alert_built_from_a_real_row_can_be_sent():
    """psycopg returns `numeric` columns as decimal.Decimal. The tests above
    build alerts from floats, which is why `Object of type Decimal is not JSON
    serializable` reached production: raised in the channel, recorded in
    last_error, retried every minute, never sent."""
    import datetime as dt
    import json
    from decimal import Decimal

    from ml4fd import notify
    when = dt.datetime(2026, 11, 17, 21, 3, tzinfo=dt.timezone.utc)
    row = ("abc:2026-11-17T21:03:00", Decimal("0.97000000"), Decimal("0.93000000"),
           "v7", when, Decimal("3"), when, "fraud_Kub", "shopping_net",
           Decimal("1204.50"))
    alert = notify.to_alert(row)
    assert isinstance(alert.score, float) and alert.score == 0.97
    assert isinstance(alert.amt, float) and alert.amt == 1204.5
    assert alert.attempts == 3
    for external in (True, False):
        json.dumps(alert.payload(external=external))          # must not raise


def test_a_payload_that_no_channel_can_send_is_refused_by_name():
    """The guard the operations payload already had: the field is named at the
    source, instead of the channel failing once a minute."""
    import datetime as dt
    from decimal import Decimal

    import pytest

    from ml4fd import notify
    when = dt.datetime(2026, 11, 17, 21, 3, tzinfo=dt.timezone.utc)
    raw = notify.Alert("abc", Decimal("0.97"), 0.93, "v7", when, 1, when,
                       "m", "c", 10.0)
    with pytest.raises(AssertionError) as refused:
        raw.payload()
    assert "score" in str(refused.value) and "Decimal" in str(refused.value)
