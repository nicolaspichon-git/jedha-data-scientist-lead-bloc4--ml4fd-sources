"""Telling somebody a transaction was flagged, and recording that it happened.

Covers: EE09, EI05, EC09, EC14, EC17
Lot: N5
Competency: C3, C4

WHAT A NOTIFICATION MAY CARRY, and this is the part worth getting right. An
alert is exactly the message where one wants to put the cardholder's name. It
cannot: fd_transactions holds no identifying column at all (EC17, decided in
S1), so a notification physically cannot leak one. The analyst resolves identity
INSIDE the system, never from the message.

client_key is excluded on purpose, and it is the only judgement call here. It is
an HMAC, so a stable PSEUDONYM linking a cardholder's whole history: a set of
them would let somebody group transactions by person without ever knowing a
name. It stays available to a channel that does not leave this system and is
never put in an outbound payload. EXTERNAL_FIELDS and INTERNAL_FIELDS say which
is which, and the SQL of sql/frauds/pending.sql does not even select it.

THE ORDER: SEND, THEN RECORD. With an external channel the two cannot be made
atomic, so the choice is which failure to prefer. A crash between them re-sends
on the next run, and txn_id travels as an idempotency key so a receiver can
discard the duplicate. The reverse order loses a fraud notification in silence
while the table claims it went out. A fraud notified twice is better than a
fraud never notified.

THE CAPACITY IS ENFORCED HERE. governance.yml declares 60 alerts a day, and
until this module nothing applied it -- the figure only ever picked a threshold.
Beyond 60 in a ROLLING 24 hours the alerts are still raised, and notification
stops with a loud line. A bad retrain then becomes visible instead of drowning
the analyst, which is what the capacity policy exists for.

THE CHANNELS, and why these two. `log` writes a structured line to the service
journal and `file` appends a JSON line to a declared path. Neither can fail for
a reason outside this stack, so the demonstration is reproducible -- and a real
channel (a webhook, a mail service) plugs into the same three-line interface
without touching anything here. There is no mail service in the stack and no way
to verify from here that SMTP would be reachable, so none is claimed.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass, fields as dataclass_fields
from pathlib import Path

from ml4fd import db

log = logging.getLogger(__name__)

RAISE_SQL = "frauds/raise_from_scores.sql"
PENDING_SQL = "frauds/pending.sql"
MARK_SQL = "frauds/mark_notified.sql"
FAILURE_SQL = "frauds/record_failure.sql"
WINDOW_SQL = "frauds/notified_last_day.sql"

# The triage facts, none of them personal: a merchant, a category, an amount, an
# instant, and the model's own figures.
EXTERNAL_FIELDS = ("txn_id", "txn_ts", "merchant", "category", "amt",
                   "score", "threshold", "model_version")
# Available to a channel that stays inside this system. client_key is a stable
# pseudonym over a cardholder's whole history, so it belongs here and nowhere
# else -- see the module docstring.
INTERNAL_FIELDS = EXTERNAL_FIELDS + ("raised_at", "attempts")

# Columns a payload may never hold, whatever a future caller passes. Checked and
# not merely documented: this is the one place where a mistake leaves the
# system.
FORBIDDEN_FIELDS = ("first", "last", "street", "city", "state", "zip", "lat",
                    "long", "city_pop", "job", "dob", "gender", "cc_num",
                    "client_key")


@dataclass(frozen=True)
class Alert:
    """One pending alert, as sql/frauds/pending.sql returns it."""
    txn_id: str
    score: float
    threshold: float
    model_version: str
    raised_at: object
    attempts: int
    txn_ts: object
    merchant: str
    category: str
    amt: float

    def payload(self, external: bool = True) -> dict:
        """What the channel is handed.

        external=True keeps only EXTERNAL_FIELDS. The filter is applied here,
        once, rather than left to each channel: a channel that forgot would leak
        quietly, and the only sign would be in somebody else's inbox.
        """
        fields = EXTERNAL_FIELDS if external else INTERNAL_FIELDS
        out = {}
        for name in fields:
            value = getattr(self, name, None)
            out[name] = (value.isoformat()
                         if isinstance(value, (dt.datetime, dt.date))
                         else value)
        leaked = [name for name in out if name in FORBIDDEN_FIELDS]
        if leaked:
            raise AssertionError(
                "the notification payload holds %s, which must never leave "
                "this system" % ", ".join(leaked))
        # THE SAME GUARD AS THE OPERATIONS PAYLOAD (health.payload_of): what a
        # channel receives is JSON-native, asserted here rather than
        # discovered in the channel. A Decimal arriving here is a defect of
        # to_alert(), and this message names the field instead of the channel
        # reporting "not JSON serializable" once a minute.
        for name, value in out.items():
            if value is not None and not isinstance(value, (str, int, float, bool)):
                raise AssertionError(
                    "the notification payload field %s is a %s, which no "
                    "channel can send: convert it in to_alert()"
                    % (name, type(value).__name__))
        return out


# The columns that are `numeric` in PostgreSQL and `float` in Alert.
NUMERIC_FIELDS = ("score", "threshold", "amt")


def to_alert(row) -> Alert:
    """Build an Alert from a row of sql/frauds/pending.sql, by POSITION.

    The order is the SELECT's, and tests/test_notify.py compares the two so an
    edit to the query cannot silently shift the amount into the category.

    THE NUMBERS ARE CONVERTED HERE, at the boundary where a row becomes an
    Alert, so that the types Alert declares are true. psycopg returns a
    `numeric` column as decimal.Decimal, and json.dumps refuses one. The defect
    of 2026-11-17: `TypeError: Object of type Decimal is not JSON
    serializable`, raised in the channel, recorded as a failure in last_error,
    and retried every minute -- an alert raised, retried for ever, never sent.
    The tests built their alerts from floats, which is why they never saw it;
    the same class of defect XCom met in S4 (dag_daily, 16 November).

    numeric(9,8) and numeric(12,2) fit a double exactly enough for a
    notification: 0.97000000 becomes 0.97, 1204.50 becomes 1204.5.
    """
    values = dict(zip((f.name for f in dataclass_fields(Alert)), row))
    for name in NUMERIC_FIELDS:
        if values.get(name) is not None:
            values[name] = float(values[name])
    if values.get("attempts") is not None:
        values["attempts"] = int(values["attempts"])
    return Alert(**values)


# --- the channels -------------------------------------------------------------

def send_log(payload: dict, settings) -> None:
    """A structured line in the service journal. Cannot fail on anything
    outside this stack, which is what makes the demonstration reproducible."""
    log.warning("ALERTE %s", json.dumps(payload, sort_keys=True,
                                        ensure_ascii=False))


def append_json_line(path, payload: dict) -> None:
    """Append one JSON object as a line, creating the parent directory.

    Created rather than assumed: this project has already lost 28 real
    observations to a directory that was not mounted, and an alert is worth
    more than an observation.

    Shared with the operations channel of N9, which writes to a DIFFERENT path
    -- the one thing the two channels must not have in common. One
    implementation, two destinations.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, ensure_ascii=False)
                     + "\n")


def send_file(payload: dict, settings) -> None:
    """One JSON line appended to the declared alert path."""
    append_json_line(settings.alert_file, payload)


CHANNELS = {"log": send_log, "file": send_file}


def channel_of(name: str):
    """The channel a name designates, refusing an unknown one.

    Refused and not defaulted: a typo in governance.yml would otherwise send
    every alert to whichever channel happened to be first, and nothing in the
    log would say so.
    """
    if name not in CHANNELS:
        raise ValueError(
            "notifications.channel is %r; known channels are %s. A real channel "
            "(webhook, mail) plugs into the same interface -- a function taking "
            "(payload, settings) -- and is declared here."
            % (name, ", ".join(sorted(CHANNELS))))
    return CHANNELS[name]


# --- the lifecycle ------------------------------------------------------------

def raise_alerts(conn) -> int:
    """Raise an alert for every flagged score that has none. Returns the count.

    Covers: EI05, EC23
    Lot: N5
    Competency: C3
    """
    raised = db.execute_file(conn, RAISE_SQL)
    if raised:
        log.info("%d alerte(s) levee(s)", raised)
    return raised


def window(conn) -> dict:
    """Notifications of the last rolling 24 hours, and what is waiting."""
    notified, pending, failed, total = db.query_file(conn, WINDOW_SQL)[0]
    return {"notified_24h": notified, "pending": pending,
            "pending_failed": failed, "total": total}


def budget(conn, capacity: int) -> int:
    """How many notifications may still go out in this rolling 24 hours."""
    return max(0, capacity - window(conn)["notified_24h"])


def pending(conn, limit: int) -> list[Alert]:
    rows = db.query_file(conn, PENDING_SQL, limit=limit)
    return [to_alert(row) for row in rows]


def notify(conn, settings, now_budget: int | None = None) -> dict:
    """Notify what is waiting, within the declared capacity.

    COMMITS AFTER EACH SUCCESSFUL SEND, and that granularity is the point: it
    bounds a crash to at most ONE duplicated notification instead of re-sending
    everything the run had already delivered.

    Covers: EI05, EC09, EC14
    Lot: N5
    Competency: C3, C4
    """
    channel_name = settings.alert_channel
    send = channel_of(channel_name)
    external = channel_name not in ("log", "file")

    state = window(conn)
    allowance = (now_budget if now_budget is not None
                 else max(0, settings.alert_capacity_per_day
                          - state["notified_24h"]))

    report = {"sent": 0, "failed": 0, "channel": channel_name,
              "pending_before": state["pending"],
              "pending_failed": state["pending_failed"],
              "notified_24h": state["notified_24h"],
              "capacity": settings.alert_capacity_per_day,
              "withheld": 0}

    if not state["pending"]:
        return report

    if allowance <= 0:
        report["withheld"] = state["pending"]
        log.error(
            "PLAFOND DE CAPACITE ATTEINT : %d notification(s) sur les 24 "
            "dernieres heures pour une capacite declaree de %d. %d alerte(s) "
            "restent LEVEES mais NON notifiees. Ce n'est pas une panne du "
            "canal : c'est la politique de capacite qui s'applique, et un "
            "volume a ce niveau signale un seuil mal place ou un "
            "reentrainement rate.",
            state["notified_24h"], settings.alert_capacity_per_day,
            state["pending"])
        return report

    waiting = pending(conn, allowance)
    report["withheld"] = max(0, state["pending"] - len(waiting))

    for alert in waiting:
        payload = alert.payload(external=external)
        try:
            send(payload, settings)
        except Exception as problem:            # noqa: BLE001
            # The alert stays raised with notified_at null: the next run picks
            # it up. Counted, not logged one by one -- 1,440 runs a day would
            # make one stuck alert unreadable.
            db.execute_file(conn, FAILURE_SQL, txn_id=alert.txn_id,
                            reason="%s: %s" % (type(problem).__name__, problem))
            conn.commit()
            report["failed"] += 1
            continue
        db.execute_file(conn, MARK_SQL, txn_id=alert.txn_id,
                        channel=channel_name)
        conn.commit()
        report["sent"] += 1

    return report


def render(report: dict) -> str:
    """One line per run, never one per alert."""
    parts = ["canal %s" % report["channel"],
             "%d notifiee(s)" % report["sent"]]
    if report["failed"]:
        parts.append("%d en echec" % report["failed"])
    if report["withheld"]:
        parts.append("%d retenue(s) par le plafond" % report["withheld"])
    if report["pending_failed"]:
        parts.append("%d en attente avaient deja echoue"
                     % report["pending_failed"])
    parts.append("%d/%d sur 24 h" % (report["notified_24h"] + report["sent"],
                                     report["capacity"]))
    return ", ".join(parts)
