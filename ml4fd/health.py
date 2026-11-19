"""The health of the ingestion pipeline, and telling the operator about it.

Covers: EC14, EC17
Lot: N9
Competency: C1, C2, C4

THE STAGE THE DESIGN HAD AND THE CODE DID NOT. Diagram 3.3 of the project
specifies a third stage of the ingestion DAG -- "(3c) dag_ingest : check" --
carrying four controls (freshness, empty run, rejection rate, volume invariant),
reading the last runs and the last rows, and emitting a "pipeline alert" to an
"ops channel (pipeline owner)". The development plan of S3 listed five tasks and
none of them was that one, so the stage was never built: the plan recorded its
own amputated version of the design as if it were the design.

THE TRACEABILITY TABLE COULD NOT SEE IT, which is the part worth remembering.
EC14 states three of these four controls in so many words -- "detect the
absence: missing run, zero-row run, stale data" -- and the table showed EC14 as
carried by code. It was: by ml4fd/guard.py, which is S2's TRAINING gate. That
code looks at a window, before training, and REFUSES TO TRAIN. This module looks
at the last minute, after ingesting, and TELLS SOMEBODY. A requirement can be
satisfied in a traceability table by code that does the job in another process,
at another moment, for another reader, and the table cannot tell the difference.

GATHERED, THEN JUDGED -- the split of guard.py, for the same reason: the
decision becomes a pure function of numbers and can be exercised without a
database. sql/checks/pipeline_health.sql collects the facts; controls() decides.

NOTIFIED ON TRANSITION, NEVER ON CONDITION. The DAG runs 1,440 times a day, so a
control that goes red stays red every minute until somebody acts: on the
condition this channel would send 1,440 identical messages a day and be ignored
by the second day. fd_ops_state holds the last state of each control and only a
CHANGE is sent -- including the change back to green, because an operator told
that ingestion stopped and never told it resumed still has to go and look, which
is the pull model this channel exists to replace.

A DIFFERENT AUDIENCE, SO A DIFFERENT DESTINATION. The analyst's channel of N5
notifies a flagged TRANSACTION; this one notifies a state of the CHAIN. Routing
pipeline noise into the analyst's queue would poison exactly the queue the
60-alerts-a-day capacity exists to protect -- an analyst who receives pipeline
messages stops reading alerts. The two share one implementation of "append a
JSON line" (notify.append_json_line) and nothing else: not the destination, not
the payload, not the policy.

AND NO TRANSACTION EVER TRAVELS HERE. The payload carries a control, a state, a
sentence and counts. FORBIDDEN_FIELDS extends N5's list with txn_id, amt,
merchant and score: not because they are personal -- they are not -- but because
an operations channel accumulating them would become a second, unmanaged copy of
the alert stream, outside the capacity policy and outside the analyst's queue.

A RED CONTROL IS NOT AN ERROR. controls() returns a verdict; it raises nothing.
A control task that failed the run on a red light would stop the ingestion it
exists to watch, and the DAG-level callback would then mark a successful run as
failed -- which is why runs.abandon() marks only what is still running.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import asdict, dataclass

from ml4fd import db, notify

log = logging.getLogger(__name__)

FACTS_SQL = "checks/pipeline_health.sql"
STATE_SQL = "ops/state.sql"
RECORD_SQL = "ops/record_state.sql"
JOURNAL_SQL = "ops/journal_notification.sql"

OK, ALERT = "ok", "alert"

# The four controls of diagram 3.3, in the order the diagram names them. The
# tuple is the reference: controls() returns exactly these, and a test compares
# the two so a control cannot be dropped the way the whole stage once was.
CONTROLS = ("freshness", "empty_run", "rejection_rate", "volume_invariant")

# THE DAG THESE CONTROLS WATCH, named once. It was spelled in the DAG, in
# report.py, in tools/ops_check.py and in tools/check_s3.py; the dashboard of
# S6 would have been the fifth. tests/test_dashboard_operations.py ties the
# DAG's own DAG_ID and the tools' defaults to this value.
WATCHED_DAG = "dag_ingest"

# What an operations message may hold. A declared whitelist and not "whatever
# the report has", so a later field added to the report cannot leave by this
# door without somebody naming it here.
#
# THE TRANSITION TRAVELS AS "became" AND NOT AS "state", and the reason was
# found by the whitelist refusing the payload. FORBIDDEN_FIELDS inherits N5's
# list, which holds "state" -- the CARDHOLDER'S US state, one of the thirteen
# identifying columns of the source. A key named "state" would therefore mean
# the control's state in this file and a person's address in the other, and a
# consumer reading both with one schema would read one as the other. "became"
# is also the more accurate word: what is sent is a change, not a condition.
OPS_FIELDS = ("control", "became", "detail", "run_id", "dag_id",
              "window_minutes", "raised_at", "facts")

# N5's list, plus the transaction's own facts. See the module docstring: the
# reason is not confidentiality, it is that this channel must not become a
# second copy of the alert stream.
FORBIDDEN_FIELDS = notify.FORBIDDEN_FIELDS + ("txn_id", "amt", "merchant",
                                              "score", "threshold")


# --- the facts ----------------------------------------------------------------

@dataclass(frozen=True)
class Facts:
    """What the journal knows about the recent runs. No judgement here."""
    dag_id: str
    window_minutes: int
    seconds_since_success: float | None
    runs_ended: int
    runs_failed: int
    runs_empty: int
    lines_read: int
    lines_rejected: int
    batches_breaking_invariant: int

    @property
    def empty_share(self) -> float:
        """Share of ended runs that stored nothing. Zero when none ended."""
        return self.runs_empty / self.runs_ended if self.runs_ended else 0.0

    @property
    def rejection_share(self) -> float:
        """Share of read lines that went to quarantine. Zero when none read."""
        return self.lines_rejected / self.lines_read if self.lines_read else 0.0

    def as_dict(self) -> dict:
        """The COUNTS, JSON-ready: XCom carries JSON, and so does the channel.

        dag_id is deliberately left out, and a test enforces it: payload_of()
        refuses a fact that is not a number, because a free-text field among
        the counts is exactly how a transaction would arrive in a message that
        must carry none. The DAG's name is identity rather than a fact about
        health, and the payload carries it at the top level, once.
        """
        out = asdict(self)
        out.pop("dag_id", None)
        out["empty_share"] = round(self.empty_share, 6)
        out["rejection_share"] = round(self.rejection_share, 6)
        if self.seconds_since_success is not None:
            out["seconds_since_success"] = round(self.seconds_since_success, 1)
        return out


def facts_of(conn, dag_id: str, window_start, window_end, as_of=None) -> Facts:
    """Read the facts of one DAG's runs over an EXPLICIT window.

    THE BOUNDS ARE THE CALLER'S, which is what lets this serve two callers that
    need opposite windows: N9's control task wants the last hour before now,
    S4's morning report wants the calendar day it covers and must be replayable.
    as_of=None leaves the instant of reference to the DATABASE clock, which is
    the right one for a freshness reading since the journal's timestamps are the
    server's.
    """
    row = db.query_file(conn, FACTS_SQL, dag_id=dag_id,
                        window_start=window_start, window_end=window_end,
                        as_of=as_of)[0]
    minutes = round((window_end - window_start).total_seconds() / 60)
    since = row[0]
    return Facts(
        dag_id=dag_id,
        window_minutes=int(minutes),
        seconds_since_success=None if since is None else float(since),
        runs_ended=int(row[1]),
        runs_failed=int(row[2]),
        runs_empty=int(row[3]),
        lines_read=int(row[4]),
        lines_rejected=int(row[5]),
        batches_breaking_invariant=int(row[6]),
    )


# --- the judgement, a pure function of the facts ------------------------------

@dataclass(frozen=True)
class Control:
    """One control's verdict, with a sentence an operator can act on."""
    control: str
    state: str
    detail: str

    @property
    def alert(self) -> bool:
        return self.state == ALERT

    def as_dict(self) -> dict:
        return {"control": self.control, "state": self.state,
                "detail": self.detail}


def freshness(facts: Facts, limit_s: float) -> Control:
    """How long since this pipeline last succeeded.

    Looks at the whole journal and not at the window: "nothing succeeded in the
    last hour" cannot distinguish an hour's outage from a day's, and the
    operator needs to know which.
    """
    if facts.seconds_since_success is None:
        return Control("freshness", ALERT,
                       "aucune execution reussie dans le journal : ce pipeline "
                       "n'a jamais abouti")
    minutes = facts.seconds_since_success / 60
    if facts.seconds_since_success > limit_s:
        return Control("freshness", ALERT,
                       "derniere execution reussie il y a %.0f min, limite "
                       "declaree %.0f min : l'ingestion est interrompue"
                       % (minutes, limit_s / 60))
    return Control("freshness", OK,
                   "derniere execution reussie il y a %.1f min" % minutes)


def empty_run(facts: Facts, max_share: float) -> Control:
    """Runs that ended having stored nothing.

    A share and not a count, because N0 measured about 5 % of calls failing: an
    empty minute happens and must not light the channel. Five calls a run make
    an entirely empty run very unlikely, so a share above a few per cent is a
    different event -- the API down, or the chain stopping before close_run.
    """
    if not facts.runs_ended:
        return Control("empty_run", OK,
                       "aucune execution terminee dans la fenetre de %d min : "
                       "c'est la fraicheur qui le dit" % facts.window_minutes)
    share = facts.empty_share
    detail = ("%d execution(s) vide(s) sur %d terminee(s) (%.1f %%), dont %d "
              "en echec" % (facts.runs_empty, facts.runs_ended, 100 * share,
                            facts.runs_failed))
    if share > max_share:
        return Control("empty_run", ALERT,
                       detail + ", au-dela de la part declaree de %.1f %%"
                       % (100 * max_share))
    return Control("empty_run", OK, detail)


def rejection_rate(facts: Facts, max_share: float) -> Control:
    """Share of read lines sent to quarantine.

    A rise means the stream changed shape -- a renamed field, a clock drifting
    past the tolerance -- and the transactions concerned are NOT scored. It is
    the control that catches a silent change at the source.
    """
    if not facts.lines_read:
        return Control("rejection_rate", OK,
                       "aucune ligne lue dans la fenetre de %d min : rien a "
                       "juger" % facts.window_minutes)
    share = facts.rejection_share
    detail = ("%d ligne(s) rejetee(s) sur %d lue(s) (%.2f %%)"
              % (facts.lines_rejected, facts.lines_read, 100 * share))
    if share > max_share:
        return Control("rejection_rate", ALERT,
                       detail + ", au-dela de la part declaree de %.2f %% : "
                       "le flux a change de forme et ces transactions ne sont "
                       "pas scorees" % (100 * max_share))
    return Control("rejection_rate", OK, detail)


def volume_invariant(facts: Facts) -> Control:
    """n_read = rows stored + lines quarantined, per batch.

    The CHECK on fd_pipeline_runs closes n_read against n_valid + n_rejected --
    the counters the run DECLARED. This closes the declaration against what is
    actually in the tables, which is the part no constraint can see.
    """
    broken = facts.batches_breaking_invariant
    if broken:
        return Control("volume_invariant", ALERT,
                       "%d lot(s) ou n_read differe des lignes stockees plus "
                       "les lignes mises en quarantaine : des transactions ont "
                       "ete lues et ne sont nulle part" % broken)
    return Control("volume_invariant", OK,
                   "les compteurs ferment sur tous les lots de la fenetre")


def controls(facts: Facts, freshness_limit_s: float, max_empty_share: float,
             max_rejection_share: float) -> list[Control]:
    """The four controls, in the order diagram 3.3 names them."""
    found = [freshness(facts, freshness_limit_s),
             empty_run(facts, max_empty_share),
             rejection_rate(facts, max_rejection_share),
             volume_invariant(facts)]
    names = tuple(control.control for control in found)
    if names != CONTROLS:
        raise AssertionError(
            "controls() returned %s; diagram 3.3 names %s" % (names, CONTROLS))
    return found


def controls_for(facts: Facts, settings) -> list[Control]:
    """The four controls at the DECLARED limits, read from governance.yml."""
    return controls(facts,
                    freshness_limit_s=settings.ops_freshness_minutes * 60,
                    max_empty_share=settings.ops_max_empty_share,
                    max_rejection_share=settings.ops_max_rejection_share)


# --- the transition -----------------------------------------------------------

@dataclass(frozen=True)
class Transition:
    """A control that CHANGED state. The only thing worth a message."""
    control: str
    state: str
    detail: str

    def as_dict(self) -> dict:
        return {"control": self.control, "state": self.state,
                "detail": self.detail}


def transitions(current: list[Control], stored: dict) -> list[Transition]:
    """What changed since the recorded state.

    A control ABSENT from the recorded state counts as ok, which is what makes
    the first run of a healthy pipeline silent: nothing has transitioned.
    """
    changed = []
    for control in current:
        was = stored.get(control.control, OK)
        if was != control.state:
            changed.append(Transition(control.control, control.state,
                                      control.detail))
    return changed


def stored_rows(conn) -> list[dict]:
    """The recorded state of each control WITH since when and why.

    The same query as stored_state() -- sql/ops/state.sql already returns the
    four columns -- read in full for whoever must say since WHEN a control has
    been red (the dashboard of S6).
    """
    return [{"control": control, "state": state, "since": since,
             "detail": detail}
            for control, state, since, detail in db.query_file(conn, STATE_SQL)]


def stored_state(conn) -> dict:
    """The recorded state of each control, by name."""
    return {row["control"]: row["state"] for row in stored_rows(conn)}


def record(conn, transition: Transition, run_id: str | None) -> None:
    """Record a transition, AFTER its message went out. See sql/ops/."""
    db.execute_file(conn, RECORD_SQL, control=transition.control,
                    state=transition.state, detail=transition.detail,
                    run_id=run_id)


def journal(conn, payload: dict, channel: str) -> None:
    """Journal a message that WAS SENT, in fd_ops_notifications (S6/R9).

    From the payload as it left -- checked by payload_of() -- and not from the
    transition: the journal says what the channel received.
    """
    db.execute_file(conn, JOURNAL_SQL, control=payload["control"],
                    state=payload["became"], detail=payload["detail"],
                    channel=channel, dag_id=payload.get("dag_id"),
                    run_id=payload.get("run_id"),
                    window_minutes=payload.get("window_minutes"),
                    facts=json.dumps(payload.get("facts") or {},
                                     sort_keys=True))


# --- the channel --------------------------------------------------------------

def payload_of(transition: Transition, report: dict) -> dict:
    """What the operations channel is handed.

    Built field by field from OPS_FIELDS and checked against FORBIDDEN_FIELDS
    here, once, rather than left to each channel: a channel that forgot would
    leak quietly, and the only sign would be in somebody else's inbox. The
    facts are counts, and that they are numbers is asserted rather than assumed.
    """
    out = {"control": transition.control,
           "became": transition.state,
           "detail": transition.detail,
           "run_id": report.get("run_id"),
           "dag_id": report.get("dag_id"),
           "window_minutes": report.get("window_minutes"),
           "raised_at": dt.datetime.now(dt.timezone.utc).isoformat(),
           "facts": report.get("facts", {})}

    unexpected = [name for name in out if name not in OPS_FIELDS]
    if unexpected:
        raise AssertionError(
            "the operations payload holds %s, which OPS_FIELDS does not declare"
            % ", ".join(sorted(unexpected)))
    leaked = [name for name in out["facts"] if name in FORBIDDEN_FIELDS]
    leaked += [name for name in out if name in FORBIDDEN_FIELDS]
    if leaked:
        raise AssertionError(
            "the operations payload holds %s, which belongs to the analyst's "
            "queue and never to this channel" % ", ".join(sorted(set(leaked))))
    for name, value in out["facts"].items():
        if value is not None and not isinstance(value, (int, float)):
            raise AssertionError(
                "facts.%s is %r, and the facts of this payload are counts: a "
                "free-text field here is how a transaction would arrive"
                % (name, value))
    return out


def send_log(payload: dict, settings) -> None:
    """A structured line in the service journal, at error level.

    Distinct from N5's ALERTE prefix on purpose: one grep separates the two
    audiences even when both channels are the journal.
    """
    log.error("EXPLOITATION %s", json.dumps(payload, sort_keys=True,
                                            ensure_ascii=False))


def send_file(payload: dict, settings) -> None:
    """One JSON line appended to the DECLARED OPERATIONS PATH.

    notify.append_json_line is shared; the path is not. ops.file and
    notifications.file must differ, and config refuses them equal.
    """
    notify.append_json_line(settings.ops_file, payload)


CHANNELS = {"log": send_log, "file": send_file}


def channel_of(name: str):
    """The operations channel a name designates, refusing an unknown one."""
    if name not in CHANNELS:
        raise ValueError(
            "ops.channel is %r; known channels are %s. A real channel (a "
            "webhook, a pager) plugs into the same interface -- a function "
            "taking (payload, settings) -- and is declared in ml4fd/health.py."
            % (name, ", ".join(sorted(CHANNELS))))
    return CHANNELS[name]


# --- the two steps the DAG runs -----------------------------------------------

def rolling_window(conn, minutes: int):
    """The last `minutes` before the DATABASE's now, as two instants.

    Read from the server and not from the container: the two clocks can differ,
    and the journal's timestamps are the server's. One round trip, once per run.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT now() - (%s * interval '1 minute'), now()",
                    (minutes,))
        return cur.fetchone()


def check(conn, settings, dag_id: str, run_id: str | None = None) -> dict:
    """Read the facts, judge them, and say what changed. RAISES ON NOTHING.

    A red control is this function's normal output, not an error: failing the
    run on one would stop the ingestion the control exists to watch.

    Covers: EC14
    Lot: N9
    Competency: C4
    """
    start, end = rolling_window(conn, settings.ops_window_minutes)
    facts = facts_of(conn, dag_id, start, end)
    current = controls_for(facts, settings)
    changed = transitions(current, stored_state(conn))
    return {"dag_id": dag_id, "run_id": run_id,
            "window_minutes": facts.window_minutes,
            "facts": facts.as_dict(),
            "controls": [control.as_dict() for control in current],
            "transitions": [change.as_dict() for change in changed],
            "in_alert": sum(1 for control in current if control.alert)}


def notify_ops(conn, settings, report: dict) -> dict:
    """Send each transition, then record it and journal it. SEND, THEN RECORD.

    The order is N5's and the reason is the same, turned around: recording
    first would mark the control as announced while nobody was told, and no
    later run would announce it -- there would no longer be a transition to
    see. That failure is silent and permanent; a duplicate is neither.

    Covers: EC14, EC17
    Lot: N9
    Competency: C3, C4
    """
    channel_name = settings.ops_channel
    send = channel_of(channel_name)
    sent, failed = 0, 0

    for change in report.get("transitions", []):
        transition = Transition(change["control"], change["state"],
                                change["detail"])
        payload = payload_of(transition, report)
        try:
            send(payload, settings)
        except Exception as problem:            # noqa: BLE001
            # NOT recorded: the state stays as it was, so the next run sees the
            # same transition and tries again.
            failed += 1
            log.exception("canal d'exploitation %r en echec sur %s : %s",
                          channel_name, transition.control, problem)
            continue
        # The state and its journal, in ONE transaction: a state recorded
        # without its journal row would be a message the dashboard never saw.
        record(conn, transition, report.get("run_id"))
        journal(conn, payload, channel_name)
        conn.commit()
        sent += 1

    return {"channel": channel_name, "sent": sent, "failed": failed,
            "transitions": len(report.get("transitions", [])),
            "in_alert": report.get("in_alert", 0)}


def render(report: dict) -> str:
    """One line per run, never one per control."""
    parts = ["%d/%d controle(s) en alerte"
             % (report.get("in_alert", 0), len(CONTROLS))]
    changed = report.get("transitions", [])
    if changed:
        parts.append("changement(s) : " + ", ".join(
            "%s -> %s" % (change["control"], change["state"])
            for change in changed))
    else:
        parts.append("aucun changement")
    facts = report.get("facts", {})
    if facts.get("seconds_since_success") is not None:
        parts.append("derniere reussite il y a %.1f min"
                     % (facts["seconds_since_success"] / 60))
    return ", ".join(parts)
