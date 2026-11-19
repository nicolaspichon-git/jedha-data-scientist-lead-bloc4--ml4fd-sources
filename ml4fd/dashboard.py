"""The reads behind the dashboard of S6. NOTHING HERE IMPORTS STREAMLIT.

Covers: EE03, EC14, EC17
Lot: R1
Competency: C4

THE DASHBOARD DECIDES NOTHING. Every decision it shows was taken elsewhere and
persisted (P5): the alert by the scoring, the notification by N5, the report by
S4. So almost every function here is a delegation to the definition another
process already uses -- report.alerts() for the alerts of a day, so the page
shows by construction the alerts the document of the same day lists;
report.window_for() for the day; report.check_document() for the digest;
notify.window() and notify.budget() for the capacity; s4_absence.sql for the
missing mornings. A dashboard that recomputed any of them would be a second
answer to a question that already has one (P12).

WHY PURE FUNCTIONS. Streamlit cannot be installed where this is developed (no
access to the package index). Everything a page needs is therefore here, tested
by pytest, and the pages only display it. Their first real run is in the
container.

WHO IT RUNS AS. connect() refuses to serve under any role but fd_dashboard, and
under any superuser: the one container open to a browser must not quietly run
with the owner's credentials because somebody reused the wrong anchor in
docker-compose.yml.
"""
from __future__ import annotations

import datetime as dt
import zoneinfo
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from ml4fd import checks, db, health, notify, report

ROLE = "fd_dashboard"
REPORTS_SQL = "dashboard/reports.sql"
ABSENCE_SQL = "checks/s4_absence.sql"
RUNS_SQL = "dashboard/recent_runs.sql"
RUNS_LISTED = 30
REJECTS_SQL = "dashboard/rejects_of_day.sql"
REJECTS_TOTAL_SQL = "dashboard/rejects_total_of_day.sql"
STREAM_RATE_SQL = "dashboard/stream_rate_of_day.sql"
REPORTS_LISTED = 90

# WHAT THE ALERTS PAGE SHOWS, and nothing else. A whitelist and not a
# blacklist, for the reason ml4fd/health.py gives: a field added to a query
# tomorrow must be DECIDED onto the page, not leak onto it. client_key is
# absent on purpose -- a pseudonym, but the analyst has no use for it, and what
# is not displayed is not copied (EC17).
FRAUD_FIELDS = ("txn_ts", "amt", "merchant", "category", "score", "threshold",
                "model_version", "raised_at", "notified", "notified_at",
                "channel", "attempts", "last_error", "f_tx_count",
                "f_avg_amount", "times_usual", "expected_loss", "txn_id")


class WrongIdentity(RuntimeError):
    """The dashboard is connected as somebody it must not be."""


# --- the connection -----------------------------------------------------------

def assert_identity(conn) -> str:
    """Refuse to serve unless the session is fd_dashboard and not a superuser.

    Checked on the SESSION and not on the configuration: what matters is who
    PostgreSQL thinks we are, whatever the environment claimed.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT current_user::text, r.rolsuper "
                    "  FROM pg_roles r WHERE r.rolname = current_user")
        user, superuser = cur.fetchone()
    if superuser:
        raise WrongIdentity(
            "le tableau de bord est connecte en SUPER-UTILISATEUR (%s). Le "
            "service doit recevoir les identifiants de fd_dashboard, jamais "
            "ceux du proprietaire : verifier qu'il n'herite pas de "
            "x-airflow-common dans docker-compose.yml." % user)
    if user != ROLE:
        raise WrongIdentity(
            "le tableau de bord est connecte en %s et non en %s : il ne sert "
            "rien sous un autre role." % (user, ROLE))
    return user


@contextmanager
def connect(settings):
    """A connection as fd_dashboard, verified before anything is read.

    No SET ROLE: the service logs in as fd_dashboard directly (S6/R0), so the
    role argument of db.connect() is None and nothing can be RESET.
    """
    with db.connect(None, settings) as conn:
        assert_identity(conn)
        yield conn


# --- the day ------------------------------------------------------------------

def today(settings, now: dt.datetime | None = None) -> dt.date:
    """Today, in the zone the reports count days in (report.timezone).

    Not the server's date: a page that counted in UTC would not find the same
    alerts as the document of the same day.
    """
    zone = zoneinfo.ZoneInfo(settings.report_timezone)
    return (now or dt.datetime.now(dt.timezone.utc)).astimezone(zone).date()


# THE LONGEST PERIOD A PAGE ACCEPTS. The quarantine is purged at 90 days, so a
# longer period of rejections would be silently incomplete; and the alerts of a
# season are a query, not a page.
MAX_PERIOD_DAYS = 92


def period_of(chosen, fallback: dt.date) -> tuple[dt.date, dt.date]:
    """(first, last) from what a date-range widget returns.

    Streamlit's range picker returns ONE date while the second click is still
    awaited, two once it is made, and the reader may click them in either
    order. Nothing selected falls back to `fallback`.
    """
    if chosen is None:
        return fallback, fallback
    if isinstance(chosen, dt.date):
        return chosen, chosen
    days = [d for d in chosen if d is not None]
    if not days:
        return fallback, fallback
    return min(days), max(days)


def bounded(first: dt.date, last: dt.date) -> tuple[dt.date, dt.date, bool]:
    """Clip a period to MAX_PERIOD_DAYS, keeping its END. Says if it did."""
    if (last - first).days + 1 > MAX_PERIOD_DAYS:
        return last - dt.timedelta(days=MAX_PERIOD_DAYS - 1), last, True
    return first, last, False


def period_label(first: dt.date, last: dt.date) -> str:
    if first == last:
        return "le %s" % report._day(first)
    return "du %s au %s" % (report._day(first), report._day(last))


# --- the morning report (R2) --------------------------------------------------

def reports(conn, limit: int = REPORTS_LISTED) -> list[dict]:
    """The reports on record, most recent first, with their counts."""
    return report._rows(conn, REPORTS_SQL, {"limit": limit})


def missing_mornings(conn) -> list[dict]:
    """The mornings with no report -- S4's own definition, not a second one.

    A wrong report is seen; an absent one is not (EC14).
    """
    return report._rows(conn, ABSENCE_SQL, {})


@dataclass(frozen=True)
class Morning:
    """One report row, and its document as judged on this container's disk."""
    row: dict
    check: report.DocumentCheck
    relocated: bool

    @property
    def day(self) -> dt.date:
        return self.row["report_date"]


def morning(settings, row: dict) -> Morning:
    """The document of one report row, and whether it is the one recorded.

    The path is report.path_for()'s -- the rule that chose where to write --
    and NOT fd_reports.path, which is a path of the Airflow container.
    `relocated` says the two differ: the containers do not mount reports/ at
    the same place, which is the defect that has already cost this project 28
    observations and a whole dataset.
    """
    here = report.path_for(settings, row["report_date"])
    check = report.check_document(here, row["content_digest"])
    return Morning(row=row, check=check,
                   relocated=Path(row["path"]) != Path(here))


# --- the alerts (R3) ----------------------------------------------------------

def frauds_of_span(conn, settings, first: dt.date, last: dt.date) -> list[dict]:
    """The alerts of calendar days first..last, projected onto FRAUD_FIELDS.

    report.alerts() over report.span_for(): the report's own query, over the
    days the report would cover one by one. Ordered as the report orders them,
    by expected loss.
    """
    rows = report.alerts(conn, report.span_for(settings, first, last))
    return [{name: row.get(name) for name in FRAUD_FIELDS} for row in rows]


def frauds_of_day(conn, settings, day: dt.date) -> list[dict]:
    """The alerts of one calendar day: the document of that day lists them."""
    return frauds_of_span(conn, settings, day, day)


def capacity(conn, settings) -> dict:
    """The notification capacity, as N5 counts it: a ROLLING 24 hours.

    Not the calendar day of the page, and the page must say so: N5 enforces
    the capacity over a rolling window so that 60 alerts at 23:50 and 60 more
    at 00:10 cannot both go out. Read with notify's own functions.
    """
    state = notify.window(conn)
    limit = settings.alert_capacity_per_day
    remaining = notify.budget(conn, limit)
    return {"capacity": limit,
            "notified_24h": state["notified_24h"],
            "pending": state["pending"],
            "pending_failed": state["pending_failed"],
            "remaining": remaining,
            "reached": remaining == 0}


def summary_of(alerts: list[dict]) -> dict:
    """Three numbers for the head of the page, from the rows it shows."""
    notified = sum(1 for a in alerts if a["notified"])
    return {"alerts": len(alerts), "notified": notified,
            "not_notified": len(alerts) - notified}


# --- the morning report, as a page shows it (R2) ------------------------------
#
# WHAT THE PAGE DISPLAYS IS DECIDED HERE, and in this order, so that pytest can
# hold it: the page itself only maps each notice to a Streamlit call. Notices
# come FIRST and the document LAST -- a warning under a long document is a
# warning nobody reads.
#
# The figures are read from the fd_reports row, never from the document's
# prose: the decision of the S4 plan, so that a reworded sentence cannot change
# a number. Numbers and dates are written by the report's own formatters, so
# the page and the document agree to the character.

ERROR, WARNING, SUCCESS, INFO = "error", "warning", "success", "info"


@dataclass(frozen=True)
class Notice:
    level: str
    text: str


def _local(settings, value: dt.datetime | None) -> str:
    """An instant in the declared zone. PostgreSQL hands timestamps back in the
    SESSION's zone, which is UTC in the postgres image: shown as such, the
    production time of a 06:00 report would read 05:00."""
    if value is None:
        return "n/a"
    return report._instant(value.astimezone(zoneinfo.ZoneInfo(settings.report_timezone)))


def _short(digest: str | None) -> str:
    return digest[:12] if digest else "--"


def morning_view(conn, settings, day: dt.date | None = None) -> dict:
    """Everything the morning page shows, for one day, in display order.

    `day` defaults to the most recent report on record; a day with no report is
    not invented -- the most recent one is shown instead and the selector says
    which.
    """
    rows = reports(conn)
    missing = [r["journee_sans_rapport"] for r in missing_mornings(conn)]
    notices: list[Notice] = []

    # ABSENCES FIRST, before any date is chosen: they concern the whole series,
    # and a reader who looks only at today's report would never learn that
    # Saturday's was never produced (EC14).
    if missing:
        notices.append(Notice(WARNING, "%d %s sans rapport : %s." % (
            len(missing), report._plural(len(missing), "matin"),
            ", ".join(report._day(d) for d in missing))))

    if not rows:
        notices.append(Notice(INFO,
            "Aucun rapport n'a encore été produit. Il est écrit chaque matin à "
            "06:00 par dag_daily : le DAG est-il activé ?"))
        return {"dates": [], "selected": None, "notices": notices,
                "figures": [], "document": None, "title": None}

    by_day = {r["report_date"]: r for r in rows}
    selected = day if day in by_day else rows[0]["report_date"]
    m = morning(settings, by_day[selected])

    # THE DIGEST, before the document it judges.
    if m.check.verdict == report.ABSENT:
        notices.append(Notice(ERROR,
            "Le document du %s est enregistré mais introuvable sur ce disque "
            "(%s). Les chiffres ci-dessous viennent de fd_reports ; le texte "
            "n'est pas affichable." % (report._day(selected), m.check.path)))
    elif m.check.verdict == report.DIFFERENTE:
        notices.append(Notice(ERROR,
            "Ce document NE CORRESPOND PAS à son empreinte enregistrée : il a "
            "été modifié après sa production. Empreinte enregistrée %s…, "
            "empreinte du fichier %s…. Le texte affiché plus bas est celui du "
            "fichier, pas celui qui a été produit."
            % (_short(m.check.expected), _short(m.check.actual))))
    else:
        notices.append(Notice(SUCCESS,
            "Document conforme à son empreinte enregistrée (sha256 %s…)."
            % _short(m.check.expected)))

    if m.relocated:
        notices.append(Notice(WARNING,
            "Le document a été enregistré à %s et ce tableau de bord le lit à "
            "%s : les deux conteneurs ne montent pas reports/ au même endroit."
            % (m.row["path"], m.check.path)))

    r = m.row
    figures = [
        ("Paiements", report._count(r["n_payments"])),
        ("Alertes", report._count(r["n_alerts"])),
        ("Verdicts échus", report._count(r["n_verdicts"])),
        ("En attente de verdict", report._count(r["n_pending"])),
        ("Modèle", r["model_version"] or "aucune alerte, aucun modèle à nommer"),
        ("Seuil", report._ratio(r["threshold"]) if r["threshold"] is not None
         else "--"),
        ("Connaissance au", _local(settings, r["knowledge_date"])),
        ("Produit le", _local(settings, r["produced_at"])),
    ]
    return {"dates": [r["report_date"] for r in rows], "selected": selected,
            "notices": notices, "figures": figures,
            "document": m.check.text,
            "title": "Rapport du %s" % report._day(selected)}


# --- the alerts of a day, as a page shows them (R3) ---------------------------
#
# Same division as the morning page: what is shown, and in what order, is
# decided here. Two clocks meet on this page and must not be confused: the DAY
# shown is a calendar day in the declared zone, like the report's; the
# CAPACITY is N5's rolling 24 hours. A capacity reached is a DECISION of the
# notifier -- alerts still raised, notification withheld -- and the page must
# say so, or it reads as an outage.

# The columns of the table, in order, with the French header the analyst reads.
# Each one is a field of FRAUD_FIELDS: nothing reaches the table that the
# whitelist did not let through.
FRAUD_COLUMNS = (
    ("txn_ts", "Date et heure"),
    ("amt", "Montant"),
    ("merchant", "Commerçant"),
    ("category", "Catégorie"),
    ("score", "Score"),
    ("threshold", "Seuil"),
    ("times_usual", "× habituel"),
    ("expected_loss", "Perte attendue"),
    ("notified_at", "Notifiée le"),
    ("channel", "Canal"),
    ("attempts", "Tentatives"),
    ("last_error", "Dernière erreur"),
    ("model_version", "Modèle"),
    ("txn_id", "Transaction"),
)


def _hour(settings, value: dt.datetime | None) -> str:
    """Date AND time: over a period, an hour alone does not say which day."""
    if value is None:
        return "n/a"
    return value.astimezone(zoneinfo.ZoneInfo(settings.report_timezone)
                            ).strftime("%Y-%m-%d %H:%M:%S")


def _cell(settings, name: str, value) -> str:
    """One cell, written as the report writes the same quantity."""
    if name == "txn_ts":
        return _hour(settings, value)
    if name in ("amt", "expected_loss"):
        return report._amount(value)
    if name in ("score", "threshold"):
        return report._ratio(value)
    if name == "times_usual":
        return "n/a" if value is None else str(value).replace(".", ",")
    if name == "notified_at":
        # "non" and not "n/a": a notification that has not happened is a state
        # of the alert, not a missing measurement.
        return "non" if value is None else _hour(settings, value)
    if name == "attempts":
        return report._count(value)
    return "" if value is None else str(value)


def frauds_view(conn, settings, first: dt.date | None = None,
                last: dt.date | None = None,
                now: dt.datetime | None = None) -> dict:
    """Everything the alerts page shows, for calendar days first..last.

    Both default to today in the declared zone. The rows are the report's own,
    through frauds_of_span(): the page lists what the documents of those days
    list, in the same order -- by expected loss.
    """
    first = first or today(settings, now)
    last = last or first
    first, last, clipped = bounded(first, last)
    rows = frauds_of_span(conn, settings, first, last)
    head = summary_of(rows)
    cap = capacity(conn, settings)
    period = period_label(first, last)
    notices: list[Notice] = []

    if clipped:
        notices.append(Notice(INFO,
            "Période ramenée à %d jours, du %s au %s." % (
                MAX_PERIOD_DAYS, report._day(first), report._day(last))))
    # FAILURES FIRST: an alert that could not be sent is the one thing on this
    # page somebody must act on.
    if cap["pending_failed"]:
        notices.append(Notice(ERROR,
            "%d %s en échec d'envoi, toutes journées confondues : le canal a "
            "refusé, la notification sera retentée au prochain passage. "
            "Certaines peuvent être hors de la période affichée ; voir la "
            "colonne « Dernière erreur »."
            % (cap["pending_failed"],
               report._plural(cap["pending_failed"], "alerte"))))
    if cap["reached"]:
        notices.append(Notice(WARNING,
            "Plafond de notification atteint : %s sur les dernières 24 heures "
            "glissantes, pour une capacité de %s. Les alertes suivantes sont "
            "levées mais leur notification est retenue (N5) : c'est une "
            "décision, pas une panne. Un réentraînement qui sur-alerte se "
            "voit ici."
            % (report._count(cap["notified_24h"]), report._count(cap["capacity"]))))
    if not rows:
        notices.append(Notice(INFO, "Aucune alerte levée %s." % period))

    figures = [
        ("Alertes de la période", report._count(head["alerts"])),
        ("Notifiées", report._count(head["notified"])),
        ("Non notifiées", report._count(head["not_notified"])),
        ("Notifiées sur 24 h glissantes",
         "%s / %s" % (report._count(cap["notified_24h"]),
                      report._count(cap["capacity"]))),
    ]
    table = [{label: _cell(settings, name, row.get(name))
              for name, label in FRAUD_COLUMNS} for row in rows]
    return {"first": first, "last": last, "notices": notices,
            "figures": figures, "table": table,
            "capacity_note": (
                "Canal déclaré : « %s » (governance.yml, notifications.channel). "
                "La colonne « Canal » porte celui qu'a enregistré l'envoi : "
                "vide, la notification n'est pas encore partie. La capacité se "
                "compte sur 24 heures glissantes, comme le notifieur, et non "
                "sur la période affichée. %s alerte(s) en attente de "
                "notification, toutes journées confondues."
                % (settings.alert_channel, report._count(cap["pending"]))),
            "title": "Alertes %s" % period}


# --- the state of the chain, as a page shows it (R6) --------------------------
#
# THE CONTROLS ARE N9's, NOT A SECOND SET. health.check() is what the task
# raise_and_notify_ops runs every minute: the same facts, the same four
# judgements, the same declared limits, compared with the same recorded state.
# It reads and raises nothing -- a red control is its normal output -- so the
# page can call it as often as it is opened. What the page shows is therefore
# what the control task would say at that instant (P9: an absence is watched
# as closely as an error).

CONTROL_LABELS = {"freshness": "Fressness check",
                  "empty_run": "Empty run",
                  "rejection_rate": "Rejection rate",
                  "volume_invariant": "Volume invariant"}


def _limit(settings, control: str) -> str:
    """The DECLARED limit of a control, as governance.yml states it."""
    if control == "freshness":
        return "%d min sans réussite" % settings.ops_freshness_minutes
    if control == "empty_run":
        return "%s des exécutions" % report._share(settings.ops_max_empty_share, 0)
    if control == "rejection_rate":
        return "%s des lignes lues" % report._share(settings.ops_max_rejection_share, 0)
    return "0 lot"


def _since(seconds) -> str:
    if seconds is None:
        return "aucune réussite enregistrée"
    minutes = int(seconds // 60)
    if minutes < 1:
        return "il y a moins d'une minute"
    if minutes < 60:
        return "il y a %d min" % minutes
    return "il y a %d h %02d min" % (minutes // 60, minutes % 60)


def recent_runs(conn, limit: int = RUNS_LISTED) -> list[dict]:
    return report._rows(conn, RUNS_SQL, {"dag_id": health.WATCHED_DAG,
                                         "limit": limit})


def operations_view(conn, settings) -> dict:
    """Everything the operations page shows, in display order."""
    checked = health.check(conn, settings, health.WATCHED_DAG)
    recorded = {row["control"]: row for row in health.stored_rows(conn)}
    facts = checked["facts"]
    window = checked["window_minutes"]
    notices: list[Notice] = []

    red = [c for c in checked["controls"] if c["state"] == health.ALERT]
    for control in red:
        notices.append(Notice(ERROR, "%s : %s." % (
            CONTROL_LABELS[control["control"]], control["detail"])))
    if checked["transitions"]:
        names = ", ".join(CONTROL_LABELS[t["control"]].lower()
                          for t in checked["transitions"])
        notices.append(Notice(WARNING,
            "Changement d'état pas encore notifié (%s) : la prochaine exécution "
            "de %s l'enverra au canal d'exploitation. Si ce message persiste, "
            "le DAG ne tourne plus." % (names, health.WATCHED_DAG)))
    if not red:
        notices.append(Notice(SUCCESS,
            "Les contrôles sont positifs sur les %d dernières minutes."
            % window))

    controls = []
    for control in checked["controls"]:
        name = control["control"]
        kept = recorded.get(name)
        controls.append({
            "Contrôle": CONTROL_LABELS[name],
            "État": "ALERTE" if control["state"] == health.ALERT else "ok",
            "Constat": control["detail"],
            "Seuil déclaré": _limit(settings, name),
            "Dernier état notifié": (kept["state"] if kept else "jamais"),
            "Depuis": (_local(settings, kept["since"]) if kept else "--"),
        })

    figures = [
        ("Dernière réussite", _since(facts.get("seconds_since_success"))),
        ("Exécutions terminées", report._count(facts["runs_ended"])),
        ("En échec", report._count(facts["runs_failed"])),
        ("Vides", report._count(facts["runs_empty"])),
        ("Lignes lues", report._count(facts["lines_read"])),
        ("Lignes rejetées", report._count(facts["lines_rejected"])),
    ]

    runs = [{"Début": _local(settings, r["started_at"]),
             "Durée (s)": "--" if r["seconds"] is None
                          else "%.0f" % float(r["seconds"]),
             "État": r["state"],
             "Lues": report._count(r["n_read"]),
             "Valides": report._count(r["n_valid"]),
             "Rejetées": report._count(r["n_rejected"]),
             "Exécution": r["run_id"]}
            for r in recent_runs(conn)]

    return {"notices": notices, "figures": figures, "controls": controls,
            "runs": runs,
            "note": ("Fenêtre glissante de %d minutes, à l'horloge du serveur "
                     "PostgreSQL. Ce sont les contrôles de la tâche "
                     "raise_and_notify_ops de %s : même définition, mêmes "
                     "seuils, lus dans governance.yml." % (window,
                                                         health.WATCHED_DAG))}


# --- the operations channel, as a page shows it (R9) --------------------------
#
# What the channel was TOLD, from fd_ops_notifications -- one row per message
# sent, written by notify_ops after the send. Not what the controls say now:
# that is the Exploitation page, and the two can differ for one run, between a
# transition seen and its message sent.

OPS_SQL = "dashboard/ops_notifications.sql"


def _lasted(seconds) -> str:
    """The length of an incident, in the largest units that say it."""
    if seconds is None:
        return "--"
    minutes = int(float(seconds) // 60)
    if minutes < 1:
        return "moins d'une minute"
    if minutes < 60:
        return "%d min" % minutes
    hours = minutes // 60
    if hours < 24:
        return "%d h %02d min" % (hours, minutes % 60)
    return "%d j %d h" % (hours // 24, hours % 24)


def ops_view(conn, settings, first: dt.date | None = None,
             last: dt.date | None = None,
             now: dt.datetime | None = None) -> dict:
    """Everything the operations-channel page shows, for days first..last."""
    first = first or today(settings, now)
    last = last or first
    first, last, clipped = bounded(first, last)
    period = period_label(first, last)
    window = report.span_for(settings, first, last)
    sent = report._rows(conn, OPS_SQL, window.params())
    recorded = health.stored_rows(conn)
    notices: list[Notice] = []

    if clipped:
        notices.append(Notice(INFO,
            "Période ramenée à %d jours, du %s au %s." % (
                MAX_PERIOD_DAYS, report._day(first), report._day(last))))
    # WHAT IS STILL RED comes first, from the recorded state: a control whose
    # alert was sent before the period is still red, and silent in the period.
    for row in recorded:
        if row["state"] == health.ALERT:
            notices.append(Notice(ERROR,
                "%s en alerte depuis le %s : %s." % (
                    CONTROL_LABELS.get(row["control"], row["control"]),
                    _local(settings, row["since"]), row["detail"])))
    if not sent:
        notices.append(Notice(INFO,
            "Aucun message envoyé %s. Le canal ne parle qu'aux changements "
            "d'état : un silence veut dire qu'aucun contrôle n'a changé, pas "
            "que le canal est muet." % period))

    raised = sum(1 for r in sent if r["state"] == health.ALERT)
    cleared = sum(1 for r in sent if r["state"] == health.OK)
    lengths = [float(r["incident_seconds"]) for r in sent
               if r["incident_seconds"] is not None]
    figures = [
        ("Messages envoyés", report._count(len(sent))),
        ("Passages en alerte", report._count(raised)),
        ("Retours au vert", report._count(cleared)),
        ("Incident le plus long", _lasted(max(lengths)) if lengths else "--"),
    ]
    table = [{"Envoyé le": _local(settings, r["sent_at"]),
              "Contrôle": CONTROL_LABELS.get(r["control"], r["control"]),
              "Devenu": "ALERTE" if r["state"] == health.ALERT else "ok",
              "Constat": r["detail"],
              "Durée de l'incident": _lasted(r["incident_seconds"]),
              "Canal": r["channel"],
              "Exécution": r["run_id"] or "--"}
             for r in sent]
    return {"first": first, "last": last, "notices": notices,
            "figures": figures, "table": table,
            "title": "Messages %s" % period,
            "note": ("Canal déclaré : « %s » (governance.yml, ops.channel). "
                     "Un message par changement d'état, envoyé par la tâche "
                     "raise_and_notify_ops de %s puis journalisé : ce journal "
                     "dit ce qui est PARTI, pas ce qui a été lu. La durée "
                     "d'un incident va du message d'alerte au message de "
                     "retour au vert." % (settings.ops_channel,
                                          health.WATCHED_DAG))}


# --- the performance of the model, as a page shows it (R10) -------------------
#
# Section 3 of the report over a period: the verdicts that BECAME KNOWN in it,
# judged against what M5 measured on the test set. report.verdicts() and
# report.reference() are the report's own functions -- the page and the
# documents compare the same numbers with the same reference.

PERF_BY_MODEL_SQL = "dashboard/performances_by_model.sql"
PERF_BY_DAY_SQL = "dashboard/verdicts_by_day.sql"

# A month by default: verdicts arrive a few a day, and one day says little.
PERF_DEFAULT_DAYS = 30


def _ratio_value(value) -> float | None:
    """A ratio as a percentage for a chart; None stays None -- a gap, not a 0."""
    return None if value is None else round(100 * float(value), 2)


def performances_view(conn, settings, first: dt.date | None = None,
                     last: dt.date | None = None,
                     now: dt.datetime | None = None) -> dict:
    """Everything the performance page shows, for days first..last."""
    last = last or today(settings, now)
    first = first or last - dt.timedelta(days=PERF_DEFAULT_DAYS - 1)
    first, last, clipped = bounded(first, last)
    period = period_label(first, last)
    window = report.span_for(settings, first, last)
    seen = report.verdicts(conn, window)
    ref = report.reference(conn, settings)
    models = report._rows(conn, PERF_BY_MODEL_SQL, window.params())
    days = report._rows(conn, PERF_BY_DAY_SQL,
                        {**window.params(), "zone": settings.report_timezone})
    notices: list[Notice] = []

    if clipped:
        notices.append(Notice(INFO,
            "Période ramenée à %d jours, du %s au %s." % (
                MAX_PERIOD_DAYS, report._day(first), report._day(last))))
    if ref is None:
        notices.append(Notice(WARNING,
            "Référence de M5 indisponible : la recette la plus récente n'a pas "
            "de mesure enregistrée en base. Si elle a été entraînée avant R10 : "
            ".\\fd-manager.ps1 reference"))
    if not seen["verdicts"]:
        notices.append(Notice(INFO,
            "Aucun verdict reçu %s. Un verdict arrive quand le porteur "
            "conteste, environ %d jours après le paiement : une période récente "
            "est vide tant que ses paiements n'ont pas mûri."
            % (period, settings.verdict_delay_days)))
    if len(models) > 1:
        notices.append(Notice(INFO,
            "%d versions du modèle ont décidé de ces paiements : le détail par "
            "version est sous le graphique." % len(models)))

    figures = [
        ("Verdicts reçus", report._count(seen["verdicts"])),
        ("Fraudes confirmées", report._count(seen["frauds"])),
        ("Fraudes manquées", report._count(seen["false_negatives"])),
        ("Fausses alertes", report._count(seen["false_positives"])),
    ]
    def reference_of(key):
        return report._percent(ref[key]) if ref else "indisponible"
    comparison = [
        {"Indicateur": "Fraudes retrouvées (rappel)",
         "Observé": report._percent(seen["recall_observed"]),
         "Mesuré en M5 (test)": reference_of("recall")},
        {"Indicateur": "Alertes justes (précision)",
         "Observé": report._percent(seen["precision_observed"]),
         "Mesuré en M5 (test)": reference_of("precision")},
        {"Indicateur": "Part de fraudes",
         "Observé": report._share(seen["base_rate_observed"], 3),
         "Mesuré en M5 (test)": (report._share(ref["base_rate_test"], 3)
                                 if ref else "indisponible")},
    ]
    matrix = [
        {"": "Alertée", "Confirmée": report._count(seen["true_positives"]),
         "Non confirmée": report._count(seen["false_positives"])},
        {"": "Non alertée", "Confirmée": report._count(seen["false_negatives"]),
         "Non confirmée": report._count(seen["true_negatives"])},
    ]
    series = [{"Jour": report._day(d["day"]),
               "Rappel (%)": _ratio_value(d["recall_observed"]),
               "Précision (%)": _ratio_value(d["precision_observed"])}
              for d in days]
    by_model = [{"Version du modèle": m["model_version"],
                 "Recette": m["dataset_version"] or "inconnue",
                 "Verdicts": report._count(m["verdicts"]),
                 "Fraudes": report._count(m["frauds"]),
                 "Rappel observé": report._percent(m["recall_observed"]),
                 "Rappel M5": report._percent(m["test_recall"]),
                 "Précision observée": report._percent(m["precision_observed"]),
                 "Précision M5": report._percent(m["test_precision"])}
                for m in models]

    if seen["verdicts"]:
        scope = ("Verdicts reçus %s : ils portent sur des paiements du %s au "
                 "%s, pas sur ceux de la période." % (
                     period, report._day(seen["payments_from"]),
                     report._day(seen["payments_to"])))
    else:
        scope = "Verdicts reçus %s." % period
    note = (scope + " Les deux séries ne se comparent que si la part de "
            "fraudes est voisine : un écart sur cette part suffit à expliquer "
            "un écart sur le rappel et la précision.")
    if ref is not None:
        note += " Référence : recette %s." % ref["version"]
    return {"first": first, "last": last, "notices": notices,
            "figures": figures, "comparison": comparison, "matrix": matrix,
            "series": series, "by_model": by_model, "note": note,
            "title": "Verdicts reçus %s" % period}


# --- the rejections of a day, as a page shows them (R7) -----------------------
#
# F07.3: the breakdown of the quarantine by reason. COUNTS ONLY -- fd_dashboard
# reads five columns of fd_quarantine and neither payload nor reason_detail.
# The vocabulary is closed (ml4fd/checks.py), so every row carries a label and
# no free text reaches the page.
#
# Two scopes, named on the page: the BREAKDOWN covers the whole quarantine of
# the day, historical reloads included; the RATE is the stream's, from the run
# counters of the watched DAG -- the one the operations control judges.


def rejects_view(conn, settings, first: dt.date | None = None,
                 last: dt.date | None = None,
                 now: dt.datetime | None = None) -> dict:
    """Everything the rejections page shows, for days first..last, in order."""
    first = first or today(settings, now)
    last = last or first
    first, last, clipped = bounded(first, last)
    period = period_label(first, last)
    window = report.span_for(settings, first, last)
    rows = report._rows(conn, REJECTS_SQL, window.params())
    total = report._rows(conn, REJECTS_TOTAL_SQL, window.params())[0]
    stream = report._rows(conn, STREAM_RATE_SQL,
                          {**window.params(), "dag_id": health.WATCHED_DAG})[0]
    read, rejected = int(stream["lues"]), int(stream["rejetees"])
    rate = rejected / read if read else None
    notices: list[Notice] = []

    if clipped:
        notices.append(Notice(INFO,
            "Période ramenée à %d jours, du %s au %s : la quarantaine est "
            "purgée à 90 jours." % (MAX_PERIOD_DAYS, report._day(first),
                                    report._day(last))))
    unknown = sorted({r["reason_code"] for r in rows}
                     - set(checks.REASON_LABELS))
    if unknown:
        # Cannot happen while checks.py is the only writer; said if it does,
        # because a code nobody labelled is a code nobody decided.
        notices.append(Notice(WARNING,
            "Motif(s) hors du vocabulaire déclaré : %s." % ", ".join(unknown)))
    if rate is not None and rate > settings.ops_max_rejection_share:
        notices.append(Notice(WARNING,
            "Le flux a rejeté %s des lignes lues sur la période, au-dessus du seuil "
            "déclaré de %s : la forme du flux a changé (un champ renommé, une "
            "horloge qui dérive), et les transactions concernées n'ont pas été "
            "scorées." % (report._share(rate), report._share(
                settings.ops_max_rejection_share, 0))))
    if not rows:
        notices.append(Notice(INFO,
            "Aucune ligne rejetée %s." % period))

    figures = [
        ("Lignes rejetées", report._count(total["lignes"])),
        ("Motifs relevés", report._count(total["motifs"])),
        ("Lots touchés", report._count(total["lots"])),
        ("Taux de rejet du flux",
         report._share(rate) if rate is not None else "aucune lecture"),
    ]
    table = [{"Classe": checks.CLASS_LABELS.get(r["reason_class"],
                                                r["reason_class"]),
              "Motif": checks.REASON_LABELS.get(r["reason_code"],
                                                r["reason_code"]),
              "Code": r["reason_code"],
              "Motifs": report._count(r["motifs"]),
              "Lignes": report._count(r["lignes"]),
              "Lots": report._count(r["lots"])}
             for r in rows]
    return {"first": first, "last": last, "notices": notices,
            "figures": figures, "table": table,
            "note": ("Une ligne peut être rejetée pour plusieurs motifs : la "
                     "colonne « Lignes » ne s'additionne pas, le total est "
                     "compté à part. La ventilation couvre toute la quarantaine "
                     "de la période, rechargements historiques compris ; le taux est "
                     "celui du flux (%s), d'après ses compteurs. Ni le contenu "
                     "d'une ligne rejetée ni le détail du motif ne sont lisibles "
                     "ici." % health.WATCHED_DAG),
            "title": "Rejets %s" % period}

