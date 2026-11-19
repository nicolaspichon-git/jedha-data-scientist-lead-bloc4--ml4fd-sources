"""Assembling the morning report, and writing it as one dated document.

Covers: EE04, EI04, EI08, EC14, EC17
Lot: P2
Competency: C2

TWO AXES OF TIME, AND CONFUSING THEM WOULD MAKE THE REPORT FALSE. A transaction
carries the date of the payment (txn_ts) and the date its verdict became known
(label_ts = txn_ts + delay, thirty days in nominal mode). A report built on the
payment axis alone shows a permanent zero in its "frauds" column, since no
transaction of yesterday has a verdict; one built on the knowledge axis alone
shows month-old frauds under the heading "yesterday". BOTH READINGS ARE WRONG
AND NEITHER RAISES AN ERROR -- which is why the document names its axis above
every section, and why section 3 prints the payment dates it actually covers.

THE DOCUMENT CARRIES NO PRODUCTION TIMESTAMP, and that is a decision rather than
an omission. The plan asked that two runs on the same window produce the same
document "to the character, bar the production timestamp". Keeping that
timestamp out of the body makes the property ABSOLUTE instead of modulo a line:
the document is byte-identical, its digest is stable, and a diff between two
mornings shows only what the data did. produced_at belongs to fd_reports (P3),
which is where one asks when a document was made.

MARKDOWN ONLY, decided 2026-11-16. A Streamlit server will present the report at
the end of S5 and renders Markdown natively, so a derived HTML would be a third
format to produce, test and keep consistent for a reader who would never read
it.

THE M5 COMPARISON DEGRADES RATHER THAN FAILS. Section 3 compares the observed
recall with the one measured on the test set, which fit_model copies into
fd_datasets beside run.json (S6/R10; until then it was read from run.json on
disk). If the latest recipe has no figures -- never fitted, or registered
before R10 and not yet backfilled -- the section still renders and says the
reference is unavailable. A morning report that refuses to exist
because a comparison is missing has failed at its one job.

NOTHING HERE NEEDS THE RAW ZONE, so every query runs under fd_report, which has
no access to it. The raw payloads still carry names, addresses and card numbers;
the exploited layer carries none (EC17), and that is what makes a report safe to
hand to a human.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import logging
import zoneinfo
from dataclasses import dataclass
from pathlib import Path

from ml4fd import db

log = logging.getLogger(__name__)

VOLUMES_SQL = "reports/daily_volumes.sql"
FRAUDS_SQL = "reports/daily_frauds.sql"
VERDICTS_SQL = "reports/daily_verdicts.sql"
REFERENCE_SQL = "reports/reference.sql"
PENDING_SQL = "reports/daily_pending.sql"
RECORD_SQL = "reports/record_report.sql"

# The order the document presents, and the order P0's mockup decided.
SECTIONS = ("volumes", "alerts", "verdicts", "pending")


# THE CONTROL NAMES AN ANALYST READS, against the identifiers the code uses.
# health.CONTROLS holds the identifiers -- freshness, empty_run,
# rejection_rate, volume_invariant -- which are the right names in a module and
# the wrong ones in a document. An unknown identifier falls through unchanged
# rather than being hidden: a control nobody translated must still be visible.
CONTROL_LABELS = {
    "freshness": "Fraîcheur de l'ingestion",
    "empty_run": "Minutes sans transaction",
    "rejection_rate": "Taux de rejet",
    "volume_invariant": "Concordance des compteurs",
}


# --- the window ---------------------------------------------------------------

@dataclass(frozen=True)
class Window:
    """The two instants that bound the report, and the one that dates its
    knowledge. All three come from the DAG's interval and never from the clock.
    """
    start: dt.datetime
    end: dt.datetime
    knowledge_at: dt.datetime
    zone: str

    def __post_init__(self) -> None:
        for name in ("start", "end", "knowledge_at"):
            value = getattr(self, name)
            if value.tzinfo is None:
                raise ValueError(
                    "Window.%s is naive (%s). A report window without a time "
                    "zone cannot say which calendar day it covers, and the two "
                    "runs that ought to produce the same document would differ "
                    "by the offset of whoever ran them." % (name, value))
        if self.end <= self.start:
            raise ValueError("the window ends before it starts: %s -> %s"
                             % (self.start, self.end))
        if self.knowledge_at < self.end:
            raise ValueError(
                "the knowledge date (%s) precedes the end of the window (%s): "
                "the report would be asked what was known before the day it "
                "covers was over" % (self.knowledge_at, self.end))

    @property
    def day(self) -> dt.date:
        """The calendar day covered, read at the START of the window.

        At the end, a half-open window ending at midnight would name the NEXT
        day -- the classic off-by-one of a daily report.
        """
        return self.start.date()

    def params(self, knowledge: bool = False, zone: bool = False) -> dict:
        out = {"window_start": self.start, "window_end": self.end}
        if knowledge:
            out["knowledge_at"] = self.knowledge_at
        if zone:
            out["zone"] = self.zone
        return out


def window_for(settings, day: dt.date,
               knowledge_at: dt.datetime | None = None) -> Window:
    """THE calendar day covered by a report: midnight to midnight, in the
    declared zone. ONE definition.

    It was written three times -- dag_daily, tools/daily_report.py,
    tools/check_s4.py -- and the dashboard of S6 would have been the fourth.
    Three copies agreeing today is not a guarantee; it is a coincidence that
    survives until one of them is edited.

    `start + timedelta(days=1)` on an aware datetime is WALL-CLOCK arithmetic:
    the window runs to the next local midnight, which makes it 23 hours long on
    the spring change and 25 on the autumn one (2026-03-29 and 2026-10-25 in
    Europe/Paris, measured). That is the calendar day, which is what a report
    covers. Adding 86,400 seconds would not be.

    knowledge_at defaults to the end of the window: what was known when the
    day was over. dag_daily passes the end of its interval instead.

    Covers: EE03   Lot: R1   Competency: C4
    """
    return span_for(settings, day, day, knowledge_at)


def span_for(settings, first: dt.date, last: dt.date,
             knowledge_at: dt.datetime | None = None) -> Window:
    """Calendar days `first` to `last` INCLUDED: midnight of the first to
    midnight after the last, in the declared zone. window_for() is the case
    first == last, so there is still one rule for what a day is.

    Read by the dashboard's period selector (S6, lot R8). Every day in between
    is a calendar day, 23 or 25 hours long on a clock change, for the reason
    window_for() gives.
    """
    if last < first:
        raise ValueError("the period ends (%s) before it starts (%s)"
                         % (last, first))
    zone = zoneinfo.ZoneInfo(settings.report_timezone)
    start = dt.datetime.combine(first, dt.time.min, tzinfo=zone)
    end = dt.datetime.combine(last + dt.timedelta(days=1), dt.time.min,
                              tzinfo=zone)
    return Window(start=start, end=end, knowledge_at=knowledge_at or end,
                  zone=settings.report_timezone)


# --- gathering ----------------------------------------------------------------

def _rows(conn, relative: str, params: dict) -> list[dict]:
    """Run a report query and return its rows as dicts, named by the cursor.

    By NAME and not by position: four queries with twelve columns between them
    would otherwise shift a rejected count into a read count on an edit to a
    SELECT, which is the defect to_alert() was guarded against in N5.
    """
    with conn.cursor() as cur:
        cur.execute(db.sql_path(relative).read_text(encoding="utf-8"), params)
        names = [column.name for column in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]


def volumes(conn, window: Window) -> dict:
    """Section 1, split by the axis column GROUPING SETS returns."""
    rows = _rows(conn, VOLUMES_SQL, window.params(zone=True))
    out = {"total": None, "category": [], "hour": []}
    for row in rows:
        axis = row.pop("axis")
        if axis == "total":
            out["total"] = row
        else:
            out[axis].append(row)
    if out["total"] is None:
        # An empty day is a legitimate answer and must render: a window with no
        # payment is what a Sunday outage looks like, and section 4 says why.
        out["total"] = {"bucket": "total", "transactions": 0, "amount": 0,
                        "cardholders": 0, "median_amount": None}
    return out


def alerts(conn, window: Window) -> list[dict]:
    return _rows(conn, FRAUDS_SQL, window.params())


def verdicts(conn, window: Window) -> dict:
    return _rows(conn, VERDICTS_SQL, window.params())[0]


def pending(conn, window: Window) -> dict:
    return _rows(conn, PENDING_SQL, window.params(knowledge=True))[0]


def reference(conn, settings) -> dict | None:
    """What M5 measured on the test set, for section 3 to compare against.

    Read from fd_datasets (S6/R10) -- the copy fit_model writes beside run.json
    -- and not from MLflow: a tracking server that is down must not stop the
    morning report. Returns None when the latest recipe carries no figures, and
    the section then says the reference is unavailable rather than disappearing.
    The same function serves the dashboard: one reference, two readers.
    """
    rows = _rows(conn, REFERENCE_SQL, {})
    if not rows:
        return None
    row = rows[0]
    if row["test_recall"] is None:
        log.warning("recette %s sans reference de M5 en base : section 3 sans "
                    "comparaison. Remede : .\\fd-manager.ps1 reference",
                    row["version"])
        return None
    return {"version": row["version"],
            "recall": float(row["test_recall"]),
            "precision": float(row["test_precision"]),
            "alerts_per_day": float(row["test_alerts_per_day"]),
            "threshold": (None if row["test_threshold"] is None
                          else float(row["test_threshold"])),
            "measured_at": row["measured_at"],
            "base_rate_test": float(row["base_rate_test"])}


def gather(conn, settings, window: Window) -> dict:
    """Everything the document needs, in one dict. No rendering here."""
    return {"window": window,
            "volumes": volumes(conn, window),
            "alerts": alerts(conn, window),
            "verdicts": verdicts(conn, window),
            "pending": pending(conn, window),
            "reference": reference(conn, settings),
            "capacity": settings.alert_capacity_per_day}


def chain_health(conn, settings, window: Window) -> dict:
    """Section 4: the state of the chain over the day the report covers.

    The window is the report's and the instant of reference is its knowledge
    date, so a replay reports the health of the day replayed. That the same
    query serves N9's rolling hour is the reason it takes bounds rather than a
    duration -- see sql/checks/pipeline_health.sql.
    """
    from ml4fd import config, health

    facts = health.facts_of(conn, health.WATCHED_DAG, window.start, window.end,
                            as_of=window.knowledge_at)
    controls = health.controls_for(facts, settings)
    return {"facts": facts.as_dict(),
            "controls": [control.as_dict() for control in controls],
            "in_alert": sum(1 for control in controls if control.alert),
            "expected_runs": config.runs_per_day(settings.realtime_schedule),
            "expected_transactions": settings.observed_per_day()}


# --- rendering ----------------------------------------------------------------

def _amount(value) -> str:
    """French formatting, deterministic: thin space thousands, decimal comma."""
    if value is None:
        return "n/a"
    whole = "{:,.2f}".format(float(value)).replace(",", " ")
    return whole.replace(".", ",")


def _count(value) -> str:
    if value is None:
        return "n/a"
    return "{:,}".format(int(value)).replace(",", " ")


def _percent(value, digits: int = 1) -> str:
    """A proportion as a percentage, which is how a reader reads one.

    0,7586 asks to be converted; 75,9 pour cent does not. The ratios stay in
    the queries and in fd_reports, where they are compared by code.
    """
    if value is None:
        return "non mesurable"
    return ("%%.%df" % digits % (100 * float(value))).replace(".", ",") + " %"


def _ratio(value, digits: int = 3) -> str:
    """None renders as "non mesurable" and NEVER as zero.

    A day with no fraud among the fallen verdicts gives a null recall. "No
    fraud to catch" and "caught none of the frauds" are opposite findings.
    """
    if value is None:
        return "non mesurable"
    return ("%%.%df" % digits % float(value)).replace(".", ",")


def _share(value, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return ("%%.%df" % digits % (100 * float(value))).replace(".", ",") + " %"


def _day(value) -> str:
    return value.strftime("%Y-%m-%d") if value is not None else "n/a"


def _instant(value) -> str:
    return value.strftime("%Y-%m-%d %H:%M %Z").strip() if value else "n/a"


def _plural(count: int, singular: str, plural: str | None = None) -> str:
    """A document read by a human agrees in number.

    The log lines of this project write "lecture(s)" because a console is not
    prose. A morning report is prose: "1 alertes" is the kind of detail a
    reader notices and a test does not.
    """
    if abs(count) <= 1:
        return singular
    return plural if plural is not None else singular + "s"


def render(data: dict, settings) -> str:
    """The document, as Markdown. NO PRODUCTION TIMESTAMP -- see the module
    docstring: two runs on the same window produce the same bytes.

    ACCENTED FRENCH, and deliberately, where the rest of this project writes
    ASCII. The log messages avoid accents because a Windows console mangles
    them; this is a document, written as UTF-8, read by a human and rendered by
    Streamlit. "journee" in a report an analyst opens every morning is a defect
    of care.
    """
    window = data["window"]
    volumes_ = data["volumes"]
    alerts_ = data["alerts"]
    verdicts_ = data["verdicts"]
    pending_ = data["pending"]
    reference_ = data["reference"]
    health_ = data.get("health")

    versions = sorted({row["model_version"] for row in alerts_})
    thresholds = sorted({row["threshold"] for row in alerts_})

    out = []
    add = out.append

    add("# Revue : journée du %s" % _day(window.day))
    add("")
    add("| | |")
    add("|---|---|")
    add("| Journée couverte | %s \u2192 %s |" % (_instant(window.start),
                                                  _instant(window.end)))
    add("| Informations arrêtées au | %s |" % _instant(window.knowledge_at))
    add("| Fuseau horaire | %s |" % window.zone)
    add("| Version(s) de modèle | %s |"
        % (", ".join(versions) or "aucune alerte, donc aucune version"))
    add("| Seuil appliqué | %s |"
        % (", ".join(_ratio(value, 2) for value in thresholds) or "n/a"))
    add("| Alertes du jour | %s, plafond déclaré %s |"
        % (_count(len(alerts_)), _count(data["capacity"])))
    add("")

    # THE SENTENCE G2 CANNOT BE ANSWERED HONESTLY WITHOUT.
    waiting = pending_["pending"]
    add("> **%s %s de cette journée %s encore leur verdict.**"
        % (_count(waiting), _plural(waiting, "transaction"),
           _plural(waiting, "attend", "attendent")))
    if pending_["last_due_at"] is not None:
        add("> Il tombera au plus tard le %s." % _day(pending_["last_due_at"]))
    if pending_["without_verdict_row"]:
        add("> **%s n'%s aucune ligne de verdict** : `dag_ingest_verdicts` n'a "
            "pas tourné, et l'échéance ne peut donc pas être annoncée."
            % (_count(pending_["without_verdict_row"]),
               _plural(pending_["without_verdict_row"], "a", "ont")))
    add("> Une fraude se découvre quand le porteur conteste : une colonne "
        "« fraudes confirmées » à zéro pour cette journée ne dit pas qu'il n'y "
        "en a pas eu.")
    add("")

    # --- 1
    total = volumes_["total"]
    add("## 1. Paiements du %s" % _day(window.day))
    add("")
    add("- Transactions : **%s**" % _count(total["transactions"]))
    add("- Montant total : **%s EUR**" % _amount(total["amount"]))
    add("- Montant médian : %s EUR" % _amount(total["median_amount"]))
    add("- Porteurs distincts : %s" % _count(total["cardholders"]))
    add("")
    if volumes_["category"]:
        add("| Catégorie | Transactions | Montant (EUR) |")
        add("|---|---:|---:|")
        for row in volumes_["category"]:
            add("| %s | %s | %s |" % (row["bucket"],
                                      _count(row["transactions"]),
                                      _amount(row["amount"])))
        add("")
    if volumes_["hour"]:
        add("Par heure locale : " + " | ".join(
            "%sh %s" % (row["bucket"], _count(row["transactions"]))
            for row in sorted(volumes_["hour"], key=lambda r: r["bucket"])))
        add("")

    # --- 2
    add("## 2. Alertes sur les paiements du %s" % _day(window.day))
    add("")
    if not alerts_:
        add("Aucune alerte sur cette journée.")
        add("")
    else:
        scores = [row["score"] for row in alerts_]
        add("**%s %s, %s de la perte attendue la plus forte à la plus "
            "faible** — la perte attendue étant le score multiplié par le "
            "montant. Scores de la journée : %s à %s."
            % (_count(len(alerts_)), _plural(len(alerts_), "alerte"),
               _plural(len(alerts_), "classée", "classées"),
               _ratio(min(scores)), _ratio(max(scores))))
        add("")
        add("| # | Heure | Marchand | Catégorie | Montant | Score | "
            "Perte attendue | Paiements passés | Montant habituel | "
            "Écart à l'habitude | Notifiée |")
        add("|---:|---|---|---|---:|---:|---:|---:|---:|---:|---|")
        for rank, row in enumerate(alerts_, 1):
            add("| %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |"
                % (rank,
                   row["txn_ts"].strftime("%H:%M"),
                   row["merchant"], row["category"],
                   _amount(row["amt"]), _ratio(row["score"]),
                   _amount(row["expected_loss"]),
                   _count(row["f_tx_count"]), _amount(row["f_avg_amount"]),
                   ("%s\u00d7" % _ratio(row["times_usual"], 1))
                   if row["times_usual"] is not None else "première",
                   "oui" if row["notified"] else "NON"))
        add("")
        add("Montant total alerté : %s EUR."
            % _amount(sum(row["amt"] for row in alerts_)))
        add("")

    # --- 3
    # THE TITLE CARRIES BOTH DATES, and that is what replaces the vocabulary
    # of "axes" this module reasons in. An analyst does not read "axe de la
    # connaissance"; two dates thirty days apart say the same thing and need no
    # glossary. The axes stay in the docstrings, where engineers read them.
    add("## 3. Verdicts reçus le %s sur les paiements du %s"
        % (_day(window.day),
           _day(verdicts_["payments_from"]) if verdicts_["verdicts"]
           else "(aucun)"))
    add("")
    if not verdicts_["verdicts"]:
        add("Aucun verdict n'a été reçu dans cette journée.")
        add("")
    else:
        add("Un verdict arrive quand le porteur conteste, soit environ un "
            "mois après le paiement. Ceux reçus dans la journée portent donc "
            "sur des paiements du %s au %s, et non sur ceux de la section 1."
            % (_day(verdicts_["payments_from"]), _day(verdicts_["payments_to"])))
        add("")
        add("- Verdicts reçus : **%s**" % _count(verdicts_["verdicts"]))
        add("- Fraudes confirmées : **%s**, soit %s des paiements concernés"
            % (_count(verdicts_["frauds"]),
               _share(verdicts_["base_rate_observed"], 3)))
        add("")
        add("| | Confirmée | Non confirmée |")
        add("|---|---:|---:|")
        add("| Alertée | %s | %s |" % (_count(verdicts_["true_positives"]),
                                       _count(verdicts_["false_positives"])))
        add("| Non alertée | %s | %s |" % (_count(verdicts_["false_negatives"]),
                                           _count(verdicts_["true_negatives"])))
        add("")
        add("- Fraudes retrouvées par le modèle : **%s** d'entre elles "
            "*(rappel)*" % _percent(verdicts_["recall_observed"]))
        add("- Alertes qui étaient de vraies fraudes : **%s** "
            "*(précision)*" % _percent(verdicts_["precision_observed"]))
        if reference_ is None:
            add("- Référence de M5 : **indisponible** \u2014 la recette n'a "
                "pas de mesure enregistrée en base, la comparaison est "
                "impossible sur cette édition.")
        else:
            add("- Pour comparaison, mesuré à la mise en service sur des "
                "données d'archive : **%s** de fraudes retrouvées et **%s** "
                "d'alertes justes, sur une part de fraudes de %s "
                "*(recette %s)*"
                % (_percent(reference_["recall"]),
                   _percent(reference_["precision"]),
                   _share(reference_["base_rate_test"], 3),  # 3 digits
                   reference_["version"]))
            add("")
            add("Ces deux séries ne se comparent que si la **part de fraudes** "
                "est voisine : %s dans la journée contre %s à la mise en "
                "service. Un écart important sur cette part suffit à expliquer "
                "un écart sur les deux autres chiffres."
                % (_share(verdicts_["base_rate_observed"], 3),
                   _share(reference_["base_rate_test"], 3)))
        add("")

    # --- 4
    if health_ is not None:
        facts = health_["facts"]
        add("## 4. Bon fonctionnement du %s" % _day(window.day))
        add("")
        add("- Exécutions : %s attendues, %s terminées, %s en échec"
            % (_count(health_["expected_runs"]), _count(facts["runs_ended"]),
               _count(facts["runs_failed"])))
        add("- Minutes sans aucune transaction : %s (%s)"
            % (_count(facts["runs_empty"]), _share(facts["empty_share"])))
        add("- Transactions lues : %s sur %s attendues"
            % (_count(facts["lines_read"]),
               _count(health_["expected_transactions"])))
        add("- Transactions écartées à la validation : %s (%s)"
            % (_count(facts["lines_rejected"]), _share(facts["rejection_share"])))
        add("- Lots dont les comptages ne concordent pas : %s"
            % _count(facts["batches_breaking_invariant"]))
        add("")
        add("Cette section s'adresse à l'exploitant du pipeline ; son "
            "détail reprend les messages des contrôles.")
        add("")
        add("| Contrôle | État | Détail |")
        add("|---|---|---|")
        for control in health_["controls"]:
            add("| %s | %s | %s |"
                % (CONTROL_LABELS.get(control["control"], control["control"]),
                   "ALERTE" if control["state"] == "alert" else "ok",
                   control["detail"]))
        add("")

    add("---")
    add("")
    # THE PROVENANCE IN THE READER'S TERMS. What an analyst needs to know is
    # whether this document can be compared with yesterday's; the mechanism
    # that makes it so -- the interval, the absent timestamp, the byte-identical
    # replay -- belongs to the docstrings and to the plan.
    add("Ce document couvre une journée fixe et ne dépend pas de l'heure à "
        "laquelle il a été produit : relancé plus tard, il donne exactement le "
        "même contenu. Deux matins se comparent donc ligne à ligne.")
    return "\n".join(out) + "\n"


# --- writing ------------------------------------------------------------------

def path_for(settings, day: dt.date) -> Path:
    """One file per covered day, named by that day. report.root is declared."""
    return Path(settings.report_root) / ("%s.md" % day.isoformat())


def digest(text: str) -> str:
    """sha256 of the document, which fd_reports stores (P3) so that a file on
    disk can be checked against the row describing it -- the same reasoning as
    run.json beside a dataset in M8."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class DocumentCheck:
    """A document on the disk, judged against the digest its row records."""
    path: Path
    verdict: str                 # CONFORME, DIFFERENTE or ABSENT
    expected: str
    actual: str | None
    text: str | None

    @property
    def conforms(self) -> bool:
        return self.verdict == CONFORME


CONFORME, DIFFERENTE, ABSENT = "conforme", "differente", "absent"


def check_document(path: Path, expected: str) -> DocumentCheck:
    """Is the file at `path` the document `expected` vouches for? ONE rule.

    tools/check_s4.py computed it with hashlib on its own; the dashboard of S6
    would have done it a third way. The digest is report.digest(), the function
    that produced the recorded value -- a check written with another function
    agrees with the producer only for as long as nobody changes either.

    WHICH PATH is the caller's to decide, and that is deliberate. check_s4.py
    runs in the Airflow container and follows the path the row RECORDS, which
    is the evidence of where the document was written. The dashboard runs in
    another container and asks path_for(), the rule that chose where to write.

    The text is returned even when the digest differs: a reader must be able to
    see WHAT differs. Whoever displays it must say so first.
    """
    path = Path(path)
    if not path.is_file():
        return DocumentCheck(path, ABSENT, expected, None, None)
    text = path.read_text(encoding="utf-8")
    actual = digest(text)
    return DocumentCheck(path, CONFORME if actual == expected else DIFFERENTE,
                         expected, actual, text)


def write(settings, day: dt.date, text: str) -> Path:
    """Write the document, creating its directory rather than assuming it.

    Created and not assumed: this project has lost 28 real observations and a
    whole dataset to directories that were not mounted, twice for the same
    reason. A report nobody can find has not been produced.
    """
    target = path_for(settings, day)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8", newline="\n")
    except OSError as problem:
        # THE FAILURE THIS PROJECT KEEPS PAYING FOR, named at last. /opt/project
        # is a directory of the image and not a volume, so a path declared in
        # governance.yml without a matching mount in docker-compose.yml fails
        # here with a bare "Permission denied" that names neither the
        # declaration nor the mount. 28 real observations went that way, then a
        # whole dataset, then this.
        raise OSError(
            "cannot write the report to %s: %s. The path comes from "
            "report.root in governance.yml; inside the container it must ALSO "
            "be mounted by docker-compose.yml (- ./reports:/opt/project/"
            "reports), because /opt/project belongs to the image and not to a "
            "volume. Declared and mounted are two different things, and both "
            "are needed." % (target, problem)) from problem
    log.info("rapport ecrit : %s (%d octets)", target,
             len(text.encode("utf-8")))
    return target


def record(conn, row: dict) -> None:
    """Record a produced report in fd_reports. AFTER the document, never before.

    The order is N5's, turned around: recording first would leave the table
    claiming a document nobody wrote, and the absence check -- which reads this
    table -- would then report a healthy morning. A crash between the write and
    the record leaves a document with no row, which the absence check DOES
    catch. One failure is silent, the other loud; the order picks the loud one.

    Covers: EC14, EC23
    Lot: P3
    Competency: C2
    """
    db.execute_file(conn, RECORD_SQL, **row)


def preview(conn, settings, window: Window) -> str:
    """Render the document and RETURN it. Writes nothing, records nothing.

    THE DEFECT THIS REPLACED, found by the acceptance of S4 on 17 November.
    produce() took a journal=False switch that skipped the table and WROTE THE
    FILE ANYWAY -- so a dry run overwrote the document whose sha256 fd_reports
    vouches for, and the criterion "empreinte du document egale
    content_digest" failed on a day nobody had touched on purpose.
    
    A mode that writes is not a dry run. The project already holds that rule
    twice -- `abandoned` and `digests` list without touching -- and S5 holds it
    a third time with plan() beside execute(). Two names, no flag: a writing
    command one flag away from a reading one gets typed by mistake.
    """
    data = gather(conn, settings, window)
    data["health"] = chain_health(conn, settings, window)
    return render(data, settings)


def produce(conn, settings, window: Window) -> dict:
    """Gather, render, write, record. Returns the row fd_reports holds.

    The document and its row go together, always: a file written without its
    row leaves the absence check announcing a sound morning for a document
    nobody vouches for, and a row without its file describes what is not
    there. preview() is how one looks without writing either.
    """
    data = gather(conn, settings, window)
    data["health"] = chain_health(conn, settings, window)
    text = render(data, settings)
    target = write(settings, window.day, text)
    # "recorded" and not "row": the comprehensions below iterate over the
    # alerts with a variable of their own, and two things called row in one
    # expression is how a reader loses track even where Python does not.
    alerts_ = data["alerts"]
    recorded = {
        "report_date": window.day,
        "knowledge_date": window.knowledge_at,
        "path": str(target),
        "content_digest": digest(text),
        "n_payments": data["volumes"]["total"]["transactions"],
        "n_alerts": len(alerts_),
        "n_verdicts": data["verdicts"]["verdicts"],
        "n_pending": data["pending"]["pending"],
        # Several versions in one day travel as a list, which is itself a
        # finding: the model changed while the day was running.
        "model_version": ",".join(
            sorted({alert["model_version"] for alert in alerts_})) or None,
        "threshold": min((alert["threshold"] for alert in alerts_),
                         default=None),
    }
    # AFTER the document, always: recording first would leave the table
    # describing a file nobody wrote, and the absence check would announce a
    # sound morning. Not conditional any more -- the row and the file go
    # together or neither goes.
    record(conn, recorded)
    conn.commit()
    return recorded
