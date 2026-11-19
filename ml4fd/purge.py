"""What a purge would delete, measured before anything is deleted.

Covers: EC26, EI13, EC16, EC19
Lot: Q2
Competency: C5

NOTHING IN THIS MODULE DELETES. It is the dry run of S5, and S5 is the only
process of the project that destroys: everywhere else a mistake is repaired by
a replay, here it is permanent. So the first thing written is the thing that
counts what would happen, and the delete of Q3 is built to consume it.

FOUR PROPERTIES, EACH ANSWERING A WAY THIS GOES WRONG.

ONE DEFINITION OF THE SET. sql/purge/candidates_*.sql decides what is beyond
retention, and the delete of Q3 takes its batch_id from here instead of
carrying a date predicate of its own. A dry run that counts with its own
predicate is worse than no dry run: it reports a set nobody will delete.

THE INSTANT COMES FROM THE CALLER. Never now(), anywhere in this module or in
its SQL. In the DAG it is data_interval_end, so a purge is replayable and can
be demonstrated on a chosen date -- the `before` override exists for that, and
Q5 keeps it out of the scheduler.

EVIDENCE BEFORE DESTRUCTION. A batch whose fingerprint was never recorded, or
no longer matches, is refused, named, and counted apart (Q1). This is the
structural guarantee of S5: what a batch held cannot stop being knowable
before it stops existing.

THE GUARD. Beyond the declared share (purge.max_share) the plan is BLOCKED and
says which store blocks it. plan() itself never raises -- a dry run that
refuses to show its figures is useless precisely when the figures matter.
assert_allowed() is what stops a caller, and the two are separate so that the
operator can always look.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import logging
from dataclasses import dataclass

from ml4fd import db, runs

log = logging.getLogger(__name__)

# The stores the purge DELETES rows from, and the file that selects them. The
# order is a report's order, deliberately: the raw zone is the volume, the
# quarantine is the store that was covered by no duration at all until Q0
# measured it.
CANDIDATE_SQL = {
    "fd_raw_events": "purge/candidates_raw.sql",
    "fd_quarantine": "purge/candidates_quarantine.sql",
}
# The file that DELETES from each store. A store with candidates but no entry
# here is reported by the dry run and left alone by the purge, and the summary
# of a run names it rather than letting the plan's figures read as a promise.
DELETE_SQL = {
    "fd_raw_events": "purge/010_raw_events.sql",
    "fd_quarantine": "purge/020_quarantine.sql",
}

# THE ORDER THE STORES ARE PURGED IN, AND IT IS NOT COSMETIC. The quarantined
# rows of a batch derive from its raw lines, and the rule that permits their
# deletion is the batch's RAW fingerprint -- which cannot be recomputed once
# the raw rows are gone. Purging the raw zone first would therefore strand its
# derived copy: the quarantine rows would be refused for ever as a mismatch
# against a digest nothing can reproduce. The derived copy goes first.
PURGE_ORDER = ("fd_quarantine", "fd_raw_events")

# HOW MANY ROWS PER TRANSACTION. One DELETE over a historical batch is 555 719
# rows and some 200 MB of write-ahead log held in one transaction, with the
# lock to match; one row at a time is 555 719 round trips. Ten thousand is the
# usual middle of that range: a slice commits in well under a second, so the
# table is free between slices and the log is recycled as the purge goes.
#
# NOT IN governance.yml: this is a tuning of how the deletion is carried out,
# not a decision about what is kept. The durations and the guard are policy and
# live there; this is the size of a bite.
CHUNK = 10_000

# The journal of the purges (Q6): one row per store and per execution.
RECORD_SQL = "purge/record_purge.sql"
# Q7, split along a privilege boundary: fd_purge may not read fd_transactions,
# so the facts it CAN see live in a file of their own and are read twice.
STORES_SQL = "checks/batch_stores.sql"
BATCH_RUNS_SQL = "checks/batch_runs.sql"      # the runs OF ONE BATCH.
# Not RUNS_SQL: test_purge_plan.py forbids that name, because Q0 gave up
# purging fd_pipeline_runs and such a constant would be the sign of a return
# to it. The journal of executions is kept; this reads three counts from it.

# The role that may destroy, and the only one. sql/ddl/120_purge_role.sql says
# why it is not fd_ingest: the append-only property of the raw zone is a
# revocation on the role that writes, and granting DELETE to it to make the
# purge possible would undo that property for the whole chain.
ROLE = "fd_purge"

# Which declared duration governs which store (governance.yml, retention).
RETENTION = {
    "fd_raw_events": "raw_retention_days",
    "fd_quarantine": "quarantine_retention_days",
}

# THE RUN JOURNAL IS NOT HERE, and that is the finding of Q4 rather than an
# omission. The plan of S5 foresaw emptying its "detail columns" at 365 days --
# and fd_pipeline_runs has none: an identifier, a DAG name, dates, a state,
# counters, and the fingerprints added by Q1. No personal field to empty.
#
# Deleting its ROWS would be worse than useless: they carry the fingerprints
# that are all that remains of a purged batch. The process that destroys the
# data must not destroy the record of what it destroyed -- which is why
# fd_purge holds no DELETE on that table (sql/ddl/120_purge_role.sql).
#
# So it is declared under retention.kept, with that reason, and this module
# neither counts it nor touches it. A figure reported by a destructive tool
# and never acted on is an invitation to finish the job.


@dataclass(frozen=True)
class Candidate:
    """One batch of one store, beyond retention, and the verdict on it."""

    store: str
    batch_id: str
    source: str
    rows: int
    first_at: dt.datetime
    last_at: dt.datetime
    n_bytes: int
    recorded_digest: str | None
    # None once the batch has been judged and nothing stands in the way. Set
    # by _judge(), never by the caller: one field holds the verdict, so
    # `purgeable` cannot disagree with `refusals` the way two lists would.
    refusal: dict | None = None

    @property
    def purgeable(self) -> bool:
        return self.refusal is None

    def as_dict(self) -> dict:
        return {"store": self.store, "batch_id": self.batch_id,
                "source": self.source, "rows": self.rows,
                "first_at": self.first_at.isoformat(),
                "last_at": self.last_at.isoformat(),
                "bytes": self.n_bytes, "digest": self.recorded_digest,
                "refusal": self.refusal}


@dataclass(frozen=True)
class Store:
    """What the purge would do to one store, and what it refuses to touch."""

    store: str
    cutoff: dt.datetime
    total_rows: int
    candidates: tuple[Candidate, ...]

    @property
    def deletable(self) -> tuple[Candidate, ...]:
        return tuple(c for c in self.candidates if c.purgeable)

    @property
    def refusals(self) -> tuple[dict, ...]:
        return tuple(dict(batch_id=c.batch_id, rows=c.rows, **c.refusal)
                     for c in self.candidates if not c.purgeable)

    @property
    def rows(self) -> int:
        """The rows the purge would actually delete -- refused ones excluded.

        The share rests on THIS and not on every candidate: the guard must
        measure what would happen, not what was considered.
        """
        return sum(c.rows for c in self.deletable)

    @property
    def refused_rows(self) -> int:
        return sum(c.rows for c in self.candidates if not c.purgeable)

    @property
    def n_bytes(self) -> int:
        return sum(c.n_bytes or 0 for c in self.deletable)

    @property
    def share(self) -> float:
        return self.rows / self.total_rows if self.total_rows else 0.0

    @property
    def first_at(self) -> dt.datetime | None:
        return min((c.first_at for c in self.deletable), default=None)

    @property
    def last_at(self) -> dt.datetime | None:
        return max((c.last_at for c in self.deletable), default=None)

    def as_dict(self) -> dict:
        return {"store": self.store, "cutoff": self.cutoff.isoformat(),
                "total_rows": self.total_rows, "rows": self.rows,
                "batches": len(self.deletable), "share": self.share,
                "bytes": self.n_bytes,
                "first_at": self.first_at.isoformat() if self.first_at else None,
                "last_at": self.last_at.isoformat() if self.last_at else None,
                "refused_batches": len(self.refusals),
                "refused_rows": self.refused_rows,
                "refusals": list(self.refusals),
                "batch_ids": [c.batch_id for c in self.deletable],
                "deletable": [{"batch_id": c.batch_id, "rows": c.rows}
                              for c in self.deletable],
                "deletes": self.store in DELETE_SQL}


class PurgeRefused(Exception):
    """Raised by assert_allowed(), never by plan()."""


def cutoffs(settings, as_of: dt.datetime, before: dt.datetime | None = None
            ) -> dict[str, dt.datetime]:
    """The instant each store is purged up to.

    From the declared durations, counted back from as_of -- or all of them from
    `before`, which is the DEMONSTRATION override: at 90 days of retention and
    three batches loaded yesterday, a purge would delete nothing before
    February, and a destructive process nobody can try is one that is
    discovered in production. Q5 keeps this override out of the scheduler: a
    parameter of a demonstration left in an orchestrator ends up being used.
    """
    if as_of.tzinfo is None:
        raise ValueError(
            "as_of must be timezone-aware: a purge boundary read in the wrong "
            "zone deletes a day too many or a day too few, and the day is the "
            "oldest one left")
    if before is not None:
        if before.tzinfo is None:
            raise ValueError("before must be timezone-aware")
        return {store: before for store in CANDIDATE_SQL}
    return {store: as_of - dt.timedelta(days=getattr(settings, key))
            for store, key in RETENTION.items()}


def read_candidates(conn, store: str, cutoff: dt.datetime
                    ) -> tuple[list[Candidate], int]:
    """The batches of one store beyond its cutoff, and the store's total.

    ONE QUERY FOR BOTH, on purpose: a total counted by a second statement could
    be read at another instant, and the share -- the figure the guard rests on
    -- would then compare two different databases. When nothing is beyond the
    cutoff there is no row and no total, and a store with nothing to purge
    needs none: its share is zero either way.
    """
    rows = db.query_file(conn, CANDIDATE_SQL[store], before=cutoff)
    # ALWAYS ONE ROW, even with nothing to purge: it carries the store's total
    # and a null batch_id. See the SQL -- "0 lignes sur 0" read as an empty
    # table, which it was not.
    found = [Candidate(store=store, batch_id=str(batch_id), source=source,
                       rows=int(n_rows), first_at=first_at, last_at=last_at,
                       n_bytes=int(n_bytes or 0),
                       recorded_digest=recorded_digest)
             for (batch_id, source, n_rows, first_at, last_at, n_bytes,
                  _n_total, recorded_digest) in rows
             if batch_id is not None]
    return found, (int(rows[0][6]) if rows else 0)


def exempt(marks: dict, store: str) -> str | None:
    """Why the fingerprint check does not apply to this store, or None.

    ONE DEFINITION, BECAUSE IT WAS WRITTEN TWICE AND THEY DISAGREED. The
    executor knew that a purge explains a mismatch; the verifying dry run did
    not, so it refused what the executor would have accepted -- and a verifying
    plan is what the DAG and the acceptance run. The quarantined rows of a
    batch whose raw zone was already purged were therefore refused for ever,
    which is the opposite of what a retention limit is for.

    Two cases, and in both the MARK is the authority rather than the digest:

      resuming   a purge of this very store was interrupted: the slices already
                 deleted are the difference, and the remainder must be
                 finishable.
      raw_gone   this batch's raw zone is gone, deliberately. Its fingerprint
                 can no longer be recomputed, so nothing can be matched; the
                 derived copy must not be held hostage to a check that has
                 become impossible.

    The raw zone itself is never exempted by its own mark being done: that
    would skip the verification of the very rows being deleted.
    """
    mine = marks.get(store) or {}
    if mine and not mine.get("done_at"):
        return "resuming"
    if store != "fd_raw_events" and (marks.get("fd_raw_events") or {}).get("done_at"):
        return "raw_gone"
    return None


def _judge(conn, candidate: Candidate, verify: bool) -> Candidate:
    """Decide whether this batch may be purged, and record why not.

    The CHEAP half is the presence of a fingerprint, read with the candidates.
    The other half -- that the batch still MATCHES it -- costs an md5 over
    every row, about 555 thousand rows and 200 MB for a historical batch, so it
    is asked here only with verify=True. It is never skipped where it decides:
    Q3 calls runs.refusal() for each batch immediately before deleting it.
    """
    if candidate.recorded_digest is None:
        return dataclasses.replace(candidate, refusal={
            "code": runs.NO_DIGEST,
            "why": "aucune empreinte relevee pour ce lot : "
                   "`fd-manager digests -Apply` la releve"})
    if verify:
        # The same exemption the executor applies, from the same function: a
        # plan that refuses what the purge would accept strands those rows.
        why_not = exempt(runs.purge_state(conn, candidate.batch_id),
                         candidate.store)
        if why_not:
            log.info("lot %s / %s : verification sans objet (%s)",
                     candidate.batch_id, candidate.store, why_not)
            return candidate
        told = runs.refusal(conn, candidate.batch_id)
        if told is not None:
            code, why = told
            return dataclasses.replace(
                candidate, refusal={"code": code, "why": why})
    return candidate


def store_plan(conn, store: str, cutoff: dt.datetime, verify: bool) -> Store:
    found, total = read_candidates(conn, store, cutoff)
    judged = tuple(_judge(conn, candidate, verify) for candidate in found)
    return Store(store=store, cutoff=cutoff, total_rows=total,
                 candidates=judged)


def plan(conn, settings, as_of: dt.datetime,
         before: dt.datetime | None = None, verify: bool = False) -> dict:
    """What the purge would delete. READS ONLY, and raises on nothing it finds.

    The one thing it refuses is an instant it cannot trust (a naive datetime),
    because a boundary read in the wrong zone deletes the wrong day.
    """
    # BEFORE THE FIRST QUERY THAT WOULD FAIL. Without the column of Q1 the
    # candidates query ends in `column p.digests does not exist`, which sends
    # the reader to the SQL rather than to the one command that fixes it.
    runs.assert_digests(conn)
    limits = cutoffs(settings, as_of, before)
    stores = [store_plan(conn, store, limits[store], verify)
              for store in CANDIDATE_SQL]

    blocked = [{"store": store.store, "share": store.share,
                "max_share": settings.purge_max_share,
                "rows": store.rows, "total_rows": store.total_rows}
               for store in stores if store.share > settings.purge_max_share]

    report = {
        "as_of": as_of.isoformat(),
        "before": before.isoformat() if before else None,
        "verified": verify,
        "max_share": settings.purge_max_share,
        "stores": [store.as_dict() for store in stores],
        "batches": sum(len(store.deletable) for store in stores),
        "rows": sum(store.rows for store in stores),
        "bytes": sum(store.n_bytes for store in stores),
        "refused_batches": sum(len(store.refusals) for store in stores),
        "refused_rows": sum(store.refused_rows for store in stores),
        "blocked": bool(blocked),
        "blocking": blocked,
    }
    log.info("purge a blanc : %d ligne(s) sur %d lot(s), %d refus, bloquee=%s",
             report["rows"], report["batches"], report["refused_batches"],
             report["blocked"])
    return report


def assert_allowed(report: dict) -> None:
    """Stop a caller that must not purge. The DAG's gate (Q5), and the CLI's.

    A purge of 90 % of a store may well be legitimate -- a retention shortened
    on purpose, a demonstration cutoff -- but it is never a routine, and what
    tells the two apart is a human saying so.
    """
    if not report.get("blocked"):
        return
    named = ", ".join(
        "%s : %.1f %% (%d/%d lignes)"
        % (entry["store"], 100 * entry["share"], entry["rows"],
           entry["total_rows"])
        for entry in report["blocking"])
    raise PurgeRefused(
        "la purge supprimerait plus que la part declaree de %.1f %% -- %s. "
        "Une purge de cette ampleur peut etre legitime, elle n'est jamais une "
        "routine : la confirmer explicitement, ou corriger la date de "
        "reference." % (100 * report["max_share"], named))


# --- the purge itself ---------------------------------------------------------
#
# Everything above counts. From here on, rows leave the database and do not
# come back. Three rules hold this apart from an ordinary delete:
#
#   1. THE SET COMES FROM THE PLAN. execute() deletes the batches plan() named
#      and no others -- so what an operator read is what happens.
#   2. THE FINGERPRINT IS VERIFIED IMMEDIATELY BEFORE, batch by batch, however
#      recently the plan was built. Between the two, a batch could have gained
#      a row; the verification is what makes "we knew what this held" true at
#      the instant of deletion rather than at the instant of the report.
#   3. AN INTERRUPTED PURGE IS RESUMABLE. The slices commit as they go, so a
#      killed process leaves a batch that no longer matches its fingerprint.
#      The marker written before the first slice is what tells that remainder
#      from tampering -- without it, the honest reading would be "refuse for
#      ever", and the batch would be unpurgeable exactly as Q1 set out to
#      prevent.

def purge_batch(conn, store: str, batch_id: str, expected: int, now,
                chunk: int = CHUNK) -> dict:
    """Delete one batch from one store, in committed slices. Returns the outcome.

    `now` is the instant written in the marker, and it has NO DEFAULT: one
    run of the purge stamps one instant, and nothing in this module reads a
    clock of its own -- a fallback that did would be the one that ends up
    being used.
    """
    stamp = now.isoformat()
    held = runs.recorded_digest(conn, batch_id)
    if held is None:
        return {"store": store, "batch_id": batch_id, "deleted": 0,
                "expected": expected, "refused": runs.NO_DIGEST,
                "why": "aucune empreinte relevee pour ce lot"}

    marks = held.get("purge") or {}
    marker = marks.get(store) or {}
    started_at = marker.get("started_at", stamp)
    why_not = exempt(marks, store)
    resuming = why_not == "resuming"
    raw_gone = why_not == "raw_gone"

    if why_not:
        # NOT verified: the digest legitimately no longer matches, because a
        # purge is what changed the batch. See exempt().
        log.warning("lot %s : %s, la verification ne s'applique plus",
                    batch_id,
                    "purge interrompue le %s, reprise" % marker.get("started_at")
                    if resuming else "zone brute deja purgee")
    else:
        told = runs.refusal(conn, batch_id)
        if told is not None:
            code, why = told
            return {"store": store, "batch_id": batch_id, "deleted": 0,
                    "expected": expected, "refused": code, "why": why}
        runs.mark_purging(conn, held["run_id"], store, stamp, expected)
        conn.commit()

    deleted = 0
    while True:
        gone = db.execute_file(conn, DELETE_SQL[store],
                               batch_id=batch_id, chunk=chunk)
        conn.commit()           # between slices: the lock goes, the log recycles
        deleted += gone
        if gone == 0:
            break
        log.info("lot %s : %d ligne(s) supprimee(s) (%d au total)",
                 batch_id, gone, deleted)

    # `deleted` is what THIS run removed. After a resumption that is the
    # remainder, not the batch: an interrupted run never got to write a count
    # (the mark is completed at the end), so there is nothing to add to, and a
    # total would have to be invented. started_at keeps the first attempt's
    # instant, which is what says the purge of this batch spans two runs.
    runs.mark_purged(conn, held["run_id"], store, stamp, deleted,
                     expected, started_at)
    conn.commit()
    return {"store": store, "batch_id": batch_id, "deleted": deleted,
            "expected": expected, "resumed": resuming, "raw_gone": raw_gone,
            "refused": None, "why": None}


def execute(conn, settings, report: dict, now, confirm: bool = False,
            chunk: int = CHUNK, run_id: str | None = None) -> dict:
    """Carry out the purge the report describes. ROWS LEAVE THE DATABASE HERE.

    `confirm` is the explicit confirmation the guard asks for, and nothing
    else grants it: without it a plan over the declared share stops here,
    before a single row goes.

    `run_id` is the Airflow run, when there is one. None when a human ran the
    purge by hand, and recorded as such: that is a fact worth keeping rather
    than hiding behind an invented identifier.
    """
    if not confirm:
        assert_allowed(report)

    outcomes, skipped = [], []
    # IN PURGE_ORDER and not in the plan's order: the derived copy before the
    # original, or the copy is stranded (see PURGE_ORDER).
    by_store = {entry["store"]: entry for entry in report["stores"]}
    ordered = ([by_store[store] for store in PURGE_ORDER if store in by_store]
               + [entry for entry in report["stores"]
                  if entry["store"] not in PURGE_ORDER])
    for store_report in ordered:
        store = store_report["store"]
        if store not in DELETE_SQL:
            # A store the dry run counts and this lot does not delete. Named,
            # so that nobody reads the plan's figures as a promise.
            if store_report["rows"]:
                skipped.append({"store": store, "rows": store_report["rows"],
                                "why": "aucune suppression ecrite pour ce "
                                       "magasin (lot Q4)"})
            continue
        for entry in store_report["deletable"]:
            outcomes.append(purge_batch(conn, store, entry["batch_id"],
                                        entry["rows"], now, chunk=chunk))

    refused = [o for o in outcomes if o["refused"]]
    summary = {
        "as_of": report["as_of"],
        "before": report["before"],
        "confirmed": confirm,
        "run_id": run_id,
        "batches": len([o for o in outcomes if not o["refused"]]),
        "rows": sum(o["deleted"] for o in outcomes),
        "planned": sum(o["expected"] for o in outcomes),
        "resumed": len([o for o in outcomes if o.get("resumed")]),
        "refused": refused,
        "skipped": skipped,
        "outcomes": outcomes,
    }
    # A DIFFERENCE IS NOT AN ERROR, IT IS NEWS. The plan counted at one instant
    # and the delete ran at another; a batch that gained a row between the two
    # would have been refused by the verification, so a gap here means the
    # refused batches -- and the summary shows both rather than averaging them.
    summary["as_planned"] = (summary["rows"] + sum(o["expected"]
                                                   for o in refused)
                             == summary["planned"])
    # THE JOURNAL, AFTER THE DELETION. One row per store, including the
    # stores that had nothing to remove: a Sunday that found nothing is the
    # ordinary case, and it is the proof that the purge RUNS -- a declared
    # duration nobody applies is not a policy. Recorded here rather than by
    # the caller, so that no caller can delete without journalling.
    for store_report in report["stores"]:
        if store_report["store"] not in DELETE_SQL:
            continue
        journal(conn, settings, report, store_report, outcomes, run_id)
    conn.commit()

    log.info("purge : %d ligne(s) supprimee(s) sur %d prevue(s), %d refus",
             summary["rows"], summary["planned"], len(refused))
    return summary


def journal(conn, settings, report: dict, store_report: dict,
            outcomes: list[dict], run_id: str | None) -> None:
    """Write one row of fd_purges for one store."""
    mine = [o for o in outcomes if o["store"] == store_report["store"]]
    deleted = sum(o["deleted"] for o in mine)
    total = store_report["total_rows"]
    db.execute_file(
        conn, RECORD_SQL,
        run_id=run_id,
        as_of=report["as_of"],
        cutoff=store_report["cutoff"],
        before_date=report["before"],
        store=store_report["store"],
        n_batches=len([o for o in mine if not o["refused"] and o["deleted"]]),
        n_deleted=deleted,
        n_refused=len([o for o in mine if o["refused"]]),
        n_resumed=len([o for o in mine if o.get("resumed")]),
        # A retention purge names no batch: it removes whatever the cutoff
        # caught. Only a targeted forget fills this (Q7).
        batch_id=None,
        # The share ACTUALLY removed, which is not the planned one when a batch
        # was refused at the last moment.
        share=(deleted / total) if total else 0)


def render_execution(summary: dict) -> str:
    """What was actually destroyed, for the operator and for the log."""
    lines = ["Purge effectuee.", ""]
    lines.append("%d ligne(s) supprimee(s) sur %d prevue(s), en %d lot(s)."
                 % (summary["rows"], summary["planned"], summary["batches"]))
    if summary["resumed"]:
        lines.append("%d lot(s) repris apres une interruption."
                     % summary["resumed"])
    lines.append("")
    for outcome in summary["outcomes"]:
        if outcome["refused"]:
            continue
        lines.append("  %-16s %s  %d ligne(s)%s"
                     % (outcome["store"], outcome["batch_id"],
                        outcome["deleted"],
                        "  (reprise)" if outcome.get("resumed") else ""))
    if summary["refused"]:
        lines += ["", "Refuses, et toujours en place :"]
        for outcome in summary["refused"]:
            lines.append("  %-16s %s  %s"
                         % (outcome["store"], outcome["batch_id"],
                            outcome["refused"]))
            lines.append("      %s" % outcome["why"])
    if summary["skipped"]:
        lines += ["", "Comptes par le mode a blanc, NON supprimes :"]
        for entry in summary["skipped"]:
            lines.append("  %-16s %d ligne(s) -- %s"
                         % (entry["store"], entry["rows"], entry["why"]))
    if not summary["as_planned"]:
        lines += ["", "Les comptes ne se ferment pas : relancer le mode a "
                      "blanc pour voir l'etat actuel."]
    lines += ["", "Les empreintes des lots purges restent dans "
                  "fd_pipeline_runs : c'est", "ce qui subsiste de ce qu'ils "
                  "contenaient."]
    return "\n".join(lines)

# --- rendering ----------------------------------------------------------------

def _bytes(value: int) -> str:
    size = float(value or 0)
    for unit in ("o", "ko", "Mo", "Go"):
        if size < 1024 or unit == "Go":
            return ("%.0f %s" if unit == "o" else "%.1f %s") % (size, unit)
        size /= 1024
    return "%.1f Go" % size


def _share(value: float) -> str:
    return ("%.1f" % (100 * value)).replace(".", ",") + " %"


def render(report: dict) -> str:
    """The dry run, for the operator who decides. French, like every output."""
    lines = ["Purge a blanc -- rien n'est supprime.", ""]
    if report["before"]:
        lines += ["Date de reference SURCHARGEE : %s" % report["before"][:19],
                  "(outil de demonstration ; l'ordonnanceur ne l'expose pas)",
                  ""]
    else:
        lines += ["Instant de reference : %s" % report["as_of"][:19], ""]

    lines.append("%-18s %10s %10s %8s %10s  %s"
                 % ("magasin", "lignes", "sur", "part", "taille", "jusqu'au"))
    for store in report["stores"]:
        lines.append("%-18s %10d %10d %8s %10s  %s"
                     % (store["store"], store["rows"], store["total_rows"],
                        _share(store["share"]), _bytes(store["bytes"]),
                        (store["last_at"] or "-")[:19]))
    lines += ["",
              "Le journal d'executions n'est pas purge : il ne porte aucune "
              "donnee personnelle,",
              "et il conserve les empreintes des lots supprimes -- tout ce "
              "qui subsiste d'eux.", ""]

    if report["refused_batches"]:
        lines.append("Refus -- ces lots ne peuvent PAS etre purges :")
        for store in report["stores"]:
            for refusal in store["refusals"]:
                lines.append("  %s  %s  (%d lignes)  %s"
                             % (store["store"], refusal["batch_id"],
                                refusal["rows"], refusal["code"]))
                lines.append("      %s" % refusal["why"])
        lines.append("")
    else:
        lines += ["Aucun refus : chaque lot concerne a son empreinte relevee.",
                  ""]

    if not report["verified"]:
        lines += ["Empreintes : presence verifiee, contenu non recalcule "
                  "(--verify le fait ;",
                  "la purge elle-meme le fait toujours, lot par lot, avant de "
                  "supprimer).", ""]

    if report["blocked"]:
        lines.append("BLOQUEE : la part declaree de %s est depassee."
                     % _share(report["max_share"]))
        for entry in report["blocking"]:
            lines.append("  %s : %s" % (entry["store"], _share(entry["share"])))
        lines.append("Une confirmation explicite est exigee.")
    else:
        lines.append("Part declaree de %s respectee."
                     % _share(report["max_share"]))
    return "\n".join(lines)


# --- Q7: forgetting ONE batch, by name ----------------------------------------
#
# The retention purge cuts on a date. Four interrupted loads of the same
# historical CSV and the load that finished arrived within minutes of each
# other, so no date separates them: what separates them is their run's outcome.
# Hence a second entry point -- and a narrow one, since a tool that deletes a
# batch by name can delete the wrong one.
#
# IT REUSES purge_batch() AND THE SAME JOURNAL. The chunked deletes, the
# fingerprint check, the resumption marker and the row in fd_purges are the
# purge's, unchanged. What is new is only WHICH batch may go, and the refusals
# that answer it.

class ForgetRefused(Exception):
    """A batch that may not be forgotten, with every reason listed."""


def run_refusals(n_runs: int, n_success: int, n_running: int
                 ) -> tuple[list[str], list[str]]:
    """The run-based verdicts of a batch: (refusals, cautions). ONE definition.

    Read twice by two roles: by forget_plan() under fd_ingest, which sees
    everything, and again by forget() under fd_purge just before it deletes --
    fd_purge may read fd_pipeline_runs, and these are the facts that must not
    have gone stale in between. Written twice they would drift, and the drift
    would be a deletion the plan had forbidden.

    A REFUSAL is absolute. A CAUTION is lifted by an explicit confirmation.
    A succeeded run is a caution and never a refusal: "success" means every
    task passed, which for a duplicate load means type_and_pseudonymize wrote
    nothing (load_transactions is idempotent). Such a batch is the origin of
    nothing. A batch that DOES own data is refused by the data refusals, which
    nothing lifts -- so the two never have to be weighed against each other.
    """
    refusals, cautions = [], []
    if n_success:
        cautions.append(
            "%d execution(s) de ce lot ont REUSSI : si c'est un doublon, son "
            "succes ne veut dire que 'toutes les taches sont passees'" % n_success)
    if n_running:
        refusals.append(
            "%d execution(s) de ce lot sont encore EN COURS : supprimer sous "
            "une tache qui ecrit encore laisserait un lot a moitie la "
            "('abandoned -Apply' les ferme)" % n_running)
    if n_runs == 0:
        # Orphan rows with no run at all are exactly what this is for -- but
        # purge_batch() needs the run that carries the fingerprint, so name the
        # remedy here instead of letting it fail untitled further down.
        refusals.append(
            "aucune execution ne porte ce lot : son empreinte n'est enregistree "
            "nulle part, et une suppression sans empreinte ne laisserait aucune "
            "trace de ce qu'il contenait ('digests -Apply' la releve)")
    return refusals, cautions


def forget_plan(conn, batch_id) -> dict:
    """What is known about one batch, and whether it may be forgotten.

    READS ONLY, like plan() beside execute() and for the same reason: an
    operator must be able to ask the question without the asking being the
    answer.

    THE CONNECTION MUST BE fd_ingest, not fd_purge. fd_purge holds no grant on
    fd_transactions -- S5's acceptance reads that from the catalogue -- so the
    role that deletes cannot evaluate the refusals that look at the exploited
    layer. The role that writes those tables can read all of them and may
    delete none of them, which is exactly what deciding requires.
    """
    batch_id = str(batch_id)
    (raw_rows, quarantine_rows, txn_rows, truth_rows, last_at,
     sources) = db.query_file(conn, STORES_SQL, batch_id=batch_id)[0]
    n_runs, n_success, n_running = db.query_file(
        conn, BATCH_RUNS_SQL, batch_id=batch_id)[0]

    # What a purge has ALREADY done to this batch, from the marker Q3 writes in
    # fd_pipeline_runs.digests -> purge. Readable by fd_ingest, unlike
    # fd_purges, and it is the same source the resumption logic reads -- so
    # "already purged" is a fact here and not a guess.
    marks = runs.purge_state(conn, batch_id)

    refusals = []
    if raw_rows == 0 and quarantine_rows == 0:
        done = ["%s : %s ligne(s) le %s" % (store, mark.get("deleted"),
                                            mark.get("done_at"))
                for store, mark in sorted(marks.items())
                if mark.get("done_at")]
        if done:
            # DON'T SAY "or a wrong identifier" when the database knows which
            # it was. A refusal that lists two possible causes sends the reader
            # to check both.
            refusals.append(
                "ce lot a DEJA ETE OUBLIE -- %s. Il n'en reste que l'empreinte, "
                "et c'est voulu" % " ; ".join(done))
        elif marks:
            refusals.append(
                "une purge de ce lot a commence sans se terminer (%s) et ses "
                "lignes sont deja parties : rien a supprimer, mais le marqueur "
                "reste ouvert" % ", ".join(sorted(marks)))
        else:
            refusals.append(
                "ce lot ne tient aucune ligne et aucune purge ne l'a touche : "
                "l'identifiant est probablement errone ('batches' les liste)")
    # THE TWO THAT NOTHING LIFTS. Transactions and verdicts point back at this
    # batch, and its raw zone is what allows them to be replayed (EC24): there
    # is no state of mind in which removing it is right.
    if txn_rows:
        refusals.append(
            "%d transaction(s) de ce lot sont dans la couche exploitee : le lot "
            "a ete traite, et sa zone brute est ce qui permet de le rejouer "
            "(EC24)" % txn_rows)
    if truth_rows:
        refusals.append(
            "%d verdict(s) portent ce lot comme source : les oublier laisserait "
            "des etiquettes sans origine" % truth_rows)
    from_runs, cautions = run_refusals(n_runs, n_success, n_running)
    refusals += from_runs

    return {"batch_id": batch_id, "raw_rows": raw_rows,
            "quarantine_rows": quarantine_rows, "txn_rows": txn_rows,
            "truth_rows": truth_rows,
            "n_runs": n_runs, "n_success": n_success, "n_running": n_running,
            "last_at": last_at, "sources": sources,
            "purged": ", ".join(
                "%s (%s ligne(s), %s)"
                % (store, mark.get("deleted"), mark.get("done_at"))
                for store, mark in sorted(marks.items())) or None,
            "refusals": refusals, "cautions": cautions,
            "forgettable": not refusals and not cautions,
            "forgettable_with_force": not refusals}


def assert_forgettable(plan: dict, force: bool = False) -> None:
    """Raise unless the batch may go. forget_plan() itself never raises.

    `force` lifts the cautions and NOT the refusals: a guard an informed
    operator cannot lift is a guard that gets bypassed outside the tool, where
    nothing is journalled -- but the data refusals answer to nobody.
    """
    if plan["refusals"]:
        raise ForgetRefused("lot %s : %s"
                            % (plan["batch_id"], " | ".join(plan["refusals"])))
    if plan["cautions"] and not force:
        raise ForgetRefused(
            "lot %s : %s -- a confirmer explicitement"
            % (plan["batch_id"], " | ".join(plan["cautions"])))


def forget(conn, plan: dict, now, confirm: bool = False,
           force: bool = False, run_id: str | None = None,
           chunk: int = CHUNK) -> dict:
    """Delete one named batch from the purged stores, and journal each one.

    `confirm` is execute()'s lock, for the same reason: a caller that did not
    say so deletes nothing, so a tool cannot delete by forgetting an argument.
    """
    assert_forgettable(plan, force=force)
    if not confirm:
        raise PurgeRefused("forget() appelee sans confirm : rien n'a ete supprime")

    batch_id = plan["batch_id"]
    # READ AGAIN, HERE, under the role that is about to delete. The plan was
    # drawn under fd_ingest, in another connection and a moment earlier; a run
    # of this batch may have been started or closed since. These two facts are
    # the only ones fd_purge may see, and they are the ones that matter.
    n_runs, n_success, n_running = db.query_file(
        conn, BATCH_RUNS_SQL, batch_id=batch_id)[0]
    again, _ = run_refusals(n_runs, n_success, n_running)
    if n_success > plan["n_success"]:
        # A run of this batch FINISHED since the plan was drawn. Whatever it
        # did, the plan no longer describes the batch -- and fd_purge cannot
        # look at the exploited layer to find out what changed.
        again.append("une execution de ce lot s'est terminee depuis le plan "
                     "(%d au lieu de %d)" % (n_success, plan["n_success"]))
    if again:
        raise ForgetRefused(
            "lot %s : l'etat a change depuis le plan : %s"
            % (batch_id, " | ".join(again)))
    outcomes = []
    for store in PURGE_ORDER:          # quarantine BEFORE raw, as ever (Q3)
        expected = (plan["quarantine_rows"] if store == "fd_quarantine"
                    else plan["raw_rows"])
        if not expected:
            continue
        outcome = purge_batch(conn, store, batch_id, expected, now, chunk=chunk)
        outcomes.append(outcome)
        if outcome.get("refused"):
            # The fingerprint check, or a purge already under way, said no.
            # Stop: the next store's permission rests on this one (PURGE_ORDER).
            break
        journal_forget(conn, plan, outcome, now, run_id)
    return {"batch_id": batch_id, "outcomes": outcomes,
            "deleted": sum(o["deleted"] for o in outcomes),
            "refused": [o for o in outcomes if o.get("refused")]}


def journal_forget(conn, plan: dict, outcome: dict, now, run_id: str | None
                   ) -> None:
    """One row of fd_purges for one store of a targeted forget.

    There is no retention boundary here, so the cutoff is the batch's own last
    row, and before_date carries the same value: this IS a deletion beyond the
    declared retention, and that column exists to keep such a deletion
    recognisable for ever after.

    CLAMPED TO `now`, because fd_purges_cutoff_before_as_of demands it and the
    table is right to: a cutoff after the instant the purge was decided means
    nothing. A batch whose last row is NEWER than `now` is ordinary -- a fixed
    as_of from a DAG run, a clock a second behind -- and since the journal is
    written AFTER the deletion, the rejected row would leave the lines gone
    with no record of them. Met for real on PostgreSQL 16 (verify_q7).
    """
    cutoff = min(plan["last_at"], now) if plan["last_at"] else now
    db.execute_file(
        conn, RECORD_SQL,
        run_id=run_id,
        as_of=now,
        cutoff=cutoff,
        before_date=cutoff,
        store=outcome["store"],
        n_batches=1 if outcome["deleted"] else 0,
        n_deleted=outcome["deleted"],
        n_refused=1 if outcome.get("refused") else 0,
        n_resumed=1 if outcome.get("resumed") else 0,
        share=None,                 # a named batch is not a share of a table
        batch_id=plan["batch_id"])
    conn.commit()


def render_forget(plan: dict) -> str:
    out = ["Lot %s" % plan["batch_id"],
           "  source(s)        : %s" % (plan["sources"] or "--"),
           "  zone brute       : %d ligne(s)" % plan["raw_rows"],
           "  quarantaine      : %d ligne(s)" % plan["quarantine_rows"],
           "  couche exploitee : %d transaction(s)" % plan["txn_rows"],
           "  verites terrain  : %d verdict(s)" % plan["truth_rows"],
           "  executions       : %d (%d reussie(s), %d en cours)"
           % (plan["n_runs"], plan["n_success"], plan["n_running"]),
           "  derniere ligne   : %s" % (plan["last_at"] or "--"),
           ""]
    if plan["purged"]:
        out[-1:] = ["  deja purge       : %s" % plan["purged"], ""]
    lines = plan["raw_rows"] + plan["quarantine_rows"]
    if plan["refusals"]:
        out.append("REFUSE, et rien ne leve ces raisons :")
        out += ["  - %s" % why for why in plan["refusals"]]
    elif plan["cautions"]:
        out.append("A CONFIRMER :")
        out += ["  - %s" % why for why in plan["cautions"]]
        out += ["",
                "Si c'est bien un doublon -- il ne possede ni transaction ni "
                "verdict -- %d ligne(s) peuvent partir." % lines]
    else:
        out.append("Oubliable : %d ligne(s) seraient supprimees." % lines)
    return "\n".join(out)

