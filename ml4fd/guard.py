"""The gate S2 passes through before it trains on anything.

Covers: EC09, EC14
Lot: M1
Competency: C3, C5

Two kinds of health, and the second is the one nobody thinks of.

INGESTION. A dataset built over a window where the ingestion was down is
truncated, and NOTHING IN THE EVALUATION METRICS REVEALS IT. The model trains
without error and is simply worse. This guard is the only device that catches
that case.

LABELLING. Measured on this source: the fraud generator stops injecting on
2020-12-22, and the 38,147 later transactions carry is_fraud = 0 because nobody
labelled them. A window holding no positive must stop the DAG exactly as a
window holding no run does -- that is EC14, "detect the absence", applied to
labels rather than to runs.

The decision is a pure function of a report, so it can be exercised without a
database. inspect_window() gathers the facts; verdict() judges them.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import psycopg

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class WindowReport:
    """What is known about a window, before any judgement."""
    runs_total: int
    runs_success: int
    runs_failed: int
    last_success: dt.datetime | None
    last_failure: dt.datetime | None
    rows_in_window: int
    labelled_rows: int
    labelled_positives: int
    cut_off: dt.datetime
    label_horizon: dt.datetime
    verdict_delay: dt.timedelta = dt.timedelta(0)

    @property
    def verdicts_known_until(self) -> dt.datetime:
        """The last instant whose verdict has fallen at the cut-off.

        Everything after it sits in the blind band. Naming this instant turns
        "no verdict known" into a refusal the reader can check against a
        calendar.
        """
        return self.cut_off - self.verdict_delay

    def as_log(self) -> dict:
        return {
            "runs": "%d (%d ok, %d failed)"
                    % (self.runs_total, self.runs_success, self.runs_failed),
            "rows_in_window": self.rows_in_window,
            "labelled": self.labelled_rows,
            "positives": self.labelled_positives,
            "base_rate": "%.4f %%" % (100 * self.labelled_positives / self.labelled_rows)
                         if self.labelled_rows else "n/a",
            "cut_off": self.cut_off.isoformat(),
        }


def verdict(report: WindowReport, min_positives: int) -> list[str]:
    """The reasons to refuse. An empty list means the window is usable.

    A list rather than the first reason found: knowing that a window is both
    unlabelled AND incomplete is more useful than learning it twice.

    Covers: EC09, EC14
    Lot: M1
    Competency: C5
    """
    reasons: list[str] = []

    if report.cut_off > report.label_horizon:
        reasons.append(
            "the cut-off %s is beyond the labelling horizon %s: the rows in "
            "between are false negatives, not negatives"
            % (report.cut_off.isoformat(), report.label_horizon.isoformat()))

    if report.runs_total == 0:
        reasons.append("no ingestion run covers this window: the dataset would "
                       "be built over data nobody confirmed arriving")
    elif report.runs_success == 0:
        reasons.append("%d ingestion run(s) over this window, none successful"
                       % report.runs_total)
    elif (report.last_failure and report.last_success
          and report.last_failure > report.last_success):
        reasons.append(
            "the most recent ingestion run FAILED (%s, after the last success "
            "at %s): the window may be truncated, and no training metric would "
            "show it" % (report.last_failure.isoformat(),
                         report.last_success.isoformat()))

    if report.rows_in_window == 0:
        reasons.append("no transaction in this window")
    elif report.labelled_rows == 0:
        reasons.append(
            "no verdict known at the cut-off for this window. With a verdict "
            "delay of %d day(s) and a cut-off of %s, verdicts are known only up "
            "to %s -- which is BEFORE this window begins. The window sits "
            "entirely inside the blind band: move the split rule earlier, or "
            "the cut-off later."
            % (report.verdict_delay.days, report.cut_off.date(),
               report.verdicts_known_until.date()))
    elif report.labelled_positives < min_positives:
        reasons.append(
            "%d labelled positive(s) in this window, %d required: a window "
            "without fraud teaches the model that this period is never "
            "fraudulent" % (report.labelled_positives, min_positives))

    return reasons


def inspect_window(conn: "psycopg.Connection", window_start: dt.datetime,
                   window_end: dt.datetime, cut_off: dt.datetime,
                   label_horizon: dt.datetime,
                   verdict_delay: dt.timedelta = dt.timedelta(0)
                   ) -> WindowReport:
    """Gather the facts about a window. Judges nothing.

    window_end is the end of the DATASET's test period, not the cut-off. The two
    differ by the blind band, and counting rows between them would report
    transactions the dataset never holds -- rows that are necessarily unlabelled,
    making every window look sicker than it is.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), "
            "       count(*) FILTER (WHERE state = 'success'), "
            "       count(*) FILTER (WHERE state = 'failed'), "
            "       max(ended_at) FILTER (WHERE state = 'success'), "
            "       max(ended_at) FILTER (WHERE state = 'failed') "
            "FROM   fd_pipeline_runs")
        runs_total, runs_ok, runs_ko, last_ok, last_ko = cur.fetchone()

        cur.execute(
            "SELECT count(*) FROM fd_transactions "
            "WHERE txn_ts >= %s AND txn_ts < %s", (window_start, window_end))
        rows_in_window = cur.fetchone()[0]

        cur.execute(
            "SELECT count(*), count(*) FILTER (WHERE g.label) "
            "FROM   fd_ground_truth g "
            "WHERE  g.txn_ts >= %s AND g.txn_ts < %s AND g.label_ts <= %s",
            (window_start, window_end, cut_off))
        labelled, positives = cur.fetchone()

    return WindowReport(
        runs_total=runs_total, runs_success=runs_ok, runs_failed=runs_ko,
        last_success=last_ok, last_failure=last_ko,
        rows_in_window=rows_in_window, labelled_rows=labelled,
        labelled_positives=positives, cut_off=cut_off,
        label_horizon=label_horizon, verdict_delay=verdict_delay)


def assert_window_healthy(conn: "psycopg.Connection", window_start: dt.datetime,
                          window_end: dt.datetime, cut_off: dt.datetime,
                          label_horizon: dt.datetime, min_positives: int,
                          verdict_delay: dt.timedelta = dt.timedelta(0)
                          ) -> WindowReport:
    """Inspect, judge, and refuse with every reason named."""
    report = inspect_window(conn, window_start, window_end, cut_off,
                            label_horizon, verdict_delay)
    log.info("window report: %s", report.as_log())
    reasons = verdict(report, min_positives)
    if reasons:
        raise ValueError("this window cannot be trained on:\n  - "
                         + "\n  - ".join(reasons))
    log.info("window usable: %d labelled rows, %d positives (%.4f %%)",
             report.labelled_rows, report.labelled_positives,
             100 * report.labelled_positives / report.labelled_rows)
    return report
