"""Collecting the real-time stream, and putting it in the raw zone untouched.

Block: B3, B4.
Lot: B3-N1, B4-L0.
Competency: B3-C2.
Covers: B3-EE01, B3-EE02, B3-EI01, B3-EC01.

EVERY VALUE IS TEXT, and that is not a style choice. Measured in N0: 7 of the
924 cards carry 19 digits and exceed 2**53, so a single float anywhere on the
path turns 4992346398065154184 into 4992346398065154048 -- a different
client_key, therefore a different cardholder, therefore history features
computed against an empty past, silently, on one transaction in eleven.

json.loads(..., parse_int=str, parse_float=str) removes the whole class of
defect rather than protecting one field: no integer and no float ever exists
between the wire and the raw zone. It is also exactly the contract S1's
fetch.read_rows holds for the CSV, so both sources reach fd_raw_events in the
same shape.

THE BODY IS DOUBLE-ENCODED: a JSON string containing JSON. Measured on 57
responses, always. A single decode yields a string, and code that indexes it
gets characters instead of fields.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    import psycopg

log = logging.getLogger(__name__)

SOURCE = "current-transactions"

# Measured in N0: a 502 costs 25 s, and it is the expensive failure -- a 429
# costs 0.23 s. A timeout below 26 s would cut off calls that were going to
# succeed.
DEFAULT_TIMEOUT_SECONDS = 35

# Fields the API sends that are NOT part of the transaction: they identify the
# source row, which is useful for measuring the draw and useless for scoring.
KEY_FIELDS = ("index", "trans_num")


@dataclass(frozen=True)
class Reading:
    """One successful call: the row as text, and what we know about the call."""
    row: dict[str, str]
    source_index: str
    trans_num: str
    server_ms: int
    our_ms: int
    latency_s: float

    @property
    def clock_gap_s(self) -> float:
        """How far the server's stamp sits from our clock at the call.

        Measured in N0: the median is +5.1 s, which IS the latency -- the server
        stamps when it answers, we when we call. A gap far beyond the latency
        means one of the two clocks moved.
        """
        return (self.server_ms - self.our_ms) / 1000.0

    def as_payload(self) -> dict:
        """What goes into the raw zone: the row plus the call's provenance.

        WHAT IS STORED, and what is lost. The decoded row and the call's
        provenance, not the verbatim body: the body's only extra information is
        key order and whitespace, neither of which means anything, while the
        values are what a replay needs.
        """
        return {
            "row": self.row,
            "source_index": self.source_index,
            "trans_num": self.trans_num,
            "server_ms": self.server_ms,
            "our_ms": self.our_ms,
            "latency_ms": round(1000 * self.latency_s),
        }

    def server_time(self) -> dt.datetime:
        """current_time read as what it is: epoch MILLISECONDS, UTC.

        Read as seconds it is out of range; read as microseconds it lands in
        1970. Both mistakes are loud, which is a small mercy.
        """
        return dt.datetime.fromtimestamp(self.server_ms / 1000.0,
                                         dt.timezone.utc)


@dataclass(frozen=True)
class Attempt:
    """What one call did, successful or not. Never raises."""
    status: int | None
    latency_s: float
    our_ms: int
    reading: Reading | None = None
    error: str = ""

    @property
    def ok(self) -> bool:
        return self.reading is not None

    @property
    def refused(self) -> bool:
        """The rate limiter said no. Cheap, and worth retrying at once."""
        return self.status == 429


def records_from_raw(conn, batch_id) -> list:
    """The batch's rows, re-read from the raw zone as (line_no, row) pairs.

    WHY EVERY STAGE AFTER THE POLL READS FROM HERE. The raw zone is the only
    durable, append-only copy of what the API said, and it is what lets the
    tasks of N7's DAG hand work to one another: Airflow passes JSON between
    tasks, not Python objects, and re-polling is not an option -- five calls
    cost about 27 seconds and the API will never return the same five
    transactions again. So the poll is the ONLY task that touches the network,
    and everything downstream is replayable from the table.

    MATERIALISED into a list, not yielded. raw.read_batch is a SERVER-SIDE
    cursor and transform.load_transactions uses COPY: a COPY and a FETCH cannot
    share one connection without deadlocking -- which L3 paid for once.

    Unwrapped by row_of, so a caller sees the shape the ingestion saw rather
    than the envelope this module wrote.
    """
    from ml4fd import raw

    reader = raw.read_batch(conn, batch_id)
    try:
        return [(line_no, row_of(payload)) for line_no, payload in reader]
    finally:
        reader.close()


def row_of(payload: dict) -> dict:
    """The transaction row inside a raw-zone payload of this source.

    THE INVERSE OF Reading.as_payload, and it lives here because this module is
    what wrote the envelope. Reading the raw zone from somewhere else and
    guessing at the shape is how N6's verdict write came to derive every
    identifier from a payload whose current_time sits one level down: the
    identifiers all came back as "<trans_num>:", matched nothing, and four
    verdicts were skipped with a message that read like normal operation.

    Tolerant of a payload that IS a row -- S1's CSV batches are stored that way
    -- so a caller reading a mixed raw zone needs no second branch.
    """
    inner = payload.get("row")
    return inner if isinstance(inner, dict) else payload


def decode(body: str) -> tuple[list[str], list[Any]]:
    """Decode a response body into (columns, values) for its single row.

    Refuses rather than guesses:
      - a body that is not a JSON string holding JSON;
      - a payload without exactly one row;
      - columns and values of different lengths -- THE check that matters,
        because a misalignment would silently put an amount in the category
        column and nothing would raise.

    Block: B3, B4.
    Lot: B3-N1, B4-L0.
    Competency: B3-C5.
    Covers: B3-EC25.
    """
    try:
        outer = json.loads(body)
    except ValueError as problem:
        raise ValueError("body is not JSON: %s" % problem) from problem

    if isinstance(outer, str):
        try:
            inner = json.loads(outer, parse_int=str, parse_float=str)
        except ValueError as problem:
            raise ValueError("the inner JSON does not parse: %s"
                             % problem) from problem
    elif isinstance(outer, dict):
        # Not what 57 measured responses did, but a single decode would then be
        # correct and refusing it would be wrong. Worth a line in the log.
        log.warning("the body was not double-encoded: the API's shape changed")
        inner = json.loads(body, parse_int=str, parse_float=str)
    else:
        raise ValueError("the body decodes to %s, expected a string or an "
                         "object" % type(outer).__name__)

    columns = inner.get("columns")
    data = inner.get("data")
    if not isinstance(columns, list) or not isinstance(data, list):
        raise ValueError("the payload has no columns/data pair")
    if len(data) != 1:
        raise ValueError("the payload holds %d row(s), expected exactly 1"
                         % len(data))

    values = data[0]
    if not isinstance(values, list) or len(values) != len(columns):
        raise ValueError(
            "%d columns and %d values: a misaligned response would put an "
            "amount in the category column, so it is refused rather than "
            "truncated" % (len(columns), len(values) if isinstance(values, list)
                           else -1))

    index = inner.get("index")
    columns = list(columns) + ["index"]
    values = list(values) + [str(index[0]) if isinstance(index, list) and index
                             else ""]
    return columns, values


def to_row(columns, values) -> dict[str, str]:
    """Pair them up, every value as text. No conversion happens here."""
    return {str(name): "" if value is None else str(value)
            for name, value in zip(columns, values)}


def read_reading(body: str, our_ms: int, latency_s: float) -> Reading:
    """A Reading from a body, or a ValueError naming what was wrong."""
    row = to_row(*decode(body))
    for field in ("current_time", "trans_num"):
        if not row.get(field):
            raise ValueError("the response carries no %s" % field)
    try:
        server_ms = int(row["current_time"])
    except ValueError as problem:
        raise ValueError("current_time is not an integer: %r"
                         % row["current_time"]) from problem

    return Reading(row=row, source_index=row.get("index", ""),
                   trans_num=row["trans_num"], server_ms=server_ms,
                   our_ms=our_ms, latency_s=latency_s)


def fetch_one(endpoint: str, session=None,
              timeout: float = DEFAULT_TIMEOUT_SECONDS) -> Attempt:
    """One call. Returns an Attempt and never raises.

    Measured in N0: 5 % of calls fail -- 2 of 60 with a 502, 1 with a 429. A
    collector that raised on those would stop five times a day for a stream that
    is simply flaky.

    Block: B3, B4.
    Lot: B3-N1, B4-L0.
    Competency: B3-C2, B4-C2.
    Covers: B3-EE01, B3-EC01, B4-EE02.
    """
    getter = session.get if session is not None else requests.get
    our_ms = int(time.time() * 1000)
    started = time.monotonic()
    try:
        response = getter(endpoint, timeout=timeout)
    except requests.RequestException as problem:
        return Attempt(status=None, latency_s=time.monotonic() - started,
                       our_ms=our_ms,
                       error="%s: %s" % (type(problem).__name__, problem))

    latency = time.monotonic() - started
    if response.status_code != 200:
        return Attempt(status=response.status_code, latency_s=latency,
                       our_ms=our_ms, error="HTTP %d" % response.status_code)
    try:
        reading = read_reading(response.text, our_ms, latency)
    except ValueError as problem:
        return Attempt(status=200, latency_s=latency, our_ms=our_ms,
                       error=str(problem))
    return Attempt(status=200, latency_s=latency, our_ms=our_ms,
                   reading=reading)


def poll(endpoint: str, calls: int, spacing_s: float = 0.0,
         timeout: float = DEFAULT_TIMEOUT_SECONDS,
         session=None) -> tuple[list[Reading], list[Attempt]]:
    """`calls` calls. Returns what was obtained and what failed, separately.

    The failures are RETURNED, not logged and dropped: EC14 asks that an absence
    be detectable, and a poll that quietly returned three rows where four were
    asked is exactly the absence nobody notices.

    A 429 is retried once, immediately: measured at 0.23 s, it is the cheapest
    thing that can happen. A 502 is not retried in the same run -- measured at
    25 s, retrying it would eat the whole minute.

    Block: B3, B4.
    Lot: B3-N1, B4-L0.
    Competency: B3-C2, B3-C4.
    Covers: B3-EE02, B3-EI01, B3-EC14.
    """
    readings: list[Reading] = []
    failures: list[Attempt] = []
    for i in range(calls):
        if i and spacing_s:
            time.sleep(spacing_s)
        attempt = fetch_one(endpoint, session=session, timeout=timeout)
        if not attempt.ok and attempt.refused:
            log.info("429 au %d-eme appel, nouvelle tentative immediate", i + 1)
            attempt = fetch_one(endpoint, session=session, timeout=timeout)
        if attempt.ok:
            readings.append(attempt.reading)
        else:
            failures.append(attempt)
            log.warning("appel %d/%d en echec : %s (%.1f s)",
                        i + 1, calls, attempt.error, attempt.latency_s)
    log.info("%d lecture(s), %d echec(s) sur %d appel(s)",
             len(readings), len(failures), calls)
    return readings, failures


def _verdicts_of(numbered, max_amount: float, tolerance_s: float):
    """The rejects of each record, from (line_no, row, our_ms, latency_s).

    THE ONE PLACE THE STREAM'S VALIDATION LIVES, and it has two callers with
    two shapes: validate() works on Readings held in memory, validate_raw() on
    the envelopes the raw zone gives back. Writing the rule twice is how a
    pipeline comes to validate differently depending on which door it came
    through -- and N7's first draft did exactly that, running validate_batch
    without the clock bound and so dropping CLOCK_IMPLAUSIBLE from the DAG
    while the manual runner kept it.

    checks.validate_batch gives the field and schema rules; the clock bound
    lives in checks.check_clock, because a rule and its reason vocabulary
    belong together.
    """
    from ml4fd import checks

    rows = [(line_no, row) for line_no, row, _, _ in numbered]
    clocks = {line_no: (our_ms, latency_s)
              for line_no, _, our_ms, latency_s in numbered}
    for line_no, row, rejects in checks.validate_batch(
            rows, max_amount, profile=checks.STREAM_PROFILE):
        our_ms, latency_s = clocks[line_no]
        yield line_no, row, list(rejects) + checks.check_clock(
            row, line_no, our_ms, latency_s, tolerance_s)


def validate_raw(records, max_amount: float, tolerance_s: float
                 ) -> tuple[list[int], list]:
    """Validate a batch read back from the raw zone.

    records are (line_no, ENVELOPE) pairs -- as raw.read_batch gives them, not
    unwrapped -- because the clock bound needs our_ms and latency_ms, which the
    envelope carries and the row does not.

    Returns (accepted_lines, items), where items is EXACTLY what
    quarantine.write consumes. The payload stored with a reason is the envelope,
    the same thing the raw zone holds, so a quarantined line reads without
    joining anything -- as in validate().

    WHY IT EXISTS. The DAG of N7 hands work between tasks through the database,
    so the task that validates has no Reading objects: it re-reads the raw zone.
    Without this function it would have called checks.validate_batch alone and
    lost the clock bound -- the rule N0 earned by measuring one reading 818 s
    out, our own process suspended.

    latency comes back rounded to the millisecond, which the tolerance of 120 s
    does not notice.

    Block: B3, B4.
    Lot: B3-N7, B4-L0.
    Competency: B3-C5.
    Covers: B3-EC25, B3-EC26, B3-EI13.
    """
    numbered = []
    envelopes = {}
    for line_no, payload in records:
        envelopes[line_no] = payload
        numbered.append((line_no, row_of(payload),
                         int(payload.get("our_ms") or 0),
                         float(payload.get("latency_ms") or 0) / 1000.0))

    accepted, items = [], []
    for line_no, row, rejects in _verdicts_of(numbered, max_amount,
                                              tolerance_s):
        if rejects:
            items.append((line_no, envelopes[line_no], rejects))
            log.warning("ligne %d rejetee : %s", line_no,
                        ", ".join(r.reason_code for r in rejects))
        else:
            accepted.append(line_no)

    log.info("%d ligne(s) acceptee(s), %d rejetee(s)",
             len(accepted), len(items))
    return accepted, items


def validate(readings: list[Reading], max_amount: float,
             tolerance_s: float) -> tuple[list[Reading], list]:
    """Split the readings into accepted and rejected.

    Returns (accepted, items) where `items` is EXACTLY what quarantine.write
    consumes: (line_no, payload, rejects) triples. Returning a flat list of
    reasons was the first shape, and it would have needed reshaping at the one
    place that uses it -- the interface is better made to fit the writer S1
    already has.

    Runs checks.validate_batch under STREAM_PROFILE, then the clock bound --
    which lives in checks.py and not here, because a rule and its reason
    vocabulary belong together.

    The line numbers are the ranks the raw zone gave, so a reason written to
    fd_quarantine points at the record that caused it.

    Block: B3, B4.
    Lot: B3-N2, B4-L0.
    Competency: B3-C5.
    Covers: B3-EC25, B3-EC26, B3-EI13.
    """
    accepted: list[Reading] = []
    items: list[tuple[int, dict, list]] = []
    by_rank = {rank: r for rank, r in enumerate(readings, start=1)}

    numbered = [(rank, r.row, r.our_ms, r.latency_s)
                for rank, r in enumerate(readings, start=1)]
    for rank, row, rejects in _verdicts_of(numbered, max_amount, tolerance_s):
        reading = by_rank[rank]
        if rejects:
            # The payload stored with the reason is the one the raw zone holds,
            # so a quarantined line can be read without joining anything.
            items.append((rank, reading.as_payload(), rejects))
            log.warning("lecture %d rejetee : %s", rank,
                        ", ".join(r.reason_code for r in rejects))
        else:
            accepted.append(reading)

    log.info("%d lecture(s) acceptee(s), %d rejetee(s)",
             len(accepted), len(items))
    return accepted, items


def valid_records(readings: list[Reading]) -> list[tuple[int, dict[str, str]]]:
    """The accepted readings as transform/truth consume them: (line_no, row).

    The rank is the same one the raw zone and the quarantine use, so the three
    tables speak about the same record by the same number.
    """
    return [(rank, reading.row) for rank, reading in enumerate(readings, start=1)]


def write_batch(conn: "psycopg.Connection", batch_id, readings: list[Reading]
                ) -> int:
    """Put the readings in the raw zone, one record per call.

    Delegates to raw.write_batch, which S1 already wrote: it carries the COPY,
    the declared column types, and the pre-check that makes re-writing a batch
    idempotent instead of doubling it. Reimplementing any of that here would
    give S3 a second raw-zone writer to keep in step with the first.

    line_no comes from that function, as the rank of the reading in the batch.
    There is no line number in a stream, and the rank keeps
    UNIQUE (batch_id, line_no) doing its job.

    Block: B3, B4.
    Lot: B3-N1, B4-L0.
    Competency: B3-C2.
    Covers: B3-EE02, B3-EI01.
    """
    from ml4fd import raw

    return raw.write_batch(conn, batch_id, SOURCE,
                           [reading.as_payload() for reading in readings])
