"""Validation, in two deliberately separated classes.

Covers: EC20, EC21
Lot: L2
Competency: C5

EC20 looks at one field at a time; EC21 looks at the relation between fields of
one record. fd_quarantine.reason_class keeps that separation visible in the data,
and F07.3 reports the breakdown over it.

WHAT THE SOURCE ACTUALLY CONTAINS, measured over the 555,719 rows of
fraudTest.csv before any rule was written:

  - no empty mandatory field, no unreadable amount, timestamp, date of birth or
    coordinate, no duplicate trans_num, no coordinate out of range;
  - amt from 1.00 to 22,768.11, all positive;
  - is_fraud exactly {'0', '1'}, 2,145 frauds for 0.386 %;
  - cc_num always numeric and ALWAYS Luhn-valid, over seven lengths
    (11, 12, 13, 14, 15, 16, 19);
  - the gap between trans_date_trans_time and unix_time is 2,557 days for
    100.000 % of the rows.

Two consequences follow, and both are design decisions rather than oversights.

First, the gap between the two time columns is SYSTEMATIC. Turned into a
per-record rule it would quarantine all 555,719 rows and S1 would produce
nothing. A systematic gap is a SOURCE defect: profile_gap() measures it,
the verdict is recorded, and the column is dropped (hypothesis H5 of D1). Only a
sporadic gap belongs in quarantine.

Second, the seven lengths of cc_num are a trait of the synthetic generator, not
a per-record anomaly -- 11 digits is a length no scheme issues, yet those 2,708
numbers pass Luhn. Rejecting on length would discard valid transactions, so
length is recorded, never rejected.
"""
from __future__ import annotations

import collections
import datetime as dt
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable, Iterator

FIELD, RECORD = "field", "record"

# Closed vocabulary: a free-text reason cannot be counted, and F07.3 has to
# report a breakdown by reason.
MISSING_MANDATORY = "MISSING_MANDATORY"
SCHEMA_MISSING_COLUMN = "SCHEMA_MISSING_COLUMN"
AMT_UNPARSABLE = "AMT_UNPARSABLE"
AMT_NEGATIVE = "AMT_NEGATIVE"
AMT_IMPLAUSIBLE = "AMT_IMPLAUSIBLE"
TS_UNPARSABLE = "TS_UNPARSABLE"
LABEL_INVALID = "LABEL_INVALID"
CC_NOT_NUMERIC = "CC_NOT_NUMERIC"
COORD_UNPARSABLE = "COORD_UNPARSABLE"
COORD_OUT_OF_RANGE = "COORD_OUT_OF_RANGE"
DUPLICATE_TXN_ID = "DUPLICATE_TXN_ID"
CC_LUHN_INVALID = "CC_LUHN_INVALID"
DOB_AFTER_TXN = "DOB_AFTER_TXN"
TIME_COLUMNS_DIVERGE = "TIME_COLUMNS_DIVERGE"
CLOCK_IMPLAUSIBLE = "CLOCK_IMPLAUSIBLE"

# WHAT EACH CODE MEANS, for a human reader (the dashboard of S6, lot R7). Next
# to the codes and not in the page: a code added above without a label here
# fails tests/test_dashboard_rejects.py, so the vocabulary and its meaning
# cannot drift apart.
REASON_LABELS = {
    MISSING_MANDATORY: "champ obligatoire vide",
    SCHEMA_MISSING_COLUMN: "colonne absente du schéma",
    AMT_UNPARSABLE: "montant illisible",
    AMT_NEGATIVE: "montant négatif",
    AMT_IMPLAUSIBLE: "montant hors de la borne déclarée",
    TS_UNPARSABLE: "horodatage illisible",
    LABEL_INVALID: "étiquette de fraude invalide",
    CC_NOT_NUMERIC: "numéro de carte non numérique",
    COORD_UNPARSABLE: "coordonnée illisible",
    COORD_OUT_OF_RANGE: "coordonnée hors des bornes terrestres",
    DUPLICATE_TXN_ID: "identifiant de transaction en double",
    CC_LUHN_INVALID: "numéro de carte invalide (Luhn)",
    DOB_AFTER_TXN: "naissance postérieure à la transaction",
    TIME_COLUMNS_DIVERGE: "les deux colonnes de temps divergent",
    CLOCK_IMPLAUSIBLE: "horloge de la source invraisemblable",
}
CLASS_LABELS = {FIELD: "champ", RECORD: "enregistrement"}

MANDATORY = ("trans_num", "trans_date_trans_time", "cc_num", "merchant",
             "category", "amt", "is_fraud")
COORDS = (("lat", 90.0), ("long", 180.0), ("merch_lat", 90.0), ("merch_long", 180.0))
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
DOB_FORMAT = "%Y-%m-%d"

# The source carries no time zone: trans_date_trans_time is a naive wall clock.
# Comparing it with unix_time, and later storing it as timestamptz, both require
# one to be chosen. UTC is the only non-arbitrary choice, and it is the reading
# the data supports: the two columns share their time of day exactly and differ
# by a whole number of days (2,557), which only holds if the naive column is
# read as UTC. Recorded as a hypothesis -- if the source turns out to be local
# time, every timestamp shifts by the offset and nothing else changes.
SOURCE_TZ = dt.timezone.utc


@dataclass(frozen=True)
class SourceProfile:
    """What differs between the CSV and the stream, named rather than branched.

    THE RULE THIS ENFORCES: the validation is parameterised by the SOURCE, never
    by the absence of a field. `trans_date_trans_time` missing is a defect in a
    CSV line and the normal state of a stream response -- a check that tested
    for its presence would be right in one case and wrong in the other, and
    nothing would say which.

    Three things differ, and the third was nearly missed:

      mandatory       the stream has no trans_date_trans_time and has
                      current_time instead;
      time_field      and its parser: a wall-clock string here, epoch
                      milliseconds there. Left unparameterised, DOB_AFTER_TXN
                      would SILENTLY never fire on the stream -- a check lost
                      without a trace;
      unique_txn_id   on the CSV a repeated trans_num is a duplicate line; on
                      the stream it is a SECOND DRAW of the same source row,
                      which is two real events. Rejecting it would throw away
                      legitimate transactions.

    Covers: EC20, EC21
    Lot: N2
    Competency: C5
    """
    name: str
    mandatory: tuple[str, ...]
    time_field: str
    time_is_epoch_ms: bool
    unique_txn_id: bool
    has_two_time_columns: bool
    # Whether a record's own identifier is enough to identify an EVENT.
    #
    # On the CSV, trans_num names one line and one transaction. On the stream it
    # names the SOURCE ROW that was drawn, and the same row comes back: two
    # draws share trans_num while being two events. Keyed on trans_num alone,
    # the second would collide with fd_transactions' primary key and be lost --
    # silently, since ON CONFLICT DO NOTHING is what makes a replay idempotent.
    event_id_includes_time: bool

    def event_id(self, record: dict[str, str]) -> str:
        """The identifier of the EVENT, which is not always the record's own.

        Readable rather than hashed: "<trans_num>:<current_time>" lets an
        operator see which source row was drawn and when, which a digest would
        hide for no gain -- the value is not a secret and does not need to be
        short.
        """
        own = record.get("trans_num", "").strip()
        if not self.event_id_includes_time:
            return own

        # REFUSED rather than truncated, and this is not caution. The first
        # version returned "%s:" when the time was missing, which is a VALID
        # STRING: on 2026-11-16 ml4fd/verdicts.py passed the raw-zone envelope
        # instead of the row it wraps, every identifier came back as
        # "<trans_num>:", not one of them matched a stored transaction, and four
        # verdicts were silently skipped with a message that read like normal
        # operation. A wrong answer, not an error.
        #
        # Every caller works on a VALIDATED record, where check_fields has
        # already required this column, so a missing one here means the caller
        # handed over something that is not a record.
        stamp = record.get(self.time_field, "").strip()
        if not stamp:
            raise ValueError(
                "cannot build the event identifier of a %s record: %r is "
                "absent or empty. The identifier would be %r -- a string that "
                "matches nothing and raises nothing. Keys received: %s"
                % (self.name, self.time_field, "%s:" % own,
                   ", ".join(sorted(record)[:12])))
        return "%s:%s" % (own, stamp)

    def timestamp(self, record: dict[str, str]):
        """The record's instant, read the way this source writes it."""
        raw = record.get(self.time_field, "")
        return parse_epoch_ms(raw) if self.time_is_epoch_ms else parse_ts(raw)


CSV_PROFILE = SourceProfile(
    name="csv", mandatory=MANDATORY, time_field="trans_date_trans_time",
    time_is_epoch_ms=False, unique_txn_id=True, has_two_time_columns=True,
    event_id_includes_time=False)

STREAM_PROFILE = SourceProfile(
    name="stream",
    mandatory=("trans_num", "current_time", "cc_num", "merchant", "category",
               "amt", "is_fraud"),
    time_field="current_time", time_is_epoch_ms=True,
    unique_txn_id=False, has_two_time_columns=False,
    event_id_includes_time=True)


@dataclass(frozen=True)
class Reject:
    line_no: int
    reason_class: str     # FIELD or RECORD
    reason_code: str
    reason_detail: str | None = None


def luhn_ok(number: str) -> bool:
    """Luhn checksum. Every cc_num in the source passes it, so a failure here
    is sporadic and genuinely belongs in quarantine."""
    total, double = 0, False
    for ch in reversed(number):
        if not ch.isdigit():
            return False
        d = ord(ch) - 48
        if double:
            d *= 2
            if d > 9:
                d -= 9
        total += d
        double = not double
    return total % 10 == 0


def parse_ts(value: str) -> dt.datetime | None:
    """Parse a source timestamp into an aware datetime in SOURCE_TZ.

    Aware rather than naive: subtracting a naive datetime from an aware one
    raises, and an epoch is absolute. Keeping both sides aware settles it.
    """
    try:
        return dt.datetime.strptime(value, TS_FORMAT).replace(tzinfo=SOURCE_TZ)
    except (ValueError, TypeError):
        return None


def parse_epoch(value: str) -> dt.datetime | None:
    """Parse a Unix epoch into an aware UTC datetime.

    fromtimestamp(value, timezone.utc) rather than the naive helper deprecated
    in Python 3.12: that one returns a naive datetime, which cannot be
    subtracted from an aware one anyway.
    """
    try:
        return dt.datetime.fromtimestamp(int(value), dt.timezone.utc)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def parse_epoch_ms(value: str) -> dt.datetime | None:
    """Read current_time: epoch MILLISECONDS, UTC, or None.

    Measured in N0: 1794792372826 is 2026-11-16T01:26:12.826Z. Read as seconds
    it is out of range; read as microseconds it lands in 1970. Both mistakes are
    loud, which is why this function can afford to be simple.
    """
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return dt.datetime.fromtimestamp(int(raw) / 1000.0, dt.timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def check_clock(record: dict[str, str], line_no: int, our_ms: int,
                latency_s: float, tolerance_s: float) -> list[Reject]:
    """Bound current_time against OUR clock at the moment of the call.

    current_time IS the txn_ts of a real-time transaction, so an implausible
    value does not merely mislabel a row: it breaks the ordering the per-row
    feature function depends on, and L4 proved that ordering.

    Measured in N0: one call in 57 showed a gap of +818 s. The cause was OUR OWN
    process suspended -- on Linux CLOCK_MONOTONIC does not advance across a
    suspend while CLOCK_REALTIME does -- and not the server. The remedy is the
    same either way, so this function attributes no cause.

    Covers: EC20, EC26
    Lot: N2
    Competency: C5
    """
    raw = record.get("current_time", "").strip()
    if not raw:
        return []                       # MISSING_MANDATORY already said so
    try:
        server_ms = int(raw)
    except ValueError:
        return []                       # TS_UNPARSABLE already said so

    gap = (server_ms - our_ms) / 1000.0
    if gap < -tolerance_s:
        return [Reject(line_no, RECORD, CLOCK_IMPLAUSIBLE,
                       "%.0f s before our call (tolerance %.0f s)"
                       % (-gap, tolerance_s))]
    if gap > latency_s + tolerance_s:
        return [Reject(line_no, RECORD, CLOCK_IMPLAUSIBLE,
                       "%.0f s after our call, which took %.1f s "
                       "(tolerance %.0f s)" % (gap, latency_s, tolerance_s))]
    return []


def check_fields(record: dict[str, str], line_no: int, max_amount: float,
                 profile: SourceProfile = CSV_PROFILE) -> list[Reject]:
    """Schema, types, nullability, outliers -- one field at a time.

    Every failure is reported, not just the first: a quarantined line is more
    useful with all its reasons, and UNIQUE (batch_id, line_no, reason_code)
    allows several rows for one line.

    Covers: EC20
    Lot: L2
    Competency: C5
    """
    out: list[Reject] = []
    def add(code, detail=None):
        out.append(Reject(line_no, FIELD, code, detail))

    for name in profile.mandatory:
        if name not in record:
            add(SCHEMA_MISSING_COLUMN, name)
        elif not record[name].strip():
            add(MISSING_MANDATORY, name)

    raw_amt = record.get("amt", "").strip()
    if raw_amt:
        try:
            amount = Decimal(raw_amt)
            if amount < 0:
                add(AMT_NEGATIVE, raw_amt)
            elif amount > Decimal(str(max_amount)):
                add(AMT_IMPLAUSIBLE, raw_amt)
        except (InvalidOperation, ValueError):
            add(AMT_UNPARSABLE, raw_amt)

    raw_ts = record.get("trans_date_trans_time", "").strip()
    if raw_ts and parse_ts(raw_ts) is None:
        add(TS_UNPARSABLE, raw_ts)

    label = record.get("is_fraud", "").strip()
    if label and label not in ("0", "1"):
        add(LABEL_INVALID, label)

    cc = record.get("cc_num", "").strip()
    if cc and not cc.isdigit():
        add(CC_NOT_NUMERIC, cc[:8] + "...")      # never log a full number

    for name, limit in COORDS:
        raw = record.get(name, "").strip()
        if not raw:
            continue
        try:
            if abs(float(raw)) > limit:
                add(COORD_OUT_OF_RANGE, "%s=%s" % (name, raw))
        except ValueError:
            add(COORD_UNPARSABLE, "%s=%s" % (name, raw))

    return out


def check_record(record: dict[str, str], line_no: int,
                 reject_time_gap: bool = False,
                 profile: SourceProfile = CSV_PROFILE) -> list[Reject]:
    """Coherence BETWEEN fields of one record.

    reject_time_gap stays False unless profile_gap() has found the divergence to
    be sporadic. On this source it is systematic, so turning it on would
    quarantine every row -- which is exactly the trap this signature makes
    explicit rather than hiding.

    Covers: EC21
    Lot: L2
    Competency: C5
    """
    out: list[Reject] = []
    def add(code, detail=None):
        out.append(Reject(line_no, RECORD, code, detail))

    cc = record.get("cc_num", "").strip()
    if cc and cc.isdigit() and not luhn_ok(cc):
        add(CC_LUHN_INVALID, "length=%d" % len(cc))

    # Through the profile, so DOB_AFTER_TXN keeps working on the stream. Read
    # from a hard-coded trans_date_trans_time, ts would be None on every stream
    # record and the check would vanish without a word.
    ts = profile.timestamp(record)
    raw_dob = record.get("dob", "").strip()
    if ts and raw_dob:
        try:
            # Compared as dates: a date of birth has no time of day, and
            # inventing one would make the comparison rest on a fiction.
            if dt.datetime.strptime(raw_dob, DOB_FORMAT).date() >= ts.date():
                add(DOB_AFTER_TXN, raw_dob)
        except ValueError:
            pass            # an unreadable dob is EC20's business, not EC21's

    if reject_time_gap and ts and profile.has_two_time_columns:
        epoch = parse_epoch(record.get("unix_time", "").strip())
        if epoch is not None:
            gap = (ts - epoch).days
            if gap:
                add(TIME_COLUMNS_DIVERGE, "%d days" % gap)
    return out


def profile_gap(records: Iterable[dict[str, str]], systematic_share: float,
                sample: int = 50_000) -> dict:
    """Measure whether the gap between the two time columns is systematic.

    To be run BEFORE any rejection rule on these two fields. Returns the gap
    distribution, the dominant value and its share, and a verdict:

      "systematic" -- a source defect: document it, rule once, drop the column
      "sporadic"   -- a record defect: quarantine it
      "absent"     -- the two columns agree

    Covers: EC21
    Lot: L2
    Competency: C1, C5
    """
    gaps: collections.Counter[int] = collections.Counter()
    seen = 0
    for record in records:
        if seen >= sample:
            break
        ts = parse_ts(record.get("trans_date_trans_time", ""))
        epoch = parse_epoch(record.get("unix_time", "").strip())
        if ts is None or epoch is None:
            continue
        gaps[(ts - epoch).days] += 1
        seen += 1

    if not seen:
        return {"verdict": "absent", "measured": 0, "distribution": {}}

    dominant, count = gaps.most_common(1)[0]
    share = count / seen
    if dominant == 0 and share >= systematic_share:
        verdict = "absent"
    elif share >= systematic_share:
        verdict = "systematic"
    else:
        verdict = "sporadic"
    return {"verdict": verdict, "measured": seen, "dominant_gap_days": dominant,
            "dominant_share": round(share, 6),
            "distribution": dict(gaps.most_common(5))}


def validate_batch(records: Iterable[tuple[int, dict[str, str]]],
                   max_amount: float, reject_time_gap: bool = False,
                   profile: SourceProfile = CSV_PROFILE
                   ) -> Iterator[tuple[int, dict, list[Reject]]]:
    """Run both classes over a batch and catch duplicate transaction ids.

    Uniqueness cannot be judged one record at a time, so it is held here rather
    than in check_fields().

    Covers: EC20, EC21
    Lot: L2
    Competency: C5
    """
    seen: set[str] = set()
    for line_no, record in records:
        rejects = check_fields(record, line_no, max_amount, profile)
        rejects += check_record(record, line_no, reject_time_gap, profile)
        # Only where the source says a transaction id is unique. On the stream
        # the same trans_num is a SECOND DRAW of the same historical row, so two
        # real events -- and rejecting the second would discard a transaction
        # that happened.
        if profile.unique_txn_id:
            txn_id = record.get("trans_num", "").strip()
            if txn_id:
                if txn_id in seen:
                    rejects.append(
                        Reject(line_no, FIELD, DUPLICATE_TXN_ID, txn_id))
                else:
                    seen.add(txn_id)
        yield line_no, record, rejects
