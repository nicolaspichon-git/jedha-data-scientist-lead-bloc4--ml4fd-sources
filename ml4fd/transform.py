"""Typing and pseudonymisation.

Covers: EC17, EE08, EI03
Lot: L3
Competency: C2, C4

Typing means converting, constraining and deciding: amt as numeric(12,2) rather
than a float, txn_ts as timestamptz, cc_num read as text and never stored.

MEASURED ON THE SOURCE, before writing any of this:
  - the amounts total 38,562,903.11, which L3's acceptance criterion checks to
    the cent;
  - 55,452 amounts carry one decimal and 500,267 carry two, so quantising to two
    widens and never rounds -- no value is altered;
  - 924 distinct cardholder numbers, which is why a plain hash would be
    reversible by enumeration.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import math
from decimal import Decimal, ROUND_HALF_UP
from typing import TYPE_CHECKING, Iterable

from ml4fd import checks, db

if TYPE_CHECKING:
    import psycopg

log = logging.getLogger(__name__)

CENT = Decimal("0.01")
MICRO = Decimal("0.000001")
MIN_PEPPER_CHARS = 32
# 128 bits. For 924 distinct values the collision probability is around 2e-36;
# the full 64 hex characters would double the index size for nothing.
KEY_CHARS = 32

OUTPUT_COLUMNS = ("txn_id", "txn_ts", "client_key", "merchant", "category",
                  "amt", "merch_lat", "merch_long", "f_home_distance_km")

# What the COPY writes. batch_id belongs here and not in the later INSERT: the
# TEMP table is created by LIKE fd_transactions, so it inherits
# "batch_id uuid NOT NULL", and a COPY that leaves the column out violates that
# constraint. The violation surfaces only when the COPY stream closes, so the
# whole file gets sent before the server reports that row 1 was already wrong.
COPY_COLUMNS = OUTPUT_COLUMNS + ("batch_id",)
COPY_TYPES = ["text", "timestamptz", "text", "text", "text",
              "numeric", "numeric", "numeric", "numeric", "uuid"]

# Mean Earth radius, IUGG. The great-circle distance over a sphere is accurate
# to a few tenths of a percent against the ellipsoid -- far below what a fraud
# feature can use, and the figure is stored to the nearest 10 m anyway.
EARTH_RADIUS_KM = 6371.0088


def _pepper_bytes(pepper: str) -> bytes:
    """The pepper as key material.

    fd-manager.ps1 generates 64 hex characters: decoded, that is the 32 bytes the
    generator actually produced. A non-hex pepper is accepted as raw text rather
    than refused, so a hand-written value still works.
    """
    if len(pepper) < MIN_PEPPER_CHARS:
        raise ValueError(
            "pepper too short (%d characters, %d required): pseudonymisation "
            "would be reversible by enumeration over 924 cardholder numbers."
            % (len(pepper), MIN_PEPPER_CHARS))
    try:
        return bytes.fromhex(pepper)
    except ValueError:
        return pepper.encode("utf-8")


def client_key(cc_num: str, pepper: str) -> str:
    """HMAC of the cardholder number with a secret pepper.

    HMAC and not a plain hash: the key space holds 924 values, so an unsalted
    digest is inverted by trying all of them. The pepper lives in the
    environment, never in governance.yml and never in the repository.

    Covers: EC17
    Lot: L3
    Competency: C4
    """
    if not cc_num:
        raise ValueError("empty cc_num: nothing to pseudonymise")
    mac = hmac.new(_pepper_bytes(pepper), cc_num.encode("utf-8"), hashlib.sha256)
    return mac.hexdigest()[:KEY_CHARS]


def home_distance_km(home_lat: str | None, home_long: str | None,
                     merch_lat: str | None, merch_long: str | None
                     ) -> Decimal | None:
    """Great-circle distance between the cardholder's home and the merchant.

    This is the minimisation EC17 allows: the home coordinates are read from the
    raw zone, consumed here, and never written to the exploited layer. A
    distance does not give back an address -- it describes a circle around the
    merchant, not a point.

    Returns None if any coordinate is missing: absence is not a distance of
    zero, which would read as "the merchant is at the cardholder's door".

    Covers: EC17
    Lot: L3
    Competency: C4
    """
    try:
        lat1, lon1 = math.radians(float(home_lat)), math.radians(float(home_long))
        lat2, lon2 = math.radians(float(merch_lat)), math.radians(float(merch_long))
    except (TypeError, ValueError):
        return None

    # The haversine form rather than the spherical law of cosines: the latter
    # loses precision on short distances, which are the common case here.
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = (math.sin(dlat / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2)
    km = 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(min(1.0, h)))
    return Decimal(repr(km)).quantize(CENT, rounding=ROUND_HALF_UP)


def _decimal(value: str | None, places: Decimal) -> Decimal | None:
    if value is None or not str(value).strip():
        return None
    return Decimal(str(value).strip()).quantize(places, rounding=ROUND_HALF_UP)


def to_transaction(record: dict[str, str], pepper: str,
                   profile: checks.SourceProfile = checks.CSV_PROFILE
                   ) -> dict:
    """Type one valid record and strip its identity.

    The output holds OUTPUT_COLUMNS and nothing else, so no identifying field
    can reach the exploited layer by accident -- the schema of fd_transactions
    has no column for one either (EC17).

    parse_ts comes from ml4fd.checks rather than being written again here: how a
    source timestamp is read, time zone included, must have a single definition.

    Covers: EE08, EI03, EC17
    Lot: L3
    Competency: C2
    """
    # Through the profile: the CSV writes a wall clock in
    # trans_date_trans_time, the stream epoch milliseconds in current_time.
    # Hard-coded, this raised on every stream record.
    ts = profile.timestamp(record)
    if ts is None:
        raise ValueError("unreadable timestamp: %r in %s"
                         % (record.get(profile.time_field), profile.time_field))
    return {
        "txn_id": profile.event_id(record),
        "txn_ts": ts,
        "client_key": client_key(record["cc_num"].strip(), pepper),
        "merchant": record["merchant"].strip(),
        "category": record["category"].strip(),
        "amt": _decimal(record["amt"], CENT),
        "merch_lat": _decimal(record.get("merch_lat"), MICRO),
        "merch_long": _decimal(record.get("merch_long"), MICRO),
        "f_home_distance_km": home_distance_km(
            record.get("lat"), record.get("long"),
            record.get("merch_lat"), record.get("merch_long")),
    }


def rejected_lines(conn: "psycopg.Connection", batch_id) -> set[int]:
    """The lines quarantined for this batch.

    Read from fd_quarantine rather than recomputed: the quarantine IS the record
    of what was rejected, and the two tasks are better coupled through the
    database than through running the same checks twice.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT line_no FROM fd_quarantine "
                    "WHERE batch_id = %s", (batch_id,))
        return {row[0] for row in cur}


def load_transactions(write_conn: "psycopg.Connection", batch_id,
                      records: Iterable[tuple[int, dict]], pepper: str,
                      skip: set[int],
                      profile: checks.SourceProfile = checks.CSV_PROFILE
                      ) -> int:
    """Type the valid records and insert them. Returns the number inserted.

    COPY into a TEMP table, then INSERT ... ON CONFLICT DO NOTHING: COPY alone
    is fast but cannot ignore a conflict, and 555,719 single inserts would take
    minutes. The two-step keeps both speed and the idempotency EC23 requires --
    a replay under a new batch_id meets the same txn_id and does nothing.

    CAUTION: `records` must NOT come from a server-side cursor on write_conn.
    psycopg serialises the operations of one connection behind a lock, so a COPY
    waiting for rows from a generator that waits for a FETCH on that same
    connection deadlocks -- with no exception and no timeout. Give the reader its
    own connection. The parameter is named write_conn to say so.

    Covers: EC23
    Lot: L3
    Competency: C2, C5
    """
    assert len(COPY_COLUMNS) == len(COPY_TYPES), "column and type lists diverged"
    columns = ", ".join(COPY_COLUMNS)

    with write_conn.cursor() as cur:
        # The same latent trap as truth.load_ground_truth, not yet sprung:
        # nothing calls this twice in one transaction today.
        db.staging_table(cur, "tmp_fd_transactions", "fd_transactions")

        typed = 0
        with cur.copy("COPY tmp_fd_transactions (%s) FROM STDIN" % columns) as copy:
            copy.set_types(COPY_TYPES)
            for line_no, record in records:
                if line_no in skip:
                    continue
                row = to_transaction(record, pepper, profile)
                copy.write_row(tuple(row[c] for c in OUTPUT_COLUMNS)
                               + (batch_id,))
                typed += 1
                if typed % 100_000 == 0:
                    log.info("  %d rows typed", typed)

        # Named columns on both sides, deliberately. The additive migration of
        # f_home_distance_km appends the column to an existing table while a
        # fresh one declares it mid-schema, so the physical column order differs
        # between the two. Naming the columns makes that difference harmless;
        # a positional INSERT would silently shift every value.
        cur.execute(
            "INSERT INTO fd_transactions (%s) SELECT %s FROM tmp_fd_transactions "
            "ON CONFLICT (txn_id) DO NOTHING" % (columns, columns))
        inserted = cur.rowcount

    log.info("batch %s: %d typed, %d inserted (%d already present)",
             batch_id, typed, inserted, typed - inserted)
    return inserted
