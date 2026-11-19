-- Delete ONE SLICE of ONE batch's quarantined records.
-- Covers: EC16, EC19   Lot: Q4   Competency: C2
--
-- WHY THIS STORE IS PURGED AT ALL, and at the same duration as the raw zone.
-- fd_quarantine.payload is the raw payload of a REJECTED record: the same
-- personal data as fd_raw_events.payload. Before Q0 measured it, no duration
-- covered this table -- so a rejected transaction would have kept its data for
-- ever while an accepted one was purged at 90 days. The inverse of what
-- limiting retention is for, and the kind of gap that is found by listing the
-- stores rather than by thinking about them.
--
-- Same shape as sql/purge/010_raw_events.sql, and for the same reasons: no
-- date predicate (the set comes from candidates_quarantine.sql), whole batches
-- (the fingerprint of Q1 is per batch), slices on the primary key (a lock held
-- for minutes and a write-ahead log inflated in one transaction are what a
-- single DELETE costs).
--
-- BEFORE THE RAW ZONE, ALWAYS. These rows derive from the raw lines of their
-- batch, and the refusal rule is the batch's raw fingerprint -- which can no
-- longer be recomputed once the raw rows are gone. Deleting the original
-- first would therefore strand its derived copy. ml4fd/purge.py holds that
-- order in PURGE_ORDER, with a test on it.
--
-- Parameters: batch_id, chunk.

WITH slice AS (
    SELECT quarantine_id
    FROM   fd_quarantine
    WHERE  batch_id = %(batch_id)s
    ORDER  BY quarantine_id
    LIMIT  %(chunk)s
)
DELETE FROM fd_quarantine
 WHERE quarantine_id IN (SELECT quarantine_id FROM slice);
