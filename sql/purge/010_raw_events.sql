-- Delete ONE SLICE of ONE batch of the raw zone.
-- Block: B3, B4.
-- Lot: B3-Q3, B4-L0.
-- Competency: B3-C2, B3-C3.
-- Covers: B3-EE05, B3-EI11, B3-EC19.
--
-- NO DATE PREDICATE HERE, and that absence is the design. What is beyond
-- retention is decided once, in sql/purge/candidates_raw.sql, and this file
-- receives the batch_id that file selected. A delete carrying a cutoff of its
-- own would be a second definition of the purgeable set, and the dry run would
-- then describe a set nobody deletes. tests/test_purge_plan.py forbids it.
--
-- BY BATCH, because the batch is the unit of ingestion AND the unit of the
-- fingerprint (Q1): half a batch is a remainder no recorded digest describes.
--
-- BY SLICES, because a historical batch holds 555 719 rows and some 200 MB of
-- JSON. One DELETE over all of it holds a lock for minutes, inflates the
-- write-ahead log by the whole volume at once, and cannot be interrupted. The
-- caller commits between slices, so the lock is released and the log is
-- recycled as it goes.
--
-- ON THE PRIMARY KEY, not on ctid: raw_id is indexed and stable, and the inner
-- SELECT uses fd_raw_events_batch_idx. ctid moves under VACUUM, which is
-- precisely what runs during a long purge.
--
-- THE ARCHITECTURE THIS WORKS AROUND, written rather than discovered. A table
-- purged by date belongs partitioned by month, where a purge is a DROP of a
-- partition -- instantaneous, no row touched, no log written. fd_raw_events is
-- not partitioned: at the volume of this demonstration that was not an error
-- in L1, but it is the first thing to change if this ever takes a real flow.
--
-- Parameters: batch_id, chunk.

WITH slice AS (
    SELECT raw_id
    FROM   fd_raw_events
    WHERE  batch_id = %(batch_id)s
    ORDER  BY raw_id
    LIMIT  %(chunk)s
)
DELETE FROM fd_raw_events
 WHERE raw_id IN (SELECT raw_id FROM slice);
