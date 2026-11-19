-- Is this batch later than everything already stored?
-- Covers: EE07   Lot: N3   Competency: C5
--
-- The question that decides whether a batch-scoped feature update is sound.
-- Returns the number of stored transactions that are NOT in the batch and are
-- later than the batch's earliest row. It must be 0.
--
-- Above 0, apply_batch.sql would leave stale features on those later rows, and
-- only a full backfill puts the table right. Nothing would raise.

SELECT count(*) AS later_rows,
       min(t.txn_ts) AS earliest_offender
FROM   fd_transactions t
WHERE  t.batch_id IS DISTINCT FROM %(batch_id)s::uuid
  AND  t.txn_ts >= (SELECT min(b.txn_ts) FROM fd_transactions b
                    WHERE b.batch_id = %(batch_id)s::uuid);
