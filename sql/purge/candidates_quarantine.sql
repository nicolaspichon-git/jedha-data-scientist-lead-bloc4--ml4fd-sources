-- The quarantined records beyond retention, by batch, with the batch fingerprint.
-- Covers: EC19, EC26   Lot: Q2   Competency: C5
--
-- WHY THIS STORE AT ALL, and why at the same duration as the raw zone.
-- fd_quarantine.payload is the raw payload of a REJECTED record: the same
-- personal data as fd_raw_events.payload. Before Q0 no duration covered it, so
-- a rejected transaction would have kept its data for ever while an accepted
-- one was purged at 90 days -- the opposite of limiting retention.
--
-- THE SAME FINGERPRINT GOVERNS IT. A quarantined row is derived from a line of
-- its batch in the raw zone, and the batch fingerprint (Q1) covers that line.
-- So the refusal rule is the batch's: no recorded fingerprint, no purge. The
-- quarantine is not fingerprinted a second time, which would be a second
-- definition of the same evidence.
--
-- Whole batches, no now(): same reasons as sql/purge/candidates_raw.sql.
--
-- Parameter: before.

WITH batches AS (
    SELECT batch_id,
           count(*)                     AS n_rows,
           min(rejected_at)             AS first_at,
           max(rejected_at)             AS last_at,
           sum(pg_column_size(payload)) AS n_bytes
    FROM   fd_quarantine
    GROUP  BY batch_id
),
total AS (SELECT count(*) AS n_total FROM fd_quarantine)
SELECT b.batch_id,
       'quarantine'::text AS source,
       b.n_rows,
       b.first_at,
       b.last_at,
       b.n_bytes,
       t.n_total,
       (SELECT p.digests #>> '{raw_batch,digest}'
          FROM   fd_pipeline_runs p
         WHERE   p.digests #>> '{raw_batch,batch_id}' = b.batch_id::text
         ORDER   BY p.started_at
         LIMIT   1) AS recorded_digest
-- LEFT JOINED ON THE TOTAL, so that a row always comes back. Read the other
-- way round -- candidates first, total attached -- a store with nothing beyond
-- its cutoff returned no row at all, and the dry run then showed "0 lignes sur
-- 0", which an operator reads as "the table is empty". It is not: there was
-- simply no candidate to carry the total. The caller skips the row whose
-- batch_id is null.
FROM   total t
LEFT   JOIN batches b ON b.last_at < %(before)s
ORDER  BY b.last_at, b.batch_id;
