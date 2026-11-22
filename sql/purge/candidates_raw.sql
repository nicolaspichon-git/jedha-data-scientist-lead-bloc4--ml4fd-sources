-- The batches of the raw zone that are beyond retention, with their fingerprint.
-- Block: B3, B4.
-- Lot: B3-Q2, B4-L0.
-- Competency: B3-C5.
-- Covers: B3-EC19, B3-EC26.
--
-- READ ONLY. This file decides WHAT IS PURGEABLE, and the delete of Q3 takes
-- its batch_id from here rather than carrying a date predicate of its own: one
-- definition, so the dry run and the purge cannot disagree about the set. Two
-- copies of a predicate is how a purge comes to delete what no dry run showed.
--
-- WHOLE BATCHES, AND THAT IS NOT A CONVENIENCE. The fingerprint of a batch
-- (Q1) covers its lines together. Deleting part of a batch would leave a
-- remainder that no recorded digest describes -- unverifiable for ever, and
-- indistinguishable from a batch someone tampered with. So the criterion is on
-- the LAST row of the batch: the whole batch is beyond retention, or none of
-- it is. A minute-DAG batch spans one minute, so this costs nothing; the
-- historical batches are the case it protects.
--
-- NO now() ANYWHERE. The instant comes from the caller, which in the DAG is
-- data_interval_end -- the same discipline as the morning report: a purge that
-- read the clock could not be replayed, and could not be demonstrated on a
-- chosen date either.
--
-- The size is APPROXIMATE: pg_column_size() of the payload ignores the
-- indexes and the TOAST compression, and the space returns to the table rather
-- than to the operating system until a VACUUM.
--
-- Parameter: before.

WITH batches AS (
    SELECT batch_id,
           min(source)                  AS source,
           count(*)                     AS n_rows,
           min(ingested_at)             AS first_at,
           max(ingested_at)             AS last_at,
           sum(pg_column_size(payload)) AS n_bytes
    FROM   fd_raw_events
    GROUP  BY batch_id
),
total AS (SELECT count(*) AS n_total FROM fd_raw_events)
SELECT b.batch_id,
       b.source,
       b.n_rows,
       b.first_at,
       b.last_at,
       b.n_bytes,
       t.n_total,
       -- The EARLIEST run that carries one, as sql/checks/batch_digest_recorded.sql
       -- reads it: a replay attaches later runs to the same batch, and the
       -- fingerprint belongs to the batch.
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
