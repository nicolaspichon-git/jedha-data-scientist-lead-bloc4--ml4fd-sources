-- The batches of the raw zone whose fingerprint was never recorded.
-- Covers: EC23, EI12   Lot: Q1   Competency: C4
--
-- READ ONLY. A batch listed here cannot be purged: the purge refuses it, and
-- says so. `fd-manager digests` lists them, and -Apply records them.
--
-- A batch is covered when ANY run of the journal carries its fingerprint in
-- digests -> raw_batch -> batch_id. Not "the run that owns it": a replay
-- attaches a second run to the same batch, and the fingerprint belongs to the
-- batch, not to whichever run wrote it down.
--
-- Counted per batch first, then tested per batch: the journal is a few thousand
-- rows and the raw zone more than a million, so the test must not run per row.

WITH batches AS (
    SELECT batch_id,
           min(source)      AS source,
           count(*)         AS n_rows,
           min(ingested_at) AS first_at,
           max(ingested_at) AS last_at
    FROM   fd_raw_events
    GROUP  BY batch_id
)
SELECT b.batch_id, b.source, b.n_rows, b.first_at, b.last_at
FROM   batches b
WHERE  NOT EXISTS (
    SELECT 1
    FROM   fd_pipeline_runs p
    WHERE  p.digests #>> '{raw_batch,batch_id}' = b.batch_id::text)
ORDER  BY b.first_at, b.batch_id;
