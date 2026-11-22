-- The fingerprint RECORDED for one batch, if any.
-- Block: B3, B4.
-- Lot: B3-Q1, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EC23, B3-EI12.
--
-- Parameter: batch_id. The earliest run that carries one wins, which is the run
-- that wrote the batch: a replay attaches later runs to the same batch.

SELECT (p.digests #>> '{raw_batch,rows}')::bigint AS n_rows,
       p.digests #>> '{raw_batch,digest}'         AS digest,
       p.digests #>> '{raw_batch,origin}'         AS origin,
       p.run_id,
       -- Q3: what a purge has already done to this batch, if one started.
       -- Read HERE rather than by a query of its own: the caller that needs
       -- the fingerprint is the caller that needs to know whether a purge was
       -- interrupted, and two round trips would let the two answers come from
       -- two instants.
       p.digests #> '{purge}'                     AS purge
FROM   fd_pipeline_runs p
WHERE  p.digests #>> '{raw_batch,batch_id}' = %(batch_id)s::text
ORDER  BY p.started_at
LIMIT  1;
