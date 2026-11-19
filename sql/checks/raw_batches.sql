-- What the raw zone holds, by batch.
-- Covers: EC12, EC23   Lot: L7   Competency: C4
--
-- THE RULE THIS FILE EXISTS TO HOLD: aggregate BEFORE joining.
--
-- batch_id is not a key of fd_pipeline_runs. A batch loaded once and replayed
-- twice carries three runs, so the join is one-to-many, and a count(*) taken
-- after it counts each raw line once per run. On 2026-11-17 this listing
-- reported 1,667,157 lines for a batch holding 555,719 -- three identical runs,
-- three times the truth -- and that figure was about to be read as "the raw
-- zone holds three copies".
--
-- So the counting happens in CTEs over ONE table each, and the joins come
-- afterwards, on already-aggregated rows. The `executions` column is what makes
-- the one-to-many visible instead of silently multiplying something.
WITH lots AS (
    SELECT batch_id,
           source,
           count(*)           AS lignes,
           min(ingested_at)   AS ingere_le
    FROM   fd_raw_events
    GROUP  BY batch_id, source
),
txn AS (
    -- The exploited layer, per batch. THE fact that says whether a batch is
    -- the origin of anything: transactions point back at it, and its raw zone
    -- is what allows them to be replayed (EC24). A batch with raw lines and no
    -- transaction is a duplicate copy of a load that happened elsewhere.
    --
    -- Aggregated here, like the rest, and joined afterwards.
    SELECT batch_id, count(*) AS txn
    FROM   fd_transactions
    GROUP  BY batch_id
),
executions AS (
    SELECT batch_id, count(*) AS executions
    FROM   fd_pipeline_runs
    WHERE  batch_id IS NOT NULL
    GROUP  BY batch_id
),
derniere AS (
    -- The most recent run of the batch: with a load and its replays, the state
    -- that matters is the last one, not an arbitrary one the grouping kept.
    SELECT DISTINCT ON (batch_id)
           batch_id, state, n_read, n_valid, n_rejected
    FROM   fd_pipeline_runs
    WHERE  batch_id IS NOT NULL
    ORDER  BY batch_id, started_at DESC
)
SELECT l.batch_id,
       l.source,
       l.lignes,
       l.ingere_le::timestamp(0) AS ingere_le,
       d.state,
       coalesce(e.executions, 0) AS execs,
       coalesce(x.txn, 0)        AS exploitee,
       d.n_read, d.n_valid, d.n_rejected
FROM       lots       l
LEFT JOIN  executions e ON e.batch_id = l.batch_id
LEFT JOIN  txn        x ON x.batch_id = l.batch_id
LEFT JOIN  derniere   d ON d.batch_id = l.batch_id
ORDER BY   l.ingere_le;
