-- Agreement between the two computation modes. MUST return zero rows.
-- Block: B3, B4.
-- Lot: B3-L4, B4-L0.
-- Competency: B3-C5.
-- Covers: B3-EE07.
--
-- STRATIFIED, not random, and that is the whole point of this file.
--
-- Measured on the source: 24 (client_key, txn_ts) pairs are shared by two
-- transactions, so 48 rows out of 555,719 sit on a timestamp tie. A random
-- sample of 1,000 rows has about an 8 % chance of containing a single one of
-- them -- meaning a random test would let a ROWS-based backfill pass nine times
-- out of ten, and ties are precisely what tells ROWS from RANGE ... EXCLUDE
-- GROUP (ruling 2). The 924 first-transactions-per-cardholder, which carry
-- ruling 6 (tx_count = 0, avg_amount = NULL), are just as under-represented.
--
-- So the sample is built deliberately:
--   every row sitting on a tie            -- probes ruling 2
--   every cardholder's first transaction  -- probes ruling 6
--   every row with no features at all     -- probes a missed apply_batch
--   1,000 random rows among the rest      -- probes the ordinary case
--
-- sql/checks/reconcile_coverage.sql reports the size of each stratum: a sample
-- whose tie stratum is empty is as blind as a random one.

-- A SEMI-JOIN AND NOT A WINDOW FUNCTION, for one reason: the planner can
-- estimate this and could not estimate that.
--
-- count(*) OVER (PARTITION BY ...) > 1 asks for a selectivity PostgreSQL has
-- no statistic for, so it assumed the whole input: 1,027,440 rows where the
-- truth is forty, an error of twenty-five thousandfold (2026-11-19). The
-- estimate then propagated -- the UNION, the sample, and the LATERAL above it
-- were all planned for a million rows, and the three IN (SELECT ...) of the
-- CASE fell back to unhashed subplans.
--
-- EXISTS over (client_key, txn_ts) is a semi-join, which the index
-- fd_transactions_client_ts_idx
-- serves it and the column statistics estimate it. Strictly equivalent -- "at
-- least two rows share this (client_key, txn_ts)" is the same predicate, and
-- txn_id being the primary key makes o.txn_id <> t.txn_id exactly "another
-- row". Verified against the window form on the loaded corpus before this was
-- committed: same count, and EXCEPT empty in both directions.
--
-- IT WORKED EITHER WAY ON THIS CORPUS, which is the whole point of changing
-- it. Forty tied rows make even a per-row rescan cheap, so the bad estimate
-- cost nothing here. A corpus with coarser timestamps would collapse the plan
-- again, and nobody would know why. A plan that holds by arithmetic is worth
-- more than one that holds by luck.
WITH tied AS (
    SELECT t.txn_id
    FROM   fd_transactions t
    WHERE  EXISTS (SELECT 1
                   FROM   fd_transactions o
                   WHERE  o.client_key = t.client_key
                     AND  o.txn_ts     = t.txn_ts
                     AND  o.txn_id    <> t.txn_id)
),
-- FOURTH STRATUM: the rows that carry NO features at all.
--
-- A first transaction has f_tx_count = 0 and f_avg_amount NULL (ruling 6), so
-- f_tx_count IS NULL means something else entirely: the features were never
-- written. On 2026-11-16 seven such rows existed -- stream transactions
-- ingested before N3 computed features at all -- and a 1,000-row random sample
-- out of 555,730 had a 1.25 % chance of catching any of them.
--
-- The reasoning is the one that produced the tied and firsts strata: a sample
-- that is nearly blind to the rows most likely to be wrong proves nothing about
-- them. Stale rows are cheap to find, so they are all taken.
stale AS (
    SELECT txn_id FROM fd_transactions WHERE f_tx_count IS NULL
),
firsts AS (
    SELECT DISTINCT ON (client_key) txn_id
    FROM     fd_transactions
    ORDER BY client_key, txn_ts
),
-- NOT EXISTS AND NOT NOT IN, and the difference is a thousandfold.
--
-- NOT IN must answer "is this value absent" in a way that copes with a NULL
-- inside the subquery, and that obligation stops the planner using a hash
-- anti-join whenever the hash would not fit in work_mem. On 1,296,675 rows it
-- did not fit: EXPLAIN showed "NOT (SubPlan 4)" and "NOT (SubPlan 6)" --
-- unhashed -- which means the whole CTE is rescanned FOR EVERY OUTER ROW. The
-- node cost 1.5e11 and the query had run fifteen minutes without finishing
-- (2026-11-19). Only firsts, at 929 rows, got "hashed SubPlan".
--
-- NOT EXISTS carries no such obligation and the planner reads it as the
-- anti-join it is. Equivalent here beyond any doubt: txn_id is the PRIMARY
-- KEY, so it is never NULL, which is the only case where the two differ.
--
-- It did not show on the former corpus. 555,719 rows hashed inside work_mem;
-- 1,296,675 did not. The defect was always there, under a threshold.
rest AS (
    SELECT t.txn_id
    FROM   fd_transactions t
    WHERE  NOT EXISTS (SELECT 1 FROM tied   d WHERE d.txn_id = t.txn_id)
      AND  NOT EXISTS (SELECT 1 FROM firsts f WHERE f.txn_id = t.txn_id)
      AND  NOT EXISTS (SELECT 1 FROM stale  s WHERE s.txn_id = t.txn_id)
    ORDER BY random()
    LIMIT  1000
),
-- MATERIALIZED, which is the other half of the repair and the bigger half.
--
-- Inlined, this CTE lets the planner move the LATERAL call to the other side
-- of a join: EXPLAIN showed a Nested Loop whose outer side was an index scan
-- over the WHOLE table, so fd_client_features was called 2.6 million times
-- instead of about two thousand. Cost 8.9e9 on that node alone.
--
-- MATERIALIZED forces the sample to be computed first -- a few thousand rows
-- -- and the lateral then runs over those and nothing else. It changes no
-- semantics: it is an instruction about execution, not a different query.
--
-- WHY THE PLANNER GOT IT SO WRONG, and it is worth knowing because no amount
-- of ANALYZE fixes it: backfill_features rewrites every feature of this table
-- and then runs this query IN THE SAME TRANSACTION. The statistics necessarily
-- predate the UPDATE, so the estimate for stale -- rows whose features are
-- still NULL -- is the opposite of the truth, by the full height of the table.
-- A query planned on statistics that cannot describe the data it will read
-- needs its plan constrained, not its statistics refreshed.
sample AS MATERIALIZED (
    SELECT t.txn_id, t.client_key, t.txn_ts, t.f_tx_count, t.f_avg_amount,
           CASE WHEN t.txn_id IN (SELECT txn_id FROM tied)   THEN 'tie'
                WHEN t.txn_id IN (SELECT txn_id FROM stale)  THEN 'stale'
                WHEN t.txn_id IN (SELECT txn_id FROM firsts) THEN 'first'
                ELSE 'random' END AS stratum
    FROM   fd_transactions t
    WHERE  t.txn_id IN (SELECT txn_id FROM tied
                        UNION SELECT txn_id FROM stale
                        UNION SELECT txn_id FROM firsts
                        UNION SELECT txn_id FROM rest)
)
SELECT s.stratum,
       s.txn_id,
       s.f_tx_count   AS batch_tx_count,   f.tx_count   AS row_tx_count,
       s.f_avg_amount AS batch_avg_amount, f.avg_amount AS row_avg_amount
FROM   sample s
CROSS JOIN LATERAL fd_client_features(s.client_key, s.txn_ts) AS f
-- IS DISTINCT FROM rather than <>: NULL = NULL has to hold true here, otherwise
-- every first transaction (ruling 6) comes out as a false divergence.
WHERE  s.f_tx_count   IS DISTINCT FROM f.tx_count
   OR  s.f_avg_amount IS DISTINCT FROM f.avg_amount;
