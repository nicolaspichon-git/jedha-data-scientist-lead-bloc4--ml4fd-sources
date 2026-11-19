-- Agreement between the two computation modes. MUST return zero rows.
-- Covers: EE07   Lot: L4   Competency: C5
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

WITH tied AS (
    SELECT txn_id
    FROM  (SELECT txn_id,
                  count(*) OVER (PARTITION BY client_key, txn_ts) AS peers
           FROM   fd_transactions) p
    WHERE peers > 1
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
rest AS (
    SELECT txn_id
    FROM   fd_transactions
    WHERE  txn_id NOT IN (SELECT txn_id FROM tied)
      AND  txn_id NOT IN (SELECT txn_id FROM firsts)
      AND  txn_id NOT IN (SELECT txn_id FROM stale)
    ORDER BY random()
    LIMIT  1000
),
sample AS (
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
