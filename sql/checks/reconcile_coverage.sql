-- Composition of the reconciliation sample. Guards against a vacuous test.
-- Block: B3, B4.
-- Lot: B3-L4, B4-L0.
-- Competency: B3-C5.
-- Covers: B3-EE07.
--
-- A reconciliation whose tie stratum holds zero rows proves nothing about
-- ruling 2. This query is what lets the DAG refuse such a run rather than
-- report a green test that probed nothing.

WITH tied AS (
    SELECT txn_id
    FROM  (SELECT txn_id,
                  count(*) OVER (PARTITION BY client_key, txn_ts) AS peers
           FROM   fd_transactions) p
    WHERE peers > 1
),
firsts AS (
    SELECT DISTINCT ON (client_key) txn_id
    FROM     fd_transactions
    ORDER BY client_key, txn_ts
)
SELECT (SELECT count(*) FROM fd_transactions)            AS total_rows,
       (SELECT count(*) FROM tied)                       AS tie_rows,
       (SELECT count(*) FROM firsts)                     AS first_rows,
       -- A VERDICT now, not an informative figure. These rows are a fourth
       -- stratum of reconcile_features.sql, so a non-zero count makes the
       -- reconciliation fail instead of being reported to nobody. Repaired by
       -- features.backfill(), which recomputes the whole table.
       (SELECT count(*) FROM fd_transactions
         WHERE f_tx_count IS NULL)                       AS rows_without_features;
