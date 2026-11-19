-- THE SAME definition -- BATCH mode (backfill).
-- Covers: EE07   Lot: L4   Competency: C2, C5
--
-- Why a second expression rather than calling the function row by row: with 924
-- cardholders and roughly 601 transactions each, the per-row call costs about
-- 601^2 / 2 ~ 180,600 row visits per cardholder, so ~167 million in total, and
-- as many times over as there are features. The window sorts once: ~11 million,
-- shared by every feature.
--
-- EXCLUDE GROUP is the delicate point (ruling 2): RANGE ... CURRENT ROW includes
-- ties, and EXCLUDE GROUP removes the current row AND its ties. That yields
-- exactly "txn_ts strictly earlier", the exact counterpart of the function's
-- WHERE clause. With ROWS, ties would diverge.
-- Requires PostgreSQL 11 or later.

UPDATE fd_transactions AS t
SET    f_tx_count   = c.tx_count,
       f_avg_amount = c.avg_amount
FROM (
    SELECT txn_id,
           count(*)       OVER w                       AS tx_count,
           round(avg(amt) OVER w, 2)::numeric(12,2)     AS avg_amount
    FROM   fd_transactions
    WINDOW w AS (PARTITION BY client_key
                 ORDER BY     txn_ts
                 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                 EXCLUDE GROUP)
) AS c
WHERE  t.txn_id = c.txn_id;
