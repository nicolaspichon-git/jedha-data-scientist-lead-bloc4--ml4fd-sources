-- The sentence G2 cannot be answered honestly without.
-- Block: B3, B4.
-- Lot: B3-P1, B4-L0.
-- Competency: B3-C2, B3-C4.
-- Covers: B3-EE04, B3-EI04, B3-EC14.
--
-- G2 asks each morning for "yesterday's payments AND FRAUDS". Taken at its
-- word the request is impossible: yesterday's frauds are not known yesterday,
-- and no system can know them -- a fraud is discovered when the cardholder
-- disputes. The report must therefore say, in figures:
--
--     N transactions of yesterday are still awaiting their verdict.
--     It falls no later than DD/MM.
--
-- Omitting that sentence would let a reader take "frauds confirmed: 0" for a
-- complete column, which is the worst possible reading of the document.
--
-- "KNOWN" IS label_ts <= knowledge_at AND NOT "a row exists". The row is
-- written the night of the payment with a label_ts in the future, so its mere
-- existence proves nothing: the filter is the withholding. knowledge_at is
-- data_interval_end, never now(), or a replay would answer with today's
-- knowledge about an older day.
--
-- without_verdict_row IS A FINDING, not a formatting detail. dag_ingest_verdicts
-- runs nightly, so by report time every transaction of yesterday should carry
-- its row with a due date. A non-zero count means that DAG did not run, and the
-- report cannot then say when the verdict falls -- so it says THAT instead.
-- Section 4 reads this number for the same reason.

WITH day AS (
    SELECT t.txn_id, t.txn_ts
      FROM fd_transactions t
     WHERE t.txn_ts >= %(window_start)s
       AND t.txn_ts <  %(window_end)s
),
pending AS (
    SELECT d.txn_id, d.txn_ts, g.label_ts
      FROM day d
      LEFT JOIN fd_ground_truth g ON g.txn_id = d.txn_id
     WHERE g.txn_id IS NULL
        OR g.label_ts > %(knowledge_at)s
)
SELECT (SELECT count(*) FROM day)                        AS transactions,
       count(*)                                          AS pending,
       count(*) FILTER (WHERE label_ts IS NULL)           AS without_verdict_row,
       min(label_ts)                                      AS first_due_at,
       max(label_ts)                                      AS last_due_at
  FROM pending;
