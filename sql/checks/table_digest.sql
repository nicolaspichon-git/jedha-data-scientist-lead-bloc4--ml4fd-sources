-- Deterministic digest of the exploited layer. Proves a replay is identical.
-- Block: B3, B4.
-- Lot: B3-L6, B4-L0.
-- Competency: B3-C5.
-- Covers: B3-EC23, B3-EC24.
--
-- to_jsonb(t) minus the bookkeeping columns, and not t::text: ingested_at
-- carries DEFAULT now() and batch_id changes on a replay under a new
-- identifier, so a digest over the whole row would differ even when every
-- business value is identical -- and would prove nothing about the data.
--
-- Removing the keys by name rather than listing the columns to keep: a column
-- added later enters the digest on its own, which is the safer default for a
-- check meant to catch an unintended change.

SELECT 'fd_transactions' AS store,
       count(*)          AS rows,
       md5(string_agg((to_jsonb(t) - 'ingested_at' - 'batch_id')::text,
                      E'\n' ORDER BY t.txn_id)) AS digest
FROM   fd_transactions t
UNION ALL
SELECT 'fd_ground_truth',
       count(*),
       md5(string_agg((to_jsonb(g) - 'source')::text,
                      E'\n' ORDER BY g.txn_id))
FROM   fd_ground_truth g
UNION ALL
SELECT 'fd_quarantine',
       count(*),
       md5(string_agg((to_jsonb(q) - 'quarantine_id' - 'rejected_at' - 'batch_id')::text,
                      E'\n' ORDER BY q.line_no, q.reason_code))
FROM   fd_quarantine q;
