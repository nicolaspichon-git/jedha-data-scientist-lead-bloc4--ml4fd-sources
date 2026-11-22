-- The alerts not yet notified, oldest first.
-- Block: B3, B4.
-- Lot: B3-N5, B4-L0.
-- Competency: B3-C3.
-- Covers: B3-EI05.
--
-- NO MAXIMUM ATTEMPT COUNT. An alert abandoned after N failures is a fraud
-- nobody is told about, and the table would hold it looking settled. So every
-- pending alert is retried at every run, and what keeps the log readable is
-- that the notifier prints ONE line per run with the counts -- not one line per
-- alert per minute.
--
-- The payload carries no identifying column because fd_transactions has none
-- (EC17). client_key is deliberately NOT selected: it is a stable pseudonym
-- linking a cardholder's whole history, and a set of them would let somebody
-- group transactions by person. The internal channels can look it up; a
-- message does not carry it.

SELECT a.txn_id,
       a.score,
       a.threshold,
       a.model_version,
       a.raised_at,
       a.attempts,
       t.txn_ts,
       t.merchant,
       t.category,
       t.amt
FROM   fd_frauds a
JOIN   fd_transactions t ON t.txn_id = a.txn_id
WHERE  a.notified_at IS NULL
ORDER  BY a.raised_at, a.txn_id
LIMIT  %(limit)s;
