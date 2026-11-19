-- Stream batches holding transactions with no verdict.
-- Covers: EI03, EC14, EC23   Lot: N6   Competency: C3, C4
--
-- WHAT "AWAITING" MEANS HERE, and it is not what one would guess. The verdict
-- is not held back by delaying its WRITE: S1 writes it immediately with
-- label_ts = txn_ts + delay, and every consumer filters on
-- label_ts <= cut_off. The withholding is that filter, and it is the mechanism
-- L4 and M3 already rest on.
--
-- So this query does not ask "is the delay over?" -- it asks which batches have
-- transactions with no row in fd_ground_truth at all, which is a REPAIR
-- question: a batch whose verdict write failed, or one ingested after N6 moved
-- the write out of the ingestion. Self-healing, like the alert raising of N5.
--
-- EXISTS AND NOT A JOIN ON fd_raw_events, and that is the correction of a real
-- defect. The first version joined fd_raw_events to fd_transactions ON
-- batch_id -- which is not a key on either side, so a batch of 4 raw lines and
-- 4 transactions produced 4 x 4 = 16 rows. The operator read "16 ligne(s) dont
-- 16 sans verdict" for a batch that holds four, and every count derived from it
-- was the square of the truth. A cartesian product is quiet: it does not fail,
-- it multiplies.
--
-- Counted on fd_transactions, because that is what a verdict attaches to: its
-- foreign key is on txn_id. The raw zone appears only to select the source,
-- which fd_transactions does not record.

SELECT t.batch_id,
       count(*)                                   AS transactions,
       count(*) FILTER (WHERE g.txn_id IS NULL)   AS without_verdict,
       min(t.txn_ts)                              AS earliest,
       max(t.txn_ts)                              AS latest
FROM   fd_transactions t
LEFT   JOIN fd_ground_truth g ON g.txn_id = t.txn_id
WHERE  EXISTS (SELECT 1
               FROM   fd_raw_events r
               WHERE  r.batch_id = t.batch_id
                 AND  r.source   = %(source)s)
GROUP  BY t.batch_id
HAVING count(*) FILTER (WHERE g.txn_id IS NULL) > 0
ORDER  BY min(t.txn_ts);
