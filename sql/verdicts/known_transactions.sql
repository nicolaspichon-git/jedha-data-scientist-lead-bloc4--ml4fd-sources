-- Which of these transaction identifiers exist in the exploited layer.
-- Block: B3, B4.
-- Lot: B3-N6, B4-L0.
-- Competency: B3-C3.
-- Covers: B3-EC14, B3-EC23.
--
-- WHY THIS EXISTS. fd_ground_truth.txn_id REFERENCES fd_transactions(txn_id),
-- so a verdict for a line that never became a transaction is refused by the
-- foreign key -- and it would take the whole batch's write with it. A line can
-- be missing for a reason that is perfectly normal: it went to the quarantine.
--
-- The ingestion knew which lines it had rejected and skipped them. This daily
-- run starts again from the raw zone and knows nothing of that, so it asks:
-- filtering on EXISTENCE rather than on a remembered reject list also covers
-- every other reason a transaction may be absent, which a reject list would
-- not.
--
-- The identifiers are derived in Python by SourceProfile.event_id, so they
-- arrive as a parameter rather than being recomputed here: one derivation, as
-- for the whole chain.

SELECT t.txn_id
FROM   fd_transactions t
WHERE  t.txn_id = ANY(%(txn_ids)s);
