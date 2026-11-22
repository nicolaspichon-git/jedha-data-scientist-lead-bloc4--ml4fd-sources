-- Raise an alert for every flagged score that has none yet.
-- Block: B3, B4.
-- Lot: B3-N5, B4-L0.
-- Competency: B3-C3.
-- Covers: B3-EI05, B3-EC23.
--
-- NOT SCOPED TO A BATCH, and that is deliberate. Scoping to the batch just
-- scored would be marginally cheaper and would lose an alert for good if this
-- statement failed once: the batch moves on and nothing ever looks back. Over
-- every flagged score it is SELF-HEALING -- a run that failed is repaired by
-- the next one.
--
-- The cost is bounded by the partial index on fd_scores (scored_at) WHERE
-- flagged and by the primary-key lookup on fd_frauds, so the statement stays an
-- index scan over the flagged rows rather than a table scan.
--
-- ON CONFLICT DO NOTHING on the primary key: the first version that flagged a
-- transaction is the one that raised its alert, and a retrain does not raise a
-- second. fd_scores keeps every version's score; fd_frauds keeps the alert.

INSERT INTO fd_frauds (txn_id, score, threshold, model_version)
SELECT s.txn_id, s.score, s.threshold, s.model_version
FROM   fd_scores s
WHERE  s.flagged
  AND  NOT EXISTS (SELECT 1 FROM fd_frauds a WHERE a.txn_id = s.txn_id)
ORDER  BY s.scored_at, s.txn_id
ON CONFLICT (txn_id) DO NOTHING;
