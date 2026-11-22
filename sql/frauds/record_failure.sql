-- One notification attempt that failed.
-- Block: B3, B4.
-- Lot: B3-N5, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EI05, B3-EC14.
--
-- The alert stays raised with notified_at null, so the next run picks it up.
-- last_error is overwritten rather than accumulated: the current reason is what
-- a human acts on, and an unbounded log of reasons in a column is a log in the
-- wrong place.

UPDATE fd_frauds
SET    attempts   = attempts + 1,
       last_error = left(%(reason)s, 500)
WHERE  txn_id = %(txn_id)s
  AND  notified_at IS NULL;
