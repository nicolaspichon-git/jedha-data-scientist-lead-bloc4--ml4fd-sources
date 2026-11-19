-- One notification attempt that failed.
-- Covers: EI05, EC14   Lot: N5   Competency: C4
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
