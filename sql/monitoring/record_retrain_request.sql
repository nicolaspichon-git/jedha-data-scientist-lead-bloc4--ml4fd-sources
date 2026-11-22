-- Record a retraining request, AFTER it was made.
-- Block: B4.
-- Lot: B4-L4.
-- Competency: B4-C5.
-- Covers: B4-EC33, B4-EC37.
--
-- Append-only: the table revokes UPDATE and DELETE from fd_ingest, so a
-- request cannot be rewritten into one that was never made.
INSERT INTO fd_retrain_requests (run_id, controls, reason)
VALUES (%(run_id)s, %(controls)s, %(reason)s);
