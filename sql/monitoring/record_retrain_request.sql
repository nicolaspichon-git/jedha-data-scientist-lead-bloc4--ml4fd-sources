-- Record a retraining request, AFTER it was made. Covers: EC33, EC37
-- Lot: L4   Competency: C5
--
-- Append-only: the table revokes UPDATE and DELETE from fd_ingest, so a
-- request cannot be rewritten into one that was never made.
INSERT INTO fd_retrain_requests (run_id, controls, reason)
VALUES (%(run_id)s, %(controls)s, %(reason)s);
