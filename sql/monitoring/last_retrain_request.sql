-- When the monitoring last asked for a retraining, or no row at all.
-- Block: B4.
-- Lot: B4-L4.
-- Competency: B4-C5.
-- Covers: B4-EC33.
--
-- ONE ROW OR NONE, and never an aggregate over an empty table: max() would
-- return NULL and the caller would have to tell a NULL apart from "never
-- asked", which are the same thing here but would not read as such.
SELECT requested_at, controls, reason
FROM   fd_retrain_requests
ORDER  BY requested_at DESC
LIMIT  1;
