-- The runs that carry one batch. Readable by fd_ingest AND by fd_purge.
-- Covers: EC18, EC23   Lot: Q7   Competency: C4
--
-- SPLIT FROM batch_stores.sql ALONG A PRIVILEGE BOUNDARY, not for tidiness.
-- fd_purge holds no grant whatsoever on fd_transactions -- that is the
-- acceptance criterion of S5, read from the catalogue -- so the role that
-- deletes cannot evaluate the refusals that look at the exploited layer. It
-- CAN read fd_pipeline_runs, and the two refusals that must not go stale
-- between the plan and the deletion are exactly the ones here: a run that
-- succeeded, a run still open. So this file is read twice, by two roles, and
-- ml4fd/purge.py turns its three numbers into refusals in one place.
--
-- Parameter: batch_id (text; cast, like batch_stores.sql).
SELECT count(*)                                            AS n_runs,
       count(*) FILTER (WHERE state = 'success')            AS n_success,
       count(*) FILTER (WHERE state = 'running')            AS n_running
FROM   fd_pipeline_runs
WHERE  batch_id = %(batch_id)s::uuid;
