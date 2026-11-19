-- Does the raw zone already hold the historical CSV?
-- Covers: EC23, EC24   Lot: L1   Competency: C2
--
-- THE one definition of "the history is already in the raw zone". preflight
-- reads it to refuse a second bootstrap; nothing else may ask the question its
-- own way.
--
-- A run that FAILED counts as much as one that succeeded: its rows are in the
-- raw zone all the same, and `replay -Batch <id>` is what takes them further.
-- Relaunching `load` instead gives a new run_id, hence a new batch_id, hence a
-- second copy of the same 555,719 lines -- which is how four copies came to sit
-- in this project's raw zone before this check existed. No purge removes them:
-- the cutoff is a date, and the copies share it.
SELECT p.state,
       p.run_id,
       r.batch_id,
       count(*)   AS n_rows,
       p.ended_at
FROM   fd_raw_events    r
JOIN   fd_pipeline_runs p ON p.batch_id = r.batch_id
WHERE  p.dag_id = %(dag_id)s
GROUP  BY p.state, p.run_id, r.batch_id, p.ended_at
ORDER  BY (p.state = 'success') DESC, p.ended_at NULLS LAST;
