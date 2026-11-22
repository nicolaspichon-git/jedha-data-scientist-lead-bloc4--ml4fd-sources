-- The latest runs of the watched DAG, most recent first. Read by the dashboard.
-- Block: B3, B4.
-- Lot: B3-R6, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EC14.
--
-- What the four controls are computed FROM, shown beside them: a red control
-- is read against the runs that made it red. One table, no join, so nothing
-- here can count a run twice.
--
-- Parameters: dag_id, limit.
SELECT run_id,
       started_at,
       ended_at,
       state,
       n_read,
       n_valid,
       n_rejected,
       extract(epoch FROM ended_at - started_at) AS seconds
  FROM fd_pipeline_runs
 WHERE dag_id = %(dag_id)s
 ORDER BY started_at DESC
 LIMIT %(limit)s;
