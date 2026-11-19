-- The latest runs of the watched DAG, most recent first. Read by the dashboard.
-- Covers: EC14   Lot: R6   Competency: C4
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
