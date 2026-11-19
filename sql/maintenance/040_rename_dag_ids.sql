-- Carry the run journal across the rename of two DAGs.
-- Covers: EC12, EC14   Lot: N9   Competency: C4
--
-- WHY THE JOURNAL HAS TO FOLLOW. fd_pipeline_runs.dag_id is written by the DAG
-- that made the run, so every run before the rename carries the old name. Two
-- readers filter on it and would go blind to all of that history:
--
--   sql/checks/s3_acceptance.sql   its stream_runs CTE, so every criterion of
--                                  S3 about runs would see an empty journal
--   sql/checks/pipeline_health.sql the window of N9's four controls, so the
--                                  freshness would read "jamais abouti" until
--                                  a new run succeeded under the new name
--
-- THE GUARD OF M1 IS NOT CONCERNED, and that is worth stating because it is the
-- one that could refuse to train: ml4fd/guard.py counts the runs of a WINDOW
-- without filtering on dag_id, so a rename neither hides nor duplicates
-- anything for it.
--
-- AIRFLOW'S OWN TABLES ARE LEFT ALONE. dag, dag_run and task_instance belong to
-- Airflow, and the runs it executed really were runs of the old dag_id: the old
-- DAG keeps its history, the new one starts its own. Rewriting another system's
-- state to tidy up our own would be the kind of fix that breaks a scheduler.
--
-- THE NEW DAG IS CREATED PAUSED by Airflow's default, so nothing runs until it
-- is unpaused: .\fd-manager.ps1 realtime -Start
--
-- Replayable: re-running renames nothing, since no row carries the old names.

WITH renamed AS (
    UPDATE fd_pipeline_runs SET dag_id = 'dag_ingest'
     WHERE dag_id = 'dag_ingest_realtime'
    RETURNING 1
)
SELECT 'dag_ingest_realtime -> dag_ingest' AS renommage,
       count(*)::text || ' execution(s)'   AS journal
  FROM renamed;

WITH renamed AS (
    UPDATE fd_pipeline_runs SET dag_id = 'dag_train'
     WHERE dag_id = 'dag_train_model'
    RETURNING 1
)
SELECT 'dag_train_model -> dag_train'    AS renommage,
       count(*)::text || ' execution(s)' AS journal
  FROM renamed;

-- What the journal holds afterwards, so the operator sees the result rather
-- than a count of rows they have to trust.
SELECT dag_id,
       count(*)                                   AS executions,
       count(*) FILTER (WHERE state = 'success')   AS reussies,
       count(*) FILTER (WHERE state = 'failed')    AS en_echec,
       count(*) FILTER (WHERE state = 'running')   AS encore_en_cours,
       min(started_at)::timestamp(0)               AS premiere,
       max(started_at)::timestamp(0)               AS derniere
FROM   fd_pipeline_runs
GROUP  BY dag_id
ORDER  BY dag_id;
