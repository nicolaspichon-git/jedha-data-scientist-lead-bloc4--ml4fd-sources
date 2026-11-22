-- Lines read and rejected by the stream over a calendar period, from the run
-- COUNTERS. Read by the dashboard (R7).
-- Block: B3, B4.
-- Lot: B3-R7, B3-R8, B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EC14, B3-EC15.
--
-- One table and no join. The rate could be computed by joining the quarantine
-- to the runs on batch_id -- and batch_id is a key of neither: a replayed
-- batch carries several runs, and the join would count its lines once per run
-- (sql/checks/raw_batches.sql, 1,667,157 lines for 555,719). The counters are
-- closed by CHECK (n_read = n_valid + n_rejected), so they say the same thing
-- without the trap.
--
-- The stream only (dag_id): a historical reload on the same day would
-- otherwise swamp the rate the operations control watches.
--
-- Parameters: dag_id, window_start, window_end.
SELECT coalesce(sum(n_read), 0)::bigint     AS lues,
       coalesce(sum(n_rejected), 0)::bigint AS rejetees
  FROM fd_pipeline_runs
 WHERE dag_id = %(dag_id)s
   AND started_at >= %(window_start)s
   AND started_at <  %(window_end)s;
