-- Close the runs nobody closed. Covers: EC14   Lot: N9 (revu en Q6)   C4
--
-- WRITES. sql/checks/abandoned_runs.sql lists what this will touch and why,
-- and one looks before one touches: `fd-manager abandoned` without -Apply.
--
-- CLOSED AS A FAILURE, never as a success. The run did not reach close_run, so
-- nothing recorded its counters: calling that a success would put a run with
-- no volumes among the healthy ones and let the guard of M1 count a window as
-- sound on the strength of it.
--
-- TWO CASES, AND THE SECOND WAS MISSING UNTIL Q6. The first is Airflow's own
-- verdict: the dagrun is over, so our row cannot legitimately still be open.
-- The second is a run Airflow never heard of -- tools/ingest_once.py and the
-- digest backfill of Q1 open runs of their own -- which the earlier inner join
-- silently excluded. Those runs can be closed by nothing else, and the old
-- file left them open for ever while `abandoned` reported nothing to do.
--
-- AN AGE ONLY WHERE THERE IS NO VERDICT. Five minutes would have to be wrong
-- for one DAG or the other, which is why Airflow's verdict comes first. For a
-- run Airflow knows nothing about there is no verdict and no other signal: one
-- hour, thirty times the longest dagrun_timeout of the minute DAG, and far
-- beyond any hand-run tool of this project.
--
-- REPLAYABLE, like the rest of this directory: both statements filter on
-- state = 'running', so a second run closes nothing more. Running it twice is
-- not a mistake somebody has to avoid.
--
-- A RUN AIRFLOW STILL CALLS RUNNING IS NEVER TOUCHED. It may really be
-- running, and a journal that contradicted Airflow would be worse than an
-- untidy one. Such a run is listed by the check with that verdict, and it is
-- unblocked from Airflow's interface.

DO $$
BEGIN
    IF to_regclass('public.dag_run') IS NULL THEN
        RAISE EXCEPTION 'la table dag_run d''Airflow est absente de cette base'
            USING HINT = 'Lancer airflow-init une fois, ou verifier que '
                       || 'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN pointe bien '
                       || 'sur POSTGRES_DB.';
    END IF;
END $$;

UPDATE fd_pipeline_runs r
   SET state      = 'failed',
       ended_at   = coalesce(d.end_date, now()),
       duration_s = round(extract(epoch FROM coalesce(d.end_date, now())
                                             - r.started_at)::numeric, 3)
FROM   (SELECT run_id, dag_id, state, end_date FROM dag_run) d
WHERE  d.run_id = r.run_id
  AND  d.dag_id = r.dag_id
  AND  r.state = 'running'
  AND  d.state <> 'running'
RETURNING r.run_id, r.dag_id, r.started_at::timestamp(0) AS demarre_le,
          r.ended_at::timestamp(0) AS ferme_a, r.duration_s,
          'Airflow la donnait terminee'::text AS motif;

-- The orphans: open in our journal, unknown to Airflow, and older than an
-- hour. A separate statement rather than one clause more, because the two
-- cases rest on different evidence and a reader must be able to tell which one
-- closed a given run -- the RETURNING of each says so.
UPDATE fd_pipeline_runs r
   SET state      = 'failed',
       ended_at   = now(),
       duration_s = round(extract(epoch FROM now() - r.started_at)::numeric, 3)
 WHERE r.state = 'running'
   AND NOT EXISTS (SELECT 1 FROM dag_run d
                    WHERE d.run_id = r.run_id AND d.dag_id = r.dag_id)
   AND r.started_at < now() - interval '1 hour'
RETURNING r.run_id, r.dag_id, r.started_at::timestamp(0) AS demarre_le,
          r.ended_at::timestamp(0) AS ferme_a, r.duration_s,
          'aucune ligne dag_run, et plus d''une heure'::text AS motif;
