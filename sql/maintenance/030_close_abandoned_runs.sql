-- Close the runs nobody closed. Covers: EC14  Lot: N9 (revu le 20/11/2026)  C4
--
-- WRITES. sql/checks/abandoned_runs.sql lists what this will touch and carries
-- the reasoning; this file carries the consequences. One looks before one
-- touches: `fd-manager abandoned` without -Apply.
--
-- CLOSED AS A FAILURE, never as a success. The run did not reach close_run, so
-- nothing recorded its counters: calling that a success would put a run with
-- no volumes among the healthy ones and let the guard of M1 count a window as
-- sound on the strength of it.
--
-- ONE STATEMENT, WHERE THERE WERE TWO. The first closed what Airflow declared
-- over, the second the orphans Airflow never heard of. Since L1 gave Airflow a
-- database of its own, dag_run cannot be read from here, and both cases now
-- rest on the same evidence: nobody closed this row, and it is old enough that
-- nobody will. One evidence, one statement -- and the RETURNING says it.
--
-- duration_s IS LEFT NULL, AND THAT IS THE POINT. The old file took Airflow's
-- end_date and used now() only as a fallback. With no end_date to be had,
-- now() is the moment somebody tidied up and not the moment the run stopped:
-- written into duration_s it would be a measurement of our own delay, in a
-- column every reader takes for a duration. NULL states what is true, that the
-- duration is unknown -- and `duration_s IS NULL` is exactly how a reader
-- tells these rows from the ones runs.abandon() closed at the instant of
-- failure, where now() really was the end. ended_at keeps now(): when we
-- closed it is a fact, and worth keeping.
--
-- REPLAYABLE, like the rest of this directory: the statement filters on
-- state = 'running', so a second run closes nothing more. Running it twice is
-- not a mistake somebody has to avoid.
--
-- A RUN YOUNGER THAN AN HOUR IS NEVER TOUCHED. It may really be running, and a
-- journal that contradicted a live execution would be worse than an untidy
-- one. The check lists it, with that verdict.

UPDATE fd_pipeline_runs r
   SET state      = 'failed',
       ended_at   = now(),
       duration_s = NULL
 WHERE r.state = 'running'
   AND r.started_at < now() - interval '1 hour'
RETURNING r.run_id, r.dag_id, r.started_at::timestamp(0) AS demarre_le,
          r.ended_at::timestamp(0) AS ferme_a,
          'personne ne l''a fermee, et plus d''une heure'::text AS motif;
