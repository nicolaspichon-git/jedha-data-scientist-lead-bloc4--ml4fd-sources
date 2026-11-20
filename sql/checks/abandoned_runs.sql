-- Every run our journal has left open, and whether anything can close it.
-- Covers: EC14   Lot: N9 (revu en Q6, puis le 20/11/2026)   Competency: C4
--
-- READ ONLY. sql/maintenance/030_close_abandoned_runs.sql closes what may be
-- closed, and one looks before one touches.
--
-- WHY A ROW GETS STUCK. fd_pipeline_runs is written as the facts become known,
-- so a run that dies between runs.start() and close_run leaves its row in
-- 'running' for ever.
--
-- WHY THAT MATTERS MORE THAN AN UNTIDY TABLE. The guard of M1 counts
-- state = 'success' and state = 'failed' to decide whether a window is healthy
-- enough to train on. A run in 'running' is in NEITHER, so it is not a failure
-- the guard can see -- it is a run that silently never happened.
--
-- AIRFLOW'S VERDICT IS NO LONGER AVAILABLE HERE, AND THAT IS DELIBERATE. Until
-- L1 this file joined dag_run: the project and Airflow shared one database, so
-- "Airflow says this run is over" was a fact one SELECT away. L1 moved
-- Airflow's metadata into a database of its own, owned by fd_airflow
-- (sql/initdb/010_databases.sh creates it, docker-compose.yml points Airflow
-- at it), so that whoever can run a DAG holds no owner rights on the ml4fd
-- tables. PostgreSQL does not join across databases, and reaching dag_run
-- again would mean dblink or postgres_fdw -- undoing a privilege boundary in
-- order to tidy a journal. The join survived L1 by a day and this file raised
-- on its own guard, which is how it was found, on 2026-11-20.
--
-- WHAT CLOSES A RUN NOW. dag_ingest carries on_failure_callback=_mark_failed on
-- the DAG, which calls runs.abandon(): every failure Airflow can report closes
-- the journal AT THE MOMENT IT HAPPENS, from inside the DAG, rather than being
-- reconstructed later from a join. That is the better arrangement of the two,
-- and it is what makes the loss below a small one.
--
-- SO ONE RULE, AN AGE, AND WHAT IT COSTS. A row still 'running' is one no
-- callback closed: the container was killed -- which is what `docker compose
-- down` does -- or a hand-run tool opened it (tools/ingest_once.py, the digest
-- backfill of Q1). One hour: thirty times the longest declared dagrun_timeout
-- of the minute DAG, and well beyond the longest historical load.
--
-- WHAT IS LOST, said plainly: a run killed with its container, which Airflow
-- marks failed when it comes back, now waits up to an hour instead of being
-- closable at once. An operator who wants Airflow's own verdict in the
-- meantime reads it from `fd-manager status` or from the Airflow interface,
-- and not from this database.
--
-- AND IT STAYS ONE RULE. Joining dag_run where it happens to exist and falling
-- back to an age where it does not would give two readings of one rule again,
-- which is the defect Q6 was spent on: `abandoned` reported nothing to close
-- while the acceptance of S3 counted one.
--
-- EVERY OPEN RUN IS LISTED, closable or not. The age decides the VERDICT and
-- never the WHERE: a filter there would hide the rows an operator most needs
-- to see, which is exactly what the inner join did before Q6.

SELECT r.run_id,
       r.dag_id,
       r.started_at::timestamp(0)      AS demarre_le,
       r.n_read,
       round(extract(epoch FROM now() - r.started_at) / 60)::text || ' min'
                                        AS age,
       CASE
         WHEN r.started_at < now() - interval '1 hour'
           THEN 'fermable : ouverte depuis plus d''une heure'
         ELSE 'attendre : moins d''une heure, elle peut etre en cours'
       END                              AS verdict
FROM   fd_pipeline_runs r
WHERE  r.state = 'running'
ORDER  BY r.started_at;
