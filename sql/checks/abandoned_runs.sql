-- Every run our journal has left open, and whether anything can close it.
-- Covers: EC14   Lot: N9 (revu en Q6)   Competency: C4
--
-- READ ONLY. sql/maintenance/030_close_abandoned_runs.sql closes what may be
-- closed, and one looks before one touches.
--
-- WHY A ROW GETS STUCK. fd_pipeline_runs is written as the facts become known,
-- so a run that dies between runs.start() and close_run leaves its row in
-- 'running' for ever. The DAG-level callback of N9 covers a task failure and a
-- dagrun timeout; it cannot cover the container being killed, which is what a
-- `docker compose down` does.
--
-- WHY THAT MATTERS MORE THAN AN UNTIDY TABLE. The guard of M1 counts
-- state = 'success' and state = 'failed' to decide whether a window is healthy
-- enough to train on. A run in 'running' is in NEITHER, so it is not a failure
-- the guard can see -- it is a run that silently never happened.
--
-- THIS FILE NOW LISTS EVERY OPEN RUN, not only the closable ones, and says for
-- each why it is or is not. The earlier version joined dag_run and therefore
-- showed NOTHING for the two cases that matter most to an operator:
--
--   * a run with no dag_run row at all -- tools/ingest_once.py and the digest
--     backfill of Q1 open runs that Airflow knows nothing about. The join hid
--     them, so `abandoned` reported "nothing to close" while the acceptance of
--     S3 counted one. Two readings of one rule, disagreeing.
--   * a run Airflow ALSO believes is running. Nothing here may close it --
--     Airflow may really be running it -- but an operator must see it, because
--     a dagrun stuck in 'running' is unblocked from the interface and from
--     nowhere else.
--
-- AIRFLOW'S VERDICT FIRST, AN AGE ONLY WHERE THERE IS NO VERDICT. "Airflow
-- says this run is over" is a fact. For a run Airflow never heard of there is
-- no such fact and no other signal, so an age is the only thing left: one
-- hour, which is thirty times the longest declared dagrun_timeout of the
-- minute DAG and leaves every hand-run tool far behind.

DO $$
BEGIN
    IF to_regclass('public.dag_run') IS NULL THEN
        RAISE EXCEPTION 'la table dag_run d''Airflow est absente de cette base'
            USING HINT = 'Lancer airflow-init une fois, ou verifier que '
                       || 'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN pointe bien '
                       || 'sur POSTGRES_DB.';
    END IF;
END $$;

SELECT r.run_id,
       r.dag_id,
       r.started_at::timestamp(0)      AS demarre_le,
       r.n_read,
       coalesce(d.state, 'aucune ligne dag_run') AS etat_airflow,
       round(extract(epoch FROM now() - r.started_at) / 60)::text || ' min'
                                        AS age,
       CASE
         WHEN d.run_id IS NOT NULL AND d.state <> 'running'
           THEN 'fermable : Airflow la donne terminee'
         WHEN d.run_id IS NULL
              AND r.started_at < now() - interval '1 hour'
           THEN 'fermable : aucune ligne dag_run, et plus d''une heure'
         WHEN d.run_id IS NULL
           THEN 'attendre : aucune ligne dag_run, moins d''une heure'
         ELSE 'intouchable : Airflow la dit encore en cours'
       END                              AS verdict
FROM   fd_pipeline_runs r
LEFT   JOIN dag_run d ON d.run_id = r.run_id AND d.dag_id = r.dag_id
WHERE  r.state = 'running'
ORDER  BY r.started_at;
