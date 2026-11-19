-- The facts about the recent runs of one DAG. ONE ROW, never a judgement.
-- Covers: EC14   Lot: N9   Competency: C4
--
-- Gathered here, judged in ml4fd/health.py -- the same split as M1's guard:
-- inspect_window() collects, verdict() decides. The decision is then a pure
-- function of these numbers and can be exercised without a database, which is
-- what tests/test_health.py does with all four controls.
--
-- WHY seconds_since_success IGNORES THE WINDOW. Freshness is the question "when
-- did this pipeline last work", and a window of an hour cannot answer it when
-- nothing has worked for a day: it would return null and a null would read as
-- "no data" rather than "down for a day". It therefore looks at the whole
-- journal, and is null only when the pipeline has NEVER succeeded.
--
-- THE BOUNDS ARE PARAMETERS AND THE INSTANT OF REFERENCE IS ONE TOO, which is
-- what lets ONE query serve two callers that need opposite windows:
--
--   N9's control task   a ROLLING window, the last hour before now
--   S4's morning report a CALENDAR window, the day it covers
--
-- The report must be replayable -- a run of 3 November has to produce the
-- health of 3 November -- so it passes explicit instants. The control task
-- passes null for as_of and gets the DATABASE clock, which is the right one for
-- a freshness reading: the container's clock and the server's can differ, and
-- the journal's timestamps are the server's.
--
-- Writing a second query for the report was the alternative, and it would have
-- given two definitions of "rejected lines". One query, two windows.
--
-- WHY runs_empty COUNTS n_read = 0 AND n_read IS NULL TOGETHER. The counters are
-- recorded by close_run, the last task, so a run that died earlier has a null
-- n_read. Both cases mean the same thing to an operator: that minute stored
-- nothing. runs_failed reports the state separately, so the two readings
-- together say whether the minute was lost to the API or to the chain.
--
-- WHY THE INVARIANT ONLY LOOKS AT RUNS WITH A KNOWN n_read. It compares the
-- lines read with the rows stored plus the lines quarantined, and a run whose
-- n_read is still null has nothing to compare: counting it would report every
-- in-flight and every crashed run as a broken invariant, which is the kind of
-- false alarm that teaches an operator to ignore the channel. count(DISTINCT
-- line_no) and not count(*), because one line can fail several checks and holds
-- one quarantine row per reason -- counting rows would exceed n_read and break
-- the invariant for a reason that has nothing to do with the data.

WITH at AS (
    SELECT coalesce(%(as_of)s::timestamptz, now()) AS instant
),
recent AS (
    SELECT *
      FROM fd_pipeline_runs
     WHERE dag_id = %(dag_id)s
       AND started_at >= %(window_start)s
       AND started_at <  %(window_end)s
)
SELECT
    (SELECT extract(epoch FROM (SELECT instant FROM at) - max(ended_at))
       FROM fd_pipeline_runs
      WHERE dag_id = %(dag_id)s
        AND state = 'success'
        AND ended_at < (SELECT instant FROM at))        AS seconds_since_success,
    (SELECT count(*) FROM recent
      WHERE state <> 'running')                         AS runs_ended,
    (SELECT count(*) FROM recent
      WHERE state = 'failed')                           AS runs_failed,
    (SELECT count(*) FROM recent
      WHERE state <> 'running'
        AND coalesce(n_read, 0) = 0)                    AS runs_empty,
    (SELECT coalesce(sum(n_read), 0) FROM recent)       AS lines_read,
    (SELECT coalesce(sum(n_rejected), 0) FROM recent)   AS lines_rejected,
    (SELECT count(*) FROM recent r
      WHERE r.batch_id IS NOT NULL
        AND r.n_read IS NOT NULL
        AND r.n_read <> (SELECT count(*) FROM fd_transactions t
                          WHERE t.batch_id = r.batch_id)
                      + (SELECT count(DISTINCT q.line_no) FROM fd_quarantine q
                          WHERE q.batch_id = r.batch_id))
                                                        AS batches_breaking_invariant;
