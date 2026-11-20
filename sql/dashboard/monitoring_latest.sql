-- Where each monitoring control stands now (S6/R11): the most recent JUDGED
-- measurement of each one, with the state it carries and since when.
-- Covers: EC45, EC49   Lot: L4   Competency: C6
--
-- JUDGED AND NOT OBSERVED. drift_measurements records two kinds of row: the
-- share of drifted columns, which carries a verdict, and one row per column,
-- which carries the figure the tool computed and the limit the tool chose,
-- with no verdict of ours. Mixing them on this page would show five drift
-- "states" where the policy declares one.
--
-- strpos AND NOT LIKE, deliberately. A LIKE pattern needs a per cent sign, and
-- a bare one anywhere in this file -- a comment included -- breaks psycopg's
-- parameter substitution. The incident is in tests/test_module_scope.py.
--
-- fd_ops_state CARRIES since, fd_monitoring DOES NOT. A measurement says what
-- was true of one window; the state table says since when the control has been
-- saying it. The two answer different questions and the page shows both.
--
-- Parameters: none.
WITH judged AS (
    SELECT m.*,
           row_number() OVER (PARTITION BY m.control
                              ORDER BY m.measured_at DESC,
                                       m.measurement_id DESC) AS rank
      FROM fd_monitoring m
     WHERE strpos(m.metric, 'column_drift:') = 0
)
SELECT j.control,
       j.metric,
       j.value,
       j.threshold,
       j.state,
       j.detail,
       j.measured_at,
       j.model_version,
       j.n_rows,
       j.n_labelled,
       j.tool,
       j.window_start,
       j.window_end,
       s.state  AS recorded_state,
       s.since  AS recorded_since
  FROM judged j
  LEFT JOIN fd_ops_state s ON s.control = j.control
 WHERE j.rank = 1
 ORDER BY j.control;
