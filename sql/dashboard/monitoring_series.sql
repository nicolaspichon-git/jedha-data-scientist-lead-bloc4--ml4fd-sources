-- The series behind the monitoring page's charts (S6/R11): every judged
-- measurement of the period, in order.
-- Covers: EC45, EC49   Lot: L4   Competency: C6
--
-- WHY THIS QUERY IS WHAT EC45 IS FOR. "Conserver l'historique des mesures,
-- afin de pouvoir constater une evolution dans le temps long" -- the keeping
-- is fd_monitoring, the constater is this. Until R11 nothing read the table
-- except the task that wrote it, so the history was stored and never seen.
--
-- THE LIMIT TRAVELS WITH THE MEASUREMENT, row by row, and is not looked up in
-- governance.yml at display time. A threshold that moved -- max_drifted_share
-- went from 0.40 to 0.70 on 2026-11-20 -- would otherwise redraw the past
-- against today's rule, and a series whose limit moves silently is a series
-- that cannot be read at all.
--
-- Parameters: window_start, window_end.
SELECT m.measured_at,
       m.control,
       m.metric,
       m.value,
       m.threshold,
       m.state,
       m.model_version,
       m.n_rows
  FROM fd_monitoring m
 WHERE strpos(m.metric, 'column_drift:') = 0
   AND m.measured_at >= %(window_start)s
   AND m.measured_at <  %(window_end)s
 ORDER BY m.control, m.metric, m.measured_at;
