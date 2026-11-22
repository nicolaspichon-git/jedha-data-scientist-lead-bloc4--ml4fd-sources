-- The messages sent to the operations channel over a calendar period, newest
-- first, each return to green with the length of the incident it closes.
-- Block: B3, B4.
-- Lot: B3-R9 (S6), B4-L0.
-- Competency: B3-C4.
-- Covers: B3-EC14, B3-EC17.
--
-- THE LENGTH IS READ ACROSS THE PERIOD'S EDGE. An incident that began before
-- the first day and ended within the period is still an incident of known
-- length: lag() runs over the whole journal, and the filter comes after it.
-- The journal holds a few rows per incident, so the whole of it is small.
--
-- Parameters: window_start, window_end.
WITH journal AS (
    SELECT notification_id, sent_at, control, state, detail, channel, run_id,
           lag(state)   OVER per_control AS previous_state,
           lag(sent_at) OVER per_control AS previous_at
      FROM fd_ops_notifications
    WINDOW per_control AS (PARTITION BY control
                           ORDER BY sent_at, notification_id)
)
SELECT sent_at, control, state, detail, channel, run_id,
       CASE WHEN state = 'ok' AND previous_state = 'alert'
            THEN extract(epoch FROM sent_at - previous_at)
       END AS incident_seconds
  FROM journal
 WHERE sent_at >= %(window_start)s
   AND sent_at <  %(window_end)s
 ORDER BY sent_at DESC, notification_id DESC;
