-- The retraining requests the monitoring made, over the period (S6/R11).
-- Covers: EC33, EC49   Lot: L4   Competency: C5, C6
--
-- WHAT THIS SHOWS THAT NOTHING ELSE DOES: the join between an alert and a
-- model. fd_monitoring says a control crossed its limit; fd_promotions says a
-- candidate was judged; this table is the link that makes the second a
-- consequence of the first, and it is append-only for that reason.
--
-- A REQUEST IS NOT A MODEL. The row says the retraining was ASKED FOR -- the
-- record follows the trigger, so a run that failed afterwards leaves a request
-- with nothing behind it. That is the honest reading and the page says so.
--
-- Parameters: window_start, window_end.
SELECT r.request_id,
       r.requested_at,
       r.run_id,
       r.controls,
       r.reason
  FROM fd_retrain_requests r
 WHERE r.requested_at >= %(window_start)s
   AND r.requested_at <  %(window_end)s
 ORDER BY r.requested_at DESC;
