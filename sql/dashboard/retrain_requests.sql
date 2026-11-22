-- The retraining requests the monitoring made, over the period (S6/R11).
-- Block: B4.
-- Lot: B4-L4.
-- Competency: B4-C5, B4-C6.
-- Covers: B4-EC33, B4-EC49.
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
