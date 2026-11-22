-- The flag rate of the model over a window, on the payment axis (EC39).
-- Block: B4.
-- Lot: B4-L4.
-- Competency: B4-C6.
-- Covers: B4-EC39.
--
-- THE PREDICTION DRIFT THAT HAS A CONSEQUENCE. A score distribution can move
-- without changing anything an operator feels; the share of transactions the
-- model flags is what fills the analyst's queue, under the capacity of 60 a
-- day. So this is the figure the control watches, and the score quantiles are
-- recorded beside it as the explanation rather than as the verdict.
--
-- ONE MODEL VERSION, named. fd_scores is keyed on (txn_id, model_version), so
-- without the filter a rescored window would mix two models and the rate would
-- be the average of two distributions that were never served together.

SELECT count(*)                                  AS scored,
       count(*) FILTER (WHERE flagged)           AS flagged,
       count(*) FILTER (WHERE flagged)::numeric
           / nullif(count(*), 0)                 AS flag_share,
       percentile_cont(0.50) WITHIN GROUP (ORDER BY score) AS score_median,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY score) AS score_p99
FROM   fd_scores s
JOIN   fd_transactions t ON t.txn_id = s.txn_id
WHERE  s.model_version = %(model_version)s
  AND  t.txn_ts >= %(window_start)s
  AND  t.txn_ts <  %(window_end)s;
