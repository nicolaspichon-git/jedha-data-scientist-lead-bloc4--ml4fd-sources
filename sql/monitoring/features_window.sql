-- The transactions of a window, with the features the model reads (EC39).
-- Block: B4.
-- Lot: B4-L4.
-- Competency: B4-C6.
-- Covers: B4-EC39.
--
-- THE SAME SELECT LIST AS sql/scoring/rows_to_score.sql, deliberately, down to
-- the derivation of txn_hour and txn_weekday. The drift being measured is the
-- drift of what the MODEL sees; a column computed differently here would
-- report a drift of this query rather than of the stream.
--
-- ON txn_ts AND NOT ON scored_at, because this is the payment axis: the
-- question is whether the transactions arriving now are shaped like those the
-- model was fitted on. The knowledge axis belongs to the performance query,
-- which filters on label_ts -- the two must not be confused, and
-- sql/reports/daily_verdicts.sql holds the same warning.
--
-- f_tx_count IS NOT NULL for the reason the scoring query gives: a transaction
-- with no features cannot be read by the model, so including it would measure
-- a gap in the pipeline as a drift in the data. The gap is already a control
-- of its own -- the reconciliation of S3.

SELECT t.txn_id,
       t.amt,
       t.f_tx_count,
       t.f_avg_amount,
       t.f_home_distance_km,
       extract(hour   FROM t.txn_ts)::int  AS txn_hour,
       extract(isodow FROM t.txn_ts)::int  AS txn_weekday,
       t.category
FROM   fd_transactions t
WHERE  t.txn_ts >= %(window_start)s
  AND  t.txn_ts <  %(window_end)s
  AND  t.f_tx_count IS NOT NULL
ORDER  BY t.txn_ts, t.txn_id;
