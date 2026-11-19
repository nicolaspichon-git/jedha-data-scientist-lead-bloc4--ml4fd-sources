-- The training set, at a cut-off, with its temporal split.
-- Covers: EI02, EI04, EE07   Lot: M3   Competency: C2, C5
--
-- THE FILTER THAT MATTERS: g.label_ts <= %(cut_off)s, and NOT t.txn_ts.
--
-- A set filtered on the TRANSACTION date would hold verdicts that were not
-- known at the cut-off. The model would then benefit from later information,
-- the measured recall would be optimistic, and the whole deferred-verdict
-- mechanism would be neutralised. The blind band of one verdict delay before
-- the cut-off exists for this reason: it is not a loss, it is the honesty of
-- the arrangement.
--
-- The features are read AS STORED. Nothing is recomputed from history here:
-- S1 persisted them on purpose (F04.1) and L4 proved both computation modes
-- agree, so training reads the very values scoring will read.
--
-- ORDER BY txn_ts, txn_id makes the output deterministic, so two builds of one
-- recipe produce the same rows in the same order.

SELECT t.txn_id,
       t.txn_ts,
       t.amt,
       t.f_tx_count,
       t.f_avg_amount,
       t.f_home_distance_km,
       extract(hour   FROM t.txn_ts)::int  AS txn_hour,
       extract(isodow FROM t.txn_ts)::int  AS txn_weekday,
       t.category,
       g.label,
       CASE WHEN t.txn_ts < %(train_end)s THEN 'train'
            ELSE 'test'
       END AS split
FROM   fd_transactions t
JOIN   fd_ground_truth g ON g.txn_id = t.txn_id
WHERE  g.label_ts <= %(cut_off)s
  AND  t.txn_ts   <  %(test_end)s
ORDER  BY t.txn_ts, t.txn_id;
