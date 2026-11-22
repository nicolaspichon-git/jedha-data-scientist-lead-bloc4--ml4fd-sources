-- Apply the history features to ONE batch, in real-time mode.
-- Block: B3, B4.
-- Lot: B3-N3, B4-L0.
-- Competency: B3-C2.
-- Covers: B3-EE07, B3-EI03.
--
-- WHY A BATCH AND NOT THE WHOLE TABLE. features_window.sql recomputes
-- everything, and its docstring says why: a history feature depends on every
-- earlier transaction of the cardholder, so a new batch MOVES the values of
-- rows already stored.
--
-- That is true of the CSV and false of the stream, under one condition: every
-- stream transaction is stamped with current_time, therefore later than
-- everything already in the table. A row that is later than all others changes
-- nobody else's history.
--
-- THE CONDITION IS CHECKED, not assumed: ml4fd/features.py refuses to run this
-- file when a stored transaction is later than the batch's earliest row. Left
-- unverified, a batch-scoped update would silently leave stale features behind.
--
-- The function is called through LATERAL rather than reimplemented: it is THE
-- definition of the features in real-time mode, and L4 proved it agrees with
-- the window mode on a stratified sample. A second expression here would be a
-- third mode to keep in step.
--
-- The subquery reads fd_transactions while the UPDATE writes it. It sees the
-- snapshot taken at statement start, which is what is wanted: the features
-- depend on amt and txn_ts, never on f_tx_count.

UPDATE fd_transactions t
SET    f_tx_count   = f.tx_count,
       f_avg_amount = f.avg_amount
FROM   (SELECT x.txn_id,
               c.tx_count,
               c.avg_amount
        FROM   fd_transactions x
        CROSS JOIN LATERAL fd_client_features(x.client_key, x.txn_ts) AS c
        WHERE  x.batch_id = %(batch_id)s) AS f
WHERE  t.txn_id = f.txn_id;
