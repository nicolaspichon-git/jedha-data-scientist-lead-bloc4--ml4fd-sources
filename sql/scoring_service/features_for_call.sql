-- The feature row of one API call, computed where the definition lives.
-- Covers: EC16, EE07   Lot: L5   Competency: C2, C3
--
-- IN SQL AND NOT IN PYTHON, which is the one decision of this file. The batch
-- path reads sql/scoring/rows_to_score.sql, whose txn_hour and txn_weekday are
-- extract(hour ...) and extract(isodow ...) over txn_ts. Computing them in
-- Python here would be a SECOND definition of the same two features, and the
-- project already pays sql/checks/reconcile_features.sql to prove the batch
-- and real-time modes of the history features agree. A cross-declaration test
-- ties this select list to that one, so the two cannot drift apart silently.
--
-- extract OVER A timestamptz READS THE SESSION'S TIME ZONE, which is an
-- exposure this file SHARES with rows_to_score.sql rather than one it adds --
-- and sharing it is the point: both paths answer the same way, whatever the
-- session is set to. N4 measured that these two features carry the hour of OUR
-- call on the stream rather than the hour of the payment, which is why
-- governance.yml drops them; the service builds whatever the champion's
-- manifest declares, so they are computed here and selected by the caller.
--
-- THE HISTORY FEATURES COME FROM THE FUNCTION and are not reimplemented:
-- fd_client_features is "THE definition", real-time mode, with its six
-- rulings -- strict bound, the whole table, numeric(12,2), NULL on an empty
-- set. A first transaction yields tx_count = 0 and avg_amount = NULL, and the
-- model was fitted on rows shaped exactly that way.
--
-- Parameters: client_key, txn_ts, amt, category, f_home_distance_km.
SELECT %(amt)s::numeric(12,2)                           AS amt,
       f.tx_count                                       AS f_tx_count,
       f.avg_amount                                     AS f_avg_amount,
       %(f_home_distance_km)s::numeric(8,2)             AS f_home_distance_km,
       extract(hour   FROM %(txn_ts)s::timestamptz)::int AS txn_hour,
       extract(isodow FROM %(txn_ts)s::timestamptz)::int AS txn_weekday,
       %(category)s::text                               AS category
FROM   fd_client_features(%(client_key)s, %(txn_ts)s::timestamptz) AS f;
