-- Record one API call and the answer it received, AFTER answering.
-- Block: B4.
-- Lot: B4-L5.
-- Competency: B4-C3, B4-C4.
-- Covers: B4-EC19.
--
-- Append-only: 190_scoring_service_calls.sql revokes UPDATE and DELETE from fd_scoring_service, so an
-- answer cannot be rewritten into one the service never gave.
--
-- NEITHER THE CARD NUMBER NOR THE HOME LOCATION, and the column list is the
-- enforcement rather than the promise: the table has no column for either.
-- client_key is the pseudonym, f_home_distance_km is a distance.
INSERT INTO fd_scoring_service_calls (txn_ts, client_key, merchant, category, amt,
                          f_tx_count, f_avg_amount, f_home_distance_km,
                          model_version, recipe_digest, score, threshold,
                          flagged)
VALUES (%(txn_ts)s, %(client_key)s, %(merchant)s, %(category)s, %(amt)s,
        %(f_tx_count)s, %(f_avg_amount)s, %(f_home_distance_km)s,
        %(model_version)s, %(recipe_digest)s, %(score)s, %(threshold)s,
        %(flagged)s)
RETURNING call_id, called_at;
