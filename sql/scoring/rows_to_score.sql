-- The transactions of a batch, with every column a recipe may ask for.
-- Block: B3, B4.
-- Lot: B3-N4, B4-L0.
-- Competency: B3-C2.
-- Covers: B3-EE07, B3-EI04.
--
-- A SUPERSET, deliberately. This query offers the columns; the recipe of the
-- loaded model version chooses which of them to use and in which ORDER, and
-- ml4fd/score.py reads that order from the manifest. So a recipe retrained
-- without txn_hour scores correctly without a line of Python or SQL changing,
-- and a recipe asking for a column this query does not offer is refused by name
-- instead of scoring against the wrong features.
--
-- THE TWO DERIVED COLUMNS MUST AGREE WITH sql/datasets/training_set.sql, WORD
-- FOR WORD. They are written there as
--     extract(hour   FROM t.txn_ts)::int  AS txn_hour
--     extract(isodow FROM t.txn_ts)::int  AS txn_weekday
-- and a model trained on one expression and scored on another is a model
-- nobody has measured. tests/test_score.py compares the two files on exactly
-- these two lines, so an edit to either one fails the suite.
--
-- Note what that agreement rests on: extract() on a timestamptz reads the
-- SESSION's TimeZone. Both sides run in the same container with the same
-- setting, so they agree -- but it is an assumption, not a property of the SQL,
-- and the measurement of N4 records the setting it ran under.

SELECT t.txn_id,
       t.amt,
       t.f_tx_count,
       t.f_avg_amount,
       t.f_home_distance_km,
       extract(hour   FROM t.txn_ts)::int  AS txn_hour,
       extract(isodow FROM t.txn_ts)::int  AS txn_weekday,
       t.category
FROM   fd_transactions t
WHERE  t.batch_id = %(batch_id)s
  -- A transaction with no features cannot be scored: the model would read NULL
  -- as "no history" where the truth is "not computed". features.apply_batch is
  -- what fills them, and it runs before this query.
  AND  t.f_tx_count IS NOT NULL
ORDER  BY t.txn_ts, t.txn_id;
