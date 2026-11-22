-- Section 1 of the morning report: yesterday's payments, on the PAYMENT axis.
-- Block: B3, B4.
-- Lot: B3-P1, B4-L0.
-- Competency: B3-C2.
-- Covers: B3-EE04, B3-EI08.
--
-- ONE STATEMENT, THREE READINGS, by GROUPING SETS: the total, the breakdown by
-- category and the breakdown by local hour. Three separate queries would let
-- the total and the breakdowns disagree -- a category sum that does not add up
-- to the headline figure is the kind of defect a reader finds before a test
-- does. Here they come from one scan, so they cannot.
--
-- THE WINDOW IS A PARAMETER AND NEVER now(). A report replayed next month must
-- produce the document of the day it covers, which is why dag_daily passes
-- data_interval_start and data_interval_end. A query carrying now() would make
-- two runs of the same day differ, and comparing two mornings is precisely what
-- an analyst does.
--
-- THE HOUR IS LOCAL, and the zone is declared in governance.yml
-- (report.timezone) rather than written here: a report whose hours are UTC
-- would show the night fraud peak three hours off, and the peak is 84 per cent
-- of the frauds between 22h and 03h (measurement M5).
--
-- RUNS UNDER fd_report, which has no access to the raw zone. Nothing here needs
-- it: the exploited layer carries the amounts, the categories and the
-- pseudonymised cardholder key.

WITH day AS (
    SELECT t.client_key,
           t.category,
           t.amt,
           extract(hour FROM t.txn_ts AT TIME ZONE %(zone)s)::int AS local_hour
      FROM fd_transactions t
     WHERE t.txn_ts >= %(window_start)s
       AND t.txn_ts <  %(window_end)s
)
SELECT CASE WHEN grouping(category)   = 0 THEN 'category'
            WHEN grouping(local_hour) = 0 THEN 'hour'
            ELSE 'total' END                                  AS axis,
       coalesce(category,
                lpad(local_hour::text, 2, '0'),
                'total')                                      AS bucket,
       count(*)                                               AS transactions,
       coalesce(sum(amt), 0)                                  AS amount,
       count(DISTINCT client_key)                             AS cardholders,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY amt)        AS median_amount
  FROM day
 GROUP BY GROUPING SETS ((), (category), (local_hour))
 -- Deterministic to the last row: the acceptance criterion of P2 is that two
 -- runs on the same window produce the same document to the character, and an
 -- unordered result set would break it on ties alone.
 ORDER BY axis, amount DESC, bucket;
