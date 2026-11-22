-- Every decision served over a calendar period, grouped by the path it came
-- from, read through fd_scoring_decisions.
-- Block: B4.
-- Lot: B4-L5.
-- Competency: B4-C3.
-- Covers: B4-EC16, B4-EC19.
--
-- READ THROUGH THE VIEW AND NOT THE TWO TABLES, which is the whole point of
-- 195_scoring_decisions.sql: a decision is recorded once, by whoever holds
-- what makes it re-derivable, and the day the service becomes the only
-- journal the view changes and this query does not. fd_dashboard has no
-- privilege on either table -- the view runs with its owner's -- so this
-- query CANNOT be rewritten against them by accident.
--
-- THE INSTANT IS decided_at, which is scored_at on the pipeline side and
-- called_at on the third-party side. They are the same kind of instant: when
-- the decision was taken. The payment's own instant is not in the view, and
-- that is deliberate -- a page about decisions is not a page about payments.
--
-- Parameters: window_start, window_end.
SELECT source,
       count(*)                                  AS decisions,
       count(*) FILTER (WHERE flagged)           AS flagged,
       min(decided_at)                           AS first_at,
       max(decided_at)                           AS last_at,
       count(DISTINCT model_version)             AS versions,
       string_agg(DISTINCT model_version, ', ' ORDER BY model_version)
                                                 AS version_list
  FROM fd_scoring_decisions
 WHERE decided_at >= %(window_start)s
   AND decided_at <  %(window_end)s
 GROUP BY source
 ORDER BY source;
