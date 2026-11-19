-- The verdicts that fell, day by day, in the declared zone (S6/R10): the
-- series behind the performance page's chart. Section 3's counts, per day.
-- Covers: EE04, EI02   Lot: R10   Competency: C4
--
-- The day is label_ts's in the DECLARED zone, passed as a parameter: the
-- session's zone is UTC in the postgres image, and a verdict received at 00:30
-- in Paris belongs to that day, not to the previous one.
--
-- alerted is an EXISTS on fd_frauds, as in daily_verdicts.sql: the same
-- definition, so the days add up to the period.
--
-- Parameters: window_start, window_end, zone.
WITH fell AS (
    SELECT (g.label_ts AT TIME ZONE %(zone)s::text)::date AS day,
           g.label,
           EXISTS (SELECT 1 FROM fd_frauds a WHERE a.txn_id = g.txn_id) AS alerted
      FROM fd_ground_truth g
     WHERE g.label_ts >= %(window_start)s
       AND g.label_ts <  %(window_end)s
)
SELECT day,
       count(*)                                       AS verdicts,
       count(*) FILTER (WHERE label)                  AS frauds,
       count(*) FILTER (WHERE alerted)                AS alerts,
       count(*) FILTER (WHERE alerted AND label)      AS true_positives,
       round(count(*) FILTER (WHERE alerted AND label)::numeric
             / nullif(count(*) FILTER (WHERE label), 0), 4)   AS recall_observed,
       round(count(*) FILTER (WHERE alerted AND label)::numeric
             / nullif(count(*) FILTER (WHERE alerted), 0), 4) AS precision_observed
  FROM fell
 GROUP BY day
 ORDER BY day;
