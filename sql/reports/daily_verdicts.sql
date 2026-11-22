-- Section 3 of the morning report: verdicts that fell yesterday.
-- ON THE KNOWLEDGE AXIS, which is the whole point of this file.
-- Block: B3, B4.
-- Lot: B3-P1, B4-L0.
-- Competency: B3-C2, B3-C4.
-- Covers: B3-EE04, B3-EI02, B3-EI03.
--
-- THE SECTION A NAIVE DESIGN OMITS, and the only place in the whole system
-- where the model's real performance becomes measurable. It filters on
-- label_ts and not on txn_ts: the verdicts that became known yesterday bear on
-- payments made one verdict delay earlier -- thirty days, in nominal mode.
--
-- Reading this section as "yesterday's frauds" would be wrong twice over, and
-- the report says which payments it covers (payments_from, payments_to) so the
-- reader cannot make that mistake.
--
-- THE CONFUSION MATRIX IS RETURNED AS FOUR COUNTS and the two ratios beside
-- them. The counts are what the acceptance check recomputes; the ratios are
-- what the reader compares with the figures M5 measured on the test set. The
-- base rate travels with them, because a recall compared across two different
-- base rates compares nothing.
--
-- nullif GUARDS BOTH RATIOS. A day with no fraud among the fallen verdicts
-- gives a null recall, not a zero: "no fraud to catch" and "caught none of the
-- frauds" are opposite findings and must not render alike.
--
-- alerted IS AN EXISTS ON fd_frauds and not a join, so a transaction cannot be
-- counted twice -- fd_frauds has one row per txn_id by its primary key, but an
-- EXISTS states the intent and survives a future change to that key.

WITH fell AS (
    SELECT g.txn_id,
           g.label,
           g.label_ts,
           t.txn_ts,
           EXISTS (SELECT 1 FROM fd_frauds a WHERE a.txn_id = g.txn_id) AS alerted
      FROM fd_ground_truth g
      JOIN fd_transactions t ON t.txn_id = g.txn_id
     WHERE g.label_ts >= %(window_start)s
       AND g.label_ts <  %(window_end)s
)
SELECT count(*)                                              AS verdicts,
       count(*) FILTER (WHERE label)                         AS frauds,
       count(*) FILTER (WHERE alerted)                       AS alerts,
       count(*) FILTER (WHERE alerted AND label)             AS true_positives,
       count(*) FILTER (WHERE alerted AND NOT label)         AS false_positives,
       count(*) FILTER (WHERE NOT alerted AND label)         AS false_negatives,
       count(*) FILTER (WHERE NOT alerted AND NOT label)     AS true_negatives,
       round(count(*) FILTER (WHERE alerted AND label)::numeric
             / nullif(count(*) FILTER (WHERE label), 0), 4)  AS recall_observed,
       round(count(*) FILTER (WHERE alerted AND label)::numeric
             / nullif(count(*) FILTER (WHERE alerted), 0), 4) AS precision_observed,
       round(count(*) FILTER (WHERE label)::numeric
             / nullif(count(*), 0), 6)                       AS base_rate_observed,
       min(txn_ts)::date                                     AS payments_from,
       max(txn_ts)::date                                     AS payments_to,
       count(DISTINCT date_trunc('day', txn_ts))             AS payment_days
  FROM fell;
