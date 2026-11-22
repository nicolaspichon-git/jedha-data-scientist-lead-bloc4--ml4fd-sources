-- The verdicts that fell over a period, by the model version that scored the
-- payment, beside what M5 measured for that version's recipe (S6/R10).
-- Block: B3, B4.
-- Lot: B3-R10, B4-L0.
-- Competency: B3-C2, B3-C4.
-- Covers: B3-EE04, B3-EI02, B3-EI03.
--
-- ON THE KNOWLEDGE AXIS, like section 3 of the report (daily_verdicts.sql):
-- the period is the one in which the verdicts BECAME KNOWN, label_ts, and they
-- bear on payments made about a verdict delay earlier.
--
-- THE DECISION IS fd_scores.flagged, the one each version took, and not the
-- existence of an alert: a payment rescored by a second version is counted
-- once per version, which is the point -- each version is judged on what IT
-- decided. Section 3 counts each payment once, against fd_frauds; the two
-- totals agree when one version scored everything.
--
-- AGGREGATE, THEN JOIN. fd_ground_truth has one row per txn_id (its key),
-- fd_scores one per (txn_id, model_version): the grouping is per version
-- BEFORE fd_datasets is joined, one row per recipe_digest, so no count is
-- multiplied by the join.
--
-- Parameters: window_start, window_end.
WITH fell AS (
    SELECT g.txn_id, g.label
      FROM fd_ground_truth g
     WHERE g.label_ts >= %(window_start)s
       AND g.label_ts <  %(window_end)s
),
by_model AS (
    SELECT s.model_version,
           s.recipe_digest,
           count(*)                                          AS verdicts,
           count(*) FILTER (WHERE f.label)                   AS frauds,
           count(*) FILTER (WHERE s.flagged)                 AS flagged,
           count(*) FILTER (WHERE s.flagged AND f.label)     AS true_positives,
           count(*) FILTER (WHERE s.flagged AND NOT f.label) AS false_positives,
           count(*) FILTER (WHERE NOT s.flagged AND f.label) AS false_negatives
      FROM fell f
      JOIN fd_scores s ON s.txn_id = f.txn_id
     GROUP BY s.model_version, s.recipe_digest
)
SELECT b.model_version,
       d.version                                              AS dataset_version,
       b.verdicts, b.frauds, b.flagged,
       b.true_positives, b.false_positives, b.false_negatives,
       round(b.true_positives::numeric / nullif(b.frauds, 0), 4)  AS recall_observed,
       round(b.true_positives::numeric / nullif(b.flagged, 0), 4) AS precision_observed,
       round(b.frauds::numeric / nullif(b.verdicts, 0), 6)        AS base_rate_observed,
       d.test_recall, d.test_precision, d.base_rate_test
  FROM by_model b
  LEFT JOIN fd_datasets d ON d.recipe_digest = b.recipe_digest
 ORDER BY b.verdicts DESC, b.model_version;
