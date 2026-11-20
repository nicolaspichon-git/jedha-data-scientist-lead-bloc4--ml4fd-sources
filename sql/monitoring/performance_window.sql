-- The performance of the model in service, on the knowledge axis (EC40, EC41).
-- Covers: EC40, EC41   Lot: L4   Competency: C6
--
-- ON label_ts AND NEVER ON txn_ts, which is the decision of
-- sql/reports/daily_verdicts.sql, taken here for the same reason: the verdicts
-- that became known in this window bear on payments made one verdict delay
-- earlier -- thirty days, in nominal mode. Filtering on txn_ts would ask for
-- the performance on recent payments, whose verdicts are precisely what is not
-- known yet, and the answer would read as a collapse in recall.
--
-- TWO CONFUSION MATRICES, AND THEY ARE NOT THE SAME MEASUREMENT.
--
--   flagged  -- fd_scores.flagged: what the MODEL decided, at its threshold.
--   alerted  -- a row in fd_frauds: what the SERVICE did about that decision.
--
-- They diverge by design, because the alert capacity of 60 a day can suppress
-- an alert the model raised. So a recall measured on fd_frauds falls when the
-- queue saturates, with no change in the model at all -- and a retraining
-- triggered by that figure would answer the wrong question, since no model can
-- refit its way out of a ceiling.
--
-- The trigger of C6 therefore reads recall_flagged. recall_alerted is measured
-- beside it as the diagnostic: the two apart means the capacity is binding,
-- the two together means the model is.
--
-- ONE SCORE PER TRANSACTION, the one of the version named. fd_scores is keyed
-- on (txn_id, model_version), so a transaction rescored by a later model holds
-- several rows; without the filter the counts would double and the recall
-- would mix two models.
--
-- nullif GUARDS EVERY RATIO. A window with no fraud among the fallen verdicts
-- gives a null recall and not a zero: "no fraud to catch" and "caught none of
-- the frauds" are opposite findings, and the caller turns the first into the
-- state 'unmeasurable' rather than into an alert.

WITH fell AS (
    SELECT g.txn_id,
           g.label,
           s.flagged,
           EXISTS (SELECT 1 FROM fd_frauds a
                    WHERE a.txn_id = g.txn_id)           AS alerted
      FROM fd_ground_truth g
      JOIN fd_scores s
        ON s.txn_id = g.txn_id
       AND s.model_version = %(model_version)s
     WHERE g.label_ts >= %(window_start)s
       AND g.label_ts <  %(window_end)s
)
SELECT count(*)                                       AS verdicts,
       count(*) FILTER (WHERE label)                  AS frauds,
       count(*) FILTER (WHERE flagged)                AS flags,
       count(*) FILTER (WHERE flagged AND label)      AS true_positives,
       count(*) FILTER (WHERE flagged AND NOT label)  AS false_positives,
       count(*) FILTER (WHERE NOT flagged AND label)  AS false_negatives,
       count(*) FILTER (WHERE alerted AND label)      AS alerted_frauds,
       count(*) FILTER (WHERE alerted)                AS alerts,
       count(*) FILTER (WHERE flagged AND label)::numeric
           / nullif(count(*) FILTER (WHERE label), 0) AS recall_flagged,
       count(*) FILTER (WHERE alerted AND label)::numeric
           / nullif(count(*) FILTER (WHERE label), 0) AS recall_alerted,
       count(*) FILTER (WHERE flagged AND label)::numeric
           / nullif(count(*) FILTER (WHERE flagged), 0) AS precision_flagged,
       count(*) FILTER (WHERE label)::numeric
           / nullif(count(*), 0)                      AS base_rate
  FROM fell;
